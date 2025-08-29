import os
import logging
import argparse
import sys
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from minio import Minio
from minio.error import S3Error
import json
from io import BytesIO
from collections import defaultdict
import signal
import time

'''

사용 명령어
docker-compose run --rm backfill-loader python app.py --mode incremental
***incremental 주의사항***
4일이 지나면 오래된 데이터 부터 누락발생. offset이 뒤로 밀려서 그런듯.
3일에 한번씩 실행 권장

docker-compose run --rm backfill-loader python app.py --mode scheduler
docker-compose run --rm backfill-loader python app.py --mode continue --from-date 2023-10-01
docker-compose run --rm backfill-loader python app.py --mode fresh

- incremental 모드에서는 마지막 커밋된 오프셋부터 시작
- scheduler 모드에서는 3일에 한번씩 자정에 incremental mode 실행
- continue 모드에서는 from_date가 지정된 경우 해당 날짜부터 시작, 지정되지 않으면 마지막 커밋된 오프셋부터 시작
- fresh 모드에서는 항상 처음부터 시작


parquet columns:
objId,rsctypeId,TEMPERATURE1,TEMPERATURE,HUMIDITY1,HUMIDITY,timestamp
timestamp는 KTC로 "2025-06-26T12:22:04.499000" 형태


docker-compose build backfill-loader

'''
# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BackfillLoader:
    def __init__(self, mode='fresh', from_date=None, to_date=None, target_date=None):
        # 실행 모드 설정
        self.mode = mode
        self.from_date = from_date
        self.to_date = to_date
        self.target_date = target_date
        
        self.date_filter = self._setup_date_filter()

        # 환경 변수에서 설정값 읽기
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'fms-temphum')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'backfill-loader')
        
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'fms-temphum')
        self.minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        
        logger.info(f"=== BackfillLoader initialized ===")
        logger.info(f"Mode: {self.mode}")
        if self.date_filter:
            logger.info(f"Date filter: {self.date_filter}")
        logger.info(f"From date: {self.from_date}")
        logger.info(f"Kafka Topic: {self.kafka_topic}")
        logger.info(f"Group ID: {self.kafka_group_id}")
        
        # MinIO 클라이언트 초기화
        self.minio_client = self._init_minio_client()
        
        # Kafka 컨슈머 초기화
        self.consumer = self._init_kafka_consumer()
        
        logger.info("BackfillLoader initialized successfully")

    def _setup_date_filter(self):
        """날짜 필터 설정 및 검증"""
        date_filter = {}
        
        try:
            # target_date가 지정된 경우 (단일 날짜)
            if self.target_date:
                target_dt = datetime.strptime(self.target_date, '%Y-%m-%d')
                date_filter['from_datetime'] = target_dt.replace(hour=0, minute=0, second=0)
                date_filter['to_datetime'] = target_dt.replace(hour=23, minute=59, second=59)
                date_filter['description'] = f"Single date: {self.target_date}"
                return date_filter
            
            # from_date 처리
            if self.from_date:
                from_dt = datetime.strptime(self.from_date, '%Y-%m-%d')
                date_filter['from_datetime'] = from_dt.replace(hour=0, minute=0, second=0)
            
            # to_date 처리
            if self.to_date:
                to_dt = datetime.strptime(self.to_date, '%Y-%m-%d')
                date_filter['to_datetime'] = to_dt.replace(hour=23, minute=59, second=59)
            
            # 날짜 범위 검증
            if 'from_datetime' in date_filter and 'to_datetime' in date_filter:
                if date_filter['from_datetime'] > date_filter['to_datetime']:
                    raise ValueError("from_date cannot be later than to_date")
            
            # 설명 생성
            if 'from_datetime' in date_filter or 'to_datetime' in date_filter:
                desc_parts = []
                if 'from_datetime' in date_filter:
                    desc_parts.append(f"from {date_filter['from_datetime'].strftime('%Y-%m-%d')}")
                if 'to_datetime' in date_filter:
                    desc_parts.append(f"to {date_filter['to_datetime'].strftime('%Y-%m-%d')}")
                date_filter['description'] = "Date range: " + " ".join(desc_parts)
            
            return date_filter if date_filter else None
            
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            raise

    def is_message_in_date_range(self, message_data):
        """메시지가 지정된 날짜 범위에 포함되는지 확인"""
        if not self.date_filter:
            return True  # 날짜 필터가 없으면 모든 메시지 허용
        
        try:
            # 메시지에서 타임스탬프 추출
            timestamp_value = None
            if '@timestamp' in message_data:
                timestamp_value = message_data['@timestamp']
            elif 'timestamp' in message_data:
                timestamp_value = message_data['timestamp']
            
            if not timestamp_value:
                return True  # 타임스탬프가 없으면 허용 (기본값)
            
            # UTC → KST 변환
            msg_kst_datetime = self.convert_utc_to_kst(timestamp_value)
            if not msg_kst_datetime:
                return True  # 변환 실패시 허용
            
            # 날짜 범위 체크
            if 'from_datetime' in self.date_filter:
                if msg_kst_datetime < self.date_filter['from_datetime']:
                    return False
            
            if 'to_datetime' in self.date_filter:
                if msg_kst_datetime > self.date_filter['to_datetime']:
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking date range for message: {e}")
            return True  # 에러 발생시 허용
        
    def _init_minio_client(self):
        """MinIO 클라이언트 초기화"""
        try:
            client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            
            # 버킷 존재 확인 및 생성
            if not client.bucket_exists(self.minio_bucket):
                client.make_bucket(self.minio_bucket)
                logger.info(f"Created bucket: {self.minio_bucket}")
            
            return client
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise

    def _log_partition_status(self, consumer, partitions):
        """각 파티션의 오프셋 상태 로깅"""
        logger.info("=== Partition Status ===")
        
        # 각 파티션의 시작/끝/현재 오프셋 확인
        beginning_offsets = consumer.beginning_offsets(partitions)
        end_offsets = consumer.end_offsets(partitions)
        
        total_available = 0
        total_remaining = 0
        
        for partition in sorted(partitions, key=lambda p: p.partition):
            current_offset = consumer.position(partition)
            beginning_offset = beginning_offsets.get(partition, 0)
            end_offset = end_offsets.get(partition, 0)
            
            available_messages = end_offset - beginning_offset
            remaining_messages = end_offset - current_offset
            
            total_available += available_messages
            total_remaining += remaining_messages
            
            logger.info(f"Partition {partition.partition:2d}: "
                    f"begin={beginning_offset:8d}, "
                    f"current={current_offset:8d}, "
                    f"end={end_offset:8d}, "
                    f"available={available_messages:8d}, "
                    f"remaining={remaining_messages:8d}")
        
        logger.info(f"TOTAL: available={total_available:8d}, remaining={total_remaining:8d}")
        logger.info("========================")
        
    def _init_kafka_consumer(self):
        """Kafka 컨슈머 초기화 - 모든 파티션 명시적 할당"""
        try:
            logger.info(f"Initializing Kafka consumer for mode: {self.mode}")
            
            # 모드별 설정
            if self.mode == 'fresh':
                auto_offset_reset = 'earliest'
                enable_auto_commit = False
                logger.info("Fresh mode: Reading from beginning, not saving offsets")
                
            elif self.mode in ['incremental', 'continue']:
                auto_offset_reset = 'earliest'
                enable_auto_commit = True
                logger.info(f"{self.mode} mode: Reading from last committed offset")
            else:
                raise ValueError(f"Invalid mode: {self.mode}")
            
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers.split(','),
                group_id=self.kafka_group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=30000,
                max_poll_records=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
            )
            
            # ===== 핵심 수정 부분: 모든 파티션 명시적 할당 =====
            
            # 1. 토픽의 모든 파티션 정보 가져오기
            # cluster_metadata = consumer.list_consumer_groups()
            topic_partitions = consumer.partitions_for_topic(self.kafka_topic)
            
            if topic_partitions is None:
                raise Exception(f"Topic '{self.kafka_topic}' not found")
            
            logger.info(f"Found {len(topic_partitions)} partitions for topic '{self.kafka_topic}': {sorted(topic_partitions)}")
            
            # 2. 모든 파티션을 TopicPartition 객체로 생성
            all_partitions = [TopicPartition(self.kafka_topic, partition) for partition in topic_partitions]
            
            # 3. 모든 파티션을 명시적으로 할당
            consumer.assign(all_partitions)
            logger.info(f"Explicitly assigned all {len(all_partitions)} partitions")
            
            # 4. 모드별 오프셋 설정
            if self.mode == 'fresh':
                # Fresh 모드: 모든 파티션을 처음부터
                consumer.seek_to_beginning()
                logger.info("Reset all partitions to beginning for fresh mode")
                
            elif self.mode in ['incremental', 'continue']:
                # Incremental/Continue 모드: 각 파티션별로 커밋된 오프셋 확인
                for partition in all_partitions:
                    try:
                        # 각 파티션별로 커밋된 오프셋 확인
                        committed_offset = consumer.committed(partition)  # ✅ 올바른 메서드
                        
                        if committed_offset is not None and committed_offset >= 0:
                            # 커밋된 오프셋이 있으면 해당 위치로 이동
                            consumer.seek(partition, committed_offset)
                            logger.info(f"Partition {partition.partition}: seeking to committed offset {committed_offset}")
                        else:
                            # 커밋된 오프셋이 없으면 처음부터 (continue 모드) 또는 최신부터 (incremental 모드)
                            if self.mode == 'continue':
                                consumer.seek_to_beginning(partition)
                                logger.info(f"Partition {partition.partition}: no committed offset, seeking to beginning")
                            else:  # incremental
                                consumer.seek_to_end(partition)
                                logger.info(f"Partition {partition.partition}: no committed offset, seeking to end")
                    except Exception as e:
                        logger.warning(f"Error getting committed offset for partition {partition.partition}: {e}")
                        # 에러 발생시 기본 동작
                        if self.mode == 'continue':
                            consumer.seek_to_beginning(partition)
                            logger.info(f"Partition {partition.partition}: error occurred, seeking to beginning")
                        else:  # incremental
                            consumer.seek_to_end(partition)
                            logger.info(f"Partition {partition.partition}: error occurred, seeking to end")
            
            # 5. 파티션별 현재 위치 확인 및 로깅
            self._log_partition_status(consumer, all_partitions)
            
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def convert_utc_to_kst(self, utc_timestamp):
        """UTC 타임스탬프를 KST로 변환 (9시간 추가)"""
        try:
            if isinstance(utc_timestamp, str):
                # 다양한 ISO 형식 지원
                if 'Z' in utc_timestamp:
                    utc_dt = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))
                elif '+' in utc_timestamp or utc_timestamp.endswith('00:00'):
                    utc_dt = datetime.fromisoformat(utc_timestamp)
                else:
                    # 타임존 정보가 없으면 UTC로 가정
                    utc_dt = datetime.fromisoformat(utc_timestamp)
            elif isinstance(utc_timestamp, (int, float)):
                utc_dt = datetime.fromtimestamp(utc_timestamp)
            else:
                utc_dt = utc_timestamp
            
            # UTC에서 KST로 변환 (9시간 추가)
            if utc_dt.tzinfo is None:
                kst_dt = utc_dt + timedelta(hours=9)
            else:
                # 타임존이 있으면 UTC로 변환 후 9시간 추가
                kst_dt = utc_dt.astimezone().replace(tzinfo=None) + timedelta(hours=9)
            
            return kst_dt
        except Exception as e:
            logger.error(f"Error converting timestamp {utc_timestamp}: {e}")
            return None
    
    def process_data_timestamps(self, all_data):
        """데이터의 @timestamp를 KST timestamp로 변환하고 날짜 범위 계산"""
        processed_data = []
        timestamps = []
        
        logger.info(f"Processing timestamps for {len(all_data)} records...")
        
        for i, data in enumerate(all_data):
            try:
                # 원본 데이터 복사
                processed_record = data.copy()
                
                # @timestamp 찾기 및 변환
                timestamp_value = None
                if '@timestamp' in data:
                    timestamp_value = data['@timestamp']
                elif 'timestamp' in data:
                    timestamp_value = data['timestamp']
                
                if timestamp_value:
                    # UTC → KST 변환
                    kst_timestamp = self.convert_utc_to_kst(timestamp_value)
                    
                    if kst_timestamp:
                        # timestamp 필드로 저장 (KST)
                        processed_record['timestamp'] = kst_timestamp.isoformat()
                        timestamps.append(kst_timestamp)
                        
                        # @timestamp 제거 (중복 방지)
                        if '@timestamp' in processed_record:
                            del processed_record['@timestamp']
                    else:
                        logger.warning(f"Failed to convert timestamp for record {i}")
                        continue
                else:
                    logger.warning(f"No timestamp found in record {i}")
                    continue
                
                processed_data.append(processed_record)
                
                # 진행 상황 로그 (10000개마다)
                if (i + 1) % 10000 == 0:
                    logger.info(f"Processed {i + 1}/{len(all_data)} records...")
                    
            except Exception as e:
                logger.warning(f"Error processing record {i}: {e}")
                continue
        
        if not timestamps:
            logger.error("No valid timestamps found in data")
            return processed_data, None, None
        
        # 날짜 범위 계산
        min_timestamp = min(timestamps)
        max_timestamp = max(timestamps)
        
        logger.info(f"Successfully processed {len(processed_data)} records")
        logger.info(f"Date range: {min_timestamp.strftime('%Y-%m-%d %H:%M:%S')} ~ {max_timestamp.strftime('%Y-%m-%d %H:%M:%S')} (KST)")
        
        return processed_data, min_timestamp, max_timestamp
    
    def generate_filename(self, min_timestamp, max_timestamp):
        """첫 번째와 마지막 날짜를 기반으로 파일명 생성"""
        if not min_timestamp or not max_timestamp:
            # 기본 파일명 (현재 날짜)
            today = datetime.now().strftime('%Y%m%d')
            return f"{today}_{today}.parquet"
        
        start_date = min_timestamp.strftime('%Y%m%d')
        end_date = max_timestamp.strftime('%Y%m%d')
        
        return f"{start_date}_{end_date}.parquet"
    
    def save_parquet_to_minio(self, df, object_name):
        """DataFrame을 Parquet으로 MinIO에 저장"""
        try:
            if df.empty:
                logger.warning(f"Empty DataFrame for {object_name}, skipping save")
                return False
                
            # timestamp로 정렬
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp').reset_index(drop=True)
            
            table = pa.Table.from_pandas(df)
            parquet_buffer = BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)
            
            self.minio_client.put_object(
                self.minio_bucket,
                object_name,
                parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            logger.info(f"Successfully saved {object_name} to MinIO ({len(df)} records, {len(parquet_buffer.getvalue())} bytes)")
            return True
        except Exception as e:
            logger.error(f"Error saving {object_name}: {e}")
            return False
    
    def collect_all_data(self):
        """Kafka에서 데이터 수집 - 모든 파티션에서 수집"""
        logger.info(f"Starting data collection in {self.mode} mode")
        if self.date_filter:
            logger.info(f"Applying date filter: {self.date_filter['description']}")
        all_data = []
        message_count = 0
        filtered_count = 0
        start_time = datetime.now()
        partition_stats = defaultdict(int)  # 파티션별 메시지 수 통계
        
        try:
            # 초기 파티션 상태 체크
            assigned_partitions = self.consumer.assignment()
            logger.info(f"Starting collection from {len(assigned_partitions)} partitions")
            
            for message in self.consumer:
                if message.value is None:
                    continue
                
                message_count += 1
                
                # 날짜 범위 필터링 적용
                if not self.is_message_in_date_range(message.value):
                    filtered_count += 1
                    
                    # 성능을 위해 필터링 통계만 기록하고 continue
                    if filtered_count % 50000 == 0:
                        logger.info(f"Filtered out {filtered_count} messages so far...")
                    continue
                
                # 파티션별 통계 (실제 수집된 메시지만)
                partition_stats[message.partition] += 1
                all_data.append(message.value)
                
                # 진행 상황 로깅
                if len(all_data) % 10000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Collected {len(all_data)} messages "
                               f"(processed {message_count}, filtered {filtered_count}) "
                               f"in {elapsed:.1f}s (mode: {self.mode})")
                    
                    # 파티션별 통계 출력
                    if partition_stats:
                        stats_str = ", ".join([f"P{p}:{c}" for p, c in sorted(partition_stats.items())])
                        logger.info(f"Partition stats: {stats_str}")
                
                # # incremental 모드에서는 제한된 수만 수집
                # if self.mode == 'incremental' and len(all_data) >= 100000:
                #     logger.info(f"Incremental mode: Collected {len(all_data)} valid messages, stopping")
                #     break
                    
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.info(f"Consumer timeout reached. "
                           f"Processed {message_count} messages, "
                           f"collected {len(all_data)} valid messages, "
                           f"filtered {filtered_count} messages")
            else:
                logger.error(f"Error during data collection: {e}")
                raise
        
        collection_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Finished data collection in {collection_time:.1f}s")
        logger.info(f"Total processed: {message_count}")
        logger.info(f"Valid messages: {len(all_data)}")
        logger.info(f"Filtered out: {filtered_count}")
        
        if self.date_filter:
            filter_ratio = (filtered_count / message_count * 100) if message_count > 0 else 0
            logger.info(f"Filter efficiency: {filter_ratio:.1f}% filtered out")
        
        # 최종 파티션별 통계
        if partition_stats:
            logger.info("=== Final Partition Statistics ===")
            total_messages = sum(partition_stats.values())
            for partition, count in sorted(partition_stats.items()):
                percentage = (count / total_messages * 100) if total_messages > 0 else 0
                logger.info(f"Partition {partition:2d}: {count:8d} messages ({percentage:5.1f}%)")
            logger.info(f"Total:        {total_messages:8d} messages")
            logger.info("==================================")
        
        # 모드별 오프셋 처리
        if self.mode in ['incremental', 'continue'] and len(all_data) > 0:
            try:
                self.consumer.commit()
                logger.info(f"Committed offsets for {self.mode} mode")
                
                # 커밋 후 파티션 상태 다시 확인
                if hasattr(self, '_log_partition_status'):
                    self._log_partition_status(self.consumer, list(self.consumer.assignment()))
                
            except Exception as e:
                logger.warning(f"Failed to commit offsets: {e}")
        
        return all_data
    
    def run(self):
        """백필 프로세스 실행 - 모드별 동작"""
        try:
            logger.info(f"Starting backfill process in {self.mode} mode...")
            
            # 1. 모드별 데이터 수집
            all_data = self.collect_all_data()
            
            if not all_data:
                logger.warning(f"No data collected in {self.mode} mode")
                if self.mode == 'incremental':
                    logger.info("This is normal for incremental mode if no new data is available")
                return
            
            # 2. 타임스탬프 처리 및 날짜 범위 계산
            logger.info("Processing timestamps and calculating date range...")
            processed_data, min_timestamp, max_timestamp = self.process_data_timestamps(all_data)
            
            if not processed_data:
                logger.warning("No valid data after timestamp processing")
                return
            
            # 3. 파일명 생성 (모든 모드에서 timestamp 기준)
            filename = self.generate_filename(min_timestamp, max_timestamp)
            
            if self.mode == 'fresh':
                object_name = f"backfill/{filename}"
            elif self.mode == 'incremental':
                object_name = f"backfill/{filename}"
            elif self.mode == 'continue':
                object_name = f"backfill/{filename}"
            
            logger.info(f"Generated filename: {filename}")
            logger.info(f"Object name: {object_name}")
            
            # 4. DataFrame 생성 및 저장
            logger.info("Creating DataFrame and saving to MinIO...")
            df = pd.DataFrame(processed_data)
            
            # 데이터 정보 로깅
            logger.info(f"DataFrame created with {len(df)} records and {len(df.columns)} columns")
            if 'timestamp' in df.columns:
                logger.info(f"Timestamp range in data: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
            
            # MinIO에 저장
            if self.save_parquet_to_minio(df, object_name):
                logger.info(f"Backfill process completed successfully in {self.mode} mode!")
                logger.info(f"Saved file: {object_name}")
                logger.info(f"Total records: {len(df)}")
                
                if min_timestamp and max_timestamp:
                    logger.info(f"Date range: {min_timestamp.strftime('%Y-%m-%d')} ~ {max_timestamp.strftime('%Y-%m-%d')} (KST)")
            else:
                logger.error("Failed to save data to MinIO")
            
        except Exception as e:
            logger.error(f"Error during backfill process: {e}")
            raise
        finally:
            # 컨슈머 종료
            if hasattr(self, 'consumer'):
                self.consumer.close()
                logger.info("Kafka consumer closed")

class BackfillScheduler:
    def __init__(self):
        self.is_running = False
        self.last_run = None
        self.job_count = 0
        
        # 종료 신호 처리
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        self.should_stop = False
        
    def _signal_handler(self, signum, frame):
        """종료 신호 처리 (Ctrl+C, Docker stop 등)"""
        logger.info(f"Received signal {signum}, gracefully shutting down...")
        self.should_stop = True
        
    def run_incremental_job(self):
        """incremental 모드 백필 작업 실행"""
        if self.is_running:
            logger.warning("Previous job is still running, skipping this execution")
            return
            
        try:
            self.is_running = True
            self.job_count += 1
            start_time = datetime.now() + timedelta(hours=9) # KST로 변환
            
            logger.info("🚀 " + "=" * 60)
            logger.info(f"Starting scheduled incremental job #{self.job_count}")
            logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 64)
            
            # BackfillLoader 인스턴스 생성 및 실행
            loader = BackfillLoader(mode='incremental')
            loader.run()
            duration_time = datetime.now() + timedelta(hours=9)  # KST로 변환
            duration = (duration_time - start_time).total_seconds()
            self.last_run = start_time
            
            logger.info("✅ " + "=" * 60)
            logger.info(f"Scheduled incremental job #{self.job_count} completed successfully")
            logger.info(f"Duration: {duration:.1f} seconds")
            logger.info("=" * 64)
            
        except Exception as e:
            logger.error(f"❌ Error in scheduled job #{self.job_count}: {e}")
            logger.exception("Full error traceback:")
        finally:
            self.is_running = False
    
    def get_next_run_time(self):
        """다음 실행 시간 계산 (3일 후 자정)"""
        now = datetime.now() + timedelta(hours=9)  # KST로 변환
        # 내일 자정
        tomorrow_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        # 3일 후 자정 (내일부터 2일 후)
        next_run = tomorrow_midnight # test 하루 후 실행
        # next_run = tomorrow_midnight + timedelta(days=2)  # 총 3일 후
        # next_run = now  + timedelta(hours=2) # 테스트용 2시간 후
        return next_run
    
    def start_scheduler(self):
        """정확한 시간에 실행하는 스케줄러"""
        KST_NOW = datetime.now() + timedelta(hours=9)  # KST로 변환
        logger.info("🎯 Starting Precise Scheduler for Backfill Incremental")
        logger.info("Will run incremental job every 3 days at midnight (00:00)")
        logger.info(f"Scheduler started at: {KST_NOW.strftime('%Y-%m-%d %H:%M:%S')}")

        while not self.should_stop:
            try:
                # 다음 실행 시간 계산
                next_run_time = self.get_next_run_time()
                now = datetime.now() + timedelta(hours=9)  # KST로 변환
                
                # 대기 시간 계산
                sleep_seconds = (next_run_time - now).total_seconds()
                
                if sleep_seconds > 0:
                    sleep_duration = timedelta(seconds=int(sleep_seconds))
                    
                    logger.info("💤 Sleeping until next scheduled run...")
                    logger.info(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"   Next run: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"   Sleep duration: {sleep_duration}")
                    logger.info("   Press Ctrl+C to stop the scheduler")
                    logger.info("-" * 50)
                    
                    # 인터럽트 가능한 sleep (큰 시간을 작은 단위로 나누어 체크)
                    remaining_seconds = sleep_seconds
                    while remaining_seconds > 0 and not self.should_stop:
                        # 최대 1시간씩 잠들고 중간에 체크
                        sleep_chunk = min(remaining_seconds, 3600)  # 1시간 또는 남은 시간
                        time.sleep(sleep_chunk)
                        remaining_seconds -= sleep_chunk
                        
                        # 24시간마다 중간 상태 로그 (선택사항)
                        if remaining_seconds > 0 and int(remaining_seconds) % (24 * 3600) == 0:
                            days_left = int(remaining_seconds / (24 * 3600))
                            logger.info(f"⏰ Still sleeping... {days_left} days left until next run")
                
                # 종료 신호가 왔으면 루프 탈출
                if self.should_stop:
                    break
                
                # 시간이 되면 작업 실행
                logger.info(f"⏰ Scheduled time reached! Current time: {KST_NOW.strftime('%Y-%m-%d %H:%M:%S')}")
                self.run_incremental_job()
                
            except KeyboardInterrupt:
                logger.info("👋 Scheduler interrupted by user")
                break
            except Exception as e:
                logger.error(f"❌ Scheduler error: {e}")
                logger.info("⏳ Waiting 5 minutes before retry...")
                
                # 5분 대기 (인터럽트 가능)
                for _ in range(300):  # 5분 = 300초
                    if self.should_stop:
                        break
                    time.sleep(1)
        
        logger.info("🔚 Precise Scheduler stopped")

def parse_arguments():
    """명령행 인수 파싱"""
    parser = argparse.ArgumentParser(description='Backfill Loader - Kafka to MinIO data loader')
    
    parser.add_argument('--mode', 
                        choices=['fresh', 'incremental', 'continue', 'scheduler'], 
                        default='fresh',
                        help='Execution mode: fresh (start from beginning), incremental (from last offset), continue (from last offset or beginning), scheduler (run as a scheduled backfill job for every 3days)')

    parser.add_argument('--from-date',
                        type=str,
                        help='Start date for continue mode (YYYY-MM-DD format)')
    
    parser.add_argument('--to-date',
                        type=str,
                        help='End date (YYYY-MM-DD format, inclusive)')
    
    parser.add_argument('--target-date',
                        type=str,
                        help='Specific single date (YYYY-MM-DD format)')
    
    parser.add_argument('--kafka-brokers',
                        type=str,
                        help='Kafka broker addresses (comma-separated)')
    
    parser.add_argument('--kafka-topic',
                        type=str,
                        help='Kafka topic name')
    
    parser.add_argument('--kafka-group-id',
                        type=str,
                        help='Kafka consumer group ID')
    
    parser.add_argument('--minio-endpoint',
                        type=str,
                        help='MinIO endpoint')
    
    parser.add_argument('--minio-bucket',
                        type=str,
                        help='MinIO bucket name')
    
    return parser.parse_args()

if __name__ == "__main__":
    # 명령행 인수 파싱
    args = parse_arguments()
    
    # 환경변수 오버라이드
    if args.kafka_brokers:
        os.environ['KAFKA_BROKERS'] = args.kafka_brokers
    if args.kafka_topic:
        os.environ['KAFKA_TOPIC'] = args.kafka_topic
    if args.kafka_group_id:
        os.environ['KAFKA_GROUP_ID'] = args.kafka_group_id
    if args.minio_endpoint:
        os.environ['MINIO_ENDPOINT'] = args.minio_endpoint
    if args.minio_bucket:
        os.environ['MINIO_BUCKET'] = args.minio_bucket
    
    # 모드 검증
    if args.target_date and (args.from_date or args.to_date):
        logger.error("Cannot use --target-date with --from-date or --to-date")
        sys.exit(1)
    
    date_args = [args.from_date, args.to_date, args.target_date]
    for date_str in filter(None, date_args):
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD")
            sys.exit(1)

    if args.mode == 'continue' and args.from_date:
        try:
            datetime.strptime(args.from_date, '%Y-%m-%d')
        except ValueError:
            logger.error("Invalid from-date format. Use YYYY-MM-DD")
            sys.exit(1)

    if args.mode == 'scheduler':
        # 스케줄러 모드
        logger.info("Starting in scheduler mode - will run incremental mode every 3 days")
        scheduler = BackfillScheduler()
        scheduler.start_scheduler()

    # 백필 로더 실행
    logger.info(f"Starting Backfill Loader with arguments: "
               f"mode={args.mode}, from_date={args.from_date}, "
               f"to_date={args.to_date}, target_date={args.target_date}")
    
    loader = BackfillLoader(
        mode=args.mode, 
        from_date=args.from_date,
        to_date=args.to_date,
        target_date=args.target_date
    )
    loader.run()