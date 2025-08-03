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

'''
docker-compose run --rm backfill-loader python app.py --mode incremental
docker-compose run --rm backfill-loader python app.py --mode fresh
docker-compose run --rm backfill-loader python app.py --mode continue --from-date 2023-10-01

'''
# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BackfillLoader:
    def __init__(self, mode='fresh', from_date=None):
        # 실행 모드 설정
        self.mode = mode
        self.from_date = from_date
        
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
        logger.info(f"From date: {self.from_date}")
        logger.info(f"Kafka Topic: {self.kafka_topic}")
        logger.info(f"Group ID: {self.kafka_group_id}")
        
        # MinIO 클라이언트 초기화
        self.minio_client = self._init_minio_client()
        
        # Kafka 컨슈머 초기화
        self.consumer = self._init_kafka_consumer()
        
        logger.info("BackfillLoader initialized successfully")
    
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
    
    def _init_kafka_consumer(self):
        """Kafka 컨슈머 초기화 - 모드별 설정"""
        try:
            logger.info(f"Initializing Kafka consumer for mode: {self.mode}")
            
            # 모드별 설정
            if self.mode == 'fresh':
                # 처음부터 전체 데이터 수집, 오프셋 저장 안함
                auto_offset_reset = 'earliest'
                enable_auto_commit = False
                logger.info("Fresh mode: Reading from beginning, not saving offsets")
                
            elif self.mode == 'incremental':
                # 마지막 저장된 오프셋부터 수집, 오프셋 저장함
                auto_offset_reset = 'earliest'  # 저장된 오프셋이 없으면 최신부터
                enable_auto_commit = True
                logger.info("Incremental mode: Reading from last committed offset")
                
            elif self.mode == 'continue':
                # 마지막 저장된 오프셋부터 수집, 오프셋 저장함
                auto_offset_reset = 'earliest'  # 저장된 오프셋이 없으면 처음부터
                enable_auto_commit = True
                logger.info("Continue mode: Reading from last committed offset or beginning")
                
            else:
                raise ValueError(f"Invalid mode: {self.mode}")
            
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_brokers.split(','),
                group_id=self.kafka_group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                auto_commit_interval_ms=5000,  # 5초마다 오프셋 커밋
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=30000,  # 30초 타임아웃
                max_poll_records=1000,  # 한 번에 최대 1000개 레코드
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
            )
            
            # 토픽에 subscribe
            consumer.subscribe([self.kafka_topic])
            
            # fresh 모드일 때만 처음부터 강제 시작
            if self.mode == 'fresh':
                # 파티션 할당을 위한 초기 poll
                consumer.poll(timeout_ms=1000)
                consumer.seek_to_beginning()
                logger.info("Reset offsets to beginning for fresh mode")
            
            logger.info(f"Kafka consumer initialized for topic: {self.kafka_topic}")
            
            # 할당된 파티션 확인
            consumer.poll(timeout_ms=1000)  # 파티션 할당 확인
            assigned_partitions = consumer.assignment()
            if assigned_partitions:
                logger.info(f"Assigned partitions: {assigned_partitions}")
                
                # 현재 오프셋 위치 확인
                for partition in assigned_partitions:
                    current_offset = consumer.position(partition)
                    logger.info(f"Partition {partition.partition}: current offset = {current_offset}")
            else:
                logger.warning("No partitions assigned yet")
            
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
        """Kafka에서 데이터 수집 - 모드별 동작"""
        logger.info(f"Starting data collection in {self.mode} mode")
        
        all_data = []
        message_count = 0
        start_time = datetime.now()
        
        try:
            # from_date 필터링을 위한 준비
            from_timestamp = None
            if self.from_date and self.mode == 'continue':
                try:
                    from_timestamp = datetime.strptime(self.from_date, '%Y-%m-%d')
                    logger.info(f"Filtering messages from: {from_timestamp}")
                except ValueError:
                    logger.warning(f"Invalid from_date format: {self.from_date}, ignoring filter")
            
            for message in self.consumer:
                if message.value is None:
                    continue
                
                # from_date 필터링 (continue 모드에서만)
                if from_timestamp and self.mode == 'continue':
                    try:
                        msg_timestamp = None
                        if '@timestamp' in message.value:
                            msg_timestamp = self.convert_utc_to_kst(message.value['@timestamp'])
                        elif 'timestamp' in message.value:
                            msg_timestamp = self.convert_utc_to_kst(message.value['timestamp'])
                        
                        if msg_timestamp and msg_timestamp.date() < from_timestamp.date():
                            continue  # 지정된 날짜 이전 메시지는 스킵
                            
                    except Exception as e:
                        logger.warning(f"Error filtering message by date: {e}")
                
                all_data.append(message.value)
                message_count += 1
                
                # 진행 상황 로깅
                if message_count % 10000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(f"Collected {message_count} messages in {elapsed:.1f}s (mode: {self.mode})")
                
                # incremental 모드에서는 제한된 수만 수집 (무한 루프 방지)
                if self.mode == 'incremental' and message_count >= 100000:
                    logger.info(f"Incremental mode: Collected {message_count} messages, stopping")
                    break
                
        except Exception as e:
            if "timeout" in str(e).lower():
                logger.info(f"Consumer timeout reached. Collected {message_count} messages total")
            else:
                logger.error(f"Error during data collection: {e}")
                raise
        
        collection_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Finished data collection in {collection_time:.1f}s. Total messages: {message_count}")
        
        # 모드별 오프셋 처리
        logger.info(f"About to commit offsets. Mode: {self.mode}, Message count: {message_count}")
        if self.mode in ['incremental', 'continue'] and message_count > 0:
            try:
                self.consumer.commit()
                logger.info(f"Committed offsets for {self.mode} mode")
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

def parse_arguments():
    """명령행 인수 파싱"""
    parser = argparse.ArgumentParser(description='Backfill Loader - Kafka to MinIO data loader')
    
    parser.add_argument('--mode', 
                        choices=['fresh', 'incremental', 'continue'], 
                        default='fresh',
                        help='Execution mode: fresh (start from beginning), incremental (from last offset), continue (from last offset or beginning)')
    
    parser.add_argument('--from-date',
                        type=str,
                        help='Start date for continue mode (YYYY-MM-DD format)')
    
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
    if args.mode == 'continue' and args.from_date:
        try:
            datetime.strptime(args.from_date, '%Y-%m-%d')
        except ValueError:
            logger.error("Invalid from-date format. Use YYYY-MM-DD")
            sys.exit(1)
    
    # 백필 로더 실행
    logger.info(f"Starting Backfill Loader with arguments: mode={args.mode}, from_date={args.from_date}")
    
    loader = BackfillLoader(mode=args.mode, from_date=args.from_date)
    loader.run()