import os
import time
import logging
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import json
from io import BytesIO
from collections import defaultdict

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KSTCompatibleRealtimeCollector:
    def __init__(self):
        # 환경 변수에서 설정값 읽기
        self.kafka_brokers = os.getenv('KAFKA_BROKERS', 'kafka:29092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'fms-temphum')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'realtime-collector')
        
        # Kafka 인증 설정 (필요시)
        self.kafka_security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
        self.kafka_sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM', None)
        self.kafka_sasl_username = os.getenv('KAFKA_SASL_USERNAME', None)
        self.kafka_sasl_password = os.getenv('KAFKA_SASL_PASSWORD', None)
        
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'fms-temphum')
        self.minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        
        # 수집 시간 설정 (한국시간 기준)
        self.collection_start_second = int(os.getenv('COLLECTION_START_SECOND', '5'))
        self.collection_duration = int(os.getenv('COLLECTION_DURATION', '50'))
        
        logger.info("=== KST Compatible Realtime Collector ===")
        logger.info(f"Kafka Brokers: {self.kafka_brokers}")
        logger.info(f"Kafka Topic: {self.kafka_topic}")
        logger.info(f"Kafka Security: {self.kafka_security_protocol}")
        logger.info(f"MinIO Endpoint: {self.minio_endpoint}")
        logger.info(f"Collection window: Every minute at :{self.collection_start_second:02d}~:{self.collection_start_second + self.collection_duration:02d} (KST)")
        
        # MinIO 클라이언트 초기화
        self.minio_client = self._init_minio_client()
        
        # Kafka 컨슈머 초기화
        self.consumer = self._init_kafka_consumer()
        
        logger.info("KST Compatible Collector initialized successfully")
    
    def _init_minio_client(self):
        """MinIO 클라이언트 초기화"""
        try:
            logger.info("Initializing MinIO client...")
            client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            
            # 연결 테스트 및 버킷 생성
            if not client.bucket_exists(self.minio_bucket):
                logger.info(f"Creating bucket: {self.minio_bucket}")
                client.make_bucket(self.minio_bucket)
            else:
                logger.info(f"Bucket {self.minio_bucket} exists")
            
            return client
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise
    
    def _init_kafka_consumer(self):
        """Kafka 컨슈머 초기화"""
        try:
            logger.info("Initializing Kafka consumer...")
            
            # 기본 컨슈머 설정
            consumer_config = {
                'bootstrap_servers': self.kafka_brokers.split(','),
                'group_id': self.kafka_group_id,
                'auto_offset_reset': 'latest',
                'enable_auto_commit': True,
                'auto_commit_interval_ms': 5000,
                'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None,
                'consumer_timeout_ms': 5000,
                'max_poll_records': 1000,
                'session_timeout_ms': 30000,
                'heartbeat_interval_ms': 3000,
            }
            
            # 인증 설정 추가 (필요시)
            if self.kafka_security_protocol != 'PLAINTEXT':
                consumer_config['security_protocol'] = self.kafka_security_protocol
                
                if self.kafka_sasl_mechanism:
                    consumer_config['sasl_mechanism'] = self.kafka_sasl_mechanism
                    consumer_config['sasl_plain_username'] = self.kafka_sasl_username
                    consumer_config['sasl_plain_password'] = self.kafka_sasl_password
                    logger.info(f"Using SASL authentication: {self.kafka_sasl_mechanism}")
            
            consumer = KafkaConsumer(self.kafka_topic, **consumer_config)
            
            # 토픽 파티션 확인
            partitions = consumer.partitions_for_topic(self.kafka_topic)
            if partitions:
                logger.info(f"Connected to topic '{self.kafka_topic}' with partitions: {partitions}")
            else:
                logger.warning(f"Topic '{self.kafka_topic}' not found or no partitions")
            
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
            # 변환 실패시 현재 한국시간 사용
            return datetime.now()  # 서버가 한국시간대라고 가정
    
    def wait_for_collection_window(self):
        """다음 수집 윈도우까지 대기 (한국시간 기준)"""
        now = datetime.now()  # 한국시간 (서버 시간대)
        current_minute = now.replace(second=0, microsecond=0)
        
        # 현재 분의 수집 시작 시간
        collection_start = current_minute + timedelta(seconds=self.collection_start_second)
        
        # 이미 수집 시간이 지났으면 다음 분으로
        if now >= collection_start + timedelta(seconds=self.collection_duration):
            collection_start = current_minute + timedelta(minutes=1, seconds=self.collection_start_second)
        elif now < collection_start:
            # 아직 수집 시간이 안 되었으면 현재 분의 수집 시간까지 대기
            pass
        else:
            # 현재 수집 시간 중이면 바로 시작
            return collection_start
        
        wait_seconds = (collection_start - now).total_seconds()
        if wait_seconds > 0:
            logger.info(f"Waiting {wait_seconds:.1f} seconds until collection window starts at {collection_start.strftime('%H:%M:%S')} (KST)")
            time.sleep(wait_seconds)
        
        return collection_start
    
    def object_exists(self, object_name):
        """MinIO 객체 존재 여부 확인"""
        try:
            self.minio_client.stat_object(self.minio_bucket, object_name)
            return True
        except:
            return False
    
    def read_parquet_from_minio(self, object_name):
        """MinIO에서 Parquet 파일 읽기"""
        try:
            response = self.minio_client.get_object(self.minio_bucket, object_name)
            parquet_data = response.read()
            response.close()
            response.release_conn()
            
            parquet_buffer = BytesIO(parquet_data)
            return pd.read_parquet(parquet_buffer)
        except Exception as e:
            logger.error(f"Error reading {object_name}: {e}")
            return None
    
    def save_to_parquet_with_kst_filename(self, data, collection_start_time):
        """데이터의 KST timestamp 기준으로 파일명 생성하여 저장"""
        try:
            if not data:
                logger.warning("No data to save")
                return
            
            logger.info(f"Processing {len(data)} records for KST-compatible saving")
            
            # 1. 데이터를 KST timestamp별로 그룹화
            kst_groups = defaultdict(list)
            
            for record in data:
                try:
                    # timestamp 필드 찾기 및 변환
                    timestamp_value = None
                    if '@timestamp' in record:
                        timestamp_value = record['@timestamp']
                        timestamp_field = '@timestamp'
                    # elif 'timestamp' in record:
                    #     timestamp_value = record['timestamp']
                    #     timestamp_field = 'timestamp'
                    
                    if timestamp_value:
                        # UTC → KST 변환
                        kst_timestamp = self.convert_utc_to_kst(timestamp_value)
                        
                        # record의 timestamp 필드를 KST로 업데이트
                        record['timestamp'] = kst_timestamp.isoformat()
                        
                        # @timestamp 제거 (중복 방지)
                        if '@timestamp' in record and timestamp_field != 'timestamp':
                            del record['@timestamp']
                    else:
                        # timestamp가 없으면 현재 한국시간 사용
                        kst_timestamp = datetime.now()
                        record['timestamp'] = kst_timestamp.isoformat()
                        logger.warning(f"No timestamp found in record, using current KST: {kst_timestamp}")
                    
                    # 분 단위로 그룹화 (초는 0으로 설정)
                    minute_key = kst_timestamp.replace(second=0, microsecond=0)
                    kst_groups[minute_key].append(record)
                    
                except Exception as e:
                    logger.warning(f"Error processing record timestamp: {e}")
                    # 에러 발생시 수집 시간 기준으로 분류
                    fallback_key = collection_start_time.replace(second=0, microsecond=0)
                    record['timestamp'] = fallback_key.isoformat()
                    kst_groups[fallback_key].append(record)
            
            # 2. 각 KST 분 그룹별로 파일 저장
            saved_files = []
            
            for kst_minute, group_data in kst_groups.items():
                # 파일명 생성 (KST 기준)
                filename = f"{kst_minute.strftime('%Y%m%d_%H%M')}.parquet"
                object_name = f"minute/{filename}"
                
                logger.info(f"Saving {len(group_data)} records to {object_name} (KST: {kst_minute.strftime('%Y-%m-%d %H:%M')})")
                
                # 기존 파일이 있으면 병합
                existing_df = None
                if self.object_exists(object_name):
                    logger.info(f"File {object_name} already exists, merging with new data")
                    existing_df = self.read_parquet_from_minio(object_name)
                
                # DataFrame 생성
                new_df = pd.DataFrame(group_data)
                
                if existing_df is not None and not existing_df.empty:
                    # 기존 데이터와 병합
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    
                    # 중복 제거 (Kafka offset 기준, 없으면 전체 중복 제거)
                    if '_kafka_offset' in combined_df.columns and '_kafka_partition' in combined_df.columns:
                        combined_df = combined_df.drop_duplicates(
                            subset=['_kafka_partition', '_kafka_offset'], 
                            keep='last'
                        )
                    else:
                        # Kafka 메타데이터가 없으면 timestamp 기준 중복 제거
                        combined_df = combined_df.drop_duplicates(
                            subset=['timestamp'], 
                            keep='last'
                        )
                    
                    # timestamp로 정렬
                    combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
                    logger.info(f"Merged with existing data. Total records: {len(combined_df)} (added: {len(new_df)})")
                    final_df = combined_df
                else:
                    # 새 파일
                    final_df = new_df.sort_values('timestamp').reset_index(drop=True)
                
                # Parquet으로 변환 및 저장
                table = pa.Table.from_pandas(final_df)
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer)
                parquet_size = len(parquet_buffer.getvalue())
                parquet_buffer.seek(0)
                
                # MinIO에 업로드
                result = self.minio_client.put_object(
                    self.minio_bucket,
                    object_name,
                    parquet_buffer,
                    length=parquet_size,
                    content_type='application/octet-stream'
                )
                
                saved_files.append(filename)
                logger.info(f"Successfully saved {filename} (size: {parquet_size} bytes, records: {len(final_df)})")
            
            logger.info(f"Data processing completed. Created/Updated {len(saved_files)} files: {saved_files}")
            
        except Exception as e:
            logger.error(f"Error in KST-compatible saving: {e}")
            raise
    
    def collect_minute_data(self):
        """한국시간 기준 수집 윈도우에서 데이터 수집"""
        collection_start_time = self.wait_for_collection_window()
        collection_end_time = collection_start_time + timedelta(seconds=self.collection_duration)
        
        logger.info(f"=== Collection window: {collection_start_time.strftime('%H:%M:%S')} ~ {collection_end_time.strftime('%H:%M:%S')} (KST) ===")
        
        collected_data = []
        message_count = 0
        
        try:
            # 수집 윈도우 동안 데이터 수집
            while datetime.now() < collection_end_time:
                try:
                    remaining_seconds = (collection_end_time - datetime.now()).total_seconds()
                    if remaining_seconds <= 0:
                        break
                    
                    timeout_ms = min(5000, max(1000, int(remaining_seconds * 1000)))
                    messages = self.consumer.poll(timeout_ms=timeout_ms)
                    
                    if not messages:
                        continue
                    
                    for topic_partition, records in messages.items():
                        for message in records:
                            if message.value is None:
                                continue
                            
                            # 메시지 데이터 복사
                            data = message.value.copy()
                            
                            # Kafka 메타데이터 추가 (중복 제거용)
                            data['_kafka_partition'] = message.partition
                            data['_kafka_offset'] = message.offset
                            data['_collection_time'] = datetime.now().isoformat()
                            
                            collected_data.append(data)
                            message_count += 1
                    
                    # 진행 상황 로그 (100개마다)
                    if message_count > 0 and message_count % 100 == 0:
                        elapsed = (datetime.now() - collection_start_time).total_seconds()
                        logger.info(f"Collected {message_count} messages in {elapsed:.1f}s")
                        
                except Exception as e:
                    logger.warning(f"Error during polling: {e}")
                    continue
            
            # 수집 완료
            collection_duration = (datetime.now() - collection_start_time).total_seconds()
            
            if collected_data:
                # KST 기준 파일명으로 저장
                self.save_to_parquet_with_kst_filename(collected_data, collection_start_time)
                logger.info(f"Successfully collected and processed {len(collected_data)} records in {collection_duration:.1f}s")
            else:
                logger.info(f"No data collected in {collection_duration:.1f}s collection window")
                
        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            if collected_data:
                try:
                    self.save_to_parquet_with_kst_filename(collected_data, collection_start_time)
                    logger.info(f"Saved {len(collected_data)} records despite error")
                except Exception as save_error:
                    logger.error(f"Failed to save data after error: {save_error}")
    
    def run(self):
        """메인 실행 루프 - 한국시간 기준 동기화"""
        logger.info("Starting KST Compatible Realtime Collector...")
        logger.info(f"Server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (assumed KST)")
        
        # 초기 상태 확인
        try:
            buckets = self.minio_client.list_buckets()
            logger.info(f"Available MinIO buckets: {[b.name for b in buckets]}")
            
            # 기존 파일 확인
            try:
                objects = list(self.minio_client.list_objects(self.minio_bucket, prefix='minute/', recursive=True))
                logger.info(f"Found {len(objects)} existing minute files in bucket")
            except:
                logger.info("No existing files found or error checking files")
                
        except Exception as e:
            logger.error(f"Initial health check failed: {e}")
        
        iteration = 0
        while True:
            iteration += 1
            try:
                logger.info(f"\n=== Collection iteration {iteration} ===")
                
                # KST 기준 동기화된 데이터 수집
                self.collect_minute_data()
                
                logger.info(f"Iteration {iteration} completed, waiting for next collection window...")
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                # 에러 발생시 다음 수집 윈도우까지 대기
                try:
                    self.wait_for_collection_window()
                except:
                    logger.error("Error in wait_for_collection_window, sleeping 60 seconds")
                    time.sleep(60)
        
        # 정리
        if hasattr(self, 'consumer'):
            self.consumer.close()
            logger.info("Kafka consumer closed")

if __name__ == "__main__":
    collector = KSTCompatibleRealtimeCollector()
    collector.run()