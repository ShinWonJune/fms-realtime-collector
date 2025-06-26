import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import argparse

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchMerger:
    def __init__(self):
        # 환경 변수에서 설정값 읽기
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.minio_bucket = os.getenv('MINIO_BUCKET', 'fms-data')
        self.minio_secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        
        # MinIO 클라이언트 초기화
        self.minio_client = self._init_minio_client()
        
        logger.info("BatchMerger initialized successfully")
    
    def _init_minio_client(self):
        """MinIO 클라이언트 초기화"""
        try:
            client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            return client
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise
    
    def get_objects_by_prefix(self, prefix):
        """지정된 prefix로 시작하는 객체 목록 반환"""
        try:
            objects = self.minio_client.list_objects(self.minio_bucket, prefix=prefix)
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"Error listing objects with prefix {prefix}: {e}")
            return []
    
    def read_parquet_from_minio(self, object_name):
        """MinIO에서 Parquet 파일 읽기"""
        try:
            response = self.minio_client.get_object(self.minio_bucket, object_name)
            parquet_data = response.read()
            response.close()
            response.release_conn()
            
            parquet_buffer = BytesIO(parquet_data)
            table = pq.read_table(parquet_buffer)
            return table.to_pandas()
        except Exception as e:
            logger.error(f"Error reading {object_name}: {e}")
            return None
    
    def save_parquet_to_minio(self, df, object_name):
        """DataFrame을 Parquet으로 MinIO에 저장"""
        try:
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
            
            logger.info(f"Successfully saved {object_name} to MinIO")
            return True
        except Exception as e:
            logger.error(f"Error saving {object_name}: {e}")
            return False
    
    def delete_objects(self, object_names):
        """MinIO에서 객체들 삭제"""
        for object_name in object_names:
            try:
                self.minio_client.remove_object(self.minio_bucket, object_name)
                logger.info(f"Deleted {object_name}")
            except Exception as e:
                logger.error(f"Error deleting {object_name}: {e}")
    
    def merge_hour(self):
        """분 단위 파일들을 시간 단위로 병합"""
        logger.info("Starting hour merge process")
        
        # 한 시간 전 시간 계산
        target_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        hour_prefix = target_time.strftime('%Y%m%d_%H')
        
        # 해당 시간의 분 단위 파일들 찾기
        minute_objects = self.get_objects_by_prefix(f"minute/{hour_prefix}")
        
        if not minute_objects:
            logger.info(f"No minute files found for {hour_prefix}")
            return
        
        logger.info(f"Found {len(minute_objects)} minute files for {hour_prefix}")
        
        # 파일들 읽고 병합
        dfs = []
        for obj_name in minute_objects:
            df = self.read_parquet_from_minio(obj_name)
            if df is not None and not df.empty:
                dfs.append(df)
        
        if not dfs:
            logger.warning("No valid data found in minute files")
            return
        
        # 데이터 병합
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df.sort_values('timestamp').reset_index(drop=True)
        
        # 시간 단위 파일로 저장
        next_hour = target_time + timedelta(hours=1)
        filename = f"{target_time.strftime('%Y%m%d_%H')}_{next_hour.strftime('%H')}.parquet"
        object_name = f"hour/{filename}"
        
        if self.save_parquet_to_minio(merged_df, object_name):
            # 원본 분 단위 파일들 삭제
            self.delete_objects(minute_objects)
            logger.info(f"Successfully merged {len(minute_objects)} files into {filename}")
    
    def merge_day(self):
        """시간 단위 파일들을 일 단위로 병합"""
        logger.info("Starting day merge process")
        
        # 어제 날짜 계산
        yesterday = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_prefix = yesterday.strftime('%Y%m%d_')
        
        # 해당 날짜의 시간 단위 파일들 찾기
        hour_objects = self.get_objects_by_prefix(f"hour/{day_prefix}")
        
        if not hour_objects:
            logger.info(f"No hour files found for {yesterday.strftime('%Y%m%d')}")
            return
        
        logger.info(f"Found {len(hour_objects)} hour files for {yesterday.strftime('%Y%m%d')}")
        
        # 파일들 읽고 병합
        dfs = []
        for obj_name in hour_objects:
            df = self.read_parquet_from_minio(obj_name)
            if df is not None and not df.empty:
                dfs.append(df)
        
        if not dfs:
            logger.warning("No valid data found in hour files")
            return
        
        # 데이터 병합
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df.sort_values('timestamp').reset_index(drop=True)
        
        # 일 단위 파일로 저장
        filename = f"{yesterday.strftime('%Y%m%d')}.parquet"
        object_name = f"day/{filename}"
        
        if self.save_parquet_to_minio(merged_df, object_name):
            # 원본 시간 단위 파일들 삭제
            self.delete_objects(hour_objects)
            logger.info(f"Successfully merged {len(hour_objects)} files into {filename}")
    
    def merge_week(self):
        """일 단위 파일들을 주 단위로 병합"""
        logger.info("Starting week merge process")
        
        # 지난주 월요일부터 일요일까지 계산
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        days_since_monday = today.weekday()
        last_monday = today - timedelta(days=days_since_monday + 7)  # 지난주 월요일
        last_sunday = last_monday + timedelta(days=6)  # 지난주 일요일
        
        # 지난주 일 단위 파일들 찾기
        day_objects = []
        current_date = last_monday
        while current_date <= last_sunday:
            day_filename = f"day/{current_date.strftime('%Y%m%d')}.parquet"
            if self.object_exists(day_filename):
                day_objects.append(day_filename)
            current_date += timedelta(days=1)
        
        if not day_objects:
            logger.info(f"No day files found for week {last_monday.strftime('%Y%m%d')}_{last_sunday.strftime('%Y%m%d')}")
            return
        
        logger.info(f"Found {len(day_objects)} day files for the week")
        
        # 파일들 읽고 병합
        dfs = []
        for obj_name in day_objects:
            df = self.read_parquet_from_minio(obj_name)
            if df is not None and not df.empty:
                dfs.append(df)
        
        if not dfs:
            logger.warning("No valid data found in day files")
            return
        
        # 데이터 병합
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df = merged_df.sort_values('timestamp').reset_index(drop=True)
        
        # 주 단위 파일로 저장
        filename = f"{last_monday.strftime('%Y%m%d')}_{last_sunday.strftime('%Y%m%d')}.parquet"
        object_name = f"week/{filename}"
        
        if self.save_parquet_to_minio(merged_df, object_name):
            # 원본 일 단위 파일들 삭제
            self.delete_objects(day_objects)
            logger.info(f"Successfully merged {len(day_objects)} files into {filename}")
    
    def object_exists(self, object_name):
        """객체가 존재하는지 확인"""
        try:
            self.minio_client.stat_object(self.minio_bucket, object_name)
            return True
        except S3Error:
            return False

def main():
    parser = argparse.ArgumentParser(description='Batch Merger for FMS Data')
    parser.add_argument('merge_type', choices=['hour', 'day', 'week'], 
                       help='Type of merge to perform')
    
    args = parser.parse_args()
    
    merger = BatchMerger()
    
    try:
        if args.merge_type == 'hour':
            merger.merge_hour()
        elif args.merge_type == 'day':
            merger.merge_day()
        elif args.merge_type == 'week':
            merger.merge_week()
        
        logger.info(f"Completed {args.merge_type} merge successfully")
        
    except Exception as e:
        logger.error(f"Error during {args.merge_type} merge: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
