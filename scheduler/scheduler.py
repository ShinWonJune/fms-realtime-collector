import os
import logging
import schedule
import time
import subprocess
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

'''

docker-compose up -d backfill-scheduler
3일에 한번씩 docker-compose run --rm backfill-loader python app.py --mode incremental 실행
3일에 한번씩 데이터를 kafka데이터 수집해서 minio에 적재


'''
def run_backfill():
    """백필 작업 실행"""
    try:
        logger.info("=" * 50)
        logger.info("Starting scheduled backfill job...")
        
        # Docker 컨테이너 내부에서 실행
        cmd = ["python", "/app/app.py", "--mode", "incremental"]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/app"
        )
        
        if result.returncode == 0:
            logger.info("Backfill job completed successfully")
        else:
            logger.error(f"Backfill job failed with return code: {result.returncode}")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
        
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Error running backfill: {e}")

def main():
    """메인 스케줄러 루프"""
    logger.info("Backfill scheduler started")
    logger.info("Schedule: Every 3 days")
    
    # 시작 시 한 번 실행 (옵션)
    run_on_start = os.getenv('RUN_ON_START', 'false').lower() == 'true' 
    if run_on_start:
        logger.info("Running initial backfill...")
        run_backfill()
    
    # 3일마다 실행 스케줄 설정
    schedule.every(3).days.do(run_backfill)
    
    # 다음 실행 시간 표시
    logger.info(f"Next run scheduled at: {schedule.next_run()}")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 체크
        
        # 다음 실행까지 남은 시간 로그 (1시간마다)
        if datetime.now().minute == 0:
            next_run = schedule.next_run()
            if next_run:
                time_until = (next_run - datetime.now()).total_seconds()
                hours = time_until / 3600
                logger.info(f"Next run in {hours:.1f} hours")

if __name__ == "__main__":
    main()
