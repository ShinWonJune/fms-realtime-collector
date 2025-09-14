FROM python:3.9-slim

WORKDIR /app

# 필수 패키지 설치
RUN pip install --no-cache-dir \
    schedule \
    kafka-python \
    pandas \
    pyarrow \
    minio

# 백필 로더 앱 복사
COPY backfill-loader/app.py /app/app.py

# 스케줄러 복사
COPY scheduler/scheduler.py /app/scheduler.py

CMD ["python", "/app/scheduler.py"]
