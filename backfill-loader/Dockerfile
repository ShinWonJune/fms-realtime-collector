FROM python:3.9-slim

WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# pip 업그레이드
RUN pip install --upgrade pip

# Python 의존성 설치 (호환성 문제 해결)
COPY requirements.txt .

# 기존 패키지 제거 후 재설치
RUN pip uninstall -y numpy pandas pyarrow || true && \
    pip install --no-cache-dir --force-reinstall -r requirements.txt

# 애플리케이션 코드 복사
COPY app.py .

# 실행
CMD ["python", "app.py"]