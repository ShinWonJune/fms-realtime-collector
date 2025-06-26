# FMS 실시간 데이터 수집기

Kafka에서 실시간으로 데이터를 수집하여 MinIO에 Parquet 형태로 저장하는 시스템입니다.

## 🏗️ 아키텍처 개요

### 핵심 컴포넌트
- **실시간 수집기 (Real-time Collector)**: 1분마다 Kafka 데이터를 수집하여 분 단위 Parquet 파일로 저장
- **배치 병합기 (Batch Merger)**: 정해진 시간에 작은 파일들을 큰 단위로 병합
- **과거 데이터 적재기 (Backfill Loader)**: 최초 한 번 Kafka의 모든 과거 데이터를 적재

### MinIO 버킷 구조
```
fms-data/
├── minute/          # 분 단위 파일 (YYYYMMDD_HHMM.parquet)
│   └── 20250612_1122.parquet
├── hour/            # 시간 단위 파일 (YYYYMMDD_HH_HH+1.parquet)
│   └── 20250612_00_01.parquet
├── day/             # 일 단위 파일 (YYYYMMDD.parquet)
│   └── 20250608.parquet
└── week/            # 주 단위 파일 (YYYYMMDD_YYYYMMDD.parquet)
    └── 20250601_20250607.parquet
```

## 🚀 배포 방법

### 1. 사전 요구사항
- Kubernetes 클러스터 (kubectl 명령어 사용 가능)
- Docker 환경
- Kafka 클러스터 실행 중
- MinIO 서버 실행 중

### 2. 설정 변경
`k8s/configmap.yaml` 파일에서 환경에 맞게 설정을 변경하세요:
```yaml
data:
  KAFKA_BROKERS: "your-kafka-service:9092"
  KAFKA_TOPIC: "your-topic-name"
  MINIO_ENDPOINT: "your-minio-service:9000"
```

### 3. 배포 실행
```bash
# Linux/Mac
./deploy.sh

# Windows
deploy.bat
```

### 4. 과거 데이터 적재 (선택사항)
```bash
kubectl apply -f k8s/backfill-loader-job.yaml
kubectl logs -f job/backfill-loader
```

## 📊 모니터링

### 실시간 수집기 상태 확인
```bash
kubectl logs -f deployment/realtime-collector
kubectl get pods -l app=realtime-collector
```

### 배치 병합기 상태 확인
```bash
kubectl get cronjobs
kubectl describe cronjob batch-merger-hour
kubectl logs job/batch-merger-hour-[job-id]
```

### MinIO 데이터 확인
MinIO 웹 콘솔에서 `fms-data` 버킷의 각 경로별 파일을 확인할 수 있습니다.

## ⚙️ 환경 변수

### 공통 환경 변수
- `KAFKA_BROKERS`: Kafka 브로커 주소 (예: "localhost:9092")
- `KAFKA_TOPIC`: Kafka 토픽 이름 (예: "fms-data")
- `MINIO_ENDPOINT`: MinIO 서버 주소 (예: "localhost:9000")
- `MINIO_ACCESS_KEY`: MinIO 액세스 키
- `MINIO_SECRET_KEY`: MinIO 시크릿 키
- `MINIO_BUCKET`: MinIO 버킷 이름 (예: "fms-data")
- `MINIO_SECURE`: HTTPS 사용 여부 (true/false)

### 컴포넌트별 추가 환경 변수
- `KAFKA_GROUP_ID`: Kafka 컨슈머 그룹 ID (컴포넌트별로 다름)

## 🕐 스케줄링

### 배치 병합기 실행 스케줄
- **시간별 병합**: 매시 1분 (`1 * * * *`)
- **일별 병합**: 매일 00시 5분 (`5 0 * * *`)
- **주별 병합**: 매주 월요일 00시 10분 (`10 0 * * 1`)

## 📁 프로젝트 구조

```
fms-realtime-collector/
├── realtime-collector/          # 실시간 수집기
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── batch-merger/               # 배치 병합기
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── backfill-loader/           # 과거 데이터 적재기
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/                       # Kubernetes 리소스
│   ├── configmap.yaml
│   ├── realtime-collector-deployment.yaml
│   ├── batch-merger-cronjobs.yaml
│   └── backfill-loader-job.yaml
├── deploy.sh                  # 배포 스크립트 (Linux/Mac)
├── deploy.bat                 # 배포 스크립트 (Windows)
└── README.md
```

## 🔧 트러블슈팅

### 자주 발생하는 문제

1. **Kafka 연결 실패**
   - `KAFKA_BROKERS` 설정이 올바른지 확인
   - Kafka 서비스가 실행 중인지 확인
   - 네트워크 연결 상태 확인

2. **MinIO 연결 실패**
   - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` 설정 확인
   - MinIO 서비스가 실행 중인지 확인
   - 버킷 권한 설정 확인

3. **Pod 재시작 반복**
   - 로그 확인: `kubectl logs -f deployment/realtime-collector`
   - 리소스 제한 확인 및 조정

### 유용한 명령어

```bash
# 전체 상태 확인
kubectl get all -l app=realtime-collector

# 로그 확인
kubectl logs -f deployment/realtime-collector
kubectl logs job/batch-merger-hour-[job-id]

# 설정 확인
kubectl describe configmap fms-config
kubectl describe secret fms-secrets

# 수동 병합 작업 실행
kubectl create job --from=cronjob/batch-merger-hour manual-merge-hour

# 리소스 삭제
kubectl delete -f k8s/
```

## 📈 성능 최적화

### 리소스 조정
각 컴포넌트의 리소스 요구사항에 따라 `k8s/*.yaml` 파일의 `resources` 섹션을 조정하세요.

### 배치 크기 조정
데이터 처리량에 따라 Kafka 컨슈머의 배치 크기나 타임아웃 설정을 조정할 수 있습니다.

## 🔒 보안 고려사항

1. **Secret 관리**: MinIO 자격 증명을 Kubernetes Secret으로 관리
2. **네트워크 정책**: 필요에 따라 NetworkPolicy 설정
3. **RBAC**: 서비스 계정에 최소 권한 부여

## 📝 라이센스

이 프로젝트는 MIT 라이센스를 따릅니다.
