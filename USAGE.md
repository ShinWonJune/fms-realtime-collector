# FMS 실시간 수집기 사용법 가이드

## 🎯 개요
이 프로젝트는 Kafka에서 실시간으로 FMS(Fleet Management System) 데이터를 수집하여 MinIO에 Parquet 형태로 저장하는 시스템입니다.

## 🚀 빠른 시작

### 1. 개발/테스트 환경 (Docker Compose)

```bash
# 1. 개발 환경 시작
./start-dev.sh  # Linux/Mac
# 또는
start-dev.bat   # Windows

# 2. 테스트 데이터 생성
pip install kafka-python
python test-data-generator.py --mode continuous --interval 5

# 3. MinIO 웹 콘솔에서 데이터 확인
# http://localhost:9001 (minioadmin/minioadmin)
```

### 2. 프로덕션 환경 (Kubernetes)

```bash
# 1. 설정 파일 수정
# k8s/configmap.yaml에서 Kafka, MinIO 주소 변경

# 2. 배포 실행
./deploy.sh     # Linux/Mac
# 또는  
deploy.bat      # Windows

# 3. 과거 데이터 적재 (선택사항)
kubectl apply -f k8s/backfill-loader-job.yaml
```

## 📊 데이터 흐름

1. **Kafka → 실시간 수집기**: 1분마다 데이터 수집
2. **실시간 수집기 → MinIO**: 분 단위 Parquet 파일 저장 (`minute/` 경로)
3. **배치 병합기**: 시간 단위별로 파일 병합
   - 분 → 시간 (매시 1분)
   - 시간 → 일 (매일 00:05)
   - 일 → 주 (매주 월요일 00:10)

## 🗂️ 파일 저장 규칙

### 데이터량에 따른 저장 전략
- **1주일 이상**: 주 단위 저장 (`20250601_20250607.parquet`)
- **1일 이상**: 일 단위 저장 (`20250608.parquet`)
- **1시간 이상**: 시간 단위 저장 (`20250612_00_01.parquet`)
- **1시간 미만**: 분 단위 저장 (`20250612_1122.parquet`)

### 타임스탬프 변환
- Kafka 데이터의 UTC 타임스탬프에 9시간을 더해 KST로 변환
- 모든 파일명은 KST 기준으로 생성

## 🔧 주요 설정

### 환경 변수
```yaml
KAFKA_BROKERS: "kafka-service:9092"
KAFKA_TOPIC: "fms-data"
MINIO_ENDPOINT: "minio-service:9000"
MINIO_ACCESS_KEY: "minioadmin"
MINIO_SECRET_KEY: "minioadmin"
MINIO_BUCKET: "fms-data"
```

### 리소스 요구사항
- **실시간 수집기**: 256Mi RAM, 100m CPU
- **배치 병합기**: 512Mi~4Gi RAM (병합 단위에 따라)
- **과거 데이터 적재기**: 2Gi~8Gi RAM

## 🐛 트러블슈팅

### 일반적인 문제

1. **Kafka 연결 실패**
   ```bash
   kubectl logs deployment/realtime-collector
   # "Connection refused" 에러 확인
   ```

2. **MinIO 권한 문제**
   ```bash
   kubectl describe secret fms-secrets
   # 액세스 키/시크릿 키 확인
   ```

3. **Pod 재시작 반복**
   ```bash
   kubectl top pods
   # 메모리/CPU 사용량 확인
   ```

### 수동 작업

```bash
# 수동 시간별 병합 실행
kubectl create job --from=cronjob/batch-merger-hour manual-hour-merge

# 특정 시간대 로그 확인
kubectl logs job/batch-merger-hour-$(date +%Y%m%d%H%M)

# 데이터 검증
kubectl exec -it deployment/realtime-collector -- python -c "
from minio import Minio
client = Minio('minio-service:9000', access_key='minioadmin', secret_key='minioadmin')
objects = list(client.list_objects('fms-data', prefix='minute/'))
print(f'Found {len(objects)} minute files')
"
```

## 📈 성능 모니터링

### 시스템 메트릭 확인
```bash
# 실시간 수집기 성능
kubectl top pod -l app=realtime-collector

# 배치 작업 히스토리
kubectl get jobs --sort-by=.metadata.creationTimestamp

# 디스크 사용량 (MinIO)
kubectl exec -it minio-pod -- df -h
```

### 데이터 품질 확인
```bash
# 분 단위 파일 개수 확인
kubectl exec -it deployment/realtime-collector -- python -c "
from minio import Minio
client = Minio('minio-service:9000', access_key='minioadmin', secret_key='minioadmin')
for prefix in ['minute/', 'hour/', 'day/', 'week/']:
    count = len(list(client.list_objects('fms-data', prefix=prefix)))
    print(f'{prefix}: {count} files')
"
```

## 🔄 운영 가이드

### 정기 점검 사항
1. **매일**: 실시간 수집기 상태 확인
2. **매주**: 디스크 사용량 및 병합 작업 로그 확인
3. **매월**: 전체 시스템 성능 리뷰

### 백업 및 복구
```bash
# 설정 백업
kubectl get configmap fms-config -o yaml > backup/configmap.yaml
kubectl get secret fms-secrets -o yaml > backup/secrets.yaml

# 데이터 백업 (MinIO 버킷 동기화)
mc mirror minio1/fms-data minio2/fms-data-backup
```

### 확장성 고려사항
- **수평 확장**: 실시간 수집기 replicas 증가
- **수직 확장**: 배치 병합기 리소스 증가
- **샤딩**: Kafka 파티션 수 증가로 병렬 처리 향상

## 🔐 보안 체크리스트

- [ ] MinIO 자격 증명을 Kubernetes Secret으로 관리
- [ ] 네트워크 정책으로 Pod 간 통신 제한
- [ ] RBAC로 서비스 계정 권한 최소화
- [ ] 이미지 스캔으로 보안 취약점 점검
- [ ] 암호화된 통신 채널 사용 (TLS)
