# FMS Collector - Backfill System

이 프로젝트는 Kafka의 데이터를 주기적으로 수집하여 MinIO 스토리지에 Parquet 포맷으로 적재하는 백필(Backfill) 및 아카이빙 시스템입니다.

주로 두 가지 핵심 파이썬 애플리케이션인 `app.py` (Backfill Loader)와 `scheduler.py` (Scheduler)로 구성되어 있습니다.

## 주요 컴포넌트

### 1. Backfill Loader (`backfill-loader/app.py`)

Kafka 토픽(`fms-temphum`)에서 메시지를 읽어 MinIO 버킷에 저장하는 핵심 로직을 담당합니다.

#### 주요 기능
- **데이터 수집**: Kafka Consumer를 통해 메시지를 대량으로 소비합니다.
- **데이터 처리**:
  - 메시지 내부의 타임스탬프를 추출합니다.
  - UTC 시간을 KST(한국 표준시)로 변환합니다.
- **데이터 저장**: 처리된 데이터를 `Parquet` 포맷으로 변환하여 MinIO에 업로드합니다.
- **실행 모드 (`--mode`)**:
  - **`incremental` (증분 모드)**: 
    - **가장 중요한 모드**입니다.
    - Kafka Consumer Group의 마지막 커밋된 오프셋(Offset)부터 데이터를 읽기 시작합니다.
    - 이전에 작업이 끝난 지점부터 이어서 데이터를 수집하므로, 중복 수집을 방지하고 누락 없이 데이터를 적재할 수 있습니다.
    - 스케줄러에 의해 주기적으로 실행될 때 사용됩니다.
  - **`fresh`**: 처음부터 모든 데이터를 다시 수집합니다.
  - **`continue`**: 특정 날짜(`--from-date`)부터 수집하거나 이어서 수집합니다.

### 2. Scheduler (`scheduler/scheduler.py`)

Backfill Loader를 정해진 주기에 따라 자동으로 실행시켜주는 관리자 역할을 합니다.

#### 동작 방식
- **Python `schedule` 라이브러리**를 사용하여 주기적인 작업을 관리합니다.
- 기본 설정으로 **3일에 한 번씩** 백필 작업을 트리거합니다.
- 작업 실행 시 `subprocess`를 통해 Docker 컨테이너 내부에서 다음 명령어를 실행합니다:
  ```bash
  python /app/app.py --mode incremental
  ```
- **초기 실행 옵션**: 컨테이너 시작 시 환경변수 `RUN_ON_START=true`가 설정되어 있다면, 스케줄 대기 시간 없이 즉시 백필 작업을 1회 실행합니다.

## 전체 시스템 워크플로우

1. **스케줄러 시작**: `docker compose up backfill-scheduler` 명령어를 통해 `backfill-scheduler` 컨테이너가 실행되면 `scheduler.py`가 구동됩니다.
2. **주기적 실행**: 
   - 매 3일마다(또는 `RUN_ON_START` 설정 시 즉시) 스케줄러가 깨어납니다.
   - 스케줄러는 `app.py`를 `incremental` 모드로 실행합니다.
3. **데이터 적재**:
   - `app.py`는 지난번 실행 이후 Kafka에 쌓인 새로운 데이터들을 가져옵니다.
   - 데이터를 메모리에서 처리한 후 MinIO에 Parquet 파일로 저장합니다.
4. **종료**: 작업이 완료되면 `app.py` 프로세스는 종료되고, 스케줄러는 다음 주기가 될 때까지 대기합니다.

## 환경 설정

시스템 동작을 제어하기 위한 주요 환경 변수들입니다.

| 변수명 | 설명 | 기본값 |
|--------|------|--------|
| `KAFKA_BROKERS` | Kafka 브로커 주소 |
| `KAFKA_TOPIC` | 수집할 Kafka 토픽 | `fms-temphum` |
| `KAFKA_GROUP_ID` | Consumer 그룹 ID | `backfill-loader` |
| `MINIO_ENDPOINT` | MinIO 서버 주소 (URL 지원) |
| `MINIO_BUCKET` | 저장할 MinIO 버킷 이름 | `fms-temphum` |
| `RUN_ON_START` | 스케줄러 시작 시 즉시 실행 여부 | `true` |

---
*Generated based on analysis of `app.py` and `scheduler.py`.*
