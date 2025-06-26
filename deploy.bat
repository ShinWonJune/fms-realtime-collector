@echo off
REM FMS 실시간 수집기 빌드 및 배포 스크립트 (Windows용)

echo === FMS 실시간 수집기 빌드 및 배포 시작 ===

REM 변수 설정
if "%REGISTRY%"=="" set REGISTRY=your-registry.com
if "%VERSION%"=="" set VERSION=v1

REM 1. Docker 이미지 빌드
echo 1. Docker 이미지 빌드 중...

echo   - 실시간 수집기 이미지 빌드
docker build -t realtime-collector:%VERSION% ./realtime-collector/
docker tag realtime-collector:%VERSION% %REGISTRY%/realtime-collector:%VERSION%

echo   - 배치 병합기 이미지 빌드
docker build -t batch-merger:%VERSION% ./batch-merger/
docker tag batch-merger:%VERSION% %REGISTRY%/batch-merger:%VERSION%

echo   - 과거 데이터 적재기 이미지 빌드
docker build -t backfill-loader:%VERSION% ./backfill-loader/
docker tag backfill-loader:%VERSION% %REGISTRY%/backfill-loader:%VERSION%

REM 2. 이미지 푸시 (레지스트리가 설정된 경우)
if not "%REGISTRY%"=="your-registry.com" (
    echo 2. Docker 이미지 푸시 중...
    docker push %REGISTRY%/realtime-collector:%VERSION%
    docker push %REGISTRY%/batch-merger:%VERSION%
    docker push %REGISTRY%/backfill-loader:%VERSION%
) else (
    echo 2. 로컬 이미지 사용 ^(레지스트리 푸시 건너뜀^)
)

REM 3. Kubernetes 리소스 배포
echo 3. Kubernetes 리소스 배포 중...

echo   - ConfigMap 및 Secret 적용
kubectl apply -f k8s/configmap.yaml

echo   - 실시간 수집기 Deployment 배포
kubectl apply -f k8s/realtime-collector-deployment.yaml

echo   - 배치 병합기 CronJob 배포
kubectl apply -f k8s/batch-merger-cronjobs.yaml

REM 4. 배포 상태 확인
echo 4. 배포 상태 확인 중...
kubectl get pods -l app=realtime-collector
kubectl get cronjobs
kubectl get configmap fms-config
kubectl get secret fms-secrets

echo.
echo === 배포 완료 ===
echo.
echo 다음 명령어로 상태를 확인할 수 있습니다:
echo   kubectl logs -f deployment/realtime-collector
echo   kubectl get cronjobs
echo   kubectl describe cronjob batch-merger-hour
echo.
echo 과거 데이터 적재를 위해 다음 명령어를 실행하세요:
echo   kubectl apply -f k8s/backfill-loader-job.yaml
echo   kubectl logs -f job/backfill-loader

pause
