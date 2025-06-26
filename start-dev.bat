@echo off
REM FMS 데이터 수집기 개발 환경 시작 스크립트 (Windows용)

echo === FMS 실시간 수집기 개발 환경 시작 ===

REM Docker Compose로 전체 환경 시작
echo 1. Kafka, MinIO, 실시간 수집기 시작...
docker-compose up -d

echo 2. 서비스 상태 확인...
timeout /t 10 /nobreak > nul

REM 서비스 상태 확인
echo   - Kafka 토픽 확인
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo   - MinIO 버킷 확인  
docker-compose exec minio-init mc ls myminio/

echo   - 실시간 수집기 로그 확인
docker-compose logs --tail=20 realtime-collector

echo.
echo === 개발 환경 준비 완료 ===
echo.
echo 다음 명령어들을 사용할 수 있습니다:
echo   # 테스트 데이터 생성
echo   python test-data-generator.py --mode continuous --interval 10
echo.
echo   # 과거 데이터 생성 ^(백필 테스트용^)
echo   python test-data-generator.py --mode historical --hours-back 48
echo.
echo   # 서비스 로그 확인
echo   docker-compose logs -f realtime-collector
echo.
echo   # MinIO 웹 콘솔 접속: http://localhost:9001
echo   # ^(계정: minioadmin / minioadmin^)
echo.
echo   # 환경 종료
echo   docker-compose down

pause
