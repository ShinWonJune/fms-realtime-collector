import pandas as pd
import sys
import os

def read_last_rows(file_path, num_rows=10):
    """Parquet 파일의 마지막 N개 행 읽기"""
    try:
        # 파일 존재 확인
        if not os.path.exists(file_path):
            print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
            return None
        
        print(f"📁 파일 읽는 중: {file_path}")
        
        # Parquet 파일 읽기
        df = pd.read_parquet(file_path)
        
        # 기본 정보 출력
        print(f"📊 총 행 수: {len(df):,}")
        print(f"📋 컬럼 수: {len(df.columns)}")
        print(f"🏷️  컬럼명: {list(df.columns)}")
        
        # 타임스탬프 범위 출력 (있는 경우)
        if 'timestamp' in df.columns:
            print(f"⏰ 시간 범위: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
        
        print(f"\n📋 마지막 {num_rows}개 행:")
        print("=" * 100)
        
        # 마지막 N개 행 출력 (보기 좋게)
        last_rows = df.tail(num_rows)
        
        # pandas 출력 옵션 설정
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        
        print(last_rows)
        
        return last_rows
        
    except Exception as e:
        print(f"❌ 파일 읽기 오류: {e}")
        return None

def main():
    """메인 함수"""
    print("🔍 Parquet 파일 리더")
    print("=" * 50)
    
    # 명령행 인수 확인
    if len(sys.argv) >= 2:
        file_path = sys.argv[1]
        num_rows = int(sys.argv[2]) if len(sys.argv) >= 3 else 10
    else:
        # 대화식 입력
        file_path = input("📂 Parquet 파일 경로를 입력하세요: ").strip()
        
        try:
            rows_input = input("📊 보여줄 행 수 (기본값: 10): ").strip()
            num_rows = int(rows_input) if rows_input else 10
        except ValueError:
            num_rows = 10
    
    # 파일 읽기
    result = read_last_rows(file_path, num_rows)
    
    if result is not None:
        print(f"\n✅ 성공적으로 {len(result)}개 행을 읽었습니다!")
    else:
        print("❌ 파일을 읽을 수 없습니다.")

if __name__ == "__main__":
    main()