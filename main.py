import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import json

def sync_data():
    try:
        # 1. 구글 인증
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        service_account_info = json.loads(os.environ['GOOGLE_JSON_KEY'])
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 2. 구글 시트 데이터 읽기
        sheet_id = "1aYlXOaRbyRDn0gneVz8n7nATIqdTU7rD4rjMEbw38sw"
        
        # [수정 1] 시트 이름을 'ever_테스트중2'로 변경
        sheet = client.open_by_key(sheet_id).worksheet("ever_테스트중2")
        
        all_values = sheet.get_all_values()
        
        # 데이터가 최소한 헤더까지는 있는지 확인
        if len(all_values) < 2:
            print("데이터가 없습니다.")
            return

        # [수정 2] 행 선택 로직 변경
        # 1행(index 0): 설명 -> 무시
        # 2행(index 1): 컬럼명(Header) -> 사용
        header = all_values[1]
        
        # 3~6행(index 2~5): 예시 데이터 -> 무시
        # 7행(index 6)부터: 실제 데이터 -> 사용
        data = all_values[6:]
        
        # 데이터프레임 생성
        df = pd.DataFrame(data, columns=header)
        
        # 데이터 정제: 브랜드가 없는 빈 행 제거
        # (만약 '브랜드'라는 컬럼명을 못 찾으면 첫 번째 컬럼 기준으로 처리)
        if '브랜드' in df.columns:
            df = df[df['브랜드'] != ""].copy()
        elif len(df.columns) > 0:
            df = df[df.iloc[:, 0] != ""].copy()

        # [기존 유지] 앞에서부터 15개 컬럼만 가져오기 (관리용 컬럼 제외)
        df = df.iloc[:, :15]

        # [기존 유지] Snowflake 컬럼명 매핑
        df.columns = [
            'BRAND',            # 1. 브랜드
            'CHANNEL',          # 2. 채널 구분
            'DIVISION',         # 3. 구분
            'PROMO_TYPE',       # 4. 유형
            'IS_EXCLUSIVE',     # 5. 단독 표시 여부
            'PROMO_NAME',       # 6. 기획전명
            'START_DATE',       # 7. 시작일
            'END_DATE',         # 8. 종료일
            'STATUS',           # 9. 진행 상태
            'SLOT',             # 10. 노출 구좌
            'IMAGE_URL',        # 11. 보고 장표 이미지
            'BENEFIT_TYPE',     # 12. 혜택 유형
            'BENEFIT_DETAIL',   # 13. 혜택 스킴
            'GOAL_SALES',       # 14. 목표 매출
            'MD_COMMENT'        # 15. MD 코멘트
        ]

        # ---------------------------------------------------------
        # 데이터 전처리 (Data Cleaning)
        # ---------------------------------------------------------
        
        # 1. 목표 매출: 콤마(,) 제거 및 숫자로 변환 (빈 값은 0)
        df['GOAL_SALES'] = (
            df['GOAL_SALES']
            .astype(str)
            .str.replace(',', '')
            .replace('', '0')
        )
        df['GOAL_SALES'] = pd.to_numeric(df['GOAL_SALES'], errors='coerce').fillna(0).astype(int)

        # 2. 단독 표시 여부: Boolean 변환
        df['IS_EXCLUSIVE'] = df['IS_EXCLUSIVE'].astype(str).str.upper() == 'TRUE'

        # ---------------------------------------------------------
        # 3. Snowflake 적재
        # ---------------------------------------------------------
        conn = snowflake.connector.connect(
            user=os.environ['SF_USER'],
            password=os.environ['SF_PASSWORD'],
            account="gv28284.ap-northeast-2.aws",
            warehouse="DEV_WH",
            database="FNF",
            schema="CRM_MEMBER"
        )

        # overwrite=True: 기존 테이블을 지우고 새로 생성 (구조 변경 반영)
        write_pandas(conn, df, "PROMOTION_PLAN", auto_create_table=True, overwrite=True)
        
        print(f"✅ 데이터 적재 성공! 총 {len(df)}개의 행이 업데이트되었습니다.")
        conn.close()
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        raise e

if __name__ == "__main__":
    sync_data()
