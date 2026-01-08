import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import json

def sync_data():
    try:
        # 1. 구글 인증 (GitHub Secrets에 넣은 JSON 키 사용)
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        service_account_info = json.loads(os.environ['GOOGLE_JSON_KEY'])
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 2. 구글 시트 데이터 읽기
        sheet_id = "1aYlXOaRbyRDn0gneVz8n7nATIqdTU7rD4rjMEbw38sw"
        sheet = client.open_by_key(sheet_id).worksheet("표준 입력 시트")
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # 3. Snowflake 연결
        conn = snowflake.connector.connect(
            user=os.environ['SF_USER'],
            password=os.environ['SF_PASSWORD'],
            account="gv28284.ap-northeast-2.aws",
            warehouse="DEV_WH",
            database="FNF",
            schema="CRM_MEMBER"
        )

        # 한글 컬럼명을 SQL 작업이 편하도록 영문으로 변경
        df.columns = ['BRAND', 'CHANNEL', 'PROMO_TYPE', 'PROMO_NAME', 'START_DATE', 'END_DATE', 
                      'STATUS', 'SLOT', 'IMAGE_URL', 'BENEFIT_TYPE', 'BENEFIT_DETAIL', 
                      'GOAL_SALES', 'ACTUAL_SALES', 'ACHIEVE_RATE', 'MD_COMMENT']

        # 4. Snowflake에 적재 (테이블 이름: PROMOTION_PLAN)
        # auto_create_table=True로 설정하면 테이블이 없을 때 자동으로 만들어줍니다.
        write_pandas(conn, df, "PROMOTION_PLAN", auto_create_table=True, overwrite=True)
        
        print("데이터 적재 성공!")
        conn.close()
        
    except Exception as e:
        print(f"에러 발생: {e}")
        raise e

if __name__ == "__main__":
    sync_data()
