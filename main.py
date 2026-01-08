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
        sheet = client.open_by_key(sheet_id).worksheet("표준 입력 시트")
        
        # [수정 포인트] get_all_values()로 전체를 가져온 뒤, 1행(설명)은 버리고 2행을 제목으로 잡습니다.
        all_values = sheet.get_all_values()
        
        # 2행(index 1)이 제목, 3행(index 2)부터가 데이터입니다.
        header = all_values[1]  # '브랜드', '채널 구분' 등
        data = all_values[2:]   # 실제 기획전 데이터들
        
        # 데이터가 아예 없는 경우를 대비해 필터링
        df = pd.DataFrame(data, columns=header)
        
        # 혹시 데이터가 없는 빈 행이 딸려올 수 있으므로 브랜드가 있는 행만 남김
        df = df[df['브랜드'] != ""].copy()

        # 3. Snowflake 연결
        conn = snowflake.connector.connect(
            user=os.environ['SF_USER'],
            password=os.environ['SF_PASSWORD'],
            account="gv28284.ap-northeast-2.aws",
            warehouse="DEV_WH",
            database="FNF",
            schema="CRM_MEMBER"
        )

        # 영문 컬럼명으로 변경 (기존 순서와 동일)
        df.columns = ['BRAND', 'CHANNEL', 'PROMO_TYPE', 'PROMO_NAME', 'START_DATE', 'END_DATE', 
                      'STATUS', 'SLOT', 'IMAGE_URL', 'BENEFIT_TYPE', 'BENEFIT_DETAIL', 
                      'GOAL_SALES', 'ACTUAL_SALES', 'ACHIEVE_RATE', 'MD_COMMENT']

        # 4. Snowflake 적재
        write_pandas(conn, df, "PROMOTION_PLAN", auto_create_table=True, overwrite=True)
        
        print(f"데이터 적재 성공! 총 {len(df)}개의 행이 업로드되었습니다.")
        conn.close()
        
    except Exception as e:
        print(f"에러 발생: {e}")
        raise e

if __name__ == "__main__":
    sync_data()
