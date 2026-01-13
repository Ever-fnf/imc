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
        
        # [수정 1] 타겟 시트를 'ever_테스트중'으로 변경
        sheet = client.open_by_key(sheet_id).worksheet("ever_테스트중")
        
        all_values = sheet.get_all_values()
        
        # 2행(index 1)이 제목, 3행(index 2)부터가 데이터
        header = all_values[1]
        data = all_values[2:]
        
        # 데이터프레임 생성
        df = pd.DataFrame(data, columns=header)
        
        # [수정 2] 빈 행 제거 (브랜드가 없는 행은 무시)
        df = df[df['브랜드'] != ""].copy()

        # [수정 3] ★핵심 안전장치★
        # 시트에 컬럼이 아무리 많아도, 앞에서부터 딱 12개(MD코멘트까지)만 잘라서 가져옵니다.
        # 이렇게 하면 나중에 뒤에 '비고'나 '낙서'가 생겨도 에러가 안 납니다.
        df = df.iloc[:, :12]

        # 3. Snowflake 연결
        conn = snowflake.connector.connect(
            user=os.environ['SF_USER'],
            password=os.environ['SF_PASSWORD'],
            account="gv28284.ap-northeast-2.aws",
            warehouse="DEV_WH",
            database="FNF",
            schema="CRM_MEMBER"
        )

        # [수정 4] 컬럼명 변경 (12개로 축소)
        # ever_테스트중 시트 순서: 브랜드, 채널, 유형, 명, 시작, 종료, 상태, 구좌, 이미지, 혜택유형, 혜택상세, 코멘트
        df.columns = [
            'BRAND', 'CHANNEL', 'PROMO_TYPE', 'PROMO_NAME', 
            'START_DATE', 'END_DATE', 'STATUS', 'SLOT', 
            'IMAGE_URL', 'BENEFIT_TYPE', 'BENEFIT_DETAIL', 'MD_COMMENT'
        ]

        # 4. Snowflake 적재
        # 테이블 구조가 바뀌므로 overwrite=True가 기존 테이블을 지우고, 12개 컬럼으로 새로 만듭니다.
        write_pandas(conn, df, "PROMOTION_PLAN", auto_create_table=True, overwrite=True)
        
        print(f"데이터 적재 성공! 총 {len(df)}개의 행이 업로드되었습니다.")
        conn.close()
        
    except Exception as e:
        print(f"에러 발생: {e}")
        raise e

if __name__ == "__main__":
    sync_data()
