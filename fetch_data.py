import snowflake.connector
import json
import os

def fetch_to_json():
    # 1. Snowflake 연결 (기존 환경변수 활용)
    conn = snowflake.connector.connect(
        user=os.environ['SF_USER'],
        password=os.environ['SF_PASSWORD'],
        account="gv28284.ap-northeast-2.aws",
        warehouse="DEV_WH",
        database="FNF",
        schema="CRM_MEMBER"
    )
    
    cursor = conn.cursor()
    try:
        # 2. 데이터 불러오기
        cursor.execute("SELECT * FROM PROMOTION_PLAN")
        
        # 컬럼명 가져오기
        columns = [col[0] for col in cursor.description]
        
        # 데이터를 딕셔너리 리스트로 변환 (HTML에서 쓰기 편하게)
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
            
        # 3. data.json 파일로 저장
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
            
        print("data.json 생성 완료!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fetch_to_json()
