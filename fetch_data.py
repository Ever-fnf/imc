import os
import json
import snowflake.connector
from datetime import datetime, timedelta, date
from decimal import Decimal

def get_connection():
    return snowflake.connector.connect(
        user=os.environ['SF_USER'],
        password=os.environ['SF_PASSWORD'],
        account="gv28284.ap-northeast-2.aws",
        warehouse="DEV_WH",
        database="FNF",
        schema="CRM_MEMBER"
    )

# [핵심] JSON 변환 시 에러 방지용 함수 (Decimal, Date 처리)
def default_converter(o):
    if isinstance(o, (date, datetime)):
        return o.strftime('%Y-%m-%d')
    if isinstance(o, Decimal):
        return int(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def fetch_and_process():
    print("🚀 Starting Data Sync Process...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # ---------------------------------------------------------
        # 1. 기획전 계획(Plan) 가져오기
        # ---------------------------------------------------------
        print("1. Fetching Promotion Plan...")
        cursor.execute("SELECT * FROM PROMOTION_PLAN")
        cols = [col[0] for col in cursor.description]
        plans = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # ---------------------------------------------------------
        # 2. 실적 데이터(Actual) 가져오기
        # ---------------------------------------------------------
        print("2. Fetching Daily Sales Data...")
        # 날짜를 문자열로 변환해서 가져옴
        cursor.execute("""
            SELECT TO_VARCHAR(SALE_DATE, 'YYYY-MM-DD') as SD, BRAND, CHANNEL, REVENUE 
            FROM DAILY_CHANNEL_SALES
        """)
        sales_data = cursor.fetchall()

        # ---------------------------------------------------------
        # 3. 매핑 테이블 생성
        # ---------------------------------------------------------
        print("3. Building Sales Map...")
        sales_map = {}
        for row in sales_data:
            date_str, brand, channel, revenue = row
            
            # [수정] revenue가 None이거나 Decimal일 경우 안전하게 int 변환
            if revenue is None:
                rev_int = 0
            else:
                rev_int = int(revenue)
                
            key = (brand, channel, date_str)
            sales_map[key] = sales_map.get(key, 0) + rev_int

        # ---------------------------------------------------------
        # 4. 실적 계산
        # ---------------------------------------------------------
        print("4. Calculating Promotion Performance...")
        final_data = []
        
        for p in plans:
            # [수정] Snowflake에서 가져온 데이터 타입 정리
            # GOAL_SALES가 Decimal일 수 있으므로 int로 변환
            if 'GOAL_SALES' in p and p['GOAL_SALES'] is not None:
                p['GOAL_SALES'] = int(p['GOAL_SALES'])
            else:
                p['GOAL_SALES'] = 0

            # 날짜 처리 (문자열 or Date객체 모두 대응)
            start_val = p.get('START_DATE')
            end_val = p.get('END_DATE')
            
            try:
                # 이미 date 객체라면 문자열로 변환하지 않고 바로 사용
                if isinstance(start_val, (date, datetime)):
                    s_date = start_val
                else:
                    s_date = datetime.strptime(str(start_val), '%Y-%m-%d').date()

                if isinstance(end_val, (date, datetime)):
                    e_date = end_val
                else:
                    e_date = datetime.strptime(str(end_val), '%Y-%m-%d').date()
                    
            except (ValueError, TypeError):
                print(f"   [Skip] Invalid Date: {p.get('PROMO_NAME', 'Unknown')}")
                p['ACTUAL_SALES'] = 0
                p['DAILY_TREND'] = []
                final_data.append(p)
                continue

            total_revenue = 0
            daily_trend = []
            
            # 기간 루프
            curr = s_date
            # date 객체끼리 비교
            while curr <= e_date:
                curr_str = curr.strftime('%Y-%m-%d')
                
                # 브랜드/채널 조회 (공백 제거 등 안전장치 추가 가능)
                p_brand = p.get('BRAND', '')
                p_channel = p.get('CHANNEL', '')
                
                rev = sales_map.get((p_brand, p_channel, curr_str), 0)
                
                total_revenue += rev
                daily_trend.append(rev)
                
                curr += timedelta(days=1)

            p['ACTUAL_SALES'] = total_revenue
            p['DAILY_TREND'] = daily_trend
            
            # JSON 저장을 위해 날짜를 문자열로 박제
            p['START_DATE'] = s_date.strftime('%Y-%m-%d')
            p['END_DATE'] = e_date.strftime('%Y-%m-%d')
            
            final_data.append(p)

        # ---------------------------------------------------------
        # 5. 결과 저장
        # ---------------------------------------------------------
        with open('data.json', 'w', encoding='utf-8') as f:
            # [핵심] default=default_converter 추가하여 Decimal/Date 에러 방지
            json.dump(final_data, f, ensure_ascii=False, indent=4, default=default_converter)
            
        print(f"✅ Success! Processed {len(final_data)} promotions.")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fetch_and_process()
