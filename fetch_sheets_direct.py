import gspread
from google.oauth2.service_account import Credentials
import os
import json

def get_clean_data(worksheet):
    """
    1행(설명)은 건너뛰고, 2행(헤더)을 기준으로 3행부터 데이터를 가져오는 함수
    """
    all_values = worksheet.get_all_values()
    
    # 데이터가 최소 2줄(설명+헤더)은 있어야 처리 가능
    if len(all_values) < 2:
        return []

    # [수정 포인트]
    # all_values[0] -> 1행 (설명/제목) : 무시
    # all_values[1] -> 2행 (실제 컬럼명/헤더) : 사용
    headers = all_values[1]
    
    # 헤더가 비어있지 않은 유효한 열(Column)의 인덱스만 찾기
    valid_indices = [i for i, h in enumerate(headers) if h.strip()]
    
    cleaned_data = []
    
    # [수정 포인트] 3행(인덱스 2)부터 실제 데이터로 간주
    for row in all_values[2:]: 
        item = {}
        # 유효한 컬럼만 골라서 데이터 매핑
        for i in valid_indices:
            header_name = headers[i]
            # 해당 열에 데이터가 없으면 빈 문자열 처리 (행 길이가 짧을 경우 대비)
            val = row[i] if i < len(row) else ""
            item[header_name] = val
            
        # 빈 행(모든 값이 비어있는 경우)은 제외
        if any(item.values()):
            cleaned_data.append(item)
        
    return cleaned_data

def fetch_extra_sheets():
    try:
        # 1. 구글 인증
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        service_account_info = json.loads(os.environ['GOOGLE_JSON_KEY'])
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 2. 스프레드시트 열기
        sheet_id = "1aYlXOaRbyRDn0gneVz8n7nATIqdTU7rD4rjMEbw38sw"
        spreadsheet = client.open_by_key(sheet_id)
        
        # ---------------------------------------------------------
        # 3. 월별 목표 매출 가져오기
        # ---------------------------------------------------------
        try:
            ws_goals = spreadsheet.worksheet("2. 월별 목표 매출 관리 시트")
            goals_data = get_clean_data(ws_goals)
            
            with open("monthly_goals.json", "w", encoding="utf-8") as f:
                json.dump(goals_data, f, ensure_ascii=False, indent=4)
            print("SUCCESS: monthly_goals.json created.")
            
        except gspread.WorksheetNotFound:
            print("WARNING: '2. 월별 목표 매출 관리 시트' 탭을 찾을 수 없습니다.")

        # ---------------------------------------------------------
        # 4. 캘린더 이슈 가져오기
        # ---------------------------------------------------------
        try:
            ws_calendar = spreadsheet.worksheet("3. imc/공휴일 일정 관리 시트(자사몰)")
            calendar_data = get_clean_data(ws_calendar)
            
            with open("calendar_issues.json", "w", encoding="utf-8") as f:
                json.dump(calendar_data, f, ensure_ascii=False, indent=4)
            print("SUCCESS: calendar_issues.json created.")

        except gspread.WorksheetNotFound:
            print("WARNING: '3. imc/공휴일 일정 관리 시트(자사몰)' 탭을 찾을 수 없습니다.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        raise e

if __name__ == "__main__":
    fetch_extra_sheets()
