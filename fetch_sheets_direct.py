import gspread
from google.oauth2.service_account import Credentials
import os
import json

def get_clean_data(worksheet):
    """
    헤더에 빈칸이나 중복이 있어도 에러 없이 데이터만 깔끔하게 가져오는 함수
    """
    all_values = worksheet.get_all_values()
    
    # 데이터가 없으면 빈 리스트 반환
    if not all_values:
        return []

    # 첫 번째 줄(헤더) 가져오기
    headers = all_values[0]
    
    # 헤더가 비어있지 않은 열의 인덱스만 찾기 (예: A열, B열, E열...)
    valid_indices = [i for i, h in enumerate(headers) if h.strip()]
    
    cleaned_data = []
    for row in all_values[1:]: # 2번째 줄부터 데이터
        item = {}
        for i in valid_indices:
            # 헤더 이름 가져오기
            header_name = headers[i]
            # 해당 열의 데이터 가져오기 (행 길이가 짧을 경우 안전하게 빈 문자열 처리)
            val = row[i] if i < len(row) else ""
            item[header_name] = val
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
            # [수정] 안전한 함수 사용
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
            # [수정] 안전한 함수 사용
            calendar_data = get_clean_data(ws_calendar)
            
            with open("calendar_issues.json", "w", encoding="utf-8") as f:
                json.dump(calendar_data, f, ensure_ascii=False, indent=4)
            print("SUCCESS: calendar_issues.json created.")

        except gspread.WorksheetNotFound:
            print("WARNING: '3. imc/공휴일 일정 관리 시트(자사몰)' 탭을 찾을 수 없습니다.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        # Github Actions에서 에러임을 알리기 위해 raise
        raise e

if __name__ == "__main__":
    fetch_extra_sheets()
