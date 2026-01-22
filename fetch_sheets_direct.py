import gspread
from google.oauth2.service_account import Credentials
import os
import json

def fetch_extra_sheets():
    try:
        # 1. 구글 인증 (기존 main.py와 동일한 방식)
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        service_account_info = json.loads(os.environ['GOOGLE_JSON_KEY'])
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        client = gspread.authorize(creds)
        
        # 2. 스프레드시트 열기 (기존 ID 그대로 사용)
        sheet_id = "1aYlXOaRbyRDn0gneVz8n7nATIqdTU7rD4rjMEbw38sw"
        spreadsheet = client.open_by_key(sheet_id)
        
        # ---------------------------------------------------------
        # 3. 월별 목표 매출 가져오기
        # (탭 이름: "2. 월별 목표 매출 관리 시트")
        # ---------------------------------------------------------
        try:
            worksheet = spreadsheet.worksheet("2. 월별 목표 매출 관리 시트")
            data = worksheet.get_all_records()
            
            with open("monthly_goals.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print("SUCCESS: monthly_goals.json created.")
            
        except gspread.WorksheetNotFound:
            print("WARNING: '2. 월별 목표 매출 관리 시트' 탭을 찾을 수 없습니다.")

        # ---------------------------------------------------------
        # 4. 캘린더 이슈 가져오기
        # (탭 이름: "3. imc/공휴일 일정 관리 시트(자사몰)")
        # ---------------------------------------------------------
        try:
            worksheet = spreadsheet.worksheet("3. imc/공휴일 일정 관리 시트(자사몰)")
            data = worksheet.get_all_records()
            
            with open("calendar_issues.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print("SUCCESS: calendar_issues.json created.")

        except gspread.WorksheetNotFound:
            print("WARNING: '3. imc/공휴일 일정 관리 시트(자사몰)' 탭을 찾을 수 없습니다.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        raise e

if __name__ == "__main__":
    fetch_extra_sheets()
