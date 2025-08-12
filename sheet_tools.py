# sheet_tools.py
import os
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SA_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials/google-service-account.json")

def open_sheet(sheet_name: str):
    creds = Credentials.from_service_account_file(SA_PATH, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open(sheet_name)

def get_or_create_ws(sh, title: str, header: list):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=500, cols=max(10, len(header)))
        ws.append_row(header, value_input_option="RAW")
    # Add header if the sheet exists but is empty
    if ws.row_count == 0 or (ws.row_count == 1 and not any(ws.row_values(1))):
        ws.resize(rows=1)
        ws.append_row(header, value_input_option="RAW")
    return ws
