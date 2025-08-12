# format_sheet.py
# Format the buoy_data tab: number formats, header styling, frozen row, widths.
# Uses your existing service account via GOOGLE_APPLICATION_CREDENTIALS.

import os
from typing import Dict, List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Change this if your tab is named differently
TAB_NAME = "buoy_data"  # or whatever your base/raw data tab is actually called

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_service():
    # Prefer GOOGLE_APPLICATION_CREDENTIALS. Fallback to service_account.json in repo.
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.path.join(os.getcwd(), "service_account.json")
    creds = Credentials.from_service_account_file(cred_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def get_sheet_id(service, spreadsheet_id: str, title: str) -> int:
    meta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title))"
    ).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == title:
            return int(props["sheetId"])
    raise RuntimeError(f"Tab not found: {title}")

def get_headers(service, spreadsheet_id: str, title: str) -> List[str]:
    res = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{title}!1:1"
    ).execute()
    return [h.strip() for h in res.get("values", [[]])[0]]

def build_format_requests(sheet_id: int, headers: List[str]) -> List[Dict]:
    requests: List[Dict] = []

    # 1) Freeze header row
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"
        }
    })

    # 2) Header style
    requests.append({
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat(textFormat,horizontalAlignment)"
        }
    })

    # 3) Column widths (optional sane defaults)
    for col_index in range(len(headers)):
        width = 210 if col_index == 0 else 110  # timestamp wider
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col_index, "endIndex": col_index + 1},
                "properties": {"pixelSize": width},
                "fields": "pixelSize"
            }
        })

    # 4) Number formats by header name pattern
    def fmt_number(col, pattern):
        return {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": col, "endColumnIndex": col + 1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": pattern}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        }

    def fmt_datetime(col, pattern="yyyy-mm-dd hh:mm:ss\"Z\""):
        return {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": col, "endColumnIndex": col + 1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": pattern}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        }

    # Map headers to formats
    for idx, name in enumerate(headers):
        lower = name.lower()

        if "time" in lower:                         # timestamp_utc, etc.
            requests.append(fmt_datetime(idx))
        elif lower.endswith("_ft"):                 # wave_height_ft, swell_height_ft
            requests.append(fmt_number(idx, "0.0"))
        elif lower.endswith("_s"):                  # dominant_period_s, swell_period_s
            requests.append(fmt_number(idx, "0.0"))
        elif lower.endswith("_deg"):                # wind_dir_deg, mean_wave_dir_deg
            requests.append(fmt_number(idx, "0"))
        elif lower.endswith("_kt"):                 # wind_speed_kt
            requests.append(fmt_number(idx, "0"))
        elif lower in ("station_id", "station"):
            requests.append(fmt_number(idx, "0"))
        else:
            # leave as text for things like wind_direction (ENE, E, etc.)
            pass

    return requests

def format_tab(spreadsheet_id: str, title: str):
    service = get_service()
    sheet_id = get_sheet_id(service, spreadsheet_id, title)
    headers = get_headers(service, spreadsheet_id, title)
    if not headers:
        raise RuntimeError(f"No headers found in first row of {title}")

    requests = build_format_requests(sheet_id, headers)
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
    print(f"Formatted tab '{title}' in spreadsheet {spreadsheet_id}")

if __name__ == "__main__":
    # Read ID and tab from env or fall back to station_config.json + default TAB_NAME.
    import json
    spreadsheet_id = os.environ.get("GOOGLE_SHEET_ID")
    title = os.environ.get("GOOGLE_SHEET_TAB") or TAB_NAME

    if not spreadsheet_id and os.path.exists("station_config.json"):
        with open("station_config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
            spreadsheet_id = cfg.get("spreadsheet_id", "")

    if not spreadsheet_id:
        raise SystemExit("Set GOOGLE_SHEET_ID env var or put 'spreadsheet_id' in station_config.json")

    format_tab(spreadsheet_id, title)
