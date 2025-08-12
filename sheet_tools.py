from __future__ import annotations

from typing import Dict, List, Optional
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_sheets_service(sa_path: str):
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _get_sheet_id_map(service, spreadsheet_id: str) -> Dict[str, int]:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}


def ensure_tab(service, spreadsheet_id: str, title: str, header: Optional[List[str]] = None):
    """Create a sheet/tab if it doesn't exist. Optionally write headers if tab is empty."""
    id_map = _get_sheet_id_map(service, spreadsheet_id)
    if title not in id_map:
        requests = [{
            "addSheet": {"properties": {"title": title}}
        }]
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()

    # Check if header is needed
    if header:
        rng = f"'{title}'!1:1"
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=rng, majorDimension="ROWS"
        ).execute()
        current = resp.get("values", [])
        if not current:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=rng,
                valueInputOption="RAW",
                body={"values": [header]},
            ).execute()


def append_rows(service, spreadsheet_id: str, title: str, rows: List[List]):
    if not rows:
        return
    rng = f"'{title}'!A1"
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()


def write_status(service, spreadsheet_id: str, title: str, message: str):
    rng = f"'{title}'!A1"
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[message]]},
    ).execute()
