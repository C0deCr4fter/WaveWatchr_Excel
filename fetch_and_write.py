# fetch_and_write.py
# WaveWatchr_Excel – fetch buoy data, filter for alerts, write to Google Sheets
# Requires: google-api-python-client, google-auth, google-auth-httplib2, google-auth-oauthlib, requests, python-dateutil

import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ---- Config ----
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID")  # must be set in your repo secret or env
BUOY_STATION = os.environ.get("NDBC_STATION", "41117")
# NDBC JSON endpoint (10‑min realtime)
NDBC_URL = f"https://www.ndbc.noaa.gov/data/realtime2/{BUOY_STATION}.json"

# Sheets
TAB_BUOY = "buoy_data"
TAB_LONG = "Longboard Alert"
TAB_SHORT = "Shortboard Alert"
TAB_SP = "Short Period Alerts"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---- Helpers ----

def load_gcp_credentials() -> Credentials:
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "credentials/google-service-account.json")
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return creds

def get_sheets(creds: Credentials):
    return build("sheets", "v4", credentials=creds).spreadsheets()

def ensure_sheet(spreadsheets, title: str):
    meta = spreadsheets.get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    if title in sheets:
        return sheets[title]
    # add sheet
    body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    spreadsheets.batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
    # fetch id after creation
    meta = spreadsheets.get(spreadsheetId=SPREADSHEET_ID).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}[title]

def clear_sheet(spreadsheets, title: str):
    spreadsheets.values().clear(
        spreadsheetId=SPREADSHEET_ID, range=f"{title}!A:Z", body={}
    ).execute()

def append_rows(spreadsheets, title: str, rows: List[List]):
    spreadsheets.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{title}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

def write_header_if_empty(spreadsheets, title: str, header: List[str]):
    resp = spreadsheets.values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"{title}!A1:Z1"
    ).execute()
    values = resp.get("values", [])
    if not values:
        append_rows(spreadsheets, title, [header])

# ---- Data & Filters ----

def fetch_ndbc_json() -> List[Dict]:
    # NDBC sometimes serves JSON lines; handle array + lines.
    r = requests.get(NDBC_URL, timeout=20)
    r.raise_for_status()
    text = r.text.strip()
    if text.startswith("["):
        return r.json()
    # Try JSON per line
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            # skip non-JSON lines
            pass
    return out

def normalize_row(d: Dict) -> Dict:
    # Expected keys (best-effort; default None)
    return {
        "timestamp_utc": d.get("time", d.get("timestamp")),
        "station_id": d.get("station", BUOY_STATION),
        "wvht_ft": safe_float(d.get("WVHT")),         # significant wave height (ft)
        "dpd_s": safe_float(d.get("DPD")),            # dominant period (s)
        "apd_s": safe_float(d.get("APD")),            # average period (s)
        "mwd_deg": safe_float(d.get("MWD")),          # mean wave direction (deg true)
        "swh_ft": safe_float(d.get("SwH")),           # swell height (ft) – if available
        "swp_s": safe_float(d.get("SwP")),            # swell period (s)
        "swd_deg": safe_float(d.get("SwD")),          # swell direction (deg true)
    }

def safe_float(x):
    try:
        if x is None or x == "MM":
            return None
        return float(x)
    except Exception:
        return None

def dir_is_NE_to_SE(deg: float) -> bool:
    # 25° to 160° inclusive
    return deg is not None and 25.0 <= deg <= 160.0

def filter_longboard(rows: List[Dict]) -> List[Dict]:
    # Long Period Swell alert for Longboarders:
    # SwP > 13 AND SwH > 0.7 AND SwD between 25° and 160°
    out = []
    for r in rows:
        if (r["swp_s"] is not None and r["swp_s"] > 13.0 and
            r["swh_ft"] is not None and r["swh_ft"] > 0.7 and
            r["swd_deg"] is not None and dir_is_NE_to_SE(r["swd_deg"])):
            out.append(r)
    return out

def filter_shortboard(rows: List[Dict]) -> List[Dict]:
    # Long Period Swell alert for Shortboarders:
    # SwP > 13 AND SwH > 1.6 AND SwD between 25° and 160°
    out = []
    for r in rows:
        if (r["swp_s"] is not None and r["swp_s"] > 13.0 and
            r["swh_ft"] is not None and r["swh_ft"] > 1.6 and
            r["swd_deg"] is not None and dir_is_NE_to_SE(r["swd_deg"])):
            out.append(r)
    return out

def filter_short_period(rows: List[Dict]) -> List[Dict]:
    # Short Period alert for all:
    # WVHT > 3 AND MWD between 25° and 160°
    out = []
    for r in rows:
        if (r["wvht_ft"] is not None and r["wvht_ft"] > 3.0 and
            r["mwd_deg"] is not None and dir_is_NE_to_SE(r["mwd_deg"])):
            out.append(r)
    return out

def rows_to_values(rows: List[Dict]) -> List[List]:
    header = ["timestamp_utc","station_id","wvht_ft","dpd_s","apd_s",
              "mwd_deg","swh_ft","swp_s","swd_deg"]
    values = []
    for r in rows:
        values.append([
            r["timestamp_utc"], r["station_id"], r["wvht_ft"], r["dpd_s"],
            r["apd_s"], r["mwd_deg"], r["swh_ft"], r["swp_s"], r["swd_deg"]
        ])
    return header, values

# ---- Main ----

def main():
    print(f"Fetching NDBC data for station {BUOY_STATION} …")
    raw = fetch_ndbc_json()
    if not raw:
        raise RuntimeError("No data from NDBC.")
    rows = [normalize_row(d) for d in raw]

    # Filter
    long_rows = filter_longboard(rows)
    short_rows = filter_shortboard(rows)
    sp_rows = filter_short_period(rows)

    print(f"Matches — Longboard: {len(long_rows)} | Shortboard: {len(short_rows)} | Short Period: {len(sp_rows)}")

    # Sheets client
    creds = load_gcp_credentials()
    spreadsheets = get_sheets(creds)

    # Always ensure tabs exist + header on first write
    for tab in (TAB_LONG, TAB_SHORT, TAB_SP):
        ensure_sheet(spreadsheets, tab)

    # Write each tab
    write_alert_block(spreadsheets, TAB_LONG, "Longboard", long_rows)
    write_alert_block(spreadsheets, TAB_SHORT, "Shortboard", short_rows)
    write_alert_block(spreadsheets, TAB_SP, "Short Period", sp_rows)

    print("Done.")

def write_alert_block(spreadsheets, tab_title: str, label: str, rows: List[Dict]):
    header, values = rows_to_values(rows)
    write_header_if_empty(spreadsheets, tab_title, header)

    if values:
        append_rows(spreadsheets, tab_title, values)
        print(f"Wrote {len(values)} {label} alert row(s) to '{tab_title}'.")
    else:
        # Ensure the tab shows activity even with zero matches
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
        status_row = [f"No {label} alerts at {ts}"] + [""] * (len(header) - 1)
        append_rows(spreadsheets, tab_title, [status_row])
        print(f"No {label} alerts — appended status row to '{tab_title}'.")

if __name__ == "__main__":
    main()
