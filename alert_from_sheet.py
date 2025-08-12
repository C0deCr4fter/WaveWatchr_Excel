# alert_from_sheet.py
"""
Reads the most-recent row from your RAW data tab and appends to:
  - "Shortboard Alert"
  - "Longboard Alert"
if rules in rules.py pass.

Env you should set in your workflow (or locally):
  GOOGLE_SHEET_NAME   -> name of the Google Sheet
  DATA_TAB_NAME       -> tab with raw data (the one your fetch job writes)
"""

import os
from datetime import datetime, timezone
import gspread
from sheet_tools import open_sheet, get_or_create_ws
from rules import longboard_ok, shortboard_ok, short_period_ok

SHEET_NAME = os.environ["GOOGLE_SHEET_NAME"]
DATA_TAB   = os.environ.get("DATA_TAB_NAME", "Data")  # change if needed

LONG_TAB = "Longboard Alert"
SHORT_TAB = "Shortboard Alert"

# Columns we’ll log on alerts tabs
ALERT_HEADER = [
    "logged_at_utc", "source_time", "station", "SwP", "SwH", "SWD", "WVHT", "MWD", "which_rule", "details"
]

def latest_row_dict(ws) -> dict:
    """Assumes first row is header; returns dict for the last non-empty row."""
    data = ws.get_all_records()  # list[dict], already keyed by header names
    if not data:
        return {}
    return data[-1]

def as_row(now_utc: str, station: str, src_time: str, row: dict, which: str, details: str):
    def g(k): return row.get(k, "")
    return [
        now_utc, src_time, station, g("SwP"), g("SwH"), g("SWD"), g("WVHT"), g("MWD"), which, details
    ]

def main():
    sh = open_sheet(SHEET_NAME)
    raw = sh.worksheet(DATA_TAB)

    row = latest_row_dict(raw)
    if not row:
        print("No data in raw tab; nothing to evaluate.")
        return

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    station = row.get("station_id", row.get("station", ""))  # try both
    src_time = row.get("time", row.get("timestamp", ""))

    # Evaluate rules
    lb_ok, lb_why = longboard_ok(row)
    sb_ok, sb_why = shortboard_ok(row)
    sp_ok, sp_why = short_period_ok(row)  # we’ll tag and log this to both tabs

    # Prepare tabs
    long_ws  = get_or_create_ws(sh, LONG_TAB, ALERT_HEADER)
    short_ws = get_or_create_ws(sh, SHORT_TAB, ALERT_HEADER)

    # Append as needed
    if lb_ok:
        long_ws.append_row(as_row(now_utc, station, src_time, row, "Longboard", lb_why), value_input_option="RAW")
        print("Logged Longboard alert")

    if sb_ok:
        short_ws.append_row(as_row(now_utc, station, src_time, row, "Shortboard", sb_why), value_input_option="RAW")
        print("Logged Shortboard alert")

    # By default, also log short-period to both tabs (so both audiences see it).
    if sp_ok:
        long_ws.append_row(as_row(now_utc, station, src_time, row, "Short-Period", sp_why), value_input_option="RAW")
        short_ws.append_row(as_row(now_utc, station, src_time, row, "Short-Period", sp_why), value_input_option="RAW")
        print("Logged Short-Period alert to both tabs")

if __name__ == "__main__":
    main()
