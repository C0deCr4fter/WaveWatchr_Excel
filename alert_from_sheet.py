#!/usr/bin/env python3
"""
Reads the 'buoy_data' sheet, applies alert rules, and writes results to:
  - Longboard Alert
  - Shortboard Alert
  - Short Period Alerts

Requirements (already in requirements.txt below):
  gspread, google-auth, pandas

Credentials:
  Expects service account JSON at ./credentials/google-service-account.json
  and a Spreadsheet ID passed via env var GOOGLE_SHEET_ID
"""

import os
import sys
import time
import json
from typing import List, Tuple

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd


# ---------- Configuration ----------
BASE_TAB = "buoy_data"
TAB_LONGBOARD = "Longboard Alert"
TAB_SHORTBOARD = "Shortboard Alert"
TAB_SHORTPERIOD = "Short Period Alerts"

# NE .. SE range in TRUE degrees
DIR_MIN = 25
DIR_MAX = 160

# Rule thresholds (tune here as needed)
LONGBOARD_MIN_SWP = 13.0   # seconds
LONGBOARD_MIN_SWH = 0.7    # feet

SHORTBOARD_MIN_SWP = 13.0  # seconds
SHORTBOARD_MIN_SWH = 1.6   # feet

SHORTPERIOD_MIN_WVHT = 3.0 # feet

# Columns we expect in buoy_data
EXPECTED_COLS = [
    "timestamp_utc",
    "station_id",
    "wvht_ft",
    "dpd_s",
    "apd_s",
    "mwd_deg",
    "swh_ft",
    "swp_s",
    "swd_text"
]
# -----------------------------------


def _open_sheet(spreadsheet_id: str):
    sa_path = os.path.join("credentials", "google-service-account.json")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(spreadsheet_id)


def _ensure_worksheet(sh, title: str, header: List[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=100, cols=max(10, len(header)))
        ws.append_row(header)
        return ws

    # Make sure header row matches what we’ll write
    existing = ws.row_values(1)
    if existing != header:
        ws.clear()
        ws.append_row(header)
    return ws


def _read_buoy_df(sh) -> pd.DataFrame:
    ws = sh.worksheet(BASE_TAB)
    rows = ws.get_all_records()  # treats first row as header
    if not rows:
        return pd.DataFrame(columns=EXPECTED_COLS)
    df = pd.DataFrame(rows)

    # Normalize columns we care about (coerce types, fill missing)
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = None

    # numeric casts
    for ncol in ["wvht_ft", "dpd_s", "apd_s", "mwd_deg", "swh_ft", "swp_s"]:
        df[ncol] = pd.to_numeric(df[ncol], errors="coerce")

    # timestamp stays as string; direction text stays as string
    return df[EXPECTED_COLS].copy()


def _between_dir(deg: float, lo: float, hi: float) -> bool:
    if pd.isna(deg):
        return False
    return (deg >= lo) and (deg <= hi)


def _filter_longboard(df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (df["swp_s"] >= LONGBOARD_MIN_SWP) &
        (df["swh_ft"] >= LONGBOARD_MIN_SWH) &
        df["mwd_deg"].apply(lambda d: _between_dir(d, DIR_MIN, DIR_MAX))
    )
    return df.loc[mask].copy()


def _filter_shortboard(df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (df["swp_s"] >= SHORTBOARD_MIN_SWP) &
        (df["swh_ft"] >= SHORTBOARD_MIN_SWH) &
        df["mwd_deg"].apply(lambda d: _between_dir(d, DIR_MIN, DIR_MAX))
    )
    return df.loc[mask].copy()


def _filter_shortperiod(df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (df["wvht_ft"] >= SHORTPERIOD_MIN_WVHT) &
        df["mwd_deg"].apply(lambda d: _between_dir(d, DIR_MIN, DIR_MAX))
    )
    return df.loc[mask].copy()


def _write_frame(ws, df: pd.DataFrame):
    if df.empty:
        # Show a one-line status so the tab is visibly updated
        ws.clear()
        ws.update("A1", [["status", "message"],
                         [time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                          "No rows matched this alert window"]])
        return

    # Prepare values: header + rows
    header = list(df.columns)
    values = [header] + df.astype(object).fillna("").values.tolist()
    ws.clear()
    ws.update("A1", values)


def main():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        print("ERROR: GOOGLE_SHEET_ID env var not set.", file=sys.stderr)
        sys.exit(2)

    sh = _open_sheet(sheet_id)

    # Ensure worksheets exist with correct headers
    long_ws = _ensure_worksheet(sh, TAB_LONGBOARD, EXPECTED_COLS)
    short_ws = _ensure_worksheet(sh, TAB_SHORTBOARD, EXPECTED_COLS)
    sp_ws    = _ensure_worksheet(sh, TAB_SHORTPERIOD, EXPECTED_COLS)

    # Read buoy_data into DataFrame
    df = _read_buoy_df(sh)

    # Apply alert filters
    long_df = _filter_longboard(df)
    short_df = _filter_shortboard(df)
    sp_df = _filter_shortperiod(df)

    # Write each tab
    _write_frame(long_ws, long_df)
    _write_frame(short_ws, short_df)
    _write_frame(sp_ws, sp_df)

    print(f"Done. Longboard={len(long_df)} Shortboard={len(short_df)} ShortPeriod={len(sp_df)}")


if __name__ == "__main__":
    main()
