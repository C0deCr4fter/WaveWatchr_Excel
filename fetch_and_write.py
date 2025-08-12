from __future__ import annotations

import csv
import io
import os
import sys
import time
import json
import math
import requests
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from sheet_tools import get_sheets_service, ensure_tab, append_rows, write_status
from rules import longboard_ok, shortboard_ok, short_period_ok


# ---------- Configuration ----------
STATION_CONFIG_PATH = "station_config.json"  # contains station + sheet info
SERVICE_ACCOUNT_PATH = "credentials/google-service-account.json"

# Tabs
RAW_TAB = "buoy_data"
LONGBOARD_TAB = "Longboard Alert"
SHORTBOARD_TAB = "Shortboard Alert"
SHORTPER_TAB = "Short Period Alerts"

# Headers for tabs
RAW_HEADERS = [
    "timestamp_utc", "station_id",
    "wvht_ft", "dpd_s", "apd_s", "mwd_deg", "swh_ft", "swp_s", "swd_text"
]
ALERT_HEADERS = [
    "timestamp_utc", "station_id",
    "wvht_ft", "dpd_s", "apd_s", "mwd_deg", "swd_text"
]


# ---------- Helpers ----------
def compass_text(deg: float) -> str:
    """Convert degrees to 16‑point compass text."""
    if deg is None or math.isnan(deg):
        return ""
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    idx = int((deg/22.5)+0.5) % 16
    return dirs[idx]


def fetch_ndbc_txt(station: str) -> str:
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def parse_ndbc_latest_row(txt: str) -> Tuple[Dict, List[str]]:
    """
    Parse the realtime2 TXT and return a dict for the most recent row
    plus the ordered header names we mapped.

    The file is space-separated with header lines starting '#'.
    """
    # Strip comment lines, keep header line starting with '#YY'
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    header_line = None
    data_lines: List[str] = []
    for ln in lines:
        if ln.startswith("#YY"):
            header_line = ln.lstrip("#").strip()
        elif not ln.startswith("#"):
            data_lines.append(ln)

    if not header_line or not data_lines:
        raise ValueError("Could not find header or data in NDBC feed.")

    # Normalize spaces → CSV for robust parsing
    header_csv = ",".join(header_line.split())
    latest_csv = ",".join(data_lines[0].split())  # first data line is most recent

    header = header_csv.split(",")
    values = latest_csv.split(",")

    # Build mapping
    rec = dict(zip(header, values))

    # Build timestamp in UTC
    # Columns typically: YY MM DD hh mm ...
    try:
        YY = int(rec.get("YY"))
        MM = int(rec.get("MM"))
        DD = int(rec.get("DD"))
        hh = int(rec.get("hh"))
        mm = int(rec.get("mm"))
        # NDBC YY is 4-digit year already on realtime2
        dt = datetime(YY, MM, DD, hh, mm, tzinfo=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)

    # Pull numeric fields (some may be MM for missing)
    def f(num_str: str) -> float:
        try:
            x = float(num_str)
            if math.isfinite(x):
                return x
            return float("nan")
        except Exception:
            return float("nan")

    WVHT_m = f(rec.get("WVHT", "nan"))  # significant wave height (meters)
    DPD = f(rec.get("DPD", "nan"))      # dominant period (s)
    APD = f(rec.get("APD", "nan"))      # average period (s)
    MWD = f(rec.get("MWD", "nan"))      # mean wave direction (deg)

    WVHT_ft = WVHT_m * 3.28084 if math.isfinite(WVHT_m) else float("nan")
    SWH_ft = WVHT_ft  # keep column naming consistent with earlier sheet
    SWD_text = compass_text(MWD)

    row_dict = {
        "timestamp_utc": dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "WVHT_ft": f"{WVHT_ft:.2f}" if math.isfinite(WVHT_ft) else "",
        "DPD": f"{DPD:.1f}" if math.isfinite(DPD) else "",
        "APD": f"{APD:.1f}" if math.isfinite(APD) else "",
        "MWD": f"{MWD:.0f}" if math.isfinite(MWD) else "",
        "SWH_ft": f"{SWH_ft:.2f}" if math.isfinite(SWH_ft) else "",
        "SWP_s": f"{DPD:.1f}" if math.isfinite(DPD) else "",  # keep legacy column name
        "SWD_text": SWD_text,
    }

    return row_dict, header


def load_config(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


def to_raw_row(station_id: str, parsed: Dict) -> List:
    return [
        parsed["timestamp_utc"],
        station_id,
        parsed["WVHT_ft"] or "",
        parsed["DPD"] or "",
        parsed["APD"] or "",
        parsed["MWD"] or "",
        parsed["SWH_ft"] or "",
        parsed["SWP_s"] or "",
        parsed["SWD_text"] or "",
    ]


def to_alert_row(station_id: str, parsed: Dict) -> List:
    return [
        parsed["timestamp_utc"],
        station_id,
        parsed["WVHT_ft"] or "",
        parsed["DPD"] or "",
        parsed["APD"] or "",
        parsed["MWD"] or "",
        parsed["SWD_text"] or "",
    ]


def main():
    cfg = load_config(STATION_CONFIG_PATH)
    station_id = str(cfg["station_id"]).strip()
    spreadsheet_id = cfg["spreadsheet_id"].strip()

    print(f"Fetching NDBC data for station {station_id} …")
    txt = fetch_ndbc_txt(station_id)
    parsed, _header = parse_ndbc_latest_row(txt)

    # Build Google Sheets service
    service = get_sheets_service(SERVICE_ACCOUNT_PATH)

    # Ensure tabs + headers
    ensure_tab(service, spreadsheet_id, RAW_TAB, RAW_HEADERS)
    ensure_tab(service, spreadsheet_id, LONGBOARD_TAB, ALERT_HEADERS)
    ensure_tab(service, spreadsheet_id, SHORTBOARD_TAB, ALERT_HEADERS)
    ensure_tab(service, spreadsheet_id, SHORTPER_TAB, ALERT_HEADERS)

    # Raw write
    raw_row = to_raw_row(station_id, parsed)
    append_rows(service, spreadsheet_id, RAW_TAB, [raw_row])

    # Evaluate rules
    # Use numeric forms for rules
    numeric_row = {
        "DPD": float(parsed["DPD"]) if parsed["DPD"] else float("nan"),
        "APD": float(parsed["APD"]) if parsed["APD"] else float("nan"),
        "WVHT_ft": float(parsed["WVHT_ft"]) if parsed["WVHT_ft"] else float("nan"),
        "MWD": float(parsed["MWD"]) if parsed["MWD"] else float("nan"),
    }

    matches = 0
    status_msgs = []

    if longboard_ok(numeric_row):
        append_rows(service, spreadsheet_id, LONGBOARD_TAB, [to_alert_row(station_id, parsed)])
        matches += 1
        status_msgs.append("Longboard ✓")

    if shortboard_ok(numeric_row):
        append_rows(service, spreadsheet_id, SHORTBOARD_TAB, [to_alert_row(station_id, parsed)])
        matches += 1
        status_msgs.append("Shortboard ✓")

    if short_period_ok(numeric_row):
        append_rows(service, spreadsheet_id, SHORTPER_TAB, [to_alert_row(station_id, parsed)])
        matches += 1
        status_msgs.append("Short Period ✓")

    # Always drop a status crumb on each alert tab so we can tell it ran
    stamp = parsed["timestamp_utc"]
    if matches == 0:
        msg = f"{stamp} – ran, no alert matches"
    else:
        msg = f"{stamp} – wrote {matches} alert row(s): {', '.join(status_msgs)}"

    write_status(service, spreadsheet_id, LONGBOARD_TAB, msg)
    write_status(service, spreadsheet_id, SHORTBOARD_TAB, msg)
    write_status(service, spreadsheet_id, SHORTPER_TAB, msg)

    print(msg)


if __name__ == "__main__":
    sys.exit(main())
