# fetch_and_write.py
import math
from datetime import datetime, timezone
import json, os
from typing import Optional, Dict, Any, List
from decimal import Decimal, ROUND_HALF_UP

import requests
import gspread
from google.oauth2.service_account import Credentials

# ===== CONFIG =====
SHEET_ID = "1OZQM-_X_sVgGYtD3YFFJH5C3nYuCDb3BBDjCw6yu4HQ"
WORKSHEET_TITLE = "buoy_data"
SA_PATH = "credentials/google-service-account.json"
STATION_CONFIG_PATH = "station_config.json"   # e.g. {"stations": ["41117"]}
DEBUG = False

HEADERS = [
    "timestamp_utc", "station_id",
    "wvht_ft", "dpd_s", "apd_s", "mwd_deg",
    "swh_ft", "swp_s", "swd_text",
]

# ===== Helpers =====
def round1_nearest(x: float) -> float:
    return float(Decimal(x).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))

def m_to_ft(m: float) -> float:
    return m * 3.28084

def _token(line: str) -> List[str]:
    return [t.lstrip("#") for t in line.split()]

def _is_numeric_row(toks: List[str]) -> bool:
    if len(toks) < 5: return False
    try:
        [float(x) for x in toks[:5]]
        return True
    except ValueError:
        return False

def _latest_row(text: str) -> tuple[datetime, List[str], Dict[str,int]]:
    """
    Return (timestamp_utc, tokens, name_to_idx) for the newest data row in a realtime2 file.
    """
    lines = [ln for ln in (l.strip() for l in text.splitlines()) if ln]
    want = {"YY","MM","DD","hh","mm"}
    header = None
    data_lines: List[str] = []
    for i, raw in enumerate(lines):
        cols = _token(raw)
        if want.issubset(set(cols)):
            header = cols
            data_lines = lines[i+1:]
            break
    if not header:
        raise ValueError("Could not find header with YY MM DD hh mm")
    name_to_idx = {name: idx for idx, name in enumerate(header)}
    # Newest row is the first numeric line after header
    for raw in data_lines:
        toks = _token(raw)
        if _is_numeric_row(toks):
            y  = int(float(toks[name_to_idx["YY"]]))
            m  = int(float(toks[name_to_idx["MM"]]))
            d  = int(float(toks[name_to_idx["DD"]]))
            hh = int(float(toks[name_to_idx["hh"]]))
            mm = int(float(toks[name_to_idx["mm"]]))
            year = 2000 + y if y < 100 else y
            ts = datetime(year, m, d, hh, mm, tzinfo=timezone.utc)
            return ts, toks, name_to_idx
    raise ValueError("No numeric data rows found")

def _to_float(s: Optional[str]) -> Optional[float]:
    if s in (None, "MM", "NaN"): return None
    try: return float(s)
    except ValueError: return None

# ===== Sheets wiring =====
def connect_ws():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_TITLE, rows=5000, cols=20)
    if ws.row_values(1) != HEADERS:
        if ws.row_values(1): ws.delete_rows(1)
        ws.insert_row(HEADERS, 1)
    return ws

def load_stations() -> List[str]:
    if os.path.exists(STATION_CONFIG_PATH):
        with open(STATION_CONFIG_PATH) as f:
            cfg = json.load(f)
        if isinstance(cfg, dict) and "stations" in cfg: return [str(s) for s in cfg["stations"]]
        if isinstance(cfg, list): return [str(s) for s in cfg]
    return ["41117"]

# ===== Main fetch/parse (independent .txt and .spec) =====
def fetch_latest_txt(station_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt", timeout=20)
    r.raise_for_status()
    ts, toks, idx = _latest_row(r.text)
    wvht_m = _to_float(toks[idx["WVHT"]]) if "WVHT" in idx else None
    return {
        "timestamp_txt": ts,  # for internal tracking
        "wvht_ft": round1_nearest(m_to_ft(wvht_m)) if wvht_m is not None else None,
        "dpd_s": _to_float(toks[idx["DPD"]]) if "DPD" in idx else None,
        "apd_s": _to_float(toks[idx["APD"]]) if "APD" in idx else None,
        "mwd_deg": _to_float(toks[idx["MWD"]]) if "MWD" in idx else None,
    }

def fetch_latest_spec(station_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.spec", timeout=20)
    r.raise_for_status()
    ts, toks, idx = _latest_row(r.text)
    swh_m = _to_float(toks[idx["SwH"]]) if "SwH" in idx else None
    return {
        "timestamp_spec": ts,  # for internal tracking
        "swh_ft": round1_nearest(m_to_ft(swh_m)) if swh_m is not None else None,
        "swp_s": _to_float(toks[idx["SwP"]]) if "SwP" in idx else None,
        "swd_text": toks[idx["SwD"]] if "SwD" in idx else None,
    }

def write_row(ws, station_id: str, payload: Dict[str, Any]):
    ws.append_row([
        payload["timestamp_utc"],
        station_id,
        payload.get("wvht_ft"),
        payload.get("dpd_s"),
        payload.get("apd_s"),
        payload.get("mwd_deg"),
        payload.get("swh_ft"),
        payload.get("swp_s"),
        payload.get("swd_text"),
    ], value_input_option="USER_ENTERED")
    print("[write]", station_id, payload)

def main():
    ws = connect_ws()
    for sid in load_stations():
        try:
            txt = fetch_latest_txt(sid)
            spec = fetch_latest_spec(sid)
            # Per your header, use the TXT timestamp as the row timestamp.
            payload = {
                "timestamp_utc": txt["timestamp_txt"].isoformat(),
                "wvht_ft": txt["wvht_ft"],
                "dpd_s": txt["dpd_s"],
                "apd_s": txt["apd_s"],
                "mwd_deg": txt["mwd_deg"],
                "swh_ft": spec["swh_ft"],
                "swp_s": spec["swp_s"],
                "swd_text": spec["swd_text"],
            }
            write_row(ws, sid, payload)
            if DEBUG:
                print(f"TXT ts:  {txt['timestamp_txt'].isoformat()}")
                print(f"SPEC ts: {spec['timestamp_spec'].isoformat()}")
        except Exception as e:
            print(f"[error] {sid}: {e}")

if __name__ == "__main__":
    main()
