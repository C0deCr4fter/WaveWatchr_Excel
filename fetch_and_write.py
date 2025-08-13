#!/usr/bin/env python3
"""
WaveWatchr_Excel • fetch_and_write.py

- Loads config from STATION_CONFIG_JSON (env) or station_config.json (file)
- Uses GOOGLE_SHEET_ID if set, else cfg["spreadsheet_id"]
- Supports "station_id" or "stations" in config
- Fetches latest from NDBC realtime2 <station>.txt and <station>.spec
- Writes base row to 'buoy_data' using short headers
- Appends matches to alert tabs
"""

from __future__ import annotations
import os, sys, json, math, typing as T
from datetime import datetime, timezone
import requests

from sheet_tools import get_sheets_service, ensure_tab, append_rows, write_status
from rules import longboard_ok, shortboard_ok, short_period_ok  # backward-compatible names

RAW_TAB = "buoy_data"
ALERT_TABS = {
    "Longboard": "Longboard Alert",
    "Shortboard": "Shortboard Alert",
    "Short Period": "Short Period Alert",
}

# Canonical header order used everywhere (short names)
FIELDS = [
    "timestamp_utc", "station_id",
    "wvht_ft",       # total wave height ft
    "dpd_s",         # dominant period s
    "apd_s",         # average period s
    "mwd_deg",       # mean wave direction deg true
    "swh_ft",        # swell height ft
    "swp_s",         # swell period s
    "swd_text",      # direction text (ENE, E, etc.)
]

NDBC_STD_URL  = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
NDBC_SPEC_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.spec"
FT_PER_M = 3.28084

# ---------- config ----------
def _json_config() -> dict:
    env_json = os.environ.get("STATION_CONFIG_JSON")
    if env_json:
        return json.loads(env_json)
    with open("station_config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def load_config() -> dict:
    cfg = _json_config()
    if "stations" in cfg and isinstance(cfg["stations"], list):
        stations = [str(s).strip() for s in cfg["stations"] if str(s).strip()]
    elif "station_id" in cfg:
        stations = [str(cfg["station_id"]).strip()]
    else:
        raise KeyError("Config must contain 'stations' (list) or 'station_id' (string).")

    spreadsheet_id = os.environ.get("GOOGLE_SHEET_ID") or cfg.get("spreadsheet_id", "").strip()
    if not spreadsheet_id:
        raise KeyError("Set GOOGLE_SHEET_ID or add 'spreadsheet_id' to the config.")

    return {"stations": stations, "spreadsheet_id": spreadsheet_id}

# ---------- utils ----------
def _safe_float(x) -> T.Optional[float]:
    try:
        if x in ("MM", "", None):
            return None
        return float(x)
    except Exception:
        return None

def _round1(x: T.Optional[float]) -> T.Optional[float]:
    return None if x is None else round(float(x), 1)

def _m_to_ft(x_m: T.Optional[float]) -> T.Optional[float]:
    return None if x_m is None else round(x_m * FT_PER_M, 2)

def _iso_utc(y,m,d,h,mi) -> str:
    return datetime((2000 + y) if y < 100 else y, m, d, h, mi, tzinfo=timezone.utc).isoformat().replace("+00:00","+00:00")

def _deg_to_cardinal(deg: T.Optional[float]) -> T.Optional[str]:
    if deg is None or (isinstance(deg, float) and math.isnan(deg)):  # type: ignore[name-defined]
        return None
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    ix = int((float(deg) % 360)/22.5 + 0.5) % 16
    return dirs[ix]

def _fetch_first_data_line(url: str) -> T.Optional[str]:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    lines = [ln for ln in r.text.splitlines() if ln and not ln.startswith("#")]
    return lines[0] if lines else None  # newest first

# ---------- data fetch ----------
def fetch_latest_obs(station: str) -> dict:
    """
    Build an obs dict that includes BOTH canonical keys used by rules.py AND
    short-header aliases used by the sheet.
    """
    std = _fetch_first_data_line(NDBC_STD_URL.format(station=station))
    spec = _fetch_first_data_line(NDBC_SPEC_URL.format(station=station))

    obs: dict = {"station_id": station}

    # ---- standard file (.txt)
    # YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD ...
    if std:
        p = std.strip().split()
        if len(p) >= 12:
            yy, mm, dd, hh, mi = map(int, p[0:5])
            wdir = _safe_float(p[5])
            wspd_ms = _safe_float(p[6])
            wvht_m  = _safe_float(p[8])
            dpd_s   = _safe_float(p[9])
            apd_s   = _safe_float(p[10])
            mwd_deg = _safe_float(p[11])

            obs["timestamp_utc"] = _iso_utc(yy, mm, dd, hh, mi)
            obs["wave_height_ft"]    = _round1(_m_to_ft(wvht_m))
            obs["dominant_period_s"] = _round1(dpd_s)
            obs["mean_wave_dir_deg"] = mwd_deg
            obs["wind_dir_deg"]      = wdir
            obs["wind_speed_kt"]     = round(float(wspd_ms) * 1.94384) if wspd_ms is not None else None
            obs["wind_direction"]    = _deg_to_cardinal(wdir)

            # short aliases
            obs["wvht_ft"] = obs["wave_height_ft"]
            obs["dpd_s"]   = obs["dominant_period_s"]
            obs["apd_s"]   = _round1(apd_s)
            obs["mwd_deg"] = mwd_deg

    # ---- spectral file (.spec)
    # YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD STEEPNESS APD MWD
    if spec:
        p = spec.strip().split()
        if len(p) >= 10:
            swh_m = _safe_float(p[6])   # SwH
            swp_s = _safe_float(p[7])   # SwP
            try:
                mwd_tail = _safe_float(p[-1])
            except Exception:
                mwd_tail = None

            obs["swell_height_ft"] = _round1(_m_to_ft(swh_m))
            obs["swell_period_s"]  = _round1(swp_s)
            if obs.get("mean_wave_dir_deg") is None and mwd_tail is not None:
                obs["mean_wave_dir_deg"] = mwd_tail

            # short aliases
            obs["swh_ft"] = obs["swell_height_ft"]
            obs["swp_s"]  = obs["swell_period_s"]
            if obs.get("mwd_deg") is None and mwd_tail is not None:
                obs["mwd_deg"] = mwd_tail

    # ---- final swd_text (prefer wave direction over wind) ----
    dir_for_card = (
        obs.get("mwd_deg") or
        obs.get("swell_dir_deg_true") or
        obs.get("wind_dir_deg")
    )
    obs["swd_text"] = _deg_to_cardinal(dir_for_card)

    return obs

# ---------- alerts ----------
def build_row(fields: T.List[str], obs: dict) -> T.List[T.Any]:
    return [obs.get(k) for k in fields]

def any_alerts(row: dict) -> T.Dict[str,bool]:
    return {
        "Longboard":   bool(longboard_ok(row)),
        "Shortboard":  bool(shortboard_ok(row)),
        "Short Period":bool(short_period_ok(row)),
    }

# ---------- main ----------
def main() -> int:
    cfg = load_config()
    stations        = cfg["stations"]
    spreadsheet_id  = cfg["spreadsheet_id"]

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "credentials/google-service-account.json"
    if not os.path.exists(sa_path):
        raise FileNotFoundError(f"Service account file not found at '{sa_path}'")
    service = get_sheets_service(sa_path)

    # Ensure tabs with standardized headers
    ensure_tab(service, spreadsheet_id, RAW_TAB, FIELDS)
    for tab in ALERT_TABS.values():
        ensure_tab(service, spreadsheet_id, tab, FIELDS)

    totals = {"Longboard":0,"Shortboard":0,"Short Period":0}
    wrote_any = False

    for st in stations:
        print(f"Fetching NDBC data for station {st} …", flush=True)
        obs = fetch_latest_obs(st)
        obs["station_id"] = st  # enforce
        row = build_row(FIELDS, obs)
        append_rows(service, spreadsheet_id, RAW_TAB, [row])
        wrote_any = True

        flags = any_alerts(obs)
        for name, hit in flags.items():
            if hit:
                append_rows(service, spreadsheet_id, ALERT_TABS[name], [row])
                totals[name] += 1

    # status lines if zero matches
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')
    for name, tab in ALERT_TABS.items():
        if totals[name] == 0:
            write_status(service, spreadsheet_id, tab, f"No matches this run at {ts}")

    if wrote_any and all(v==0 for v in totals.values()):
        print(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000')} – ran, no alert matches")
    else:
        print(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000')} – matches " +
              ", ".join(f"{k}:{v}" for k,v in totals.items()))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
