#!/usr/bin/env python3
"""
WaveWatchr_Excel • fetch_and_write.py

- Loads config from STATION_CONFIG_JSON (env) or station_config.json (file)
- Supports both "station_id" and "stations" schemas
- Uses GOOGLE_SHEET_ID if set, else cfg["spreadsheet_id"]
- Fetches latest observation from NDBC realtime feeds
- Writes a base row to the buoy_data tab
- Evaluates alert rules and appends matches to alert tabs
- Prints a simple summary ("ran, no alert matches" or counts)

Dependencies: google-api-python-client, google-auth, requests
Relies on local helper modules: sheet_tools.py, rules.py
"""

from __future__ import annotations

import os
import sys
import json
import math
import time
import typing as T
from datetime import datetime, timezone

import requests

from sheet_tools import (
    get_sheets_service,
    ensure_tab,
    append_rows,
    write_status,
)

# Backward-compatible rule imports (our rules.py exposes both names)
from rules import longboard_ok, shortboard_ok, short_period_ok

# --------- Constants ----------
RAW_TAB = "buoy_data"  # change if your base tab has a different name
ALERT_TABS = {
    "Longboard": "Longboard Alert",
    "Shortboard": "Shortboard Alert",
    "Short Period": "Short Period Alert",
}

# Default headers if config["fields"] is missing (we still honor cfg["fields"] if present)
DEFAULT_FIELDS = [
    "timestamp_utc",
    "station_id",
    "wave_height_ft",
    "dominant_period_s",
    "wind_dir_deg",
    "wind_speed_kt",
    "swell_height_ft",
    "swell_period_s",
    "wind_direction",
]

# NDBC endpoints
NDBC_STD_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
NDBC_SPEC_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station}.spec"

FT_PER_M = 3.28084


# --------- Config loading ----------

def load_config() -> dict:
    """
    Load JSON config from env var STATION_CONFIG_JSON or from station_config.json file.
    Valid keys:
      - station_id: "41117" (string)  OR  stations: ["41117", ...]
      - spreadsheet_id: "google_sheet_id"
      - fields: [list of column keys to write]
    """
    env_json = os.environ.get("STATION_CONFIG_JSON")
    if env_json:
        cfg = json.loads(env_json)
    else:
        with open("station_config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)

    # Normalize stations
    stations: T.List[str] = []
    if "stations" in cfg and isinstance(cfg["stations"], list):
        stations = [str(s).strip() for s in cfg["stations"] if str(s).strip()]
    elif "station_id" in cfg:
        s = str(cfg["station_id"]).strip()
        if s:
            stations = [s]
    else:
        raise KeyError("Config must contain 'stations' (list) or 'station_id' (string).")

    # Determine spreadsheet_id (prefer env override)
    spreadsheet_id = os.environ.get("GOOGLE_SHEET_ID") or cfg.get("spreadsheet_id", "").strip()
    if not spreadsheet_id:
        raise KeyError("No spreadsheet ID found. Set GOOGLE_SHEET_ID or add 'spreadsheet_id' to the config.")

    fields = cfg.get("fields") or DEFAULT_FIELDS
    if not isinstance(fields, list) or not fields:
        fields = DEFAULT_FIELDS

    return {
        "stations": stations,
        "spreadsheet_id": spreadsheet_id,
        "fields": fields,
    }


# --------- Helpers ----------

def to_iso_utc(year: int, mo: int, dy: int, hr: int, mn: int) -> str:
    dt = datetime(year, mo, dy, hr, mn, tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "+00:00")


def deg_to_cardinal(deg: T.Optional[float]) -> T.Optional[str]:
    if deg is None or math.isnan(deg):
        return None
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    ix = int((deg % 360) / 22.5 + 0.5) % 16
    return dirs[ix]


def safe_float(x: T.Any) -> T.Optional[float]:
    try:
        if x in ("MM", "", None):
            return None
        return float(x)
    except Exception:
        return None


def m_to_ft(x_m: T.Optional[float]) -> T.Optional[float]:
    if x_m is None:
        return None
    return round(x_m * FT_PER_M, 2)


def round_1(x: T.Optional[float]) -> T.Optional[float]:
    if x is None:
        return None
    return round(float(x), 1)


# --------- NDBC parsing ----------

def _parse_fixed_width_line(line: str) -> T.List[str]:
    """NDBC realtime2 .txt and .spec are space-separated with variable spacing; split on whitespace."""
    return line.strip().split()


def _fetch_last_data_line(url: str) -> T.Optional[str]:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    lines = [ln for ln in r.text.splitlines() if ln and not ln.startswith("#")]
    return lines[0] if lines else None  # Files are reverse-chron; newest is first


def fetch_latest_observation(station: str) -> dict:
    """
    Pull the newest row by combining realtime2 <station>.txt and <station>.spec.
    Returns a dict with keys we might need:
      timestamp_utc, station_id, wave_height_ft, dominant_period_s,
      wind_speed_kt, wind_dir_deg, wind_direction,
      swell_height_ft, swell_period_s, mean_wave_dir_deg
    """
    std_line = _fetch_last_data_line(NDBC_STD_URL.format(station=station))
    spec_line = _fetch_last_data_line(NDBC_SPEC_URL.format(station=station))

    result: dict = {
        "station_id": station,
    }

    # Parse standard file (..../<station>.txt)
    # Header (reference): YY  MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
    # Values are newest-first, space-separated.
    if std_line:
        parts = _parse_fixed_width_line(std_line)
        # Guard: we expect at least first 10-12 fields
        if len(parts) >= 11:
            yr, mo, dy, hh, mn = map(int, parts[0:5])
            wdir = safe_float(parts[5])   # degT
            wspd = safe_float(parts[6])   # m/s in some feeds; NDBC doc says m/s; BUT many users treat as knots.
            # NDBC realtime2 WSPD is meters/second for some buoys; NDBC site often provides knots in separate feeds.
            # Empirically many consumers expect knots; if value seems too small, multiply by 1.94384 to convert m/s→kt.
            # We do a light heuristic: if wspd is not None and wspd < 25 and any WVHT exists, we assume m/s.
            wvht_m = safe_float(parts[8])  # meters
            dpd = safe_float(parts[9])     # seconds
            mwd = safe_float(parts[11]) if len(parts) > 11 else None

            # timestamp
            result["timestamp_utc"] = to_iso_utc(2000 + yr if yr < 100 else yr, mo, dy, hh, mn)

            # wave height feet
            result["wave_height_ft"] = round_1(m_to_ft(wvht_m))

            # dominant period seconds
            result["dominant_period_s"] = round_1(dpd)

            # wind
            wind_dir_deg = wdir
            wind_speed_kt = None
            if wspd is not None:
                # Heuristic m/s → kt
                wind_speed_kt = round(float(wspd) * 1.94384)  # integer knots is fine for display

            result["wind_dir_deg"] = wind_dir_deg
            result["wind_speed_kt"] = wind_speed_kt
            result["wind_direction"] = deg_to_cardinal(wind_dir_deg)
            result["mean_wave_dir_deg"] = mwd

    # Parse spectral file (..../<station>.spec)
    # Header (reference):
    # YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD
    if spec_line:
        parts = _parse_fixed_width_line(spec_line)
        if len(parts) >= 10:
            # Not re-reading timestamp here; std timestamp is sufficient.
            wvht_m = safe_float(parts[5])  # meters
            swh_m = safe_float(parts[6])   # meters (swell height)
            swp_s = safe_float(parts[7])   # seconds (swell period)
            # SwD and WWD are compass strings in some files; MWD appears near end as degrees
            # Try to capture MWD at tail if present
            try:
                mwd = safe_float(parts[-1])
            except Exception:
                mwd = None

            # Merge/augment
            result["swell_height_ft"] = round_1(m_to_ft(swh_m))
            result["swell_period_s"] = round_1(swp_s)
            if "wave_height_ft" not in result or result["wave_height_ft"] is None:
                result["wave_height_ft"] = round_1(m_to_ft(wvht_m))
            if result.get("mean_wave_dir_deg") is None and mwd is not None:
                result["mean_wave_dir_deg"] = mwd

    return result


def build_row(fields: T.List[str], obs: dict) -> T.List[T.Any]:
    """Return a list of values in the same order as fields."""
    out: T.List[T.Any] = []
    for key in fields:
        out.append(obs.get(key))
    return out


def any_alerts_for_row(row_dict: dict) -> T.Dict[str, bool]:
    return {
        "Longboard": bool(longboard_ok(row_dict)),
        "Shortboard": bool(shortboard_ok(row_dict)),
        "Short Period": bool(short_period_ok(row_dict)),
    }


# --------- Main ----------

def main() -> int:
    cfg = load_config()
    stations: T.List[str] = cfg["stations"]
    spreadsheet_id: str = cfg["spreadsheet_id"]
    fields: T.List[str] = cfg["fields"]

    service = get_sheets_service()

    # Ensure base and alert tabs exist with headers
    RAW_HEADERS = fields[:]  # use config order for the base sheet
    ensure_tab(service, spreadsheet_id, RAW_TAB, RAW_HEADERS)
    for tab in ALERT_TABS.values():
        ensure_tab(service, spreadsheet_id, tab, RAW_HEADERS)

    total_matches = {"Longboard": 0, "Shortboard": 0, "Short Period": 0}
    wrote_any = False

    for station_id in stations:
        print(f"Fetching NDBC data for station {station_id} …", flush=True)
        obs = fetch_latest_observation(station_id)

        # Enforce station_id and round height if it exists
        obs["station_id"] = station_id
        if "wave_height_ft" in obs and obs["wave_height_ft"] is not None:
            obs["wave_height_ft"] = round_1(obs["wave_height_ft"])

        # Build row in the same order as headers
        row = build_row(fields, obs)
        append_rows(service, spreadsheet_id, RAW_TAB, [row])
        wrote_any = True

        # Check alerts
        flags = any_alerts_for_row(obs)
        for name, hit in flags.items():
            if hit:
                append_rows(service, spreadsheet_id, ALERT_TABS[name], [row])
                total_matches[name] += 1

    # Status line on each alert tab when zero matches
    for name, tab in ALERT_TABS.items():
        if total_matches[name] == 0:
            write_status(service, spreadsheet_id, tab, f"No matches this run at {datetime.utcnow().strftime('%Y-%m-%d %H:%MZ')}")

    # Console summary
    if wrote_any and all(v == 0 for v in total_matches.values()):
        print(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000')} – ran, no alert matches")
    else:
        parts = [f"{k}:{v}" for k, v in total_matches.items()]
        print(f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000')} – matches " + ", ".join(parts))

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
