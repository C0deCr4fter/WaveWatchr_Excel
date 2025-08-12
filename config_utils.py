# config_utils.py
import json
import os

def load_station_config():
    """
    Returns (stations_list, fields_list)
    Reads from env STATION_CONFIG_JSON if present; otherwise station_config.json.
    Accepts either:
      {"station_id": "41117", "fields":[...]}
    or {"stations": ["41117", ...], "fields":[...]}
    """
    raw = os.environ.get("STATION_CONFIG_JSON")
    if raw:
        cfg = json.loads(raw)
    else:
        with open("station_config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)

    # normalize
    stations = []
    if "stations" in cfg and isinstance(cfg["stations"], list):
        stations = [str(s).strip() for s in cfg["stations"] if str(s).strip()]
    elif "station_id" in cfg:
        s = str(cfg["station_id"]).strip()
        if s:
            stations = [s]
    else:
        raise KeyError("Config must contain 'stations' (list) or 'station_id' (string).")

    fields = cfg.get("fields", [])
    if not isinstance(fields, list) or not fields:
        raise KeyError("Config must contain non-empty 'fields' list.")

    return stations, fields
