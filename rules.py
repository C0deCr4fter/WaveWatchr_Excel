# rules.py
# Centralized surf alert logic with backward-compatible function names.

# ===== Editable thresholds =====
DIR_MIN = 25            # deg true  NE
DIR_MAX = 160           # deg true  SE

LP_LONGBOARD_MIN_PERIOD_S = 13.0
LP_LONGBOARD_MIN_HEIGHT_FT = 0.7

LP_SHORTBOARD_MIN_PERIOD_S = 13.0
LP_SHORTBOARD_MIN_HEIGHT_FT = 1.6

SP_MIN_WVHT_FT = 3.0
# ===== End thresholds =====

def _get_num(row, *keys):
    """Return first available numeric value from row for any of the keys."""
    for k in keys:
        if k in row and row[k] not in (None, ""):
            try:
                return float(row[k])
            except Exception:
                pass
    return None

def _deg_in_window(deg, lo=DIR_MIN, hi=DIR_MAX):
    try:
        d = float(deg)
    except Exception:
        return False
    return lo <= d <= hi

def _swell_period_s(row):
    # Prefer real swell period, else dominant period.
    return _get_num(row, "swell_period_s", "SwP", "dominant_period_s", "DPD")

def _swell_height_ft(row):
    # Prefer swell height, else total wave height.
    return _get_num(row, "swell_height_ft", "SwH_ft", "SwH", "wave_height_ft", "WVHT_ft", "WVHT")

def _swell_dir_deg(row):
    # Prefer swell or mean wave direction; fallback to wind direction.
    return _get_num(
        row,
        "swell_dir_deg_true", "SwD_deg",
        "mean_wave_dir_deg", "MWD",
        "wind_dir_deg"
    )

def _mean_wave_dir_deg(row):
    return _get_num(row, "mean_wave_dir_deg", "MWD", "wind_dir_deg")

# ---- New canonical rule functions ----
def is_long_period_longboard(row):
    period_s = _swell_period_s(row)
    height_ft = _swell_height_ft(row)
    dir_deg = _swell_dir_deg(row)
    return (
        period_s is not None and period_s >= LP_LONGBOARD_MIN_PERIOD_S
        and height_ft is not None and height_ft >= LP_LONGBOARD_MIN_HEIGHT_FT
        and _deg_in_window(dir_deg)
    )

def is_long_period_shortboard(row):
    period_s = _swell_period_s(row)
    height_ft = _swell_height_ft(row)
    dir_deg = _swell_dir_deg(row)
    return (
        period_s is not None and period_s >= LP_SHORTBOARD_MIN_PERIOD_S
        and height_ft is not None and height_ft >= LP_SHORTBOARD_MIN_HEIGHT_FT
        and _deg_in_window(dir_deg)
    )

def is_short_period_all(row):
    wvht_ft = _get_num(row, "wave_height_ft", "WVHT_ft", "WVHT")
    dir_deg = _mean_wave_dir_deg(row)
    return (
        wvht_ft is not None and wvht_ft > SP_MIN_WVHT_FT
        and _deg_in_window(dir_deg)
    )

# ---- Backward-compatible aliases expected by fetch_and_write.py ----
def longboard_ok(row):
    return is_long_period_longboard(row)

def shortboard_ok(row):
    return is_long_period_shortboard(row)

def short_period_ok(row):
    return is_short_period_all(row)

# Optional dispatch map if needed elsewhere
RULE_FUNCS = {
    "Longboard": is_long_period_longboard,
    "Shortboard": is_long_period_shortboard,
    "Short Period": is_short_period_all,
}
