# rules.py — Matt’s alert logic (with short/long header aliases)

# ---- editable thresholds ----
DIR_MIN = 25
DIR_MAX = 160

LP_LONGBOARD_MIN_PERIOD_S = 13.0
LP_LONGBOARD_MIN_HEIGHT_FT = 0.7

LP_SHORTBOARD_MIN_PERIOD_S = 13.0
LP_SHORTBOARD_MIN_HEIGHT_FT = 1.6

SP_MIN_WVHT_FT = 3.0
# ---- end thresholds ----

def _first_num(row, *keys):
    for k in keys:
        v = row.get(k)
        if v in ("", None):
            continue
        try:
            return float(v)
        except Exception:
            pass
    return None

def _deg_ok(x, lo=DIR_MIN, hi=DIR_MAX):
    try:
        d = float(x)
    except Exception:
        return False
    return lo <= d <= hi

def _swell_period_s(row):
    # Prefer real swell period, then dominant period; support both short & long keys.
    return _first_num(row, "swell_period_s", "swp_s", "SwP",
                           "dominant_period_s", "dpd_s", "DPD")

def _swell_height_ft(row):
    return _first_num(row, "swell_height_ft", "swh_ft", "SwH_ft", "SwH",
                           "wave_height_ft", "wvht_ft", "WVHT_ft", "WVHT")

def _swell_or_mean_dir_deg(row):
    return _first_num(row, "swell_dir_deg_true", "mean_wave_dir_deg", "mwd_deg", "MWD", "wind_dir_deg")

def _mean_dir_deg(row):
    return _first_num(row, "mean_wave_dir_deg", "mwd_deg", "MWD", "wind_dir_deg")

def is_long_period_longboard(row):
    p = _swell_period_s(row)
    h = _swell_height_ft(row)
    d = _swell_or_mean_dir_deg(row)
    return (p is not None and p >= LP_LONGBOARD_MIN_PERIOD_S
            and h is not None and h >= LP_LONGBOARD_MIN_HEIGHT_FT
            and _deg_ok(d))

def is_long_period_shortboard(row):
    p = _swell_period_s(row)
    h = _swell_height_ft(row)
    d = _swell_or_mean_dir_deg(row)
    return (p is not None and p >= LP_SHORTBOARD_MIN_PERIOD_S
            and h is not None and h >= LP_SHORTBOARD_MIN_HEIGHT_FT
            and _deg_ok(d))

def is_short_period_all(row):
    wvht = _first_num(row, "wave_height_ft", "wvht_ft", "WVHT_ft", "WVHT")
    d = _mean_dir_deg(row)
    return (wvht is not None and wvht > SP_MIN_WVHT_FT and _deg_ok(d))

# Backward-compatible names expected by older code
def longboard_ok(row):   return is_long_period_longboard(row)
def shortboard_ok(row):  return is_long_period_shortboard(row)
def short_period_ok(row):return is_short_period_all(row)

RULE_FUNCS = {
    "Longboard": is_long_period_longboard,
    "Shortboard": is_long_period_shortboard,
    "Short Period": is_short_period_all,
}
