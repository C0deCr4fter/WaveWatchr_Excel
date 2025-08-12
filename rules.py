# rules.py
from typing import Dict, Tuple

DIR_MIN = 25.0   # deg true
DIR_MAX = 160.0  # deg true

def _f(d: Dict, key: str) -> float:
    """Safe float getter (blank/missing -> NaN)."""
    v = (d.get(key) or "").strip()
    try:
        return float(v)
    except Exception:
        return float("nan")

def _dir_in_window(deg: float) -> bool:
    return (deg >= DIR_MIN) and (deg <= DIR_MAX)

def longboard_ok(row: Dict) -> Tuple[bool, str]:
    """SwP > 13s AND SwH > 0.7 AND SWD in [25,160]."""
    swp = _f(row, "SwP")   # swell period (s)
    swh = _f(row, "SwH")   # swell height (m)
    swd = _f(row, "SWD")   # swell direction (deg true)
    ok = (swp > 13.0) and (swh > 0.7) and _dir_in_window(swd)
    why = f"SwP={swp:g}s, SwH={swh:g}m, SWD={swd:g}°"
    return ok, why

def shortboard_ok(row: Dict) -> Tuple[bool, str]:
    """SwP > 13s AND SwH > 1.6 AND SWD in [25,160]."""
    swp = _f(row, "SwP")
    swh = _f(row, "SwH")
    swd = _f(row, "SWD")
    ok = (swp > 13.0) and (swh > 1.6) and _dir_in_window(swd)
    why = f"SwP={swp:g}s, SwH={swh:g}m, SWD={swd:g}°"
    return ok, why

def short_period_ok(row: Dict) -> Tuple[bool, str]:
    """WVHT > 3 (m) AND mean wave direction in [25,160]."""
    wvht = _f(row, "WVHT")  # significant wave height (m)
    mwd  = _f(row, "MWD")   # mean wave direction (deg true)
    ok = (wvht > 3.0) and _dir_in_window(mwd)
    why = f"WVHT={wvht:g}m, MWD={mwd:g}°"
    return ok, why
