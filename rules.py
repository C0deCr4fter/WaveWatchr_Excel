from typing import Dict

def within_direction_window(mwd_deg: float) -> bool:
    """Return True if direction is between 25° and 160° inclusive (NE→SE window)."""
    try:
        d = float(mwd_deg)
    except (TypeError, ValueError):
        return False
    return 25.0 <= d <= 160.0

def longboard_ok(row: Dict) -> bool:
    """Long period swell for longboarders: DPD >= 13s AND WVHT_ft >= 0.7 AND dir window."""
    try:
        dpd = float(row.get("DPD", "nan"))
        wvht_ft = float(row.get("WVHT_ft", "nan"))
    except (TypeError, ValueError):
        return False
    return (dpd >= 13.0) and (wvht_ft >= 0.7) and within_direction_window(row.get("MWD"))

def shortboard_ok(row: Dict) -> bool:
    """Long period swell for shortboarders: DPD >= 13s AND WVHT_ft >= 1.6 AND dir window."""
    try:
        dpd = float(row.get("DPD", "nan"))
        wvht_ft = float(row.get("WVHT_ft", "nan"))
    except (TypeError, ValueError):
        return False
    return (dpd >= 13.0) and (wvht_ft >= 1.6) and within_direction_window(row.get("MWD"))

def short_period_ok(row: Dict) -> bool:
    """Short period alert for all: WVHT_ft >= 3 AND dir window (period unconstrained)."""
    try:
        wvht_ft = float(row.get("WVHT_ft", "nan"))
    except (TypeError, ValueError):
        return False
    return (wvht_ft >= 3.0) and within_direction_window(row.get("MWD"))
