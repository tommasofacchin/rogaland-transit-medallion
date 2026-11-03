"""Silver layer, step 2: convert flattened rows to the types of `schema.py` and derive delays.

Input rows come from `flatten.py` (strings). Times become timezone-aware UTC datetimes with
microsecond precision (Entur sends up to 9 fractional digits). `service_date` is taken from the
feed (`DataFrameRef`), so night trips already belong to the right day. Delay = actual or
expected time minus aimed time, in whole seconds; None when either time is missing.
"""
import re
from datetime import date, datetime, timezone

TIME_COLUMNS = [
    "aimed_arrival",
    "aimed_departure",
    "actual_or_expected_arrival",
    "actual_or_expected_departure",
    "snapshot_time",
]


def parse_ts(value):
    """ISO 8601 string with offset -> UTC datetime. None if missing; ValueError if malformed."""
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    # fromisoformat on Python 3.10 only accepts 3 or 6 fractional digits: normalise to 6
    value = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), value)
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _delay_s(actual, aimed):
    if actual is None or aimed is None:
        return None
    return round((actual - aimed).total_seconds())


def standardize_row(row):
    """Return a copy of `row` with parsed times, a date, and the two delay columns."""
    out = dict(row)
    for column in TIME_COLUMNS:
        out[column] = parse_ts(row.get(column))
    out["service_date"] = date.fromisoformat(row["service_date"])
    out["arrival_delay_s"] = _delay_s(out["actual_or_expected_arrival"], out["aimed_arrival"])
    out["departure_delay_s"] = _delay_s(
        out["actual_or_expected_departure"], out["aimed_departure"]
    )
    return out
