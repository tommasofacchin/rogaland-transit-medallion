from datetime import date, datetime, timezone

import pytest

from entur_pipeline.silver.standardize import parse_ts, standardize_row

UTC = timezone.utc


def test_parse_ts_converts_to_utc_and_truncates_nanoseconds():
    assert parse_ts("2026-10-27T14:00:02.54588114+01:00") == datetime(
        2026, 10, 27, 13, 0, 2, 545881, tzinfo=UTC
    )


def test_parse_ts_short_fraction_and_summer_offset():
    assert parse_ts("2026-07-01T10:00:00.856+02:00") == datetime(
        2026, 7, 1, 8, 0, 0, 856000, tzinfo=UTC
    )


def test_parse_ts_missing_is_none_but_malformed_raises():
    assert parse_ts(None) is None
    assert parse_ts("") is None
    with pytest.raises(ValueError):
        parse_ts("not a time")


def _row(**kw):
    base = {
        "service_date": "2026-10-27",
        "snapshot_time": "2026-10-27T14:00:02.5+01:00",
        "aimed_arrival": None,
        "aimed_departure": "2026-10-27T14:40:00+01:00",
        "actual_or_expected_arrival": None,
        "actual_or_expected_departure": "2026-10-27T14:40:10.856+01:00",
    }
    base.update(kw)
    return base


def test_standardize_row_types_and_departure_delay():
    out = standardize_row(_row())
    assert out["service_date"] == date(2026, 10, 27)
    assert out["aimed_departure"] == datetime(2026, 10, 27, 13, 40, tzinfo=UTC)
    assert out["departure_delay_s"] == 11  # 10.856 s, rounded


def test_missing_arrival_gives_none_delay():
    out = standardize_row(_row())
    assert out["aimed_arrival"] is None
    assert out["arrival_delay_s"] is None


def test_early_departure_is_negative():
    out = standardize_row(_row(actual_or_expected_departure="2026-10-27T14:39:00+01:00"))
    assert out["departure_delay_s"] == -60
