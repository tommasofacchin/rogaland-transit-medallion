from pathlib import Path

from entur_pipeline.silver.flatten import flatten_snapshot

FIXTURE = Path(__file__).parent / "fixtures" / "siri_sample.xml"


def _rows():
    return flatten_snapshot(FIXTURE, run_id="test")


def test_one_row_per_call():
    rows = _rows()
    assert len(rows) == 6  # 3 journeys x 2 stops
    assert sum(r["call_status"] == "recorded" for r in rows) == 1
    assert sum(r["call_status"] == "estimated" for r in rows) == 5


def test_first_stop_has_no_arrival():
    first = [r for r in _rows() if r["stop_order"] == 1]
    assert all(r["aimed_arrival"] is None for r in first)
    assert all(r["aimed_departure"] is not None for r in first)


def test_last_stop_has_no_departure():
    last = [r for r in _rows() if r["stop_order"] == 2]
    assert all(r["aimed_departure"] is None for r in last)
    assert all(r["aimed_arrival"] is not None for r in last)


def test_recorded_call_uses_actual_time():
    recorded = next(r for r in _rows() if r["call_status"] == "recorded")
    assert recorded["actual_or_expected_departure"] == "2026-10-27T13:46:35+01:00"


def test_cancelled_flag():
    rows = _rows()
    cancelled = [r for r in rows if r["journey_id"].endswith("_1006")]
    assert len(cancelled) == 2
    assert all(r["cancelled"] is True for r in cancelled)
    assert not any(r["cancelled"] for r in rows if r not in cancelled)


def test_journey_level_fields():
    row = _rows()[0]
    assert row["service_date"] == "2026-10-27"
    assert row["snapshot_time"] == "2026-10-27T14:00:02.54588114+01:00"
    assert row["run_id"] == "test"
