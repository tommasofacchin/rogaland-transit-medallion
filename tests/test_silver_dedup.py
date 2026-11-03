from datetime import date, datetime, timezone

import pytest
from pyspark.sql import SparkSession

from entur_pipeline.silver.dedup import dedup_stop_calls
from entur_pipeline.silver.schema import STOP_CALLS_SCHEMA

UTC = timezone.utc


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(**kw):
    """One stop call with every non-nullable column filled; the other columns stay None."""
    base: dict = {name: None for name in STOP_CALLS_SCHEMA.fieldNames()}
    base.update(
        journey_id="J1",
        service_date=date(2026, 10, 27),
        stop_order=1,
        call_status="estimated",
        snapshot_time=datetime(2026, 10, 27, 13, 0, tzinfo=UTC),
    )
    base.update(kw)
    return base


def _dedup(spark, rows):
    df = spark.createDataFrame(rows, STOP_CALLS_SCHEMA)
    return dedup_stop_calls(df).collect()


def test_same_key_in_two_snapshots_keeps_the_latest(spark):
    old = _row(snapshot_time=datetime(2026, 10, 27, 13, 0, tzinfo=UTC), run_id="old")
    new = _row(snapshot_time=datetime(2026, 10, 27, 13, 5, tzinfo=UTC), run_id="new")
    out = _dedup(spark, [old, new])
    assert len(out) == 1
    assert out[0]["run_id"] == "new"


def test_recorded_wins_over_a_later_estimated(spark):
    recorded = _row(
        call_status="recorded",
        snapshot_time=datetime(2026, 10, 27, 13, 0, tzinfo=UTC),
        run_id="recorded",
    )
    estimated = _row(
        call_status="estimated",
        snapshot_time=datetime(2026, 10, 27, 13, 5, tzinfo=UTC),
        run_id="estimated",
    )
    out = _dedup(spark, [recorded, estimated])
    assert len(out) == 1
    assert out[0]["run_id"] == "recorded"


def test_different_keys_are_all_kept(spark):
    rows = [
        _row(stop_order=1),
        _row(stop_order=2),
        _row(journey_id="J2"),
        _row(service_date=date(2026, 10, 28)),
    ]
    assert len(_dedup(spark, rows)) == 4


def test_exact_duplicate_rows_collapse_to_one(spark):
    assert len(_dedup(spark, [_row(), _row()])) == 1


def test_schema_is_unchanged(spark):
    df = spark.createDataFrame([_row()], STOP_CALLS_SCHEMA)
    assert dedup_stop_calls(df).schema == STOP_CALLS_SCHEMA
