from datetime import date, datetime

from silver_rows import T0, call, df_of

from entur_pipeline.gold.build import (
    build_gold,
    by_line_hour,
    by_stop,
    check_silver,
    days_between,
)
from entur_pipeline.silver.rules import add_flags


def silver_of(spark, *rows):
    """Hand-made rows with the flags from rules.py, like real Silver."""
    return add_flags(df_of(spark, *rows))


def late(seconds, **overrides):
    """A call whose departure is `seconds` late."""
    return call(arrival_delay_s=None, aimed_departure=T0, departure_delay_s=seconds, **overrides)


def test_hour_is_norway_time(spark):
    # 27 Oct is after the clock change, so Norway is UTC+1: 07:00 UTC is 8 o'clock
    rows = by_line_hour(silver_of(spark, call())).collect()
    assert [r["hour"] for r in rows] == [8]


def test_hour_in_summer_time(spark):
    # 20 Oct is still summer time (UTC+2)
    summer = datetime(2026, 10, 20, 7, 0)
    rows = by_line_hour(silver_of(spark, call(aimed_arrival=summer))).collect()
    assert rows[0]["hour"] == 9


def test_on_time_limits(spark):
    df = silver_of(spark, late(-60, stop_order=1), late(180, stop_order=2),
                   late(181, stop_order=3), late(-61, stop_order=4))
    r = by_line_hour(df).first()
    assert (r["on_time_calls"], r["late_calls"], r["early_calls"]) == (2, 1, 1)
    assert r["on_time_share"] == 0.5


def test_uses_arrival_delay_when_there_is_no_departure(spark):
    # the last stop of a trip only has an arrival
    r = by_line_hour(silver_of(spark, call(arrival_delay_s=600))).first()
    assert r["late_calls"] == 1
    assert r["avg_delay_s"] == 600


def test_excluded_rows_are_counted_but_not_measured(spark):
    df = silver_of(spark,
                   late(60, stop_order=1),
                   late(5 * 3600, stop_order=2),           # outlier
                   late(60, stop_order=3, cancelled=True))  # cancelled
    r = by_line_hour(df).first()
    assert (r["calls"], r["measured_calls"], r["cancelled_calls"]) == (3, 1, 1)
    assert r["avg_delay_s"] == 60
    assert r["cancelled_share"] == 0.333


def test_no_measured_calls_gives_null_share(spark):
    r = by_line_hour(silver_of(spark, late(60, cancelled=True))).first()
    assert r["on_time_share"] is None


def test_stop_without_name_takes_name_from_other_rows(spark):
    df = silver_of(spark, call(stop_order=1, stop_name=None), call(stop_order=2))
    rows = by_stop(df).collect()
    assert len(rows) == 1
    assert rows[0]["stop_name"] == "Kannik"
    assert rows[0]["calls"] == 2


def test_every_silver_row_lands_in_one_group(spark):
    rows = [call(stop_order=i, line=f"L{i % 3}", stop_id=f"S{i % 4}") for i in range(1, 13)]
    df = silver_of(spark, *rows)
    assert sum(r["calls"] for r in by_line_hour(df).collect()) == 12
    assert sum(r["calls"] for r in by_stop(df).collect()) == 12


def test_check_silver_finds_missing_and_small_days():
    days = days_between(date(2026, 11, 1), date(2026, 11, 3))
    counts = {date(2026, 11, 1): 55_000, date(2026, 11, 2): 500}
    assert check_silver(counts, days) == ["2026-11-02: only 500 rows in Silver",
                                          "2026-11-03: only 0 rows in Silver"]


def test_check_silver_ok():
    assert check_silver({date(2026, 11, 1): 55_000}, [date(2026, 11, 1)]) == []


def _write_silver(spark, tmp_path, rows):
    path = tmp_path / "silver" / "stop_calls"
    silver_of(spark, *rows).write.partitionBy("service_date").parquet(str(path))


def test_build_gold_writes_tables_and_status(spark, tmp_path, monkeypatch):
    monkeypatch.setattr("entur_pipeline.gold.build.MIN_SILVER_ROWS", 1)
    _write_silver(spark, tmp_path, [call(stop_order=1), call(stop_order=2)])

    record = build_gold(spark, {"data_root": str(tmp_path)}, run_id="t1")

    assert record["status"] == "ok"
    assert record["silver_rows"] == 2
    gold = spark.read.parquet(str(tmp_path / "gold" / "punctuality_by_line_hour"))
    assert gold.agg({"calls": "sum"}).first()[0] == 2
    status = (tmp_path / "gold" / "_status" / "status.jsonl").read_text()
    assert '"status": "ok"' in status


def test_build_gold_writes_nothing_when_silver_is_too_small(spark, tmp_path):
    _write_silver(spark, tmp_path, [call()])

    record = build_gold(spark, {"data_root": str(tmp_path)},
                        date(2026, 10, 27), date(2026, 10, 27), run_id="t2")

    assert record["status"] == "failed"
    assert not (tmp_path / "gold" / "punctuality_by_line_hour").exists()
    assert (tmp_path / "gold" / "_status" / "status.jsonl").exists()