from datetime import date, datetime

from silver_rows import call, df_of

from entur_pipeline.silver import quality, rules
from entur_pipeline.silver.dedup import dedup_stop_calls


def test_duplicate_keys(spark):
    later = call(snapshot_time=datetime(2026, 10, 27, 7, 5))
    assert quality.duplicate_keys(df_of(spark, call(), later)) == 1
    assert quality.duplicate_keys(df_of(spark, call(), call(stop_order=3))) == 0


def test_schema_check(spark):
    df = df_of(spark, call())
    assert quality.schema_ok(df)
    assert not quality.schema_ok(df.drop("stop_id"))
    assert not quality.schema_ok(df.withColumn("stop_order", df.stop_order.cast("string")))


def test_metrics_and_long_format(spark):
    later = call(snapshot_time=datetime(2026, 10, 27, 7, 5), run_id="local-2")
    no_line = call(stop_order=3, line=None)
    cancelled = call(stop_order=4, cancelled=True)
    df = df_of(spark, call(), later, no_line, cancelled)
    clean, rejected = rules.apply_rules(df)
    silver = dedup_stop_calls(clean)
    m = quality.collect_metrics(clean, rejected, silver, unparseable=2)
    assert m["rejected_MISSING_LINE_OR_STOP"] == 1
    assert m["rejected_MISSING_KEY"] == 0
    assert m["rows_clean"] == 3 and m["rows_silver"] == 2
    assert m["duplicate_keys_before_dedup"] == 1 and m["duplicate_keys_after_dedup"] == 0
    assert m["flagged_exclude_from_punctuality"] == 1
    assert m["schema_ok"] == 1 and m["rows_unparseable"] == 2
    assert m["measured_estimated_only"] == 0

    day = date(2026, 10, 27)
    out = quality.quality_rows(spark, "local-1", m, date_from=day, date_to=day)
    assert out.schema == quality.QUALITY_SCHEMA
    rows = out.collect()
    assert {r.metric: r.value for r in rows}["rows_silver"] == 2
    assert all(r.date_from == day and r.date_to == day for r in rows)


def test_estimated_only_count(spark):
    df = df_of(spark, call(), call(stop_order=3, call_status="estimated"))
    clean, rejected = rules.apply_rules(df)
    m = quality.collect_metrics(clean, rejected, dedup_stop_calls(clean))
    assert m["measured_estimated_only"] == 1


def test_estimated_without_delay_is_not_counted(spark):
    # not excluded (it has an expected time) but Gold has no delay for it, so not measured
    df = df_of(spark, call(), call(stop_order=3, call_status="estimated", arrival_delay_s=None))
    clean, rejected = rules.apply_rules(df)
    m = quality.collect_metrics(clean, rejected, dedup_stop_calls(clean))
    assert m["measured_estimated_only"] == 0
