"""Quality metrics for one Silver run, saved as rows: run_id | run_time | date_from | date_to | metric | value.

One row per metric, so adding a metric later does not change the table.
"""
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from entur_pipeline.silver.dedup import KEY
from entur_pipeline.silver.rules import FLAGS, REJECT_CODES
from entur_pipeline.silver.schema import STOP_CALLS_SCHEMA

QUALITY_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("run_time", TimestampType()),
    StructField("date_from", DateType()),  # service dates of the run
    StructField("date_to", DateType()),
    StructField("metric", StringType()),
    StructField("value", DoubleType()),
])

ESSENTIAL = KEY + ["line", "stop_id", "aimed_arrival", "aimed_departure"]


def duplicate_keys(df):
    """Number of rows with a key that appears more than once (should be 0 after dedup)."""
    return df.count() - df.select(*KEY).distinct().count()


def schema_ok(df):
    """True if all columns from schema.py are there with the right type."""
    types = {f.name: f.dataType for f in df.schema.fields}
    return all(types.get(f.name) == f.dataType for f in STOP_CALLS_SCHEMA.fields)


def collect_metrics(clean, rejected, silver, unparseable=0):
    """Return a dict with all metrics of one run.

    clean = rows that passed the rules (before dedup), rejected = rows the rules removed,
    silver = rows that get written (after dedup), unparseable = rows build.py could not parse.
    """
    m = {"rows_unparseable": unparseable}

    for code in REJECT_CODES:
        m["rejected_" + code] = 0
    for row in rejected.groupBy("reason_code").count().collect():
        m["rejected_" + row["reason_code"]] = row["count"]

    m["rows_clean"] = clean.count()

    # flags and nulls are counted after dedup, so each stop call counts once
    flag_sums = [F.sum(F.col(f).cast("int")).alias("flagged_" + f) for f in FLAGS]
    null_sums = [F.sum(F.col(c).isNull().cast("int")).alias("nulls_" + c) for c in ESSENTIAL]
    row = silver.agg(F.count("*").alias("rows_silver"), *flag_sums, *null_sums).first()
    for name, value in row.asDict().items():
        m[name] = value or 0
    # measured stops that only have a predicted time (never became a RecordedCall).
    # "measured" is the same test as in gold/build.py, so the numbers match Gold.
    delay = F.coalesce("departure_delay_s", "arrival_delay_s")
    measured = ~F.col("exclude_from_punctuality") & delay.isNotNull()
    m["measured_estimated_only"] = silver.filter(
        measured & (F.col("call_status") == "estimated")).count()

    m["duplicate_keys_before_dedup"] = duplicate_keys(clean)
    m["duplicate_keys_after_dedup"] = duplicate_keys(silver)
    m["schema_ok"] = int(schema_ok(silver))
    return m


def quality_rows(spark, run_id, metrics, run_time=None, date_from=None, date_to=None):
    """Turn the metrics dict into a DataFrame that can be appended to silver/quality."""
    run_time = run_time or datetime.now(timezone.utc)
    rows = [(run_id, run_time, date_from, date_to, name, float(value))
            for name, value in sorted(metrics.items())]
    return spark.createDataFrame(rows, QUALITY_SCHEMA)