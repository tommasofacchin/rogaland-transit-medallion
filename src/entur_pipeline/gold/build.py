"""Gold layer: punctuality tables built from silver.stop_calls.

punctuality_by_line_hour: one row per service date, line and hour of the day (Norway time).
punctuality_by_stop:      one row per service date and stop.

Both tables keep the counts and not only the shares, so a consumer can add up several days
and still get the right share. Gold is only written when Silver looks complete, and every run
adds one line to gold/_status/status.jsonl, so a consumer can see when Gold was last updated.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from entur_pipeline.silver.build import in_range

# on time = from 1 min early to 3 min late, the same definition Norwegian operators use
EARLY_S = -60
LATE_S = 3 * 60
LOCAL_TZ = "Europe/Oslo"
# a normal Sunday has about 55k stops, so fewer than this means collection was broken
MIN_SILVER_ROWS = 20_000


def add_columns(df):
    """Add delay_s, hour (Norway time) and measured (row counts for punctuality)."""
    # departure delay, or the arrival delay at the last stop (it has no departure)
    delay = F.coalesce("departure_delay_s", "arrival_delay_s")
    aimed = F.coalesce("aimed_departure", "aimed_arrival")
    measured = ~F.col("exclude_from_punctuality") & delay.isNotNull()
    return (df.withColumn("delay_s", delay)
            # Silver times are UTC, but "8 o'clock" should mean 8 in Norway
            .withColumn("hour", F.hour(F.from_utc_timestamp(aimed, LOCAL_TZ)))
            .withColumn("measured", measured)
            .withColumn("is_cancelled", F.coalesce("cancelled", F.lit(False))))


def measures():
    m = F.col("measured")
    d = F.col("delay_s")
    return [
        F.count("*").alias("calls"),
        F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_calls"),
        F.sum(m.cast("int")).alias("measured_calls"),
        F.sum((m & d.between(EARLY_S, LATE_S)).cast("int")).alias("on_time_calls"),
        F.sum((m & (d > LATE_S)).cast("int")).alias("late_calls"),
        F.sum((m & (d < EARLY_S)).cast("int")).alias("early_calls"),
        F.round(F.avg(F.when(m, d)), 1).alias("avg_delay_s"),
        F.percentile_approx(F.when(m, d), 0.9).alias("p90_delay_s"),
    ]


def add_shares(df):
    measured = F.col("measured_calls")
    return (df.withColumn("on_time_share",
                          F.when(measured > 0, F.round(F.col("on_time_calls") / measured, 3)))
            .withColumn("cancelled_share", F.round(F.col("cancelled_calls") / F.col("calls"), 3)))


def by_line_hour(silver):
    grouped = add_columns(silver).groupBy("service_date", "line", "hour").agg(*measures())
    return add_shares(grouped)


def by_stop(silver):
    # some lines have no stop names, so group by stop_id and just take one of the names
    grouped = (add_columns(silver).groupBy("service_date", "stop_id")
               .agg(F.max("stop_name").alias("stop_name"), *measures()))
    return add_shares(grouped)


def check_silver(day_counts, days):
    """Return a list of problems. Gold is only written when the list is empty."""
    problems = []
    for day in days:
        rows = day_counts.get(day, 0)
        if rows < MIN_SILVER_ROWS:
            problems.append(f"{day}: only {rows} rows in Silver")
    return problems


def days_between(date_from, date_to):
    return [date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)]


def write_status(root, record):
    folder = root / "_status"
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / "status.jsonl", "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def build_gold(spark, cfg, date_from=None, date_to=None, run_id="local"):
    """Build both Gold tables for the given service dates. Returns the status record."""
    silver_path = Path(cfg["data_root"]) / "silver" / "stop_calls"
    root = Path(cfg["data_root"]) / "gold"
    silver = in_range(spark.read.parquet(str(silver_path)), date_from, date_to).cache()

    day_counts = {r["service_date"]: r["count"]
                  for r in silver.groupBy("service_date").count().collect()}
    if date_from and date_to:
        days = days_between(date_from, date_to)
    else:
        days = sorted(day_counts)
    problems = check_silver(day_counts, days) if days else ["no Silver data for these dates"]

    line_hour = by_line_hour(silver).cache()
    stop = by_stop(silver).cache()

    # every Silver row must end up in exactly one group of each table
    silver_rows = sum(day_counts.values())
    line_hour_calls = line_hour.agg(F.sum("calls")).first()[0] or 0
    stop_calls = stop.agg(F.sum("calls")).first()[0] or 0
    if line_hour_calls != silver_rows or stop_calls != silver_rows:
        problems.append(f"counts do not match: silver {silver_rows}, "
                        f"line_hour {line_hour_calls}, stop {stop_calls}")

    if not problems:
        # overwrite only the days of this run, like Silver
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (line_hour.write.mode("overwrite").partitionBy("service_date")
         .parquet(str(root / "punctuality_by_line_hour")))
        (stop.write.mode("overwrite").partitionBy("service_date")
         .parquet(str(root / "punctuality_by_stop")))

    record = {
        "run_id": run_id,
        "run_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "date_from": days[0] if days else None,
        "date_to": days[-1] if days else None,
        "status": "failed" if problems else "ok",
        "problems": problems,
        "silver_rows": silver_rows,
        "line_hour_rows": line_hour.count(),
        "stop_rows": stop.count(),
    }
    write_status(root, record)

    silver.unpersist()
    line_hour.unpersist()
    stop.unpersist()
    return record