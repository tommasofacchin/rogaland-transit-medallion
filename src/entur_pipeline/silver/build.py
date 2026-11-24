"""Silver layer, step 4: read Bronze snapshots and build `silver.stop_calls` as Parquet.

Pipeline: list Bronze files -> flatten -> standardize -> rules -> dedup -> Parquet partitioned
by service_date. Rejected rows go to `silver/rejected_calls` and the run's metrics to
`silver/quality`. A snapshot that cannot be read is skipped and counted, not fatal.

Night trips: a trip can have service_date D but show up in the Bronze files of day D+1 (or
D-1, because dt= is a UTC day). So we also read the last hours of D-1 and the first hours
of D+1 (FIRST_HOUR_BEFORE / LAST_HOUR_AFTER, measured on prod with scripts/window_check.py).
"""
import xml.etree.ElementTree as ET
import zlib
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from entur_pipeline.silver.dedup import dedup_stop_calls
from entur_pipeline.silver.flatten import flatten_snapshot
from entur_pipeline.silver.quality import collect_metrics, quality_rows
from entur_pipeline.silver.rules import apply_rules
from entur_pipeline.silver.schema import STOP_CALLS_SCHEMA
from entur_pipeline.silver.standardize import standardize_row

REQUIRED = ["call_status", "snapshot_time"]
# More skipped files than this share stops the run before anything is written: a few broken
# files are normal, many usually mean a bug in our code (a day reads ~864 files).
MAX_SKIPPED_SHARE = 0.05
# Bronze hours (UTC) next to the range that can still hold its stops: day D-1 from
# FIRST_HOUR_BEFORE, day D+1 up to and including LAST_HOUR_AFTER. Measured on prod with
# scripts/window_check.py, plus a margin of 2 hours (see decisions).
FIRST_HOUR_BEFORE = 20
LAST_HOUR_AFTER = 2

# Same columns as STOP_CALLS_SCHEMA but all nullable, so a row with a missing key
# gets to the rules and is rejected with a reason instead of crashing createDataFrame.
INPUT_SCHEMA = StructType([StructField(f.name, f.dataType, True)
                           for f in STOP_CALLS_SCHEMA.fields])


def list_bronze_files(data_root, date_from=None, date_to=None, first_hour=0, last_hour=23):
    """Bronze snapshots (.xml or .xml.gz) in [date_from first_hour:00, date_to last_hour:59] (UTC)."""
    base = Path(data_root) / "bronze" / "entur_siri"
    paths = []
    for folder in sorted(base.glob("dt=*")):
        day = date.fromisoformat(folder.name.removeprefix("dt="))
        if (date_from and day < date_from) or (date_to and day > date_to):
            continue
        for hour_folder in sorted(folder.glob("hour=*")):
            hour = int(hour_folder.name.removeprefix("hour="))
            if (day == date_from and hour < first_hour) or (day == date_to and hour > last_hour):
                continue
            paths.extend(sorted(hour_folder.glob("entur_siri_*.xml*")))
    return paths


def bronze_window(date_from=None, date_to=None):
    """First and last Bronze hour (UTC) to read for these service dates."""
    start = end = None
    if date_from:
        start = datetime.combine(date_from - timedelta(days=1), time(FIRST_HOUR_BEFORE))
    if date_to:
        end = datetime.combine(date_to + timedelta(days=1), time(LAST_HOUR_AFTER))
    return start, end


def rows_at_window_edge(df, start, end):
    """Silver rows whose kept version comes from the first or the last Bronze hour we read.
    Should be 0: if not, the stop may have newer versions outside the window, so make it wider.
    (A few trips are in the feed a day early, but their kept version is from day D.)"""
    edge = F.lit(False)
    if start:
        edge = edge | (F.col("snapshot_time") < F.lit(start + timedelta(hours=1)))
    if end:
        edge = edge | (F.col("snapshot_time") >= F.lit(end))
    return df.filter(edge).count()


def run_id_from_path(path):
    """entur_siri_20261027T140002Z_<run_id>.xml.gz -> <run_id>."""
    name = Path(path).name.removesuffix(".gz").removesuffix(".xml")
    return name.split("_", 3)[3]


def read_file(path):
    """Flatten and standardize one file. Returns (rows, skipped, dropped).

    `skipped` is None, or (path, error) if the file could not be read. Runs on the workers.
    """
    try:
        flat = flatten_snapshot(path, run_id_from_path(path))
    except (ET.ParseError, EOFError, OSError, ValueError, zlib.error) as exc:
        return [], (str(path), repr(exc)), 0
    rows, dropped = [], 0
    for raw in flat:
        try:
            row = standardize_row(raw)
        except (TypeError, ValueError):
            dropped += 1
            continue
        if any(row[column] is None for column in REQUIRED):
            dropped += 1
            continue
        rows.append(row)
    return rows, None, dropped


def read_rows(paths):
    """Flatten and standardize every file in the driver. Returns (rows, skipped, dropped)."""
    rows, skipped, dropped = [], [], 0
    for path in paths:
        file_rows, file_skipped, file_dropped = read_file(path)
        rows.extend(file_rows)
        if file_skipped is not None:
            skipped.append(file_skipped)
        dropped += file_dropped
    return rows, skipped, dropped


def in_range(df, date_from=None, date_to=None):
    """Keep rows whose service_date is inside [date_from, date_to]."""
    if date_from:
        df = df.filter(F.col("service_date") >= date_from)
    if date_to:
        df = df.filter(F.col("service_date") <= date_to)
    return df


def check_skipped(skipped, n_files, max_share=None):
    """Print the first skipped files; raise if too many were skipped to trust the run."""
    if not skipped:
        return
    max_share = MAX_SKIPPED_SHARE if max_share is None else max_share
    print(f"skipped {len(skipped)} of {n_files} Bronze files, first ones:")
    for path, reason in skipped[:5]:
        print(f"  {path}: {reason}")
    if len(skipped) > max_share * n_files:
        raise RuntimeError(
            f"{len(skipped)} of {n_files} Bronze files could not be read "
            f"(limit {max_share:.0%}), nothing was written. First: {skipped[0]}")


def build_silver(spark, cfg, date_from=None, date_to=None, run_id="local"):
    """Build `stop_calls` for the given service dates. Returns the run's metrics as a dict."""
    # the last hours of D-1 and the first hours of D+1 too, so night trips are complete
    start, end = bronze_window(date_from, date_to)
    paths = [str(p) for p in list_bronze_files(
        cfg["data_root"], start and start.date(), end and end.date(),
        first_hour=start.hour if start else 0, last_hour=end.hour if end else 23)]
    # a run that starts too early would miss the night trips of D+1 without noticing
    if end and datetime.now(timezone.utc).replace(tzinfo=None) < end + timedelta(hours=1):
        raise RuntimeError(f"Bronze until {end:%Y-%m-%d %H}:59 UTC is not collected yet, "
                           "run Silver later")

    # One task per file, so a task only holds one snapshot in memory
    # (with 4 big groups one day did not fit in memory on prod)
    sc = spark.sparkContext
    slices = max(1, len(paths))
    results = sc.parallelize(paths, slices).map(read_file).persist(StorageLevel.MEMORY_AND_DISK)

    df = spark.createDataFrame(results.flatMap(lambda r: r[0]), INPUT_SCHEMA).cache()
    clean, rejected = apply_rules(df)
    clean = in_range(clean, date_from, date_to)
    rejected = in_range(rejected, date_from, date_to)
    silver = dedup_stop_calls(clean).cache()

    skipped = results.filter(lambda r: r[1] is not None).map(lambda r: r[1]).collect()
    check_skipped(skipped, len(paths))

    # Overwrite only the service dates present in this run, not the whole table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    root = Path(cfg["data_root"]) / "silver"    
    # one task per day, so each day is one Parquet file and not one per task
    (silver.repartition("service_date").write.mode("overwrite")
     .partitionBy("service_date").parquet(str(root / "stop_calls")))
    (rejected.repartition("service_date").write.mode("overwrite")
     .partitionBy("service_date").parquet(str(root / "rejected_calls")))

    unparseable = results.map(lambda r: r[2]).sum()
    metrics = collect_metrics(clean, rejected, silver, unparseable=unparseable)
    metrics["files_read"] = len(paths) - len(skipped)
    metrics["files_skipped"] = len(skipped)
    metrics["rows_at_window_edge"] = rows_at_window_edge(silver, start, end)
    if metrics["rows_at_window_edge"]:
        print(f"WARNING: {metrics['rows_at_window_edge']} rows in the first or last Bronze hour "
              "read, make FIRST_HOUR_BEFORE / LAST_HOUR_AFTER wider")
    (quality_rows(spark, run_id, metrics, date_from=date_from, date_to=date_to)
     .write.mode("append").parquet(str(root / "quality")))

    results.unpersist()
    df.unpersist()
    silver.unpersist()
    return metrics
