"""Silver layer, step 4: read Bronze snapshots and build `silver.stop_calls` as Parquet.

Pipeline: list Bronze files -> flatten -> standardize -> rules -> dedup -> Parquet partitioned
by service_date. Rejected rows go to `silver/rejected_calls` and the run's metrics to
`silver/quality`. A snapshot that cannot be read is skipped and counted, not fatal.

Night trips: a trip can have service_date D but show up in the Bronze files of day D+1 (or
D-1, because dt= is a UTC day). So we read one extra Bronze day on each side of the range and
only write the service dates inside the range.
"""
import xml.etree.ElementTree as ET
from datetime import date, timedelta
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

# Same columns as STOP_CALLS_SCHEMA but all nullable, so a row with a missing key
# gets to the rules and is rejected with a reason instead of crashing createDataFrame.
INPUT_SCHEMA = StructType([StructField(f.name, f.dataType, True)
                           for f in STOP_CALLS_SCHEMA.fields])


def list_bronze_files(data_root, date_from=None, date_to=None):
    """Bronze snapshots (.xml or .xml.gz) whose dt= folder is within [date_from, date_to]."""
    base = Path(data_root) / "bronze" / "entur_siri"
    paths = []
    for folder in sorted(base.glob("dt=*")):
        day = date.fromisoformat(folder.name.removeprefix("dt="))
        if (date_from and day < date_from) or (date_to and day > date_to):
            continue
        paths.extend(sorted(folder.glob("hour=*/entur_siri_*.xml*")))
    return paths


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
    except (ET.ParseError, EOFError, OSError) as exc:
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


def build_silver(spark, cfg, date_from=None, date_to=None, run_id="local"):
    """Build `stop_calls` for the given service dates. Returns the run's metrics as a dict."""
    # one extra Bronze day on each side, so night trips are complete
    read_from = date_from - timedelta(days=1) if date_from else None
    read_to = date_to + timedelta(days=1) if date_to else None
    paths = [str(p) for p in list_bronze_files(cfg["data_root"], read_from, read_to)]

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

    # Overwrite only the service dates present in this run, not the whole table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    root = Path(cfg["data_root"]) / "silver"
    silver.write.mode("overwrite").partitionBy("service_date").parquet(str(root / "stop_calls"))
    (rejected.write.mode("overwrite").partitionBy("service_date")
     .parquet(str(root / "rejected_calls")))

    skipped = results.filter(lambda r: r[1] is not None).map(lambda r: r[1]).collect()
    unparseable = results.map(lambda r: r[2]).sum()
    metrics = collect_metrics(clean, rejected, silver, unparseable=unparseable)
    metrics["files_read"] = len(paths) - len(skipped)
    metrics["files_skipped"] = len(skipped)
    quality_rows(spark, run_id, metrics).write.mode("append").parquet(str(root / "quality"))

    results.unpersist()
    df.unpersist()
    silver.unpersist()
    return metrics
