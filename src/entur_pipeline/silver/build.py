"""Silver layer, step 4: read Bronze snapshots and build `silver.stop_calls` as Parquet.

Pipeline: list Bronze files -> flatten -> standardize -> dedup -> Parquet partitioned by
service_date. A snapshot that cannot be read is skipped and reported, not fatal. A row with a
missing key, call status or snapshot time cannot be stored (the schema is not nullable there)
and is dropped and counted; proper rejection rules with reason codes come with the cleaning step.

Limit: dedup only sees the snapshots of the requested dates. A service date that spans two
Bronze days (night trips) loses the versions found in the other day when it is rewritten.
"""
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

from pyspark import StorageLevel

from entur_pipeline.silver.dedup import KEY, dedup_stop_calls
from entur_pipeline.silver.flatten import flatten_snapshot
from entur_pipeline.silver.schema import STOP_CALLS_SCHEMA
from entur_pipeline.silver.standardize import standardize_row

REQUIRED = KEY + ["call_status", "snapshot_time"]  # not nullable in STOP_CALLS_SCHEMA


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


def build_silver(spark, cfg, date_from=None, date_to=None):
    """Build `stop_calls` for the given Bronze dates. Returns a small dict of counts."""
    paths = [str(p) for p in list_bronze_files(cfg["data_root"], date_from, date_to)]

    # One task per file group: the workers read and flatten, the driver only holds the paths
    sc = spark.sparkContext
    slices = max(1, min(len(paths), sc.defaultParallelism))
    results = sc.parallelize(paths, slices).map(read_file).persist(StorageLevel.MEMORY_AND_DISK)

    df = spark.createDataFrame(results.flatMap(lambda r: r[0]), STOP_CALLS_SCHEMA)
    deduped = dedup_stop_calls(df).cache()

    # Overwrite only the service dates present in this run, not the whole table
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out = Path(cfg["data_root"]) / "silver" / "stop_calls"
    deduped.write.mode("overwrite").partitionBy("service_date").parquet(str(out))

    skipped = results.filter(lambda r: r[1] is not None).map(lambda r: r[1]).collect()
    summary = {
        "files_read": len(paths) - len(skipped),
        "files_skipped": len(skipped),
        "rows_dropped": results.map(lambda r: r[2]).sum(),
        "rows_before_dedup": results.map(lambda r: len(r[0])).sum(),
        "rows_after_dedup": deduped.count(),
        "output": str(out),
    }
    results.unpersist()
    return summary
