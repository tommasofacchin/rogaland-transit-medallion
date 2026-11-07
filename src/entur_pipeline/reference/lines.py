"""Reference table `line_names`: the public line number and name for every line id in Silver.

Kolumbus only sends ids like KOL:Line:8_2043 in the SIRI feed (PublishedLineName is empty),
so we ask Entur's Journey Planner API for the names and save them as a small table that Gold
joins. The table is rebuilt on every run, so new lines get a name too.
"""
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

JP_URL = "https://api.entur.io/journey-planner/v3/graphql"
QUERY = "query($ids: [ID]!) { lines(ids: $ids) { id publicCode name transportMode } }"
BATCH = 100  # line ids per request

LINE_NAMES_SCHEMA = StructType([
    StructField("line", StringType(), False),
    StructField("public_code", StringType(), True),   # what is on the bus, e.g. "X39"
    StructField("line_name", StringType(), True),     # often the same as public_code
    StructField("transport_mode", StringType(), True),
    StructField("fetched_at", TimestampType(), False),
])


def fetch_lines(line_ids, client_name, timeout=60):
    """Ask Journey Planner about the line ids. Returns a list of dicts; unknown ids are left out."""
    found = []
    for i in range(0, len(line_ids), BATCH):
        resp = requests.post(JP_URL, timeout=timeout,
                             headers={"ET-Client-Name": client_name},
                             json={"query": QUERY, "variables": {"ids": line_ids[i:i + BATCH]}})
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"Journey Planner error: {body['errors']}")
        found.extend(line for line in body["data"]["lines"] if line)
    return found


def to_rows(lines, fetched_at):
    return [(line["id"], line.get("publicCode"), line.get("name"), line.get("transportMode"),
             fetched_at) for line in lines]


def build_line_names(spark, cfg):
    """Fetch names for all lines in Silver and overwrite reference/line_names."""
    root = Path(cfg["data_root"])
    silver = spark.read.parquet(str(root / "silver" / "stop_calls"))
    ids = sorted(r["line"] for r in silver.select("line").distinct().collect() if r["line"])

    lines = fetch_lines(ids, cfg["entur"]["client_name"])
    rows = to_rows(lines, datetime.now(timezone.utc))
    df = spark.createDataFrame(rows, LINE_NAMES_SCHEMA)
    df.write.mode("overwrite").parquet(str(root / "reference" / "line_names"))
    return {"line_ids": len(ids), "found": len(lines), "missing": len(ids) - len(lines)}