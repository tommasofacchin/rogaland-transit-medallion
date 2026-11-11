"""Reference table `line_names`: the public line number and name for every line id in Silver.

Kolumbus only sends ids like KOL:Line:8_2043 in the SIRI feed (PublishedLineName is empty),
so we ask Entur's Journey Planner API for the names and save them as a small table that Gold
joins. Names hardly ever change, so we only ask about line ids that are not in the table yet and keep
the old rows. A line that stops running keeps its name, and most nights need no API call.
Ids that Journey Planner does not know are saved too, with empty names, so we ask only once.
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


def new_line_ids(silver_ids, known_ids):
    """Line ids from Silver that have no name in the table yet."""
    return sorted(set(silver_ids) - set(known_ids))


def build_line_names(spark, cfg):
    """Fetch names for lines that are new in Silver and add them to reference/line_names."""
    root = Path(cfg["data_root"])
    path = root / "reference" / "line_names"
    silver = spark.read.parquet(str(root / "silver" / "stop_calls"))
    ids = [r["line"] for r in silver.select("line").distinct().collect() if r["line"]]

    # small table: read it into Python, Spark can't overwrite a folder it is still reading
    old = []
    if path.exists():
        table = spark.read.parquet(str(path)).select(*LINE_NAMES_SCHEMA.names)
        old = [tuple(r) for r in table.collect()]
    todo = new_line_ids(ids, [r[0] for r in old])

    found = fetch_lines(todo, cfg["entur"]["client_name"]) if todo else []
    if todo:
        now = datetime.now(timezone.utc)
        found_ids = {line["id"] for line in found}
        unknown = [(i, None, None, None, now) for i in todo if i not in found_ids]
        rows = old + to_rows(found, now) + unknown
        df = spark.createDataFrame(rows, LINE_NAMES_SCHEMA)
        df.write.mode("overwrite").parquet(str(path))
    return {"line_ids": len(ids), "known": len(old), "asked": len(todo), "found": len(found),
            "unknown": len(todo) - len(found)}