"""Silver layer, step 3: keep one row per stop call.

The same call appears in every snapshot until the trip leaves the feed, with updated times.
For each key (journey_id + service_date + stop_order) we keep a single row: a `recorded` call
(real time) wins over an `estimated` one (prediction), and among rows with the same status the
one from the latest snapshot wins. Exact duplicates (e.g. a snapshot fetched twice) collapse too.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

KEY = ["journey_id", "service_date", "stop_order"]

def dedup_stop_calls(df):
    """Return `df` with exactly one row per key, chosen as described in the module docstring."""
    w =Window.partitionBy("journey_id", "service_date", "stop_order").orderBy(
        (F.col("call_status") == "recorded").desc(),
        F.col("snapshot_time").desc(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
