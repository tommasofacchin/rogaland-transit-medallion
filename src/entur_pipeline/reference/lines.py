"""Reference table `line_names`: the public line number and name for every Kolumbus line id.

Kolumbus only sends ids like KOL:Line:8_2043 in the SIRI feed (PublishedLineName is empty).
The step `bronze-lines` saves Entur's line list in bronze/entur_lines once a day; this step
reads all those files and keeps the newest name of every line id. A line that stops running
keeps its last name, so old Gold rows still show it. Gold joins the table with a left join.
"""
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# the part of the Journey Planner answer we use; given, so Spark does not have to guess it
ANSWER_SCHEMA = StructType([StructField("data", StructType([
    StructField("lines", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("publicCode", StringType()),
        StructField("name", StringType()),
        StructField("transportMode", StringType()),
    ]))),
]))])


def read_bronze_lines(spark, data_root):
    """One row per line per saved answer, with the time the answer was fetched."""
    files = Path(data_root) / "bronze" / "entur_lines"
    raw = spark.read.schema(ANSWER_SCHEMA).option("multiLine", True).json(
        str(files / "dt=*" / "entur_lines_*.json.gz"))
    # entur_lines_20261121T031700Z_<run_id>.json.gz -> 2026-11-21 03:17:00
    stamp = F.regexp_extract(F.col("_metadata.file_name"), r"entur_lines_(\d{8}T\d{6})Z", 1)
    return (raw.select(F.explode("data.lines").alias("l"),
                       F.to_timestamp(stamp, "yyyyMMdd'T'HHmmss").alias("fetched_at"))
            .select(F.col("l.id").alias("line"),
                    F.col("l.publicCode").alias("public_code"),
                    F.col("l.name").alias("line_name"),
                    F.col("l.transportMode").alias("transport_mode"),
                    "fetched_at")
            .filter(F.col("line").isNotNull()))


def newest_names(lines):
    """Keep the newest row of every line id (same idea as the Silver dedup)."""
    w = Window.partitionBy("line").orderBy(F.col("fetched_at").desc())
    return lines.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_line_names(spark, cfg):
    """Rebuild reference/line_names from Bronze and count Silver lines that have no name."""
    root = Path(cfg["data_root"])
    names = newest_names(read_bronze_lines(spark, root)).cache()
    names.write.mode("overwrite").parquet(str(root / "reference" / "line_names"))

    # only a check: Gold still works, a line without a name shows its raw id
    missing = 0
    silver_path = root / "silver" / "stop_calls"
    if silver_path.exists():
        silver_lines = spark.read.parquet(str(silver_path)).select("line").distinct()
        missing = silver_lines.join(names, "line", "left_anti").count()
    result = {"line_names": names.count(), "silver_lines_without_name": missing}
    names.unpersist()
    return result