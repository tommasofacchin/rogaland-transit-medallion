"""Bronze collection summary with Spark: how many runs per day, how many failed, any gaps.

Same idea as the pipeline validation summary in Lab 2 (Part 11), applied to the Bronze
metadata. Usage:  python jobs/bronze_summary.py --config config/prod.yaml
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from entur_pipeline.config import load_config

EXPECTED_RUNS_PER_DAY = 96  # one run every 15 minutes

# Explicit schema (Lab 2, Part 2): no inference pass over the files, and fixed types.
METADATA_SCHEMA = StructType([
    StructField("_bronze_ingestion_time", StringType()),
    StructField("_bronze_source", StringType()),
    StructField("_bronze_run_id", StringType()),
    StructField("_bronze_env", StringType()),
    StructField("_bronze_status", StringType()),
    StructField("_bronze_error", StringType()),
    StructField("http_status", LongType()),
    StructField("bytes", LongType()),
    StructField("sha256", StringType()),
    StructField("file", StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)

    meta_dir = Path(cfg["data_root"]) / "bronze" / "entur_siri" / "_metadata"
    spark = (SparkSession.builder.master("local[*]").appName("bronze-summary")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    meta = (spark.read.schema(METADATA_SCHEMA).option("pathGlobFilter", "*.jsonl").json(str(meta_dir))
            .withColumn("ingested_at", F.to_timestamp("_bronze_ingestion_time"))
            .withColumn("dt", F.to_date("ingested_at")))

    print("=" * 60)
    print(f"BRONZE COLLECTION SUMMARY [{cfg['env']}]")
    print("=" * 60)

    per_day = (meta.groupBy("dt").agg(
        F.count("*").alias("runs"),
        F.sum(F.when(F.col("_bronze_status") == "valid", 1).otherwise(0)).alias("valid"),
        F.sum(F.when(F.col("_bronze_status") != "valid", 1).otherwise(0)).alias("failed"),
        F.countDistinct("sha256").alias("distinct_snapshots"),
        F.round(F.avg("bytes") / 1e6, 2).alias("avg_mb"),
        F.round(F.sum("bytes") / 1e6, 1).alias("total_mb"),
    ).withColumn("missing_runs", F.greatest(F.lit(0), F.lit(EXPECTED_RUNS_PER_DAY) - F.col("runs")))
     .orderBy("dt"))
    per_day.show(50, truncate=False)

    print("Runs by status:")
    meta.groupBy("_bronze_status").count().orderBy("_bronze_status").show(truncate=False)

    # Largest gap between consecutive runs (window function, as in Lab 3 Part 1).
    w = Window.orderBy("ingested_at")
    gaps = (meta.withColumn("prev", F.lag("ingested_at").over(w))
            .withColumn("gap_min", (F.col("ingested_at").cast("long")
                                    - F.col("prev").cast("long")) / 60)
            .filter(F.col("gap_min") > 20)
            .select("prev", "ingested_at", F.round("gap_min", 1).alias("gap_min"))
            .orderBy(F.desc("gap_min")))
    print("Gaps longer than 20 minutes:")
    gaps.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
