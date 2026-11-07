"""Summary of the Bronze collection: runs per day, failed runs and gaps.

Usage: python jobs/bronze_summary.py --config config/prod.yaml
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from entur_pipeline.config import load_config

# fixed schema, so Spark doesn't have to guess the types
METADATA_SCHEMA = StructType([
    StructField("_bronze_ingestion_time", StringType()),
    StructField("_bronze_source", StringType()),
    StructField("_bronze_run_id", StringType()),
    StructField("_bronze_env", StringType()),
    StructField("_bronze_status", StringType()),
    StructField("_bronze_error", StringType()),
    StructField("http_status", LongType()),
    StructField("bytes", LongType()),
    StructField("stored_bytes", LongType()),
    StructField("sha256", StringType()),
    StructField("file", StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)
    interval = cfg["interval_min"]

    meta_dir = Path(cfg["data_root"]) / "bronze" / "entur_siri" / "_metadata"
   # dt= folders are UTC days, so Spark should use UTC too
    spark = (SparkSession.builder.master("local[*]").appName("bronze-summary")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    meta = (spark.read.schema(METADATA_SCHEMA).option("pathGlobFilter", "*.jsonl").json(str(meta_dir))
            .withColumn("ingested_at", F.to_timestamp("_bronze_ingestion_time"))
            .withColumn("dt", F.to_date("ingested_at")))

    print("=" * 60)
    print(f"BRONZE COLLECTION SUMMARY [{cfg['env']}]")
    print("=" * 60)
    
    # a full day has 24 * 60 / 5 = 288 runs. The first day and today are not full days, so they always show some missing runs
    runs_per_day = 24 * 60 // interval

    per_day = (meta.groupBy("dt").agg(
        F.count("*").alias("runs"),
        F.sum(F.when(F.col("_bronze_status") == "valid", 1).otherwise(0)).alias("valid"),
        F.sum(F.when(F.col("_bronze_status") != "valid", 1).otherwise(0)).alias("failed"),
        F.countDistinct("sha256").alias("distinct_snapshots"),
        F.round(F.avg("bytes") / 1e6, 2).alias("avg_mb"),
        F.round(F.sum("bytes") / 1e6, 1).alias("total_mb"),
        F.round(F.sum(F.coalesce("stored_bytes", "bytes")) / 1e6, 1).alias("on_disk_mb"),
    )
        .withColumn("missing_runs", F.greatest(F.lit(0), runs_per_day - F.col("runs")))
        .orderBy("dt"))
    per_day.show(50, truncate=False)

    print("Runs by status:")
    meta.groupBy("_bronze_status").count().orderBy("_bronze_status").show(truncate=False)

    # gaps between valid snapshots; failed runs don't count because they have no data
    w = Window.orderBy("ingested_at")
    gaps = (meta.filter(F.col("_bronze_status") == "valid")
            .withColumn("prev", F.lag("ingested_at").over(w))
            .withColumn("gap_min", (F.col("ingested_at").cast("long")
                                    - F.col("prev").cast("long")) / 60)
            .filter(F.col("gap_min") > 2 * interval)
            .select("prev", "ingested_at", F.round("gap_min", 1).alias("gap_min"))
            .orderBy(F.desc("gap_min")))
    print(f"Gaps between valid snapshots longer than {2 * interval} minutes:")
    gaps.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
