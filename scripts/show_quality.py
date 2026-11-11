"""Print the Silver quality numbers for one service date.

python scripts/show_quality.py --config config/prod.yaml --day 2026-11-12
"""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from entur_pipeline.config import load_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--day", required=True, help="service date, YYYY-MM-DD")
    args = parser.parse_args()
    cfg = load_config(args.config)

    spark = SparkSession.builder.appName("show-quality").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # older rows have no date columns, mergeSchema gives them nulls
    q = spark.read.option("mergeSchema", "true").parquet(
        str(Path(cfg["data_root"]) / "silver" / "quality"))
    day = q.filter(f"date_from <= '{args.day}' AND date_to >= '{args.day}'")
    day.select("run_id", "run_time", "metric", "value").orderBy("run_time", "metric").show(100, False)
    spark.stop()


if __name__ == "__main__":
    main()