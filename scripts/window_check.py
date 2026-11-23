"""In which Bronze hours (UTC) do the stops of one service date show up?

Reads the full Bronze days D-1, D and D+1 (the old way) and prints, for service date D, the
rows and trips per Bronze hour. Used once to pick FIRST_HOUR_BEFORE and LAST_HOUR_AFTER in
silver/build.py. Run it for a weekday, a Friday and a Saturday (night buses).

    python scripts/window_check.py --config config/prod.yaml --day 2026-11-06
"""
import argparse
import os
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from entur_pipeline.config import load_config
from entur_pipeline.silver.build import INPUT_SCHEMA, list_bronze_files, read_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--day", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    cfg = load_config(args.config)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName("window-check")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    day = args.day
    paths = [str(p) for p in list_bronze_files(cfg["data_root"], day - timedelta(days=1),
                                               day + timedelta(days=1))]
    rows = spark.sparkContext.parallelize(paths, max(1, len(paths))).flatMap(
        lambda path: read_file(path)[0])
    df = spark.createDataFrame(rows, INPUT_SCHEMA).filter(F.col("service_date") == day)

    per_hour = (df.groupBy(F.date_trunc("hour", "snapshot_time").alias("bronze_hour_utc"))
                .agg(F.count("*").alias("rows"), F.countDistinct("journey_id").alias("trips"))
                .orderBy("bronze_hour_utc"))
    per_hour.show(80, truncate=False)
    first, last = per_hour.agg(F.min("bronze_hour_utc"), F.max("bronze_hour_utc")).first()
    print(f"service date {day}: first Bronze hour {first}, last Bronze hour {last} (UTC)")
    spark.stop()


if __name__ == "__main__":
    main()
