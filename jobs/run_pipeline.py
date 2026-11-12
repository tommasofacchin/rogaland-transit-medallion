"""One entry point for every pipeline step: python jobs/run_pipeline.py <step> --config <file>."""

import argparse
import json
import os
import sys
from datetime import date

from entur_pipeline.bronze.ingest import ingest
from entur_pipeline.config import load_config

STEPS = ["bronze", "silver", "lines", "gold"]


def start_spark(name):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    # default is 1 GB, too little for a day of snapshots (the VMs have 8 GB)
    return (SparkSession.builder.appName(name)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.session.timeZone", "UTC")  # all our times are UTC
            .config("spark.sql.shuffle.partitions", "8")  # ~150k rows a day, see decisions
            .getOrCreate())


def main(argv=None):
    parser = argparse.ArgumentParser(description="DAT535 Entur pipeline")
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--config", required=True, help="config/dev.yaml or config/prod.yaml")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat,
                        help="silver/gold: first service date, YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat,
                        help="silver/gold: last service date, YYYY-MM-DD")

    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    print(f"[{cfg['env']}] running step: {args.step}")
    if args.step == "bronze":
        record = ingest(cfg)
        print(json.dumps(record, indent=2))
        if record["_bronze_status"] != "valid":
            return 1
    elif args.step == "silver":
        from entur_pipeline.silver.build import build_silver

        spark = start_spark("entur-silver")
        try:
            result = build_silver(spark, cfg, args.date_from, args.date_to, run_id=run_id)
        finally:
            spark.stop()
        print(json.dumps(result, indent=2))
    elif args.step == "lines":
        from entur_pipeline.reference.lines import build_line_names

        spark = start_spark("entur-lines")
        try:
            result = build_line_names(spark, cfg)
        finally:
            spark.stop()
        print(json.dumps(result, indent=2))
    elif args.step == "gold":
        from entur_pipeline.gold.build import build_gold

        spark = start_spark("entur-gold")
        try:
            result = build_gold(spark, cfg, args.date_from, args.date_to, run_id=run_id)
        finally:
            spark.stop()
        print(json.dumps(result, indent=2, default=str))
        if result["status"] != "ok":
            return 1  # so the workflow run shows up red

    return 0


if __name__ == "__main__":
    sys.exit(main())