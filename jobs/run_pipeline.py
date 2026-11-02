"""One entry point for every pipeline step: python jobs/run_pipeline.py <step> --config <file>.

`bronze` and `silver` exist so far; `gold` is added later.
"""

import argparse
import json
import os
import sys
from datetime import date

from entur_pipeline.bronze.ingest import ingest
from entur_pipeline.config import load_config

STEPS = ["bronze", "silver"]


def main(argv=None):
    parser = argparse.ArgumentParser(description="DAT535 Entur pipeline")
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--config", required=True, help="config/dev.yaml or config/prod.yaml")
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat,
                        help="silver: first Bronze day (dt=), YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat,
                        help="silver: last Bronze day (dt=), YYYY-MM-DD")

    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    print(f"[{cfg['env']}] running step: {args.step}")
    if args.step == "bronze":
        record = ingest(cfg)
        print(json.dumps(record, indent=2))
        if record["_bronze_status"] != "valid":
            return 1
    elif args.step == "silver":

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

        from pyspark.sql import SparkSession
        
        from entur_pipeline.silver.build import build_silver

        spark = SparkSession.builder.appName("entur-silver").getOrCreate()
        try:
            result = build_silver(spark, cfg, args.date_from, args.date_to)
        finally:
            spark.stop()
        print(json.dumps(result, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
