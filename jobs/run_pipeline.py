"""One entry point for every pipeline step: python jobs/run_pipeline.py <step> --config <file>.

Only `bronze` exists so far; `silver` and `gold` are added later.
"""
import argparse
import json
import sys

from entur_pipeline.bronze.ingest import ingest
from entur_pipeline.config import load_config

STEPS = ["bronze"]


def main(argv=None):
    parser = argparse.ArgumentParser(description="DAT535 Entur pipeline")
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--config", required=True, help="config/dev.yaml or config/prod.yaml")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    print(f"[{cfg['env']}] running step: {args.step}")
    if args.step == "bronze":
        record = ingest(cfg)
        print(json.dumps(record, indent=2))
        if record["_bronze_status"] != "valid":
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
