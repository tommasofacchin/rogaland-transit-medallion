"""Short Markdown summary of one day for the GitHub run page: Bronze runs and the Gold status.

python scripts/run_summary.py --config config/prod.yaml --day 2026-11-10
On prod it exits with 1 if Bronze has fewer than MIN_VALID_RUNS valid snapshots that day.
"""
import argparse
import json
import sys
from pathlib import Path

from entur_pipeline.config import load_config

# 288 runs a day at 5 min; a few missing is normal, more than ~6% means cron had a problem
MIN_VALID_RUNS = 270


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--day", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    cfg = load_config(args.config)
    root = Path(cfg["data_root"])

    # Bronze metadata is per UTC day, close enough to the service day for this check
    runs = read_jsonl(root / "bronze" / "entur_siri" / "_metadata" / f"dt={args.day}.jsonl")
    valid = sum(r.get("_bronze_status") == "valid" for r in runs)
    gold = [r for r in read_jsonl(root / "gold" / "_status" / "status.jsonl")
            if r.get("date_from") == args.day]
    last = gold[-1] if gold else {}

    print(f"### Pipeline for {args.day} ({cfg['env']})")
    print("| | |")
    print("|---|---|")
    print(f"| Bronze runs | {len(runs)} ({valid} valid, 288 expected) |")
    print(f"| Silver rows | {last.get('silver_rows', '-')} |")
    print(f"| Gold status | {last.get('status', 'no run')} |")
    print(f"| Gold problems | {', '.join(last.get('problems', [])) or 'none'} |")

    # dev only has a few test snapshots, so the check is only for prod
    if cfg["env"] == "prod" and valid < MIN_VALID_RUNS:
        print(f"\nBronze is incomplete: {valid} valid runs, at least {MIN_VALID_RUNS} needed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())