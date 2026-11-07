#!/usr/bin/env bash
# Run by cron on the prod VM every 5 minutes.
# flock skips a run if the previous one is still going (same idea as `concurrency` in Actions).
set -uo pipefail
ENV="${1:-prod}"
APP="${APP_DIR:-$HOME/app}"
LOG_DIR="$HOME/logs"; mkdir -p "$LOG_DIR"
cd "$APP" || exit 1
exec flock -n "/tmp/entur-bronze-${ENV}.lock" \
  "$HOME/venv/bin/python" jobs/run_pipeline.py bronze --config "config/${ENV}.yaml" \
  >> "$LOG_DIR/bronze-${ENV}.log" 2>&1
