"""Bronze: save Entur's list of Kolumbus lines (Journey Planner API) as it was received.

Same rules as the SIRI snapshots in ingest.py: the raw answer is kept as gzip, files are
never overwritten, and every run (also a failed one) adds one metadata line. The list is
small (a few hundred lines), so we save the whole list once a day.
"""
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from entur_pipeline.bronze.ingest import run_id_from_env, write_metadata, write_snapshot

SOURCE = "entur_lines"
JP_URL = "https://api.entur.io/journey-planner/v3/graphql"
# Kolumbus lines are filed under 4 authorities in Journey Planner (checked 21 Nov: 316 + 2 + 1 + 1
# = 320 KOL lines). A new authority would show up as silver_lines_without_name in step `lines`.
AUTHORITIES = ["KOL:Authority:8", "KOL:Authority:KOL", "KOL:Authority:30", "KOL:Authority:26"]
QUERY = ("query($authorities: [String]) { lines(authorities: $authorities) "
         "{ id publicCode name transportMode } }")


def lines_paths(data_root, ts, run_id):
    base = Path(data_root) / "bronze" / SOURCE
    raw_file = base / f"dt={ts:%Y-%m-%d}" / f"entur_lines_{ts:%Y%m%dT%H%M%SZ}_{run_id}.json.gz"
    meta_file = base / "_metadata" / f"dt={ts:%Y-%m-%d}.jsonl"
    return raw_file, meta_file


def check_answer(body):
    """Return (status, error, number of lines). An empty list counts as an error."""
    try:
        answer = json.loads(body)
    except ValueError as exc:
        return "parse_error", str(exc), None
    if answer.get("errors"):
        return "api_error", str(answer["errors"])[:500], None
    lines = (answer.get("data") or {}).get("lines") or []
    if not lines:
        return "empty", "no lines in the answer", 0
    return "valid", None, len(lines)


def ingest_lines(cfg, now=None, timeout=60):
    """Fetch the line list and save it. If the request fails, write the metadata line, then raise."""
    ts = now or datetime.now(timezone.utc)
    run_id = run_id_from_env()
    raw_file, meta_file = lines_paths(cfg["data_root"], ts, run_id)
    record = {
        "_bronze_ingestion_time": ts.isoformat(),
        "_bronze_source": SOURCE,
        "_bronze_run_id": run_id,
        "_bronze_env": cfg["env"],
        "_bronze_endpoint": JP_URL,
        "_bronze_status": None,
        "_bronze_error": None,
        "http_status": None,
        "bytes": None,
        "sha256": None,
        "lines": None,
        "file": None,
    }
    try:
        resp = requests.post(JP_URL, timeout=timeout,
                             headers={"ET-Client-Name": cfg["entur"]["client_name"]},
                             json={"query": QUERY, "variables": {"authorities": AUTHORITIES}})
        resp.raise_for_status()
    except requests.RequestException as exc:
        record.update(_bronze_status="request_error", _bronze_error=repr(exc))
        write_metadata(meta_file, record)
        raise

    body = resp.content
    status, error, n_lines = check_answer(body)
    write_snapshot(raw_file, gzip.compress(body))  # also a bad answer is kept, like SIRI
    record.update(_bronze_status=status, _bronze_error=error, http_status=resp.status_code,
                  bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), lines=n_lines,
                  file=str(raw_file))
    write_metadata(meta_file, record)
    return record