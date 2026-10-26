"""Bronze layer: store one Entur SIRI ET snapshot exactly as received, plus lineage metadata.

Follows the Bronze pattern from Lab 2 (Part 10): the raw data is kept unchanged and every
ingestion gets `_bronze_*` metadata (ingestion time, source, status, error). A response that
is not well-formed XML is still stored, but marked `parse_error`, so nothing is silently lost.

Layout under data_root:
    bronze/entur_siri/dt=YYYY-MM-DD/hour=HH/entur_siri_<UTC time>_<run_id>.xml
    bronze/entur_siri/_metadata/dt=YYYY-MM-DD.jsonl   (one JSON line per run, also failed runs)
"""
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

SOURCE = "entur_siri_et"
RETRY_WAIT_S = 15  # 15 s, 30 s between attempts: stays under Entur's 5 requests/minute


def run_id_from_env():
    """GitHub run id when running in Actions, otherwise 'local' (cron or by hand)."""
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    return f"{run_id}-{attempt}"


def fetch(url, params, headers, timeout, attempts=3, wait_s=RETRY_WAIT_S):
    """GET with retries on network errors, 429 and 5xx. Returns the response, or raises."""
    error = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code != 429 and resp.status_code < 500:
                return resp
            error = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            error = repr(exc)
        if attempt < attempts - 1:
            time.sleep(wait_s * (attempt + 1))
    raise RuntimeError(f"Entur request failed after {attempts} attempts: {error}")


def check_xml(body):
    """Return (status, error) like Lab 2's parse_event_with_metadata: 'valid' or 'parse_error'."""
    try:
        ET.fromstring(body)
        return "valid", None
    except ET.ParseError as exc:
        return "parse_error", str(exc)


def bronze_paths(data_root, ts, run_id):
    base = Path(data_root) / "bronze" / "entur_siri"
    folder = base / f"dt={ts:%Y-%m-%d}" / f"hour={ts:%H}"
    raw_file = folder / f"entur_siri_{ts:%Y%m%dT%H%M%SZ}_{run_id}.xml"
    meta_file = base / "_metadata" / f"dt={ts:%Y-%m-%d}.jsonl"
    return raw_file, meta_file


def write_metadata(meta_file, record):
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    with open(meta_file, "a") as f:
        f.write(json.dumps(record) + "\n")


def ingest(cfg, now=None, wait_s=RETRY_WAIT_S):
    """Fetch one snapshot and store it in Bronze. Returns the metadata record.

    Raises after writing the metadata if the request failed, so a cron job or GitHub
    Actions run shows up as failed while the failure is still recorded in _metadata.
    """
    ts = now or datetime.now(timezone.utc)
    run_id = run_id_from_env()
    e = cfg["entur"]
    params = {"datasetId": e["dataset_id"]}
    headers = {"ET-Client-Name": e["client_name"]}
    raw_file, meta_file = bronze_paths(cfg["data_root"], ts, run_id)

    record = {
        "_bronze_ingestion_time": ts.isoformat(),
        "_bronze_source": SOURCE,
        "_bronze_run_id": run_id,
        "_bronze_env": cfg["env"],
        "_bronze_endpoint": e["et_url"],
        "_bronze_params": params,
        "_bronze_status": None,
        "_bronze_error": None,
        "http_status": None,
        "bytes": None,
        "sha256": None,
        "duration_s": None,
        "file": None,
    }

    started = time.monotonic()
    try:
        resp = fetch(e["et_url"], params, headers, e.get("timeout_s", 60), wait_s=wait_s)
    except RuntimeError as exc:
        record.update(_bronze_status="request_error", _bronze_error=str(exc),
                      duration_s=round(time.monotonic() - started, 2))
        write_metadata(meta_file, record)
        raise

    body = resp.content
    record.update(http_status=resp.status_code, bytes=len(body),
                  sha256=hashlib.sha256(body).hexdigest(),
                  duration_s=round(time.monotonic() - started, 2))

    if not resp.ok:
        record.update(_bronze_status="http_error", _bronze_error=f"HTTP {resp.status_code}")
        write_metadata(meta_file, record)
        resp.raise_for_status()

    status, error = check_xml(body)
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    if raw_file.exists():
        raise FileExistsError(f"Bronze is never overwritten: {raw_file}")
    raw_file.write_bytes(body)  # exactly as received, no parsing or cleaning
    record.update(_bronze_status=status, _bronze_error=error, file=str(raw_file))
    write_metadata(meta_file, record)
    return record
