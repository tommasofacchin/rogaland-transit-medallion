import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from entur_pipeline.bronze import ingest as bronze

NOW = datetime(2026, 10, 27, 14, 30, 5, tzinfo=timezone.utc)
SAMPLE = b"<Siri><ServiceDelivery><EstimatedTimetableDelivery/></ServiceDelivery></Siri>"


class FakeResponse:
    def __init__(self, body, status=200):
        self.content = body
        self.status_code = status
        self.ok = status < 400

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def make_cfg(tmp_path):
    return {
        "env": "test",
        "data_root": str(tmp_path),
        "entur": {"et_url": "https://example.test/et", "dataset_id": "KOL",
                  "client_name": "test", "timeout_s": 5},
    }


def fake_get(responses):
    """requests.get replacement returning the given responses (or raising exceptions) in order."""
    calls = iter(responses)

    def _get(*args, **kwargs):
        item = next(calls)
        if isinstance(item, Exception):
            raise item
        return item
    return _get


def read_meta(tmp_path):
    meta = tmp_path / "bronze" / "entur_siri" / "_metadata" / "dt=2026-10-27.jsonl"
    return [json.loads(line) for line in meta.read_text().splitlines()]


def test_raw_body_is_stored_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze.requests, "get", fake_get([FakeResponse(SAMPLE)]))
    record = bronze.ingest(make_cfg(tmp_path), now=NOW)

    assert Path(record["file"]).read_bytes() == SAMPLE
    assert record["_bronze_status"] == "valid"
    assert record["http_status"] == 200
    assert record["bytes"] == len(SAMPLE)
    assert read_meta(tmp_path) == [record]


def test_partition_layout(tmp_path):
    raw_file, meta_file = bronze.bronze_paths(tmp_path, NOW, "123-1")
    assert raw_file.relative_to(tmp_path).as_posix() == (
        "bronze/entur_siri/dt=2026-10-27/hour=14/entur_siri_20261027T143005Z_123-1.xml")
    assert meta_file.name == "dt=2026-10-27.jsonl"


def test_invalid_xml_is_kept_but_flagged(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze.requests, "get", fake_get([FakeResponse(b"<Siri><broken")]))
    record = bronze.ingest(make_cfg(tmp_path), now=NOW)

    assert record["_bronze_status"] == "parse_error"
    assert Path(record["file"]).read_bytes() == b"<Siri><broken"


def test_http_error_is_logged_and_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze.requests, "get", fake_get([FakeResponse(b"bad", status=400)]))
    with pytest.raises(requests.HTTPError):
        bronze.ingest(make_cfg(tmp_path), now=NOW)

    [record] = read_meta(tmp_path)
    assert record["_bronze_status"] == "http_error"
    assert record["file"] is None


def test_retries_then_succeeds(tmp_path, monkeypatch):
    responses = [requests.ConnectionError("down"), FakeResponse(b"", status=503),
                 FakeResponse(SAMPLE)]
    monkeypatch.setattr(bronze.requests, "get", fake_get(responses))
    record = bronze.ingest(make_cfg(tmp_path), now=NOW, wait_s=0)
    assert record["_bronze_status"] == "valid"


def test_request_failure_is_logged_and_raises(tmp_path, monkeypatch):
    errors = [requests.ConnectionError("down")] * 3
    monkeypatch.setattr(bronze.requests, "get", fake_get(errors))
    with pytest.raises(RuntimeError):
        bronze.ingest(make_cfg(tmp_path), now=NOW, wait_s=0)

    [record] = read_meta(tmp_path)
    assert record["_bronze_status"] == "request_error"


def test_bronze_is_never_overwritten(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze.requests, "get",
                        fake_get([FakeResponse(SAMPLE), FakeResponse(SAMPLE)]))
    bronze.ingest(make_cfg(tmp_path), now=NOW)
    with pytest.raises(FileExistsError):
        bronze.ingest(make_cfg(tmp_path), now=NOW)


def test_compressed_snapshot_decompresses_to_original(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze.requests, "get", fake_get([FakeResponse(SAMPLE)]))
    cfg = make_cfg(tmp_path) | {"compress": True}
    record = bronze.ingest(cfg, now=NOW)

    assert record["file"].endswith(".xml.gz")
    assert gzip.decompress(Path(record["file"]).read_bytes()) == SAMPLE
    assert record["bytes"] == len(SAMPLE)
    assert record["stored_bytes"] == Path(record["file"]).stat().st_size
