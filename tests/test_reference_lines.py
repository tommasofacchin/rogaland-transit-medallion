import gzip
import json
from datetime import datetime, timezone
 
from entur_pipeline.bronze import lines as bronze_lines
from entur_pipeline.bronze.lines import check_answer, ingest_lines, lines_paths
from entur_pipeline.reference.lines import build_line_names
 
 
class FakeResponse:
    status_code = 200
 
    def __init__(self, body):
        self.content = json.dumps(body).encode()
 
    def raise_for_status(self):
        pass
 
 
def answer(*lines):
    return {"data": {"lines": [{"id": i, "publicCode": code, "name": "x",
                                "transportMode": "bus"} for i, code in lines]}}
 
 
def save_answer(root, ts, body):
    raw_file, _ = lines_paths(root, ts, "local-1")
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_bytes(gzip.compress(json.dumps(body).encode()))
 
 
def test_check_answer():
    assert check_answer(json.dumps(answer(("L1", "1"))))[0] == "valid"
    assert check_answer(json.dumps({"data": {"lines": []}}))[0] == "empty"
    assert check_answer(json.dumps({"errors": [{"message": "bad"}]}))[0] == "api_error"
    assert check_answer(b"<html>")[0] == "parse_error"
 
 
def test_ingest_lines_saves_raw_answer_and_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_lines.requests, "post",
                        lambda *a, **k: FakeResponse(answer(("L1", "1"), ("L2", "2"))))
    cfg = {"env": "test", "data_root": str(tmp_path), "entur": {"client_name": "test"}}
    now = datetime(2026, 11, 21, 3, 17, tzinfo=timezone.utc)
 
    record = ingest_lines(cfg, now=now)
 
    assert record["_bronze_status"] == "valid" and record["lines"] == 2
    raw_file, meta_file = lines_paths(tmp_path, now, record["_bronze_run_id"])
    assert json.loads(gzip.decompress(raw_file.read_bytes())) == answer(("L1", "1"), ("L2", "2"))
    assert json.loads(meta_file.read_text())["file"] == str(raw_file)
 
 
def test_build_line_names_keeps_newest_name_and_old_lines(spark, tmp_path):
    save_answer(tmp_path, datetime(2026, 11, 20, 3, 17), answer(("L1", "old"), ("L2", "2")))
    # next day L1 got a new code and L2 is gone from the list
    save_answer(tmp_path, datetime(2026, 11, 21, 3, 17), answer(("L1", "new"), ("L3", "3")))
    spark.createDataFrame([("L1",), ("L9",)], ["line"]).write.parquet(
        str(tmp_path / "silver" / "stop_calls"))
 
    result = build_line_names(spark, {"data_root": str(tmp_path)})
 
    saved = {r.line: r.public_code for r in
             spark.read.parquet(str(tmp_path / "reference" / "line_names")).collect()}
    assert saved == {"L1": "new", "L2": "2", "L3": "3"}
    assert result == {"line_names": 3, "silver_lines_without_name": 1}  # L9
 