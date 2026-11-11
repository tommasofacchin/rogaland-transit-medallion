from datetime import datetime

from entur_pipeline.reference import lines
from entur_pipeline.reference.lines import fetch_lines, new_line_ids, to_rows


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_fetch_lines_asks_in_batches_and_skips_unknown_ids(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append(json["variables"]["ids"])
        found = [{"id": i, "publicCode": i[-1], "name": "x", "transportMode": "bus"}
                 for i in json["variables"]["ids"] if i != "L-unknown"]
        return FakeResponse({"data": {"lines": found + [None]}})

    monkeypatch.setattr(lines, "BATCH", 2)
    monkeypatch.setattr(lines.requests, "post", fake_post)

    result = fetch_lines(["L1", "L2", "L-unknown"], "test-client")

    assert calls == [["L1", "L2"], ["L-unknown"]]
    assert [line["id"] for line in result] == ["L1", "L2"]


def test_fetch_lines_raises_on_graphql_error(monkeypatch):
    monkeypatch.setattr(lines.requests, "post",
                        lambda *a, **k: FakeResponse({"errors": [{"message": "bad"}]}))
    try:
        fetch_lines(["L1"], "test-client")
    except RuntimeError as exc:
        assert "bad" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_to_rows():
    t = datetime(2026, 11, 7, 15, 0)
    line = {"id": "KOL:Line:2_6", "publicCode": "6", "name": "Sola", "transportMode": "bus"}
    assert to_rows([line], t) == [("KOL:Line:2_6", "6", "Sola", "bus", t)]


def test_new_line_ids():
    assert new_line_ids(["L2", "L1", "L3"], ["L1"]) == ["L2", "L3"]
    assert new_line_ids(["L1"], ["L1", "L9"]) == []


def test_build_line_names_keeps_old_names_and_asks_only_for_new(spark, tmp_path, monkeypatch):
    asked = []

    def fake_fetch(ids, client_name):
        asked.append(list(ids))
        return [{"id": i, "publicCode": i[-1], "name": "x", "transportMode": "bus"}
                for i in ids]

    monkeypatch.setattr(lines, "fetch_lines", fake_fetch)
    cfg = {"data_root": str(tmp_path), "entur": {"client_name": "test"}}
    silver = str(tmp_path / "silver" / "stop_calls")

    spark.createDataFrame([("L1",), ("L2",)], ["line"]).write.parquet(silver)
    lines.build_line_names(spark, cfg)
    # next day L2 is gone from Silver and L3 is new
    spark.createDataFrame([("L1",), ("L3",)], ["line"]).write.mode("overwrite").parquet(silver)
    result = lines.build_line_names(spark, cfg)

    assert asked == [["L1", "L2"], ["L3"]]
    assert result == {"line_ids": 2, "known": 2, "asked": 1, "found": 1, "unknown": 0}
    saved = spark.read.parquet(str(tmp_path / "reference" / "line_names"))
    assert sorted(r.line for r in saved.collect()) == ["L1", "L2", "L3"]


def test_build_line_names_saves_unknown_ids_and_asks_only_once(spark, tmp_path, monkeypatch):
    asked = []

    def fake_fetch(ids, client_name):
        asked.append(list(ids))
        return [{"id": i, "publicCode": "1", "name": "x", "transportMode": "bus"}
                for i in ids if i != "L9"]  # Journey Planner does not know L9

    monkeypatch.setattr(lines, "fetch_lines", fake_fetch)
    cfg = {"data_root": str(tmp_path), "entur": {"client_name": "test"}}
    spark.createDataFrame([("L1",), ("L9",)], ["line"]).write.parquet(
        str(tmp_path / "silver" / "stop_calls"))

    first = lines.build_line_names(spark, cfg)
    second = lines.build_line_names(spark, cfg)

    assert asked == [["L1", "L9"]]
    assert first["unknown"] == 1 and second["asked"] == 0
    saved = spark.read.parquet(str(tmp_path / "reference" / "line_names"))
    l9 = [r for r in saved.collect() if r.line == "L9"][0]
    assert l9.public_code is None and l9.line_name is None
