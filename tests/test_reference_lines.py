from datetime import datetime

from entur_pipeline.reference import lines
from entur_pipeline.reference.lines import fetch_lines, to_rows


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