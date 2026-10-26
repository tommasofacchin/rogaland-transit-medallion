import pytest

from entur_pipeline.config import load_config

VALID = """\
env: test
interval_min: 5
data_root: /tmp/x
entur:
  et_url: https://example.test/et
  dataset_id: KOL
  client_name: test
"""


def write(tmp_path, text):
    path = tmp_path / "cfg.yaml"
    path.write_text(text)
    return path


def test_valid_config_loads(tmp_path):
    assert load_config(write(tmp_path, VALID))["entur"]["dataset_id"] == "KOL"


def test_empty_config_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="empty"):
        load_config(write(tmp_path, ""))


def test_missing_entur_key_is_reported(tmp_path):
    with pytest.raises(ValueError, match="entur.client_name"):
        load_config(write(tmp_path, VALID.replace("  client_name: test\n", "")))


def test_shipped_configs_are_valid():
    for env in ("dev", "prod"):
        load_config(f"config/{env}.yaml")
