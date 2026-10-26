"""Load the YAML config for one environment (dev or prod)."""
import yaml

REQUIRED_KEYS = ["env", "data_root", "interval_min", "entur"]
REQUIRED_ENTUR_KEYS = ["et_url", "dataset_id", "client_name"]


def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        # ValueError, not TypeError: a bad file is bad config, same as a missing key.
        raise ValueError(f"Config {path} is empty or not a YAML mapping")  # noqa: TRY004
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if isinstance(cfg.get("entur"), dict):
        missing += [f"entur.{k}" for k in REQUIRED_ENTUR_KEYS if k not in cfg["entur"]]
    elif "entur" in cfg:
        missing += [f"entur.{k}" for k in REQUIRED_ENTUR_KEYS]
    if missing:
        raise ValueError(f"Config {path} is missing keys: {missing}")
    return cfg
