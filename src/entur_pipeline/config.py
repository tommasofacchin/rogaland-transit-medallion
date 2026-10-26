"""Load the YAML config for one environment (dev or prod)."""
import yaml

REQUIRED_KEYS = ["env", "data_root", "entur"]


def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        raise ValueError(f"Config {path} is missing keys: {missing}")
    return cfg
