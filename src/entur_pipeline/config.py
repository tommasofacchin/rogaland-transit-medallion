"""Read config/dev.yaml or config/prod.yaml."""
import yaml

REQUIRED = ["env", "data_root", "interval_min", "entur"]
REQUIRED_ENTUR = ["et_url", "dataset_id", "client_name"]


def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f) or {}
    if not cfg:
        raise ValueError(f"Config {path} is empty")
    missing = [k for k in REQUIRED if k not in cfg]
    missing += ["entur." + k for k in REQUIRED_ENTUR if k not in cfg.get("entur", {})]
    if missing:
        raise ValueError(f"Config {path} is missing keys: {missing}")
    return cfg