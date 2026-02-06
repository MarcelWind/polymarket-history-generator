import yaml
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class AppConfig:
    market_queries: list[str]
    candle_interval_seconds: int = 60
    discovery_interval_seconds: int = 300
    flush_interval_seconds: int = 120
    data_dir: str = "data"
    log_level: str = "INFO"
    verbose: bool = False


def load_config(config_path: str = "config.yaml") -> AppConfig:
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not raw.get("market_queries"):
        raise ValueError("config.yaml must have at least one entry in 'market_queries'")

    return AppConfig(**raw)
