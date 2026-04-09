import json
from pathlib import Path
from typing import Any, Dict

_CONFIG: Dict[str, Any] = {}


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load config.json. Cached after first call."""
    global _CONFIG
    if _CONFIG:
        return _CONFIG

    if config_path is None:
        # config.json lives next to the graph/ package directory
        config_path = str(Path(__file__).parent.parent / "config.json")

    with open(config_path, encoding="utf-8") as f:
        _CONFIG = json.load(f)
    return _CONFIG
