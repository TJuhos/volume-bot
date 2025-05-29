"""Volume Bot package initialization"""

from pathlib import Path
from typing import Union
import re

import yaml

__version__ = "0.1.0"


def _validate_api_key(key: str) -> str:
    """Validate and clean API key format."""
    # Remove any whitespace
    key = key.strip()
    
    # Check length (Binance API keys are typically 64-72 characters)
    if len(key) < 64 or len(key) > 72:
        raise ValueError(f"API key must be between 64 and 72 characters long, got {len(key)}")
    
    # Check if it contains only valid characters (alphanumeric)
    if not re.match(r'^[a-zA-Z0-9]+$', key):
        raise ValueError("API key must contain only alphanumeric characters (a-z, A-Z, 0-9)")
    
    return key


def load_config(path: Union[str, Path, None] = None) -> dict:
    """Load and parse the YAML configuration.

    Parameters
    ----------
    path
        Path to a YAML file. If *None*, defaults to ``config.yaml`` in the
        package directory.

    Returns
    -------
    dict
        Parsed configuration dictionary.
    """
    path = Path(path) if path else Path(__file__).with_name("config.yaml")
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Validate API credentials
    if "exchange" in config:
        if "api_key" in config["exchange"]:
            config["exchange"]["api_key"] = _validate_api_key(config["exchange"]["api_key"])
        if "api_secret" in config["exchange"]:
            config["exchange"]["api_secret"] = _validate_api_key(config["exchange"]["api_secret"])
    
    return config