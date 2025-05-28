"""Volume Bot package initialization"""

from pathlib import Path
from typing import Union

import yaml

__version__ = "0.1.0"


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
        return yaml.safe_load(f)