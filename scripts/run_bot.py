"""Thin wrapper so users can simply `python -m scripts.run_bot -c custom.yaml`."""

import importlib
import sys
from pathlib import Path

# Allow `python -m scripts.run_bot` from repo root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.main import cli  # noqa: E402  pylint: disable=wrong-import-position


if __name__ == "__main__":
    cli()
