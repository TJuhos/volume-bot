"""Opinionated coloured stdout + optional rotating‑file logger."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


COL_FMT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FMT = "%Y‑%m‑%d %H:%M:%S"


def _build_handler(to_file: Optional[Path] = None) -> logging.Handler:
    if to_file:
        handler = RotatingFileHandler(to_file, maxBytes=5 * 1024 * 1024, backupCount=3)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt=COL_FMT, datefmt=DATE_FMT))
    return handler


def setup_logger(level: str = "INFO", path: str | None = None) -> logging.Logger:
    """Create and return the root logger configured once per process."""
    root = logging.getLogger("volbot")
    if root.handlers:  # already configured
        return root

    lvl = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(lvl)

    # Console always
    root.addHandler(_build_handler())
    # Optional file
    if path:
        root.addHandler(_build_handler(Path(path)))

    root.debug("Logger initialised at level %s", level)
    return root


def get_child_logger(parent: logging.Logger, name: str) -> logging.Logger:
    """Return a namespaced child logger under *parent*."""
    return parent.getChild(name)
