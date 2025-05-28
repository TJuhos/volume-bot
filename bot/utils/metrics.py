"""Simple Prometheus‑style metrics stub (expandable)."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path
from typing import Dict


class Metrics:
    """Naïve async‑safe counter & gauge collector writing to a textfile gateway."""

    def __init__(self, path: str | Path = "/tmp/volbot_metrics.txt"):
        self._metrics: Dict[str, float] = defaultdict(float)
        self._lock = asyncio.Lock()
        self._path = Path(path)

    async def incr(self, key: str, amt: float = 1.0):
        async with self._lock:
            self._metrics[key] += amt

    async def set(self, key: str, val: float):
        async with self._lock:
            self._metrics[key] = val

    async def flush(self):
        content = "\n".join(f"{k} {v}" for k, v in self._metrics.items()) + "\n"
        self._path.write_text(content)
