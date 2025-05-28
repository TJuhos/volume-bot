"""Centralised risk controls: inventory caps, fill‑ratio sanity, connectivity health."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Optional

from collections import deque


@dataclass
class RiskConfig:
    max_inventory: float
    kill_on_fill_ratio: float           # e.g. 0.1 → if capture/share <10%
    disconnect_threshold: int = 5       # consecutive WS drops to trigger killswitch
    disconnect_window_sec: int = 60


class RiskManager:
    """Tracks multiple health metrics and raises a fatal flag when any breaches."""

    def __init__(self, cfg: RiskConfig, logger):
        self._cfg = cfg
        self._logger = logger

        # rolling windows
        self._disconnects: Deque[datetime] = deque(maxlen=cfg.disconnect_threshold)
        self._buy_filled = 0.0
        self._sell_filled = 0.0
        self._maker_volume = 0.0
        self._taker_volume = 0.0

        self._kill_event = asyncio.Event()

    # ------- public API ------- #

    @property
    def killed(self) -> bool:  # noqa: D401
        return self._kill_event.is_set()

    async def wait(self):
        """Await until the kill‑switch is pulled."""
        await self._kill_event.wait()

    # ------- inventory ------- #

    def check_inventory(self, position: float):
        if abs(position) >= self._cfg.max_inventory:
            self._logger.error("Hard inventory cap breached: %.2f", position)
            self._kill_event.set()

    # ------- fill ratio ------- #

    def record_fill(self, side: str, size: float):
        if side.lower() == "buy":
            self._buy_filled += size
        else:
            self._sell_filled += size
        self._maker_volume += size
        self._check_fill_ratio()

    def record_trade_seen(self, size: float):
        """Called by data layer whenever a public trade is observed."""
        self._taker_volume += size
        self._check_fill_ratio()

    def _check_fill_ratio(self):
        total = self._taker_volume
        if total == 0:
            return
        share = self._maker_volume / total
        if share < self._cfg.kill_on_fill_ratio:
            self._logger.error("Fill ratio %.3f below threshold %.3f → KILL", share, self._cfg.kill_on_fill_ratio)
            self._kill_event.set()

    # ------- connectivity ------- #

    def record_disconnect(self):
        now = datetime.utcnow()
        self._disconnects.append(now)
        self._logger.warning("WebSocket disconnect recorded @ %s", now.isoformat())
        self._check_disconnects(now)

    def _check_disconnects(self, now: datetime):
        if len(self._disconnects) < self._cfg.disconnect_threshold:
            return
        window = now - self._disconnects[0]
        if window <= timedelta(seconds=self._cfg.disconnect_window_sec):
            self._logger.error(
                "⚠️ %d disconnects inside %ds window → KILL",
                self._cfg.disconnect_threshold,
                self._cfg.disconnect_window_sec,
            )
            self._kill_event.set()
