"""Abstract connector interface for any exchange."""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    last_update_id: int
    timestamp: datetime


@dataclass
class Trade:
    trade_id: int
    price: float
    size: float
    side: str  # "buy" or "sell"
    timestamp: datetime


@dataclass
class Order:
    client_order_id: str
    exchange_order_id: Optional[int]
    symbol: str
    side: str  # "BUY" | "SELL"
    price: float
    size: float
    status: str  # NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED
    timestamp: datetime


class BaseConnector(abc.ABC):
    """Minimal async interface every concrete exchange connector must satisfy."""

    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger

    # ---------- Market‑data ---------- #

    @abc.abstractmethod
    async def get_order_book_snapshot(self, symbol: str) -> OrderBookSnapshot:  # noqa: D401
        """Return an initial full depth snapshot."""

    @abc.abstractmethod
    async def stream_order_book_diffs(self, symbol: str):
        """Asynchronous generator yielding incremental book diffs."""

    @abc.abstractmethod
    async def stream_trades(self, symbol: str):
        """Asynchronous generator yielding public trades."""

    # ---------- Trading ---------- #

    @abc.abstractmethod
    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        client_order_id: str,
    ) -> Order:
        """Send a LIMIT_MAKER (maker‑only) order."""

    @abc.abstractmethod
    async def cancel_order(self, symbol: str, client_order_id: str) -> None:  # noqa: D401
        """Cancel an open order."""

    @abc.abstractmethod
    async def stream_user_events(self):
        """Asynchronous generator for auth'd user events (fills, cancels)."""

    # ---------- Housekeeping ---------- #

    @abc.abstractmethod
    async def close(self):
        """Gracefully shut down sessions / sockets."""

    # ---------- Helpers ---------- #

    async def _sleep(self, seconds: float):
        """Wrapper around asyncio.sleep so connectors can monkey‑patch latency in tests."""
        await asyncio.sleep(seconds)