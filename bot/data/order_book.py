"""In‑memory order book helper for best‑bid/ask, mid, and VWAP utilities."""

from __future__ import annotations

from typing import List

from bot.connectors.base import OrderBookLevel, OrderBookSnapshot


class OrderBook:
    """Lightweight order‑book representation."""

    def __init__(self, snapshot: OrderBookSnapshot):
        self._bids: List[OrderBookLevel] = snapshot.bids
        self._asks: List[OrderBookLevel] = snapshot.asks

    # ---------- Core quotes ---------- #

    @property
    def best_bid(self) -> float:
        return self._bids[0].price if self._bids else 0.0

    @property
    def best_ask(self) -> float:
        return self._asks[0].price if self._asks else 0.0

    @property
    def mid_price(self) -> float:
        if not (self._bids and self._asks):
            return 0.0
        return (self.best_bid + self.best_ask) / 2

    # ---------- Maintenance ---------- #

    def update(self, snapshot: OrderBookSnapshot):
        """Replace depth with a fresh snapshot."""
        self._bids = snapshot.bids
        self._asks = snapshot.asks

    # ---------- Utilities ---------- #

    def vwap_for_size(self, side: str, size: float) -> float:
        """Average execution price to fill *size* units via market order."""
        levels = self._asks if side.lower() == "buy" else self._bids
        remaining = size
        notional = 0.0
        for lvl in levels:
            take = min(remaining, lvl.size)
            notional += take * lvl.price
            remaining -= take
            if remaining <= 0:
                break
        if remaining > 0:
            raise ValueError("Insufficient book depth for requested size")
        return notional / size
