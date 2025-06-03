"""Core quoting algorithm for the volume-providing market maker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from bot.data.order_book import OrderBook
from .inventory import InventoryManager
from .pricing import QuoteBand


@dataclass
class MarketMakerConfig:
    maker_spread_bps: float  # half‑spread in bps
    order_size: float        # per‑side quote size


class MarketMakerStrategy:
    """Compute target bid/ask prices & sizes based on order book and inventory state."""

    def __init__(self, mm_cfg: MarketMakerConfig, inv_mgr: InventoryManager):
        self._cfg = mm_cfg
        self._inv = inv_mgr

    # ---- public API ---- #

    async def quote(self, ob: OrderBook) -> Tuple[QuoteBand, float, float]:
        """Return (QuoteBand, bid_size, ask_size)."""
        mid = ob.mid_price
        if mid == 0.0:
            raise ValueError("Cannot quote without a valid mid‑price")

        base_spread = mid * (self._cfg.maker_spread_bps / 10_000)
        skew_spread = mid * (self._inv.skew_bps() / 10_000)

        bid = mid - base_spread - skew_spread
        ask = mid + base_spread - skew_spread

        # Round to 2 decimals — adjust as per exchange tick size
        band = QuoteBand(bid=round(bid, 2), ask=round(ask, 2))

        # Size gating vs hard cap
        size = self._cfg.order_size
        bid_size = size if self._inv.position > -self._inv.max_position else 0.0
        ask_size = size if self._inv.position < self._inv.max_position else 0.0

        return band, bid_size, ask_size

    def _round_price(self, price: float) -> float:
        """Round price to meet exchange precision requirements."""
        if self._price_precision is None:
            return price  # Return as is if precision not yet known
        return round(price, self._price_precision)

    async def _get_symbol_info(self, symbol: str) -> None:
        """Get symbol info from exchange if not already cached."""
        if self._symbol_info is None:
            self._symbol_info = await self._c.get_symbol_info(symbol)
            # Extract price and quantity precision from filters
            for f in self._symbol_info["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    self._price_precision = len(str(float(f["tickSize"])).split(".")[-1].rstrip("0"))
                elif f["filterType"] == "LOT_SIZE":
                    self._quantity_precision = len(str(float(f["stepSize"])).split(".")[-1].rstrip("0"))
