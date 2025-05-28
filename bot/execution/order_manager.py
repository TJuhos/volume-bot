"""High‑level orchestrator that reconciles desired quotes vs. live orders."""

from __future__ import annotations

import asyncio
import itertools
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

from bot.connectors.base import BaseConnector, Order
from bot.data import CombinedStream, OrderBook
from bot.strategy.inventory import InventoryManager
from bot.strategy.market_maker import MarketMakerConfig, MarketMakerStrategy
from bot.utils.logger import get_child_logger


@dataclass
class LiveOrder:
    client_id: str
    side: str  # BUY / SELL
    price: float
    size: float
    birth: datetime = field(default_factory=datetime.utcnow)

    def stale(self, now: datetime, ttl: timedelta) -> bool:  # noqa: D401
        """Return True if order is older than ttl."""
        return now - self.birth >= ttl


class OrderManager:
    """Keeps the *actual* order state in sync with the strategy‑desired state."""

    def __init__(self, connector: BaseConnector, config: dict, logger):
        self._c = connector
        self._cfg = config
        self._logger = get_child_logger(logger, "order_mgr")

        # Strategy + supporting state
        mm_cfg = MarketMakerConfig(
            maker_spread_bps=config["trading"]["maker_spread_bps"],
            order_size=config["trading"]["order_size"],
        )
        self._inv = InventoryManager(
            max_position=config["risk"]["max_position_usd"],
            soft_cap=config["trading"]["inventory_cap_usd"],
        )
        self._strat = MarketMakerStrategy(mm_cfg, self._inv)

        # Live orders keyed by client_id
        self._live: Dict[str, LiveOrder] = {}
        self._id_seq = itertools.count(1)

        # Timing controls
        self._order_ttl = timedelta(seconds=8)

    # ---------- public API ---------- #

    async def run(self) -> None:
        symbol = self._cfg["trading"]["symbol"].upper()
        stream = CombinedStream(self._c, symbol)

        # Fan‑in user events (fills / cancels)
        fills_task = asyncio.create_task(self._handle_fills())

        async for ob_snapshot, trades in stream.stream():
            ob = OrderBook(ob_snapshot)
            band, bid_size, ask_size = self._strat.quote(ob)
            now = datetime.utcnow()
            await self._reconcile(symbol, band.bid, bid_size, band.ask, ask_size, now)

        fills_task.cancel()

    # ---------- internals ---------- #

    async def _reconcile(
        self,
        symbol: str,
        bid_px: float,
        bid_sz: float,
        ask_px: float,
        ask_sz: float,
        ts: datetime,
    ) -> None:
        """Ensure exactly two GTC orders represent the current desired quote band."""
        desired = {
            "BUY": (bid_px, bid_sz),
            "SELL": (ask_px, ask_sz),
        }

        # Cancel / replace logic
        for side, (price, size) in desired.items():
            live: Optional[LiveOrder] = next((o for o in self._live.values() if o.side == side), None)

            # If we do *not* want to quote that side, cancel existing
            if size == 0.0 and live:
                await self._cancel(symbol, live)
                continue

            # (Re)place if none live, price moved, size changed, or order is stale
            if (
                live is None
                or abs(live.price - price) > 1e-8
                or abs(live.size - size) > 1e-8
                or live.stale(ts, self._order_ttl)
            ):
                if live:
                    await self._cancel(symbol, live, silent=True)
                await self._place(symbol, side, price, size)

    async def _place(self, symbol: str, side: str, price: float, size: float):
        client_id = f"volbot-{next(self._id_seq)}-{uuid.uuid4().hex[:8]}"
        order: Order = await self._c.place_limit_order(symbol, side, price, size, client_id)
        self._live[client_id] = LiveOrder(
            client_id=client_id,
            side=side,
            price=order.price,
            size=order.size,
        )
        self._logger.debug("Placed %s %s @ %.2f size %.6f", order.side, symbol, price, size)

    async def _cancel(self, symbol: str, live: LiveOrder, silent: bool = False):
        try:
            await self._c.cancel_order(symbol, live.client_id)
        except Exception as exc:  # noqa: BLE001
            if not silent:
                self._logger.warning("Cancel failed for %s: %s", live.client_id, exc)
        finally:
            self._live.pop(live.client_id, None)
            if not silent:
                self._logger.debug("Canceled %s order %s", symbol, live.client_id)

    async def _handle_fills(self):
        async for evt in self._c.stream_user_events():
            if evt.get("e") != "executionReport":
                continue
            status = evt.get("X")
            side = evt.get("S")
            client_id = evt.get("c")
            filled_qty = float(evt.get("l", 0))
            filled_px = float(evt.get("L", 0))

            if status in {"FILLED", "PARTIALLY_FILLED"} and filled_qty > 0:
                self._inv.update_with_fill(side, filled_qty, filled_px)
                self._logger.info("Fill %s %s @ %.2f qty %.6f", side, evt.get("s"), filled_px, filled_qty)

            # Drop from live map if the order is closed
            if status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                self._live.pop(client_id, None)
