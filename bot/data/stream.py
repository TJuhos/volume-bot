"""Combine snapshot + incremental depth + trade streams into a unified async feed."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Tuple

from bot.connectors.base import BaseConnector, OrderBookLevel, OrderBookSnapshot, Trade


class CombinedStream:
    """Yields *coherent* (order_book_snapshot, trades_since_last_emit) tuples.

    Downstream strategy code can await `CombinedStream(connector, symbol).stream()` and
    process every update without worrying about loss‑of‑sync between Binance’s depth
    snapshots and diff events.
    """

    def __init__(self, connector: BaseConnector, symbol: str):
        self._c = connector
        self._symbol = symbol.upper()

    async def stream(self) -> AsyncGenerator[Tuple[OrderBookSnapshot, List[Trade]], None]:
        """Async generator producing coherent updates."""
        # --- Priming snapshot --- #
        snapshot = await self._c.get_order_book_snapshot(self._symbol)
        bids = {lvl.price: lvl.size for lvl in snapshot.bids}
        asks = {lvl.price: lvl.size for lvl in snapshot.asks}
        last_update_id = snapshot.last_update_id

        # --- Fan‑in tasks --- #
        diff_q: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=1000)
        trade_q: asyncio.Queue[Trade] = asyncio.Queue(maxsize=1000)

        async def _pump_diffs():
            async for d in self._c.stream_order_book_diffs(self._symbol):
                await diff_q.put(d)

        async def _pump_trades():
            async for t in self._c.stream_trades(self._symbol):
                await trade_q.put(t)

        diff_task = asyncio.create_task(_pump_diffs())
        trade_task = asyncio.create_task(_pump_trades())

        try:
            while True:
                diff = await diff_q.get()
                print('Received diff')

                # 1️⃣ Skip stale messages
                if diff["u"] <= last_update_id:
                    continue

                # 2️⃣ Detect lost packets → resync via fresh snapshot
                if diff["U"] != last_update_id + 1:
                    snapshot = await self._c.get_order_book_snapshot(self._symbol)
                    bids = {lvl.price: lvl.size for lvl in snapshot.bids}
                    asks = {lvl.price: lvl.size for lvl in snapshot.asks}
                    last_update_id = snapshot.last_update_id
                    continue

                # 3️⃣ Apply diff
                for p_str, q_str in diff["b"]:  # bids
                    p = float(p_str)
                    q = float(q_str)
                    if q == 0:
                        bids.pop(p, None)
                    else:
                        bids[p] = q
                for p_str, q_str in diff["a"]:  # asks
                    p = float(p_str)
                    q = float(q_str)
                    if q == 0:
                        asks.pop(p, None)
                    else:
                        asks[p] = q

                last_update_id = diff["u"]

                # 4️⃣ Materialise new coherent snapshot
                ob_snapshot = OrderBookSnapshot(
                    bids=[OrderBookLevel(p, q) for p, q in sorted(bids.items(), key=lambda x: -x[0])],
                    asks=[OrderBookLevel(p, q) for p, q in sorted(asks.items(), key=lambda x: x[0])],
                    last_update_id=last_update_id,
                    timestamp=datetime.utcfromtimestamp(diff.get("E", 0) / 1000.0)
                    if diff.get("E") else datetime.utcnow(),
                )

                # 5️⃣ Drain accumulated trades without blocking
                trades: List[Trade] = []
                while not trade_q.empty():
                    trades.append(trade_q.get_nowait())

                yield ob_snapshot, trades
        finally:
            diff_task.cancel()
            trade_task.cancel()