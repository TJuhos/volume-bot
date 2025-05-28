"""Replay‑mode back‑tester for the market‑maker codebase.

Example usage:
    $ python -m scripts.backtest --snapshot snapshots/2025-05-25T00.parquet \
                                 --diffs diffs/2025-05-25T00.parquet \
                                 --config bot/config.yaml

The script mocks out a Connector that feeds historical order‑book changes to the
CombinedStream, while recording resulting fills and inventory.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator, Dict, List
import sys

import pandas as pd

from bot import load_config  # noqa: E402  pylint: disable=wrong-import-position
from bot.connectors.base import (
    BaseConnector,
    Order,
    OrderBookLevel,
    OrderBookSnapshot,
    Trade,
)
from bot.execution.order_manager import OrderManager  # noqa: E402
from bot.utils.logger import setup_logger  # noqa: E402

# Ensure repo root importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

@dataclass
class _HistBar:
    price: float
    qty: float
    side: str
    ts: int


class OfflineConnector(BaseConnector):
    """Feed historical depth/trade events instead of live data."""

    def __init__(self, config: Dict, snapshot_df: pd.DataFrame, diff_df: pd.DataFrame, trades_df: pd.DataFrame, logger):
        super().__init__(config, logger)
        self._snapshot_df = snapshot_df
        self._diff_df = diff_df
        self._trades_df = trades_df
        self._order_id_seq = itertools.count(1)
        self._listen_q: asyncio.Queue[Dict] = asyncio.Queue()

    # -------- Market‑data -------- #
    async def get_order_book_snapshot(self, symbol: str):
        row = self._snapshot_df.iloc[0]
        bids = [OrderBookLevel(float(row.bid_price), float(row.bid_qty))]
        asks = [OrderBookLevel(float(row.ask_price), float(row.ask_qty))]
        return OrderBookSnapshot(bids, asks, int(row.update_id), datetime.utcfromtimestamp(row.ts/1e9))

    async def stream_order_book_diffs(self, symbol: str):
        for _, row in self._diff_df.iterrows():
            await asyncio.sleep(0)  # relinquish control
            yield row.to_dict()

    async def stream_trades(self, symbol: str):
        for _, row in self._trades_df.iterrows():
            await asyncio.sleep(0)
            yield Trade(int(row.trade_id), float(row.price), float(row.qty), row.side, datetime.utcfromtimestamp(row.ts/1e9))

    # -------- Trading -------- #
    async def place_limit_order(self, symbol, side, price, size, client_order_id):
        order = Order(client_order_id, next(self._order_id_seq), symbol, side, price, size, "FILLED", datetime.utcnow())
        # immediate fill assumption in backtest
        await self._listen_q.put({"e": "executionReport", "X": "FILLED", "S": side, "c": client_order_id, "l": size, "L": price, "s": symbol})
        return order

    async def cancel_order(self, symbol, client_order_id):
        pass  # no‑op in backtest

    async def stream_user_events(self):
        while True:
            evt = await self._listen_q.get()
            yield evt

    async def close(self):
        pass


async def _run_backtest(args):
    cfg = load_config(args.config)
    logger = setup_logger("INFO")

    snapshot_df = pd.read_parquet(args.snapshot)
    diff_df = pd.read_parquet(args.diffs)
    trades_df = pd.read_parquet(args.trades)

    connector = OfflineConnector(cfg["exchange"], snapshot_df, diff_df, trades_df, logger)
    om = OrderManager(connector, cfg, logger)
    await om.run()


def main():
    p = argparse.ArgumentParser(description="Historical back‑test runner")
    p.add_argument("--snapshot", type=Path, required=True, help="Initial snapshot parquet file")
    p.add_argument("--diffs", type=Path, required=True, help="Depth diffs parquet file")
    p.add_argument("--trades", type=Path, required=True, help="Trades parquet file")
    p.add_argument("--config", type=Path, default=Path("bot/config.yaml"))
    args = p.parse_args()

    asyncio.run(_run_backtest(args))


if __name__ == "__main__":
    main()
