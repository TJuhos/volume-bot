"""Binance Spot connector implementing the BaseConnector interface."""

from __future__ import annotations

import asyncio
from datetime import datetime
import hmac
import json
import time
from base64 import b64encode
from hashlib import sha256
from typing import Any, AsyncGenerator, Dict, Optional

import aiohttp

from bot.connectors.base import (
    BaseConnector,
    Order,
    OrderBookLevel,
    OrderBookSnapshot,
    Trade,
)

_BINA_BASE_PATH = "/v3"
_WS_DEPTH = "@depth@100ms"
_WS_TRADE = "@aggTrade"


class BinanceConnector(BaseConnector):
    """Thin async wrapper around Binance REST + WebSocket."""

    def __init__(self, config: Dict[str, Any], logger):
        super().__init__(config, logger)
        self._api_key = config["api_key"]
        self._api_secret = config["api_secret"].encode()
        self._base_url = config["base_url"].rstrip("/")
        self._ws_url = config["ws_url"].rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._listen_key: Optional[str] = None
        self._user_stream_task: Optional[asyncio.Task] = None

    # ------------------- REST helpers ------------------- #

    async def _rest(self, method: str, path: str, params: Dict[str, Any] | None = None, requires_auth: bool = False) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(json_serialize=json.dumps)

        url = self._base_url + path
        headers = {"X-MBX-APIKEY": self._api_key} if requires_auth else {}
        signed_params = params or {}

        if requires_auth:
            signed_params["timestamp"] = int(time.time() * 1000)
            # Create query string for signing
            query_string = "&".join([f"{k}={v}" for k, v in sorted(signed_params.items())])
            signature = hmac.new(self._api_secret, query_string.encode(), sha256).hexdigest()
            signed_params["signature"] = signature

        async with self._session.request(method, url, params=signed_params, headers=headers) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"Binance API error {resp.status}: {text}")
            return await resp.json()

    # ------------------- Market‑data ------------------- #

    async def get_order_book_snapshot(self, symbol: str) -> OrderBookSnapshot:
        path = f"{_BINA_BASE_PATH}/depth"
        data = await self._rest("GET", path, {"symbol": symbol.upper(), "limit": 1000}, requires_auth=False)
        bids = [OrderBookLevel(float(p), float(q)) for p, q in data["bids"]]
        asks = [OrderBookLevel(float(p), float(q)) for p, q in data["asks"]]
        return OrderBookSnapshot(
            bids=bids,
            asks=asks,
            last_update_id=data["lastUpdateId"],
            timestamp=datetime.utcnow(),
        )

    async def stream_order_book_diffs(self, symbol: str) -> AsyncGenerator[dict, None]:
        url = f"{self._ws_url}/{symbol.lower()}{_WS_DEPTH}"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        yield data
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

    async def stream_trades(self, symbol: str) -> AsyncGenerator[Trade, None]:
        url = f"{self._ws_url}/stream?streams={symbol.lower()}{_WS_TRADE}"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)["data"]
                        yield Trade(
                            trade_id=data["a"],
                            price=float(data["p"]),
                            size=float(data["q"]),
                            side="buy" if data["m"] else "sell",
                            timestamp=datetime.utcfromtimestamp(data["T"] / 1000),
                        )
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

    # ------------------- Trading ------------------- #

    async def _ensure_listen_key(self):
        if self._listen_key is None:
            res = await self._rest("POST", f"{_BINA_BASE_PATH}/userDataStream", requires_auth=False)
            self._listen_key = res["listenKey"]
            self.logger.debug("Obtained listenKey %s", self._listen_key)

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        price: float,
        size: float,
        client_order_id: str,
    ) -> Order:
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "LIMIT_MAKER",
            "price": f"{price:.8f}",
            "quantity": f"{size:.8f}",
            "newClientOrderId": client_order_id,
            "timeInForce": "GTC",
        }
        data = await self._rest("POST", f"{_BINA_BASE_PATH}/order", params, requires_auth=True)
        return Order(
            client_order_id=data["clientOrderId"],
            exchange_order_id=data["orderId"],
            symbol=data["symbol"],
            side=data["side"],
            price=float(data["price"]),
            size=float(data["origQty"]),
            status=data["status"],
            timestamp=datetime.utcfromtimestamp(data["transactTime"] / 1000),
        )

    async def cancel_order(self, symbol: str, client_order_id: str) -> None:
        params = {
            "symbol": symbol.upper(),
            "origClientOrderId": client_order_id,
        }
        await self._rest("DELETE", f"{_BINA_BASE_PATH}/order", params, requires_auth=True)

    async def stream_user_events(self) -> AsyncGenerator[dict, None]:
        await self._ensure_listen_key()
        url = f"{self._ws_url}/stream?streams={self._listen_key}"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        yield data["data"]
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

    # ------------------- Housekeeping ------------------- #

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._user_stream_task:
            self._user_stream_task.cancel()