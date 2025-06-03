"""Binance Spot connector implementing the BaseConnector interface."""

from __future__ import annotations

import asyncio
from datetime import datetime
import hmac
import json
import time
import urllib.parse
from base64 import b64encode
from hashlib import sha256
from typing import Any, AsyncGenerator, Dict, Optional, List

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
        # Clean and validate API key
        self._api_key = config["api_key"].strip()
        if not self._api_key or len(self._api_key) < 10:
            raise ValueError("Invalid API key: too short or empty")
        if any(c.isspace() for c in self._api_key):
            raise ValueError("Invalid API key: contains whitespace")
            
        # Clean and validate API secret
        self._api_secret = config["api_secret"].strip()
        if not self._api_secret or len(self._api_secret) < 10:
            raise ValueError("Invalid API secret: too short or empty")
        if any(c.isspace() for c in self._api_secret):
            raise ValueError("Invalid API secret: contains whitespace")
            
        self._base_url = config["base_url"].rstrip("/")
        self._ws_url = config["ws_url"].rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self._listen_key: Optional[str] = None
        self._user_stream_task: Optional[asyncio.Task] = None
        
        # Debug logging for API key format
        self.logger.debug("Initializing BinanceConnector")
        self.logger.debug("API key length: %d", len(self._api_key))
        self.logger.debug("API key first/last 4 chars: %s...%s", 
                         self._api_key[:4], 
                         self._api_key[-4:] if len(self._api_key) > 8 else "")
        self.logger.debug("Base URL: %s", self._base_url)

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """Generate signature according to Binance API documentation.
        
        Args:
            params: Dictionary of parameters to sign
            
        Returns:
            HMAC SHA256 signature as hex string
        """
        # Create query string using urlencode
        query_string = urllib.parse.urlencode(params)
        self.logger.debug("Query string for signing: %s", query_string)
        
        # Generate signature using the raw API secret
        signature = hmac.new(
            self._api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            sha256
        ).hexdigest()
        
        self.logger.debug("Generated signature: %s", signature)
        return signature

    async def _get_server_time(self) -> int:
        """Get server time from Binance."""
        if self._session is None:
            self._session = aiohttp.ClientSession(json_serialize=json.dumps)
            
        async with self._session.get(f"{self._base_url}/v3/time") as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"Failed to get server time: {text}")
            data = await resp.json()
            return data["serverTime"]

    # ------------------- REST helpers ------------------- #

    async def _rest(self, method: str, path: str, params: Dict[str, Any] | None = None, requires_auth: bool = False) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(json_serialize=json.dumps)

        url = self._base_url + path
        headers = {}
        request_params = params or {}

        if requires_auth:
            # Add API key to headers
            headers["X-MBX-APIKEY"] = self._api_key
            
            # Add server timestamp
            request_params["timestamp"] = await self._get_server_time()
            
            # Generate signature
            signature = self._generate_signature(request_params)
            request_params["signature"] = signature

            # Debug logging
            self.logger.debug("Making authenticated request to %s", url)
            self.logger.debug("Request parameters: %s", request_params)
            self.logger.debug("Request headers: %s", headers)

        async with self._session.request(
            method, 
            url, 
            params=request_params,  # Always send as query parameters
            headers=headers
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                self.logger.error("Request failed: %s %s", resp.status, text)
                self.logger.error("Request details - URL: %s, Method: %s", url, method)
                self.logger.error("Request params: %s", request_params)
                self.logger.error("Request headers: %s", headers)
                raise RuntimeError(f"Binance API error {resp.status}: {text}")
            return await resp.json()

    # ------------------- Market‑data ------------------- #

    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """Get symbol information including filters and precision."""
        path = f"{_BINA_BASE_PATH}/exchangeInfo"
        data = await self._rest("GET", path, requires_auth=False)
        
        # Find the specific symbol info
        for s in data["symbols"]:
            if s["symbol"] == symbol.upper():
                return s
                
        raise ValueError(f"Symbol {symbol} not found in exchange info")

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
            # Debug logging for user data stream request
            self.logger.debug("Getting listen key with API key: %s", self._api_key)
            # The user data stream endpoint requires the API key but not a signature
            headers = {"X-MBX-APIKEY": self._api_key}
            async with self._session.post(f"{self._base_url}{_BINA_BASE_PATH}/userDataStream", headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    self.logger.error("Failed to get listen key: %s", text)
                    raise RuntimeError(f"Failed to get listen key: {text}")
                res = await resp.json()
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
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": f"{price:.8f}",
            "quantity": f"{size:.8f}",
            "newClientOrderId": client_order_id,
        }
        data = await self._rest("POST", f"{_BINA_BASE_PATH}/order", params, requires_auth=True)
        
        # Debug log the response
        self.logger.debug("Order placement response: %s", data)
        
        try:
            return Order(
                client_order_id=data["clientOrderId"],
                exchange_order_id=data["orderId"],
                symbol=data["symbol"],
                side=side.upper(),
                price=price,
                size=size,
                status="NEW",
                timestamp=datetime.utcfromtimestamp(data["transactTime"] / 1000),
            )
        except KeyError as e:
            self.logger.error("Missing field in order response: %s", e)
            self.logger.error("Full response data: %s", data)
            raise RuntimeError(f"Invalid order response from Binance: missing field {e}")

    async def cancel_order(self, symbol: str, client_order_id: str) -> None:
        params = {
            "symbol": symbol.upper(),
            "origClientOrderId": client_order_id,
        }
        await self._rest("DELETE", f"{_BINA_BASE_PATH}/order", params, requires_auth=True)

    async def stream_user_events(self) -> AsyncGenerator[dict, None]:
        await self._ensure_listen_key()
        # Ensure ws_url doesn't end with /ws
        ws_base = self._ws_url.rstrip("/ws").rstrip("/")
        url = f"{ws_base}/stream?streams={self._listen_key}"
        self.logger.debug("Connecting to user data stream at: %s", url)
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.ws_connect(url, heartbeat=20) as ws:
                    self.logger.info("Successfully connected to user data stream")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            yield data["data"]
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            self.logger.error("WebSocket error: %s", msg.data)
                            break
            except Exception as e:
                self.logger.error("Failed to connect to user data stream: %s", e)
                raise

    async def get_order_status(self, symbol: str, client_order_id: str) -> Optional[Order]:
        """Get the current status of an order from the exchange."""
        params = {
            "symbol": symbol.upper(),
            "origClientOrderId": client_order_id,
        }
        try:
            data = await self._rest("GET", f"{_BINA_BASE_PATH}/order", params, requires_auth=True)
            return Order(
                client_order_id=data["clientOrderId"],
                exchange_order_id=data["orderId"],
                symbol=data["symbol"],
                side=data["side"],
                price=float(data["price"]),
                size=float(data["origQty"]),
                status=data["status"],
                timestamp=datetime.utcfromtimestamp(data["time"] / 1000),
            )
        except Exception as e:
            if "Unknown order" in str(e):
                return None
            raise

    async def get_open_orders(self, symbol: str) -> List[Order]:
        """Get all open orders for a symbol."""
        params = {"symbol": symbol.upper()}
        data = await self._rest("GET", f"{_BINA_BASE_PATH}/openOrders", params, requires_auth=True)
        return [
            Order(
                client_order_id=order["clientOrderId"],
                exchange_order_id=order["orderId"],
                symbol=order["symbol"],
                side=order["side"],
                price=float(order["price"]),
                size=float(order["origQty"]),
                status=order["status"],
                timestamp=datetime.utcfromtimestamp(order["time"] / 1000),
            )
            for order in data
        ]

    # ------------------- Housekeeping ------------------- #

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._user_stream_task:
            self._user_stream_task.cancel()

    async def get_account_info(self) -> dict:
        """Get account information including balances."""
        return await self._rest("GET", f"{_BINA_BASE_PATH}/account", requires_auth=True)