"""High‑level orchestrator that reconciles desired quotes vs. live orders."""

from __future__ import annotations

import asyncio
import itertools
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple

from bot.connectors.base import BaseConnector, Order
from bot.data import CombinedStream, OrderBook
from bot.strategy.inventory import InventoryManager
from bot.strategy.market_maker import MarketMakerConfig, MarketMakerStrategy
from bot.strategy.pnl_tracker import PnLTracker
from bot.utils.logger import get_child_logger


@dataclass
class LiveOrder:
    client_id: str
    side: str  # BUY / SELL
    price: float
    size: float
    birth: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Fill:
    side: str
    price: float
    size: float
    timestamp: datetime
    fee: float = 0.0


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

        # Order tracking
        self._live: Dict[str, LiveOrder] = {}
        self._id_seq = itertools.count(1)
        self._order_lock = asyncio.Lock()

        # Grid trading parameters
        self._grid_spacing_bps = config["trading"]["grid_spacing_bps"]
        self._grid_size = config["trading"]["order_size"]
        
        # Status reporting
        self._last_status_time = datetime.utcnow()
        self._status_interval = timedelta(seconds=10)
        
        # Fill tracking
        self._fills: List[Fill] = []
        self._last_fill_report = datetime.utcnow()
        self._fill_report_interval = timedelta(minutes=1)
        
        # Latest orderbook state
        self._latest_ob: Optional[OrderBook] = None
        self._last_ob_update: Optional[datetime] = None
        
        # Exchange info
        self._symbol_info = None
        self._price_precision = None
        self._quantity_precision = None
        
        # PnL tracking
        self._pnl_tracker: Optional[PnLTracker] = None

    async def run(self) -> None:
        """Main entry point for the order manager."""
        symbol = self._cfg["trading"]["symbol"].upper()
        
        # Initialize exchange info and balances
        await self._initialize(symbol)
        
        # Start market data stream
        stream = CombinedStream(self._c, symbol)
        
        # Wait for first valid orderbook
        await self._wait_for_orderbook(stream)
        
        # Start user events handler
        fills_task = asyncio.create_task(self._handle_user_events())
        
        try:
            # Main trading loop
            async for ob_snapshot, trades in stream.stream():
                await self._process_market_update(symbol, ob_snapshot, trades)
                
                # Check if fills task is still running
                if fills_task.done():
                    exc = fills_task.exception()
                    if exc:
                        self._logger.error("Fills task failed: %s", exc)
                        raise exc
                    else:
                        self._logger.error("Fills task completed unexpectedly")
                        raise RuntimeError("Fills task completed unexpectedly")
        finally:
            fills_task.cancel()
            try:
                await fills_task
            except asyncio.CancelledError:
                pass

    async def _initialize(self, symbol: str) -> None:
        """Initialize exchange info and balances."""
        # Get symbol info
        await self._get_symbol_info(symbol)
        
        # Clean up existing orders
        await self._cleanup_existing_orders(symbol)
        
        # Load initial balances
        await self._load_initial_balances(symbol)

    async def _wait_for_orderbook(self, stream: CombinedStream) -> None:
        """Wait for first valid orderbook."""
        self._logger.info("Waiting for first valid orderbook...")
        async for ob_snapshot, _ in stream.stream():
            self._latest_ob = OrderBook(ob_snapshot)
            if self._latest_ob.best_bid and self._latest_ob.best_ask:
                self._last_ob_update = ob_snapshot.timestamp
                self._logger.info("Received first valid orderbook with timestamp: %s", self._last_ob_update)
                break

    async def _process_market_update(self, symbol: str, ob_snapshot: dict, trades: list) -> None:
        """Process a market update from the stream."""
        # Update latest orderbook state with timestamp from the snapshot
        self._latest_ob = OrderBook(ob_snapshot)
        self._last_ob_update = ob_snapshot.timestamp
        
        # Initialize PnL tracker if we have valid orderbook and it's not initialized yet
        if self._pnl_tracker is None and self._latest_ob.best_bid and self._latest_ob.best_ask:
            base_balance = self._inv.available_balances["base"]
            quote_balance = self._inv.available_balances["quote"]
            self._pnl_tracker = PnLTracker(
                initial_mid_price=self._latest_ob.mid_price,
                initial_base_balance=base_balance,
                initial_quote_balance=quote_balance
            )
            self._logger.info("PnL tracker initialized with mid price: %.2f", self._latest_ob.mid_price)
        
        # Update PnL tracker with new mid price
        if self._pnl_tracker and self._latest_ob.best_bid and self._latest_ob.best_ask:
            self._pnl_tracker.update_mid_price(self._latest_ob.mid_price)
        
        # Skip if we don't have a valid orderbook yet
        if not self._latest_ob.best_bid or not self._latest_ob.best_ask:
            self._logger.debug("Waiting for valid orderbook...")
            return
            
        # Safety check - if we have no orders and valid orderbook, place new orders
        # But only if we're not in the middle of handling a fill
        if not self._live and not self._order_lock.locked():
            self._logger.info("No live orders detected, placing new grid orders")
            await self._place_grid_orders(symbol, self._latest_ob.mid_price)
            return
        
        # Get strategy quotes
        band, bid_size, ask_size = await self._strat.quote(self._latest_ob)
        now = datetime.utcnow()
        
        # Report status periodically
        if now - self._last_status_time >= self._status_interval:
            await self._report_status(symbol, self._latest_ob)
            self._report_fills()
            self._last_status_time = now
        
        # Check if orders need to be cancelled due to price drift
        await self._check_order_drift(symbol, band.bid, bid_size, band.ask, ask_size, now)

    async def _handle_user_events(self) -> None:
        """Handle user events stream (fills, cancels, etc)."""
        self._logger.info("Starting user events stream handler")
        
        # Place initial orders if we don't have any
        if not self._live and self._latest_ob:
            await self._place_grid_orders(
                self._cfg["trading"]["symbol"].upper(),
                self._latest_ob.mid_price
            )
        
        while True:  # Keep trying to reconnect if stream ends
            try:
                async for evt in self._c.stream_user_events():
                    try:
                        self._logger.debug("Received user event: %s", evt)
                        if evt.get("e") != "executionReport":
                            continue
                        
                        # Log the full execution report
                        self._logger.debug(
                            "Processing execution report:\n"
                            f"  Order ID: {evt.get('c')}\n"
                            f"  Status: {evt.get('X')}\n"
                            f"  Side: {evt.get('S')}\n"
                            f"  Price: {evt.get('p')}\n"
                            f"  Quantity: {evt.get('q')}\n"
                            f"  Filled: {evt.get('l')}\n"
                            f"  Fill Price: {evt.get('L')}\n"
                            f"  Fee: {evt.get('n', 0)} {evt.get('N', '')}\n"
                            f"  Symbol: {evt.get('s')}"
                        )
                        
                        await self._process_execution_report(evt)
                    except Exception as e:
                        self._logger.error("Error processing user event: %s", e, exc_info=True)
                        continue  # Continue processing other events even if one fails
                
                # If we get here, the stream ended normally
                self._logger.warning("User events stream ended, attempting to reconnect...")
                await asyncio.sleep(1)  # Wait a bit before reconnecting
                
            except asyncio.CancelledError:
                self._logger.info("User events handler cancelled")
                raise
            except Exception as e:
                self._logger.error("Error in user events stream: %s", e, exc_info=True)
                await asyncio.sleep(1)  # Wait a bit before retrying
                continue  # Try to reconnect

    async def _process_execution_report(self, evt: dict) -> None:
        """Process an execution report event."""
        status = evt.get("X")
        side = evt.get("S")
        client_id = evt.get("c")
        filled_qty = float(evt.get("l", 0))
        filled_px = float(evt.get("L", 0))
        fee = float(evt.get("n", 0))  # Get fee amount
        fee_asset = evt.get("N", "")  # Get fee asset
        symbol = evt.get("s")
        
        # Log all execution reports for debugging
        self._logger.debug(
            "Received execution report:\n"
            f"  Client ID: {client_id}\n"
            f"  Status: {status}\n"
            f"  Side: {side}\n"
            f"  Filled Qty: {filled_qty}\n"
            f"  Fill Price: {filled_px}\n"
            f"  Fee: {fee} {fee_asset}\n"
            f"  Symbol: {symbol}"
        )
        
        # Only process orders that we placed (they start with 'volbot-')
        if not client_id or not client_id.startswith('volbot-'):
            self._logger.debug("Skipping execution report for non-bot order: %s", client_id)
            return
            
        self._logger.debug(
            "Processing execution report for bot order %s:\n"
            f"  Status: {status}\n"
            f"  Side: {side}\n"
            f"  Filled Qty: {filled_qty}\n"
            f"  Fill Price: {filled_px}\n"
            f"  Fee: {fee} {fee_asset}\n"
            f"  Symbol: {symbol}",
            client_id
        )
        
        if status in {"FILLED", "PARTIALLY_FILLED"} and filled_qty > 0:
            await self._handle_fill(symbol, client_id, side, filled_px, filled_qty, fee)
        
        # Drop from live map if the order is closed
        if status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            self._logger.info("Removing order %s from live orders (status: %s)", client_id, status)
            self._live.pop(client_id, None)

    async def _handle_fill(self, symbol: str, client_id: str, side: str, price: float, size: float, fee: float = 0.0) -> None:
        """Handle a fill event by updating the PnL tracker and placing new orders."""
        self._logger.info(
            "Fill: %s %.6f @ %.2f (fee: %.6f)",
            side, size, price, fee
        )
        
        # Update PnL tracker
        if self._pnl_tracker:
            try:
                # Log the order info before processing
                if client_id in self._pnl_tracker._orders:
                    order_info = self._pnl_tracker._orders[client_id]
                    self._logger.debug(
                        "Processing fill PnL:\n"
                        f"  Order side: {order_info.side}\n"
                        f"  Order size: {order_info.size}\n"
                        f"  Order mid price: {order_info.mid_price:.2f}\n"
                        f"  Fill price: {price:.2f}\n"
                        f"  Fill size: {size:.6f}\n"
                        f"  Fee: {fee:.6f}\n"
                        f"  Current grid PnL: {self._pnl_tracker.grid_pnl:.2f}"
                    )
                self._pnl_tracker.process_fill(client_id, price, fee)
                self._logger.info(
                    "Updated grid PnL: %.2f USD (%.2f%%)",
                    self._pnl_tracker.grid_pnl,
                    self._pnl_tracker.grid_return_pct
                )
            except ValueError as e:
                self._logger.error("Error processing fill PnL: %s", e)
        
        # Record the fill
        self._fills.append(Fill(
            timestamp=datetime.now(),
            side=side,
            price=price,
            size=size,
            fee=fee
        ))
        
        # Remove the filled order from live orders
        self._live.pop(client_id, None)
        
        # Immediately refresh the entire grid
        if self._latest_ob and self._latest_ob.best_bid and self._latest_ob.best_ask:
            self._logger.info("Refreshing grid after fill")
            await self._place_grid_orders(symbol, self._latest_ob.mid_price)

    async def _check_order_drift(self, symbol: str, bid_px: float, bid_sz: float, ask_px: float, ask_sz: float, ts: datetime) -> None:
        """Check if orders still exist on the exchange and remove any that don't."""
        if not self._latest_ob:
            return

        for order in list(self._live.values()):  # Create a copy to avoid modification during iteration
            try:
                # Verify order still exists on exchange
                exchange_order = await self._c.get_order_status(symbol, order.client_id)
                if exchange_order is None:
                    self._logger.debug("Order %s not found on exchange, removing from local tracking", 
                                     order.client_id)
                    self._live.pop(order.client_id, None)
            except Exception as e:
                self._logger.error("Error checking order %s status: %s", order.client_id, e)
                continue

    async def _place_grid_orders(self, symbol: str, mid_price: float) -> None:
        """Place 5 grid orders on each side of the mid price, all spaced 1 bps apart."""
        async with self._order_lock:
            # Cancel any existing orders first
            remaining_orders = list(self._live.values())
            
            # Check which orders still exist and need to be cancelled
            async def check_and_cancel_order(order):
                try:
                    exchange_order = await self._c.get_order_status(symbol, order.client_id)
                    if exchange_order and exchange_order.status in {"NEW", "PARTIALLY_FILLED"}:
                        return order
                    else:
                        self._live.pop(order.client_id, None)
                        return None
                except Exception as e:
                    self._logger.error("Error checking order status: %s", e)
                    return None

            # Check all orders in parallel
            orders_to_cancel = await asyncio.gather(*[check_and_cancel_order(order) for order in remaining_orders])
            orders_to_cancel = [o for o in orders_to_cancel if o is not None]

            # Cancel all existing orders in parallel
            if orders_to_cancel:
                await asyncio.gather(*[self._cancel_order_safe(symbol, order.client_id) for order in orders_to_cancel])

            # Get current mid price from orderbook
            if not self._latest_ob or not self._latest_ob.best_bid or not self._latest_ob.best_ask:
                self._logger.warning("No valid orderbook data available, skipping order placement")
                return

            current_mid = self._latest_ob.mid_price
            
            # Define grid levels (in basis points from mid)
            grid_levels = list(range(1, 3))  # 1 to 2 bps
            
            try:
                # Prepare all order parameters
                buy_orders = []
                sell_orders = []
                
                for level in grid_levels:
                    # Buy orders
                    buy_price = self._round_price(current_mid * (1 - level / 10000))
                    buy_size = self._round_quantity(self._grid_size)
                    buy_orders.append((buy_price, buy_size))
                    
                    # Sell orders
                    sell_price = self._round_price(current_mid * (1 + level / 10000))
                    sell_size = self._round_quantity(self._grid_size)
                    sell_orders.append((sell_price, sell_size))
                    
                    # Log expected grid spacing
                    self._logger.debug(
                        f"Grid Level {level} bps:\n"
                        f"  Buy: {buy_price:.2f} (mid - {level} bps)\n"
                        f"  Sell: {sell_price:.2f} (mid + {level} bps)\n"
                        f"  Expected spread: {current_mid * (2 * level / 10000):.2f}\n"
                        f"  Actual spread: {(sell_price - buy_price):.2f}"
                    )

                # Place all buy orders in parallel
                async def place_buy_order(price, size):
                    try:
                        order = await self._place(symbol, "BUY", price, size)
                        if order and self._pnl_tracker:
                            self._pnl_tracker.register_order(order.client_id, "BUY", size)
                            self._logger.debug(
                                f"Placed BUY order:\n"
                                f"  Price: {price:.2f}\n"
                                f"  Size: {size:.6f}\n"
                                f"  Order ID: {order.client_id}"
                            )
                        return order
                    except Exception as e:
                        self._logger.error("Failed to place buy order at %.2f: %s", price, e)
                        return None

                # Place all sell orders in parallel
                async def place_sell_order(price, size):
                    try:
                        order = await self._place(symbol, "SELL", price, size)
                        if order and self._pnl_tracker:
                            self._pnl_tracker.register_order(order.client_id, "SELL", size)
                            self._logger.debug(
                                f"Placed SELL order:\n"
                                f"  Price: {price:.2f}\n"
                                f"  Size: {size:.6f}\n"
                                f"  Order ID: {order.client_id}"
                            )
                        return order
                    except Exception as e:
                        self._logger.error("Failed to place sell order at %.2f: %s", price, e)
                        return None

                # Place all orders in parallel
                buy_results = await asyncio.gather(*[place_buy_order(price, size) for price, size in buy_orders])
                sell_results = await asyncio.gather(*[place_sell_order(price, size) for price, size in sell_orders])
                
                # Log placed orders
                self._logger.info(
                    "\n=== New Grid Orders ===\n"
                    f"Mid Price: {current_mid:.2f}\n"
                    "Buy Orders:\n" +
                    "\n".join(f"  {level} bps: {size:.6f} @ {price:.2f}" 
                             for (price, size), level in zip(buy_orders, grid_levels)) +
                    "\nSell Orders:\n" +
                    "\n".join(f"  {level} bps: {size:.6f} @ {price:.2f}"
                             for (price, size), level in zip(sell_orders, grid_levels)) +
                    "\n====================="
                )
                
            except Exception as e:
                self._logger.error("Failed to place grid orders: %s", e)
                # Cancel any orders that might have been placed
                for order in list(self._live.values()):
                    try:
                        exchange_order = await self._c.get_order_status(symbol, order.client_id)
                        if exchange_order and exchange_order.status in {"NEW", "PARTIALLY_FILLED"}:
                            await self._cancel_order_safe(symbol, order.client_id)
                        else:
                            self._live.pop(order.client_id, None)
                    except Exception as cancel_error:
                        self._logger.error("Error cancelling order after grid placement failure: %s", cancel_error)
                        continue
                
                # Retry once after a short delay
                await asyncio.sleep(0.5)
                if self._latest_ob and self._latest_ob.best_bid and self._latest_ob.best_ask:
                    await self._place_grid_orders(symbol, self._latest_ob.mid_price)

    def _round_quantity(self, quantity: float) -> float:
        """Round quantity to meet exchange precision requirements."""
        if self._quantity_precision is None:
            return quantity
        return round(quantity, self._quantity_precision)

    async def _place(self, symbol: str, side: str, price: float, size: float):
        """Place an order, ensuring it's on the correct side of the mid price."""
        if self._latest_ob is None:
            self._logger.warning("No orderbook data available, skipping order placement")
            return
            
        mid_price = self._latest_ob.mid_price
        
        # Verify order is on correct side of mid price
        if side == "BUY" and price >= mid_price:
            self._logger.warning(
                "Buy order price %.2f is above mid price %.2f, adjusting down",
                price, mid_price
            )
            price = self._round_price(mid_price * (1 - self._grid_spacing_bps / 10000))
        elif side == "SELL" and price <= mid_price:
            self._logger.warning(
                "Sell order price %.2f is below mid price %.2f, adjusting up",
                price, mid_price
            )
            price = self._round_price(mid_price * (1 + self._grid_spacing_bps / 10000))
            
        client_id = f"volbot-{next(self._id_seq)}-{uuid.uuid4().hex[:8]}"
        try:
            self._logger.debug("Placing %s order %s @ %.2f", side, client_id, price)
            await self._c.place_limit_order(symbol, side, price, size, client_id)
            
            # Create LiveOrder with our known values
            self._live[client_id] = LiveOrder(
                client_id=client_id,
                side=side,
                price=price,
                size=size,
            )
            
            self._logger.debug("Successfully placed %s order %s @ %.2f", side, client_id, price)
            return self._live[client_id]
            
        except Exception as exc:
            self._logger.error("Failed to place order: %s", exc)
            raise

    async def _cleanup_existing_orders(self, symbol: str) -> None:
        """Clean up any existing orders on startup."""
        self._logger.info("Cleaning up any existing orders...")
        try:
            open_orders = await self._c.get_open_orders(symbol)
            for order in open_orders:
                try:
                    await self._c.cancel_order(symbol, order.client_order_id)
                    self._logger.info("Cancelled existing order: %s", order.client_order_id)
                except Exception as e:
                    if "Unknown order" not in str(e):
                        self._logger.error("Error cancelling order %s: %s", order.client_order_id, e)
        except Exception as e:
            self._logger.error("Error getting open orders: %s", e)
        
        # Clear our local order tracking
        self._live.clear()

    async def _load_initial_balances(self, symbol: str) -> None:
        """Load initial balances from exchange."""
        try:
            account_info = await self._c.get_account_info()
            base_currency = symbol[:-4]  # e.g., BTC from BTCUSDT
            quote_currency = symbol[-4:]  # e.g., USDT from BTCUSDT
            
            base_balance = 0.0
            quote_balance = 0.0
            
            for balance in account_info["balances"]:
                if balance["asset"] == base_currency:
                    base_balance = float(balance["free"])
                elif balance["asset"] == quote_currency:
                    quote_balance = float(balance["free"])
            
            self._inv.set_available_balance(base_balance, quote_balance)
            
            # Initialize PnL tracker with initial balances and price
            if self._latest_ob and self._latest_ob.best_bid and self._latest_ob.best_ask:
                self._pnl_tracker = PnLTracker(
                    initial_mid_price=self._latest_ob.mid_price,
                    initial_base_balance=base_balance,
                    initial_quote_balance=quote_balance
                )
                self._logger.info("PnL tracker initialized with mid price: %.2f", self._latest_ob.mid_price)
            else:
                self._logger.warning("Cannot initialize PnL tracker - waiting for valid orderbook")
            
            self._logger.info(
                "Initial balances loaded - Base: %.6f %s, Quote: %.2f %s",
                base_balance, base_currency, quote_balance, quote_currency
            )
        except Exception as e:
            self._logger.error("Failed to load initial balances: %s", e)
            raise

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
            
            self._logger.info(
                "Symbol info loaded - Price precision: %d, Quantity precision: %d",
                self._price_precision, self._quantity_precision
            )

    def _round_price(self, price: float) -> float:
        """Round price to meet exchange precision requirements."""
        if self._price_precision is None:
            return price  # Return as is if precision not yet known
        return round(price, self._price_precision)

    async def _update_balances_from_exchange(self, symbol: str) -> None:
        """Fetch and update balances from the exchange."""
        try:
            account_info = await self._c.get_account_info()
            base_currency = symbol[:-4]  # e.g., BTC from BTCUSDT
            quote_currency = symbol[-4:]  # e.g., USDT from BTCUSDT
            
            base_balance = 0.0
            quote_balance = 0.0
            
            for balance in account_info["balances"]:
                if balance["asset"] == base_currency:
                    base_balance = float(balance["free"])
                elif balance["asset"] == quote_currency:
                    quote_balance = float(balance["free"])
            
            self._inv.set_available_balance(base_balance, quote_balance)
            self._logger.debug(
                "Updated balances from exchange - Base: %.6f %s, Quote: %.2f %s",
                base_balance, base_currency, quote_balance, quote_currency
            )
        except Exception as e:
            self._logger.error("Failed to update balances from exchange: %s", e)
            raise

    async def _report_status(self, symbol: str, ob: OrderBook) -> None:
        """Report current status including PnL metrics."""
        # Get current account value
        account_value = self._inv.get_account_value(ob.mid_price)
        
        # Get live orders info
        live_orders = []
        for order in self._live.values():
            live_orders.append(f"{order.side} {order.size} @ {order.price}")
        
        # Sort orders by price in descending order (like an orderbook)
        live_orders.sort(key=lambda x: float(x.split('@')[1]), reverse=True)
        
        # Get PnL info if available
        pnl_info = ""
        if self._pnl_tracker is not None:
            pnl_info = (
                f"\nPnL Metrics:\n"
                f"  Grid Trading PnL: {self._pnl_tracker.grid_pnl:.2f} USD ({self._pnl_tracker.grid_return_pct:.2f}%)\n"
                f"  Total PnL: {self._pnl_tracker.total_pnl:.2f} USD ({self._pnl_tracker.realized_return_pct:.2f}%)\n"
                f"  Exposure PnL: {self._pnl_tracker.expo_pnl:.2f} USD ({self._pnl_tracker.exposure_return_pct:.2f}%)"
            )
        else:
            pnl_info = "\nPnL Metrics: Not yet initialized (waiting for valid orderbook)"
        
        # Log status
        self._logger.info(
            "\n=== Status Report ===\n"
            f"Symbol: {symbol}\n"
            f"Value: {account_value:.2f} USDT\n"
            f"Base Balance: {self._inv.available_balances['base']:.6f}\n"
            f"Quote Balance: {self._inv.available_balances['quote']:.2f} USDT\n"
            f"Mid Price: {ob.mid_price:.2f}\n"
            f"Live Orders ({len(live_orders)}):\n" + 
            "\n".join(f"  - {order}" for order in live_orders) +
            f"{pnl_info}\n"
            "==================="
        )

    def _report_fills(self) -> None:
        """Report fill statistics."""
        if not self._fills:
            return
            
        now = datetime.utcnow()
        if now - self._last_fill_report < self._fill_report_interval:
            return
            
        # Calculate statistics
        total_fills = len(self._fills)
        buy_fills = [f for f in self._fills if f.side == "BUY"]
        sell_fills = [f for f in self._fills if f.side == "SELL"]
        
        # Get PnL info if available
        pnl_info = ""
        if self._pnl_tracker is not None:
            pnl_info = (
                f"Grid Trading PnL: {self._pnl_tracker.grid_pnl:.2f} USD ({self._pnl_tracker.grid_return_pct:.2f}%)\n"
                f"Total PnL: {self._pnl_tracker.total_pnl:.2f} USD ({self._pnl_tracker.realized_return_pct:.2f}%)\n"
                f"Exposure PnL: {self._pnl_tracker.expo_pnl:.2f} USD ({self._pnl_tracker.exposure_return_pct:.2f}%)"
            )
        else:
            pnl_info = "PnL Metrics: Not yet initialized (waiting for valid orderbook)"
        
        # Log fill report
        self._logger.info(
            "\n=== Fill Report ===\n"
            f"Total Fills: {total_fills}\n"
            f"Buy Fills: {len(buy_fills)}\n"
            f"Sell Fills: {len(sell_fills)}\n"
            f"{pnl_info}\n"
            "==================="
        )
        
        self._last_fill_report = now

    async def _cancel_order_safe(self, symbol: str, client_id: str) -> bool:
        """Safely cancel an order, checking if it exists first."""
        try:
            # Check if order exists and is in a cancellable state
            exchange_order = await self._c.get_order_status(symbol, client_id)
            if exchange_order is None:
                self._logger.debug("Order %s not found on exchange, removing from local tracking", client_id)
                self._live.pop(client_id, None)
                return True
                
            if exchange_order.status not in {"NEW", "PARTIALLY_FILLED"}:
                self._logger.debug("Order %s is in non-cancellable state %s, removing from local tracking", 
                                 client_id, exchange_order.status)
                self._live.pop(client_id, None)
                return True
                
            # Order exists and is cancellable, try to cancel it
            await self._c.cancel_order(symbol, client_id)
            self._live.pop(client_id, None)
            return True
            
        except Exception as e:
            if "Unknown order" in str(e):
                self._logger.debug("Order %s not found on exchange (Unknown order), removing from local tracking", client_id)
                self._live.pop(client_id, None)
                return True
            self._logger.error("Error cancelling order %s: %s", client_id, e)
            return False
