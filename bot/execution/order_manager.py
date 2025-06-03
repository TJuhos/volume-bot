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
    pnl: float = 0.0  # PnL for this fill if it's part of a round trip


@dataclass
class PnLStats:
    initial_account_value: float
    initial_base_balance: float
    initial_quote_balance: float
    initial_base_price: float
    current_account_value: float = 0.0
    grid_pnl: float = 0.0  # PnL from grid trading only
    last_update: datetime = field(default_factory=datetime.utcnow)


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
        self._order_lock = asyncio.Lock()  # Add lock for order operations

        # Grid trading parameters
        self._grid_spacing_bps = config["trading"]["grid_spacing_bps"]  # Distance from mid price in basis points
        self._grid_size = config["trading"]["order_size"]  # Size per order
        
        # Status reporting
        self._last_status_time = datetime.utcnow()
        self._status_interval = timedelta(seconds=10)
        
        # Fill tracking
        self._fills: List[Fill] = []
        self._last_fill_report = datetime.utcnow()
        self._fill_report_interval = timedelta(minutes=1)
        
        # Latest orderbook state
        self._latest_ob: Optional[OrderBook] = None
        
        # Exchange info
        self._symbol_info = None
        self._price_precision = None
        self._quantity_precision = None
        
        # PnL tracking
        self._pnl_stats: Optional[PnLStats] = None

    async def run(self) -> None:
        symbol = self._cfg["trading"]["symbol"].upper()
        stream = CombinedStream(self._c, symbol)

        # Get symbol info first
        await self._get_symbol_info(symbol)
        
        # Get initial balances
        base_currency = symbol[:-4]  # e.g., BTC from BTCUSDT
        quote_currency = symbol[-4:]  # e.g., USDT from BTCUSDT
        
        try:
            # Clean up any existing orders first
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
            
            account_info = await self._c.get_account_info()
            base_balance = 0.0
            quote_balance = 0.0
            
            for balance in account_info["balances"]:
                if balance["asset"] == base_currency:
                    base_balance = float(balance["free"])
                elif balance["asset"] == quote_currency:
                    quote_balance = float(balance["free"])
            
            self._inv.set_available_balance(base_balance, quote_balance)
            self._logger.info(
                "Initial balances loaded - Base: %.6f %s, Quote: %.2f %s",
                base_balance, base_currency, quote_balance, quote_currency
            )
        except Exception as e:
            self._logger.error("Failed to load initial balances: %s", e)
            raise

        # Wait for first valid orderbook
        self._logger.info("Waiting for first valid orderbook...")
        async for ob_snapshot, trades in stream.stream():
            self._latest_ob = OrderBook(ob_snapshot)
            if self._latest_ob.best_bid and self._latest_ob.best_ask:
                break
        
        # Initialize PnL stats
        self._pnl_stats = PnLStats(
            initial_account_value=self._inv.get_account_value(self._latest_ob.mid_price),
            initial_base_balance=base_balance,
            initial_quote_balance=quote_balance,
            initial_base_price=self._latest_ob.mid_price
        )
        
        # Create event for initial order placement
        initial_order_event = asyncio.Event()
        
        # Fan‑in user events (fills / cancels)
        fills_task = asyncio.create_task(self._handle_fills(initial_order_event))
        
        try:
            # Signal that we're ready to place initial orders
            initial_order_event.set()
            
            async for ob_snapshot, trades in stream.stream():
                # Update latest orderbook state
                self._latest_ob = OrderBook(ob_snapshot)
                
                # Skip if we don't have a valid orderbook yet
                if not self._latest_ob.best_bid or not self._latest_ob.best_ask:
                    self._logger.debug("Waiting for valid orderbook...")
                    continue
                
                band, bid_size, ask_size = await self._strat.quote(self._latest_ob)
                now = datetime.utcnow()
                
                # Report status periodically
                if now - self._last_status_time >= self._status_interval:
                    await self._report_status(symbol, self._latest_ob)
                    self._report_fills()  # Add fills report to status interval
                    self._last_status_time = now
                
                # Only check for orders that need to be cancelled
                await self._reconcile(symbol, band.bid, bid_size, band.ask, ask_size, now)
                
                # Check if fills task is still running
                if fills_task.done():
                    exc = fills_task.exception()
                    if exc:
                        self._logger.error("Fills task failed: %s", exc)
                        raise exc
                    else:
                        self._logger.error("Fills task completed unexpectedly")
                        raise RuntimeError("Fills task completed unexpectedly")

        except Exception as e:
            self._logger.error("Error in main loop: %s", e, exc_info=True)
            raise
        finally:
            fills_task.cancel()
            try:
                await fills_task
            except asyncio.CancelledError:
                pass

    async def _verify_orders(self, symbol: str) -> None:
        """Verify that our tracked orders match the exchange state."""
        # Create a copy of live orders to avoid modification during iteration
        orders_to_verify = list(self._live.values())
        orders_removed = False
        
        for order in orders_to_verify:
            try:
                exchange_order = await self._c.get_order_status(symbol, order.client_id)
                if exchange_order is None:
                    self._logger.warning(
                        "Order %s not found on exchange, removing from tracking",
                        order.client_id
                    )
                    self._live.pop(order.client_id, None)
                    orders_removed = True
                elif exchange_order.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                    self._logger.info(
                        "Order %s is %s on exchange, removing from tracking",
                        order.client_id, exchange_order.status
                    )
                    self._live.pop(order.client_id, None)
                    orders_removed = True
                    
                    # If order was filled, record it
                    if exchange_order.status == "FILLED":
                        self._fills.append(Fill(
                            side=order.side,
                            price=order.price,
                            size=order.size,
                            timestamp=datetime.utcnow()
                        ))
                        # Update balances from exchange
                        await self._update_balances_from_exchange(symbol)
            except Exception as e:
                self._logger.error("Error verifying order %s: %s", order.client_id, e)
        
        # If any orders were removed and we have a valid orderbook, place new orders
        if orders_removed and self._latest_ob and len(self._live) < 2:
            self._logger.info("Orders were removed, placing new grid orders...")
            await self._place_grid_orders(symbol, self._latest_ob.mid_price)

    async def _report_status(self, symbol: str, ob: OrderBook) -> None:
        """Report current position, orders, and market state."""
        # Verify orders with exchange
        await self._verify_orders(symbol)
        
        # Get current position
        position = self._inv.position
        position_value = position * ob.mid_price
        account_value = self._inv.get_account_value(ob.mid_price)
        
        # Format live orders
        live_orders = []
        # Create a copy of live orders to iterate over
        orders_to_check = list(self._live.values())
        for order in orders_to_check:
            try:
                exchange_order = await self._c.get_order_status(symbol, order.client_id)
                age = (datetime.utcnow() - order.birth).total_seconds()
                live_orders.append(
                    f"{order.side} @ {order.price:.2f} ({age:.1f}s)"
                )
            except Exception as e:
                self._logger.error("Error getting order status: %s", e)
                live_orders.append(
                    f"{order.side} @ {order.price:.2f} (ERROR)"
                )
        
        # Calculate PnL metrics if available
        pnl_info = ""
        if self._pnl_stats:
            total_return = (account_value - self._pnl_stats.initial_account_value) / self._pnl_stats.initial_account_value * 100
            grid_return = self._pnl_stats.grid_pnl / self._pnl_stats.initial_account_value * 100
            price_return = ((ob.mid_price - self._pnl_stats.initial_base_price) / self._pnl_stats.initial_base_price) * 100
            
            pnl_info = (
                f"\nPnL Metrics:\n"
                f"  Grid Strategy PnL: {self._pnl_stats.grid_pnl:.2f} USD ({grid_return:.2f}%)\n"
                f"  Total Return: {total_return:.2f}%\n"
                f"  Base Asset Return: {price_return:.2f}%"
            )
            
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
        """Report fill statistics and round trips."""
        if not self._fills:
            return
            
        now = datetime.utcnow()
        if now - self._last_fill_report < self._fill_report_interval:
            return
            
        # Calculate statistics
        total_fills = len(self._fills)
        buy_fills = [f for f in self._fills if f.side == "BUY"]
        sell_fills = [f for f in self._fills if f.side == "SELL"]
        
        # Calculate round trips
        round_trips: List[Tuple[Fill, Fill]] = []
        remaining_buys = buy_fills.copy()
        remaining_sells = sell_fills.copy()
        
        # Match buys with sells to form round trips
        for buy in buy_fills:
            for sell in remaining_sells:
                if sell.timestamp > buy.timestamp:
                    pnl = (sell.price - buy.price) * buy.size
                    buy.pnl = pnl
                    sell.pnl = pnl
                    round_trips.append((buy, sell))
                    remaining_sells.remove(sell)
                    break
        
        # Calculate PnL
        total_pnl = sum(trip[0].pnl for trip in round_trips)
        
        # Update PnL stats if we have them
        if self._pnl_stats and self._latest_ob:
            self._pnl_stats.grid_pnl = total_pnl
            self._pnl_stats.current_account_value = self._inv.get_account_value(self._latest_ob.mid_price)
            self._pnl_stats.last_update = now
            
            # Calculate total return and grid strategy return
            total_return = (self._pnl_stats.current_account_value - self._pnl_stats.initial_account_value) / self._pnl_stats.initial_account_value * 100
            grid_return = self._pnl_stats.grid_pnl / self._pnl_stats.initial_account_value * 100
            price_return = ((self._latest_ob.mid_price - self._pnl_stats.initial_base_price) / self._pnl_stats.initial_base_price) * 100
            
            # Log fill report with both PnL metrics
            self._logger.info(
                "\n=== Fill Report ===\n"
                f"Total Fills: {total_fills}\n"
                f"Buy Fills: {len(buy_fills)}\n"
                f"Sell Fills: {len(sell_fills)}\n"
                f"Round Trips: {len(round_trips)}\n"
                f"Grid Strategy PnL: {total_pnl:.2f} USD\n"
                f"Grid Strategy Return: {grid_return:.2f}%\n"
                f"Total Account Value: {self._pnl_stats.current_account_value:.2f} USD\n"
                f"Total Return: {total_return:.2f}%\n"
                f"Base Asset Return: {price_return:.2f}%\n"
                "==================="
            )
        else:
            # Log basic fill report if PnL stats not available
            self._logger.info(
                "\n=== Fill Report ===\n"
                f"Total Fills: {total_fills}\n"
                f"Buy Fills: {len(buy_fills)}\n"
                f"Sell Fills: {len(sell_fills)}\n"
                f"Round Trips: {len(round_trips)}\n"
                f"Total PnL: {total_pnl:.2f} USD\n"
                f"Average PnL per Round Trip: {total_pnl/len(round_trips) if round_trips else 0:.2f} USD\n"
                "==================="
            )
        
        self._last_fill_report = now

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

    async def _place_grid_orders(self, symbol: str, mid_price: float) -> None:
        """Place a new pair of grid orders around the mid price."""
        async with self._order_lock:
            # Cancel any existing orders first
            remaining_orders = list(self._live.values())
            for order in remaining_orders:
                await self._cancel_order_safe(symbol, order.client_id)

            # Place new buy order below mid price
            buy_price = self._round_price(mid_price * (1 - self._grid_spacing_bps / 10000))
            await self._place(symbol, "BUY", buy_price, self._grid_size)
            
            # Place new sell order above mid price
            sell_price = self._round_price(mid_price * (1 + self._grid_spacing_bps / 10000))
            await self._place(symbol, "SELL", sell_price, self._grid_size)
            
            self._logger.info(
                "\n=== New Orders ===\n"
                f"BUY  @ {buy_price:.2f}\n"
                f"SELL @ {sell_price:.2f}\n"
                f"(Mid: {mid_price:.2f})\n"
                "================"
            )

    async def _reconcile(
        self,
        symbol: str,
        bid_px: float,
        bid_sz: float,
        ask_px: float,
        ask_sz: float,
        ts: datetime,
    ) -> None:
        """Check if existing orders are too far from mid price and cancel them if needed."""
        if not self._latest_ob:
            return

        mid_price = self._latest_ob.mid_price
        
        # Check if any existing orders are too far from mid price
        orders_to_cancel = []
        for order in self._live.values():
            price_diff_bps = abs(order.price - mid_price) / mid_price * 10000
            if price_diff_bps > self._grid_spacing_bps * 1.5:  # Add some buffer
                orders_to_cancel.append(order)
        
        # Cancel orders that are too far from mid price
        for order in orders_to_cancel:
            await self._cancel_order_safe(symbol, order.client_id)

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

    async def _handle_fills(self, initial_order_event: asyncio.Event):
        """Handle user events stream and process fills."""
        self._logger.info("Starting user events stream handler")
        
        # Wait for signal to place initial orders
        await initial_order_event.wait()
        
        # Place initial orders if we don't have any
        if not self._live and self._latest_ob:
            self._logger.info("Placing initial orders...")
            await self._place_grid_orders(self._cfg["trading"]["symbol"].upper(), self._latest_ob.mid_price)
        
        async for evt in self._c.stream_user_events():
            try:
                # Debug log the raw event
                self._logger.debug("Received user event: %s", evt)
                
                if evt.get("e") != "executionReport":
                    continue
                    
                status = evt.get("X")
                side = evt.get("S")
                client_id = evt.get("c")
                filled_qty = float(evt.get("l", 0))
                filled_px = float(evt.get("L", 0))
                symbol = evt.get("s")
                order_type = evt.get("o")  # Order type
                time_in_force = evt.get("f")  # Time in force
                
                self._logger.debug(
                    "Processing execution report - Status: %s, Side: %s, Type: %s, TIF: %s, Qty: %.6f, Price: %.2f",
                    status, side, order_type, time_in_force, filled_qty, filled_px
                )

                if status in {"FILLED", "PARTIALLY_FILLED"} and filled_qty > 0:
                    self._logger.info(
                        "\n=== Fill ===\n"
                        f"{side} @ {filled_px:.2f}\n"
                        "==========="
                    )
                    
                    # Remove the filled order from live orders first
                    self._live.pop(client_id, None)
                    
                    # Update balances from exchange
                    await self._update_balances_from_exchange(symbol)
                    
                    # Record fill for statistics
                    self._fills.append(Fill(
                        side=side,
                        price=filled_px,
                        size=filled_qty,
                        timestamp=datetime.utcnow()
                    ))
                    
                    # Place new grid orders
                    if self._latest_ob:
                        await self._place_grid_orders(symbol, self._latest_ob.mid_price)
                    
                    # Report fill statistics
                    self._report_fills()
                    self._logger.debug("Fill report generated with %d fills", len(self._fills))

                # Drop from live map if the order is closed
                if status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                    self._live.pop(client_id, None)
                    
            except Exception as e:
                self._logger.error("Error processing user event: %s", e, exc_info=True)

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
            # Log balances at DEBUG level
            self._logger.debug(
                "Current balances before placing %s order - Base: %.6f, Quote: %.2f",
                side, self._inv.available_balances["base"], self._inv.available_balances["quote"]
            )
            
            order: Order = await self._c.place_limit_order(symbol, side, price, size, client_id)
            
            # Verify the order was actually placed
            exchange_order = await self._c.get_order_status(symbol, client_id)
            if exchange_order is None:
                raise RuntimeError(f"Order {client_id} not found on exchange after placement")
                
            self._live[client_id] = LiveOrder(
                client_id=client_id,
                side=side,
                price=order.price,
                size=order.size,
            )
            
            # Log balances at DEBUG level
            self._logger.debug(
                "Balances after placing %s order - Base: %.6f, Quote: %.2f",
                side, self._inv.available_balances["base"], self._inv.available_balances["quote"]
            )
            
            self._logger.debug("%s @ %.2f", side, price)
            
        except Exception as exc:
            self._logger.error("Failed to place order: %s", exc)
            raise

    async def _cancel_order_safe(self, symbol: str, client_id: str) -> bool:
        """Safely cancel an order, checking if it exists first.
        
        Returns:
            bool: True if the order was cancelled or didn't exist, False if there was an error
        """
        try:
            # Check if order exists and is in a cancellable state
            exchange_order = await self._c.get_order_status(symbol, client_id)
            if exchange_order is None or exchange_order.status not in {"NEW", "PARTIALLY_FILLED"}:
                self._live.pop(client_id, None)
                return True
                
            # Order exists and is cancellable, try to cancel it
            await self._c.cancel_order(symbol, client_id)
            self._live.pop(client_id, None)
            return True
            
        except Exception as e:
            if "Unknown order" in str(e):
                self._live.pop(client_id, None)
                return True
            self._logger.error("Error cancelling order %s: %s", client_id, e)
            return False
