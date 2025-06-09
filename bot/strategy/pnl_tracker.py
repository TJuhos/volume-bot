from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from bot.utils.logger import get_child_logger


@dataclass
class OrderInfo:
    """Information about a placed order."""
    side: str
    size: float
    mid_price: float  # Mid price when order was placed


@dataclass
class PnLTracker:
    """Tracks both Total PnL and Pure grid-trading PnL."""
    
    initial_mid_price: float
    initial_base_balance: float
    initial_quote_balance: float
    
    # State variables
    current_mid_price: float = field(init=False)
    base_balance: float = field(init=False)
    quote_balance: float = field(init=False)
    expo_pnl: float = 0.0  # Exposure PnL (mark-to-market)
    grid_pnl: float = 0.0  # Grid trading PnL
    total_pnl: float = 0.0  # Total PnL (grid + exposure)
    
    # Order tracking
    _orders: Dict[str, OrderInfo] = field(default_factory=dict)
    _open_positions: List[Tuple[float, float]] = field(default_factory=list)  # List of (size, price) for open positions
    
    def __post_init__(self):
        self.current_mid_price = self.initial_mid_price
        self.base_balance = self.initial_base_balance
        self.quote_balance = self.initial_quote_balance
        self._logger = get_child_logger(None, "pnl_tracker")
    
    def update_mid_price(self, new_mid_price: float) -> None:
        """Update PnL when mid price changes."""
        # Calculate drift since last update
        price_drift = new_mid_price - self.current_mid_price
        drift_pnl = self.base_balance * price_drift
        
        # Update exposure PnL with the new drift
        self.expo_pnl += drift_pnl
        
        # Update current price
        self.current_mid_price = new_mid_price
        
        # Calculate current account value
        current_value = self.account_equity
        initial_value = self.initial_quote_balance + (self.initial_base_balance * self.initial_mid_price)
        
        # Calculate total PnL (change in account value)
        self.total_pnl = current_value - initial_value
        
        # Grid PnL is what's left after subtracting exposure PnL
        self.grid_pnl = self.total_pnl - self.expo_pnl
        
        self._logger.debug(
            f"Updated PnL metrics:\n"
            f"  Price drift: {price_drift:.2f}\n"
            f"  Drift PnL: {drift_pnl:.2f}\n"
            f"  Exposure PnL: {self.expo_pnl:.2f}\n"
            f"  Grid PnL: {self.grid_pnl:.2f}\n"
            f"  Total PnL: {self.total_pnl:.2f}"
        )
    
    def register_order(self, order_id: str, side: str, size: float) -> None:
        """Register a new order."""
        self._orders[order_id] = OrderInfo(
            side=side,
            size=size,
            mid_price=self.current_mid_price
        )
        self._logger.debug(
            f"Registered {side} order:\n"
            f"  Order ID: {order_id}\n"
            f"  Size: {size:.6f}\n"
            f"  Mid Price: {self.current_mid_price:.2f}"
        )
    
    def process_fill(self, order_id: str, fill_price: float, fee: float) -> None:
        """Process a fill by updating balances."""
        if order_id not in self._orders:
            raise ValueError(f"Order {order_id} not found in order tracking")
            
        order = self._orders[order_id]
        size = order.size
        
        # Update balances
        if order.side == "BUY":
            self.base_balance += size
            self.quote_balance -= (size * fill_price + fee)
            # Add to open positions
            self._open_positions.append((size, fill_price))
        else:  # SELL
            self.base_balance -= size
            self.quote_balance += (size * fill_price - fee)
            # Match against open positions (FIFO)
            remaining_size = size
            while remaining_size > 0 and self._open_positions:
                buy_size, buy_price = self._open_positions[0]
                match_size = min(remaining_size, buy_size)
                # Calculate realized PnL for this match
                self.expo_pnl += (fill_price - buy_price) * match_size
                remaining_size -= match_size
                # Update or remove the matched position
                if match_size == buy_size:
                    self._open_positions.pop(0)
                else:
                    self._open_positions[0] = (buy_size - match_size, buy_price)
            
            # Subtract fee from total trade PnL
            self.expo_pnl -= fee
        
        # Update PnL metrics
        self.update_mid_price(self.current_mid_price)
        
        self._logger.info(
            f"Processed fill:\n"
            f"  Order ID: {order_id}\n"
            f"  Size: {size:.6f}\n"
            f"  Fill Price: {fill_price:.2f}\n"
            f"  Fee: {fee:.6f}\n"
            f"  Grid PnL: {self.grid_pnl:.2f}\n"
            f"  Total PnL: {self.total_pnl:.2f}"
        )
    
    @property
    def account_equity(self) -> float:
        """Current account equity in quote currency."""
        return self.quote_balance + (self.base_balance * self.current_mid_price)
    
    @property
    def total_return_pct(self) -> float:
        """Total return as a percentage of initial account value."""
        initial_value = self.initial_quote_balance + (self.initial_base_balance * self.initial_mid_price)
        return (self.account_equity - initial_value) / initial_value * 100
    
    @property
    def grid_return_pct(self) -> float:
        """Grid trading return as a percentage of initial account value."""
        initial_value = self.initial_quote_balance + (self.initial_base_balance * self.initial_mid_price)
        return self.grid_pnl / initial_value * 100
    
    @property
    def exposure_return_pct(self) -> float:
        """Exposure return as a percentage of initial account value."""
        initial_value = self.initial_quote_balance + (self.initial_base_balance * self.initial_mid_price)
        return self.expo_pnl / initial_value * 100
    
    @property
    def realized_return_pct(self) -> float:
        """Realized return as a percentage of initial account value."""
        initial_value = self.initial_quote_balance + (self.initial_base_balance * self.initial_mid_price)
        return self.total_pnl / initial_value * 100 