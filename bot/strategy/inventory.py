"""Track running inventory and translate it into quoting skew."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class InventoryManager:
    """Keeps net position and derives skew bps."""

    max_position: float  # hard absolute cap (quote currency)
    soft_cap: float     # inventory level where we start to skew quotes aggressively

    position: float = 0.0  # +long, -short notionals
    available_balances: Dict[str, float] = field(default_factory=lambda: {"base": 0.0, "quote": 0.0})  # Available balances in base and quote currencies

    # --- position updates --- #

    def update_with_fill(self, side: str, size: float, price: float):
        """Apply a trade fill to inventory."""
        notional = size * price
        if side.lower() == "buy":
            self.position += notional
            self.available_balances["quote"] -= notional  # Decrease quote currency balance
            self.available_balances["base"] += size  # Increase base currency balance
        else:
            self.position -= notional
            self.available_balances["quote"] += notional  # Increase quote currency balance
            self.available_balances["base"] -= size  # Decrease base currency balance

    def set_available_balance(self, base_balance: float, quote_balance: float):
        """Set the available balances for both base and quote currencies."""
        self.available_balances["base"] = base_balance
        self.available_balances["quote"] = quote_balance

    def get_account_value(self, current_price: float) -> float:
        """Calculate total account value (position value + available balances)."""
        position_value = self.position * current_price
        base_value = self.available_balances["base"] * current_price
        quote_value = self.available_balances["quote"]
        return position_value + base_value + quote_value

    # --- skew logic --- #

    def skew_bps(self) -> float:
        """Linear skew: ±100 bps when at ±soft_cap (clipped)."""
        pct = max(-1.0, min(1.0, self.position / self.soft_cap)) if self.soft_cap else 0.0
        return 100.0 * pct  # bps adjustment applied asymmetrically

    def breached_hard_cap(self) -> bool:
        return abs(self.position) >= self.max_position
