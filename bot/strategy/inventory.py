"""Track running inventory and translate it into quoting skew."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class InventoryManager:
    """Keeps net position and derives skew bps."""

    max_position: float  # hard absolute cap (quote currency)
    soft_cap: float     # inventory level where we start to skew quotes aggressively

    position: float = 0.0  # +long, -short notionals

    # --- position updates --- #

    def update_with_fill(self, side: str, size: float, price: float):
        """Apply a trade fill to inventory."""
        notional = size * price
        if side.lower() == "buy":
            self.position += notional
        else:
            self.position -= notional

    # --- skew logic --- #

    def skew_bps(self) -> float:
        """Linear skew: ±100 bps when at ±soft_cap (clipped)."""
        pct = max(-1.0, min(1.0, self.position / self.soft_cap)) if self.soft_cap else 0.0
        return 100.0 * pct  # bps adjustment applied asymmetrically

    def breached_hard_cap(self) -> bool:
        return abs(self.position) >= self.max_position
