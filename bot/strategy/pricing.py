"""Small numeric helpers for spread and price-band calculations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QuoteBand:
    """Represents a target bid/ask band around a reference price."""

    bid: float
    ask: float

    @property
    def mid(self) -> float:  # noqa: D401
        """Return band mid-price."""
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    def within(self, price: float) -> bool:
        """Check whether *price* lies within the band."""
        return self.bid <= price <= self.ask
