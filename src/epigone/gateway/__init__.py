"""The HyperliquidGateway seam.

ALL Hyperliquid I/O — positions, fills, portfolio stats, leaderboard, archive
reads — goes through this interface and nowhere else (ADR-0001; V1 spec
"Testing Decisions", GitHub issue #1). Tests inject a fake; production wires
the real client.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Protocol


class Side(Enum):
    LONG = "long"
    SHORT = "short"


@dataclass(frozen=True)
class Position:
    """An open perp position held by a Trader (an observed wallet, never a User)."""

    coin: str
    side: Side
    size_usd: Decimal
    leverage: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal


class HyperliquidGateway(Protocol):
    async def get_open_positions(self, address: str) -> list[Position]:
        """Current open perp positions for a Trader's address."""
        ...
