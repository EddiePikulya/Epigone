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


class GatewayError(Exception):
    """A Hyperliquid data source failed (network, HTTP, or malformed payload)."""


class Side(Enum):
    LONG = "long"
    SHORT = "short"


class Window(Enum):
    """Timeframes Hyperliquid precomputes portfolio stats for (spec-defaults two-stage scan)."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    ALL_TIME = "allTime"


@dataclass(frozen=True)
class LeaderboardEntry:
    """One row of the leaderboard source — a candidate Trader for the Universe."""

    address: str
    display_name: str | None
    account_value: Decimal


@dataclass(frozen=True)
class PortfolioWindow:
    """One timeframe of a Trader's portfolio stats, from a single weight-20 call."""

    pnl: Decimal
    volume: Decimal
    account_value: Decimal
    starting_account_value: Decimal


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

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        """Candidate Traders from the leaderboard source. Raises GatewayError on failure."""
        ...

    async def get_portfolio(self, address: str) -> dict[Window, PortfolioWindow]:
        """Windowed portfolio stats for a Trader. Raises GatewayError on failure."""
        ...
