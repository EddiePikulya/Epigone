"""The HyperliquidGateway seam.

ALL Hyperliquid I/O — positions, fills, portfolio stats, leaderboard, archive
reads — goes through this interface and nowhere else (ADR-0001; V1 spec
"Testing Decisions", GitHub issue #1). Tests inject a fake; production wires
the real client.
"""

from dataclasses import dataclass
from datetime import datetime
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
    """One row of the leaderboard source — a candidate Trader for the Universe.

    Deliberately thin: metrics come from the coarse portfolio pass, so the
    seam carries only what seeding persists."""

    address: str
    display_name: str | None


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


@dataclass(frozen=True)
class Fill:
    """One fill from a Trader's history — the raw material of fine metrics (issue #8).

    `direction` keeps Hyperliquid's raw dir string ("Open Long", "Close Short",
    "Long > Short", "Settlement", "Buy", …); the classification properties own
    its semantics so callers never string-match themselves."""

    coin: str
    price: Decimal
    size: Decimal  # unsigned, in coin units
    direction: str
    closed_pnl: Decimal  # realized PnL this fill, before fees; 0 for opens
    start_position: Decimal  # signed position size before this fill (negative = short)
    crossed: bool  # True = taker (crossed the book), False = maker (resting order)
    order_id: int
    time: datetime

    @property
    def is_perp(self) -> bool:
        """Perp fills name a Long/Short leg; Settlement force-closes a delisted
        perp market. Everything else ("Buy"/"Sell"/dust conversion) is spot."""
        return (
            "Long" in self.direction or "Short" in self.direction or self.direction == "Settlement"
        )

    @property
    def closes_position(self) -> bool:
        """Fills that realize PnL: closes, flips ("Long > Short"), liquidations,
        and settlements. Opens and spot fills never do."""
        return (
            self.direction.startswith("Close")
            or ">" in self.direction
            or "Liquidat" in self.direction
            or self.direction == "Settlement"
        )


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

    async def get_fills(self, address: str) -> list[Fill]:
        """A Trader's recent fills, newest first (the info API caps at ~2000).
        Raises GatewayError on failure."""
        ...
