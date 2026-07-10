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
class LeaderboardWindow:
    """One timeframe's precomputed performance carried by a leaderboard row.

    Hyperliquid ships these for every row of the leaderboard download, so the
    coarse Metric Library is populated Universe-wide at zero per-account API
    cost (issue #26). `roi` is Hyperliquid's own net-deposit-adjusted figure."""

    pnl: Decimal
    roi: Decimal
    volume: Decimal


@dataclass(frozen=True)
class LeaderboardEntry:
    """One row of the leaderboard source — a candidate Trader for the Universe.

    Carries the coarse metrics the row already includes (issue #26):
    `account_value` (account-wide) plus per-window pnl/roi/volume. Seeding
    persists them straight into `coarse_metrics`, retiring the old per-account
    portfolio scan."""

    address: str
    display_name: str | None
    account_value: Decimal
    windows: dict[Window, LeaderboardWindow]


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
    async def get_open_positions(self, address: str, dex: str | None = None) -> list[Position]:
        """Current open perp positions for a Trader's address.

        `dex` selects a HIP-3 builder-deployed perp DEX (e.g. "xyz", issue #21);
        None reads the core venue. Builder-DEX coins are namespaced `dex:COIN`
        (e.g. `xyz:META`), keeping them distinct from core positions."""
        ...

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        """Candidate Traders from the leaderboard source, each carrying its coarse
        metrics (issue #26). Raises GatewayError on failure."""
        ...

    async def get_fills(self, address: str) -> list[Fill]:
        """A Trader's recent fills, newest first (the info API caps at ~2000).
        Raises GatewayError on failure."""
        ...
