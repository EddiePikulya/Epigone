"""The HyperliquidGateway seam.

ALL Hyperliquid I/O — positions, fills, portfolio stats, leaderboard, archive
reads — goes through this interface and nowhere else (ADR-0001; V1 spec
"Testing Decisions", GitHub issue #1). Tests inject a fake; production wires
the real client.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol


class GatewayError(Exception):
    """A Hyperliquid data source failed (network, HTTP, or malformed payload)."""


class RateLimitedError(GatewayError):
    """Hyperliquid kept answering 429 after backoff-and-retry (issue #28).

    Pacing, not an outage: callers should treat the item as retryable later and
    never count it toward outage-style abort thresholds."""


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
    """An open perp position held by a Trader (an observed wallet, never a User).

    `size_usd` is the leveraged notional; `margin` is the real money the Trader
    put up (issue #35). Hyperliquid returns both directly — `marginUsed` and
    `returnOnEquity` — but they're optional here so a synthesized Position (tests,
    a snapshot replay) can omit them and fall back to notional/leverage."""

    coin: str
    side: Side
    size_usd: Decimal
    leverage: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal
    margin_used: Decimal | None = None  # exact marginUsed from the API; None → derive
    return_on_equity: Decimal | None = None  # returnOnEquity (PnL over margin), a ratio

    @property
    def margin(self) -> Decimal:
        """Money at risk: the API's exact `marginUsed`, else notional/leverage
        (issue #35). Leverage is always positive for an open position, so the
        fallback never divides by zero."""
        if self.margin_used is not None:
            return self.margin_used
        return self.size_usd / self.leverage

    @property
    def return_on_margin(self) -> Decimal | None:
        """Unrealized return on the money put up — the figure that makes leverage
        legible (+357% on $96, issue #35). The API's `returnOnEquity` when present,
        else derived from uPnL over margin; None only if margin is zero."""
        if self.return_on_equity is not None:
            return self.return_on_equity
        margin = self.margin
        return self.unrealized_pnl / margin if margin else None


@dataclass(frozen=True)
class OpenOrder:
    """A resting (unfilled) order on a Trader's book — their plan before it
    executes (issue #115).

    `size` is what remains to fill, in coin units — except 0, which on a
    position-wide TP/SL (`is_position_tpsl`) means "the whole position at
    trigger time", never an empty order. For a trigger order `trigger_price`
    is the price that arms it and `limit_price` only the slippage bound the
    fill is capped at (observed live: a stop's limitPx sits ~8% past its
    triggerPx); for a plain resting limit `trigger_price` is None and
    `limit_price` is where it rests."""

    coin: str
    is_buy: bool  # side "B" (bid) is a buy, "A" (ask) a sell
    limit_price: Decimal
    size: Decimal
    order_id: int
    placed_at: datetime
    order_type: str  # the API's raw string: "Limit", "Stop Market", "Take Profit Market", …
    is_trigger: bool
    trigger_price: Decimal | None
    is_position_tpsl: bool
    reduce_only: bool

    @property
    def notional_usd(self) -> Decimal | None:
        """The order's dollar size: size × the price it aims to execute near —
        the trigger price for a trigger order (its limit is just the slippage
        cap). None for a whole-position TP/SL (size 0): that notional is the
        position's at trigger time, unknowable from the order, so callers
        treat None as "never floor-suppressed", exactly like a position event
        without a notional."""
        if self.size == 0:
            return None
        price = self.trigger_price if self.trigger_price is not None else self.limit_price
        return self.size * price

    @property
    def tpsl(self) -> str | None:
        """The "TP"/"SL" tag a trigger order renders with (issue #115): its
        orderType family names the intent ("Take Profit …" / "Stop …"). None
        for a plain resting limit."""
        if not self.is_trigger:
            return None
        return "TP" if self.order_type.startswith("Take Profit") else "SL"

    def to_wire(self) -> dict[str, Any]:
        """This order as one entry of an order_alerts batch (issue #115) — the
        JSONB payload the stream queues and the bot renders (ADR-0002: the
        processes meet only in Postgres). Decimals ride as strings so they
        round-trip exactly (the criteria.filters precedent); `notional_usd` is
        stored too — redundant with the prices, but it documents what the
        min-size floor judged and keeps the row greppable in SQL."""
        notional = self.notional_usd
        return {
            "coin": self.coin,
            "is_buy": self.is_buy,
            "limit_price": str(self.limit_price),
            "size": str(self.size),
            "order_id": self.order_id,
            "placed_at": self.placed_at.isoformat(),
            "order_type": self.order_type,
            "is_trigger": self.is_trigger,
            "trigger_price": str(self.trigger_price) if self.trigger_price is not None else None,
            "is_position_tpsl": self.is_position_tpsl,
            "reduce_only": self.reduce_only,
            "notional_usd": str(notional) if notional is not None else None,
        }

    @classmethod
    def from_wire(cls, entry: Mapping[str, Any]) -> "OpenOrder":
        """The inverse of to_wire, for the delivery side. `notional_usd` is not
        read back — the property recomputes it from the exactly round-tripped
        prices, so the stored copy can never drift from the rendered one."""
        trigger_price = entry["trigger_price"]
        return cls(
            coin=entry["coin"],
            is_buy=entry["is_buy"],
            limit_price=Decimal(entry["limit_price"]),
            size=Decimal(entry["size"]),
            order_id=entry["order_id"],
            placed_at=datetime.fromisoformat(entry["placed_at"]),
            order_type=entry["order_type"],
            is_trigger=entry["is_trigger"],
            trigger_price=Decimal(trigger_price) if trigger_price is not None else None,
            is_position_tpsl=entry["is_position_tpsl"],
            reduce_only=entry["reduce_only"],
        )


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

    async def get_open_orders(self, address: str, dex: str | None = None) -> list[OpenOrder]:
        """A Trader's resting orders on ONE venue, trigger/TP-SL legs included
        (issue #115; the info endpoint is `frontendOpenOrders`).

        PER-DEX, verified live 2026-07-24 against wallets holding xyz orders
        (the #63 never-assume-coverage lesson): with no `dex` the endpoint
        returns ONLY core-venue orders — a resting xyz ladder comes back []
        until queried with dex="xyz" — so full coverage means one call per
        POSITION_VENUES entry, exactly like clearinghouseState. Builder-DEX
        coins arrive already namespaced (`xyz:BB`); implementations keep them
        so (prefixing is idempotent, matching positions).

        Response shape (recorded live 2026-07-24, tests/test_gateway_http_parsing):
        a flat array; `side` is "B" (bid/buy) or "A" (ask/sell). Trigger
        orders carry isTrigger true, the arming price in triggerPx, and an
        orderType naming the intent ("Stop Market", "Take Profit Market", …);
        their limitPx is only the slippage bound. A position-wide TP/SL adds
        isPositionTpsl true with sz "0.0" — sized to whatever the position is
        when it triggers. Raises GatewayError on failure."""
        ...

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        """Candidate Traders from the leaderboard source, each carrying its coarse
        metrics (issue #26). Raises GatewayError on failure."""
        ...

    async def get_fills(self, address: str) -> list[Fill]:
        """A Trader's recent fills in EXECUTION ORDER — oldest first, and
        same-millisecond fills in the sequence they executed. The stream is the
        UNION of regular and TWAP slice executions: Hyperliquid serves TWAP
        slices only from userTwapSliceFills, never from userFills (issue #63),
        so a single-endpoint read walks a TWAP-heavy Trader's positions wrong.
        Same-order fills share one timestamp, so list order is the only
        within-ms signal and the round-trip engine (#58) depends on it;
        implementations must normalize whatever the APIs serve. Each endpoint
        caps at ~2000 fills, so the two sources' windows can differ — for a
        TWAP whale the slice history is hours where the regular history is
        days; the engine's continuity guard (#63) owns that truncation.
        Raises GatewayError on failure."""
        ...

    async def get_fills_since(self, address: str, start: datetime) -> list[Fill]:
        """A Trader's fills at or after `start` — the same regular ∪ TWAP-slice
        union as get_fills (userFillsByTime plus userTwapSliceFillsByTime) —
        for the incremental fine refresh (issue #11): a fast-tier pass fetches
        only what is new since its checkpoint instead of re-pulling full
        history. Both endpoints are startTime-inclusive at millisecond
        resolution, so a caller stepping +1ms past its checkpoint gets a
        stream disjoint from everything already folded, across BOTH sources.
        The returned stream must also be COMPLETE for the union of both
        sources up to its own newest fill — implementations fetching sources
        sequentially must bound them to a shared coverage horizon (#63
        review), or a fill landing between the fetches would sit below the
        advanced checkpoint and be skipped forever. Same ~2000 cap per call
        per endpoint, so callers checkpoint forward far enough that a window
        never overflows. Same execution-order contract as get_fills. Raises
        GatewayError on failure."""
        ...


# Every info endpoint one fill fetch hits: the regular fills endpoint plus the
# TWAP slice endpoint (issue #63). The fine pass bills its base weight per
# endpoint from this same count — the POSITION_VENUES billing pattern below —
# so changing what a fetch hits means changing this number in the same file.
FILL_ENDPOINTS = 2

# The HIP-3 builder DEXes Epigone covers for POSITIONS (fills-side metrics are
# account-wide across all dexs regardless): xyz hosts ~90% of non-core activity
# (equity/"stock" perps: xyz:META, xyz:BB, …); mkts (Markets by Kinetiq) adds
# the index perps worth alerting on (mkts:US500, mkts:QQQ). Covering a venue
# costs one weight-2 clearinghouseState call per tracked wallet per poll — three
# venues = weight 6/wallet, so the stream reserve's instant-claim floor (120)
# guarantees 20 wallets, not the 30 its weight-4 sizing note assumed; ample at
# current tracking levels, revisit the reserve before raising the track cap.
# Coins come back namespaced (`xyz:META`, `mkts:US500`), never colliding with
# core. Hard-coded rather than discovered via perpDexs (stable deployments;
# issue #21 left that lookup optional) — the remaining seven dexs are too thin
# to justify their poll cost today.
XYZ_DEX = "xyz"
MKTS_DEX = "mkts"

# Every venue fetch_open_positions queries per Trader, as `dex` args: the core
# perps (None) then the covered builder DEXes. The poller bills budget one
# spend per entry from this same tuple, so its weight accounting can never
# drift from the calls the helper actually makes (issue #31).
POSITION_VENUES: tuple[str | None, ...] = (None, XYZ_DEX, MKTS_DEX)


async def fetch_open_positions(gateway: HyperliquidGateway, address: str) -> list[Position]:
    """A Trader's open positions across every venue Epigone covers (POSITION_VENUES:
    the core perps plus the xyz builder DEX, issue #21), merged into one list.

    The lists merge cleanly — xyz coins are namespaced (`xyz:META`), core coins
    are not — so callers can render or diff them together with no collision.
    Every venue must succeed to return: a partial fetch would read an unqueried
    venue as all-closed, which the poller would diff into false CLOSE alerts and
    a display would show as a wallet that flattened everything. A failure on any
    venue therefore raises GatewayError (issues #21, #31)."""
    positions: list[Position] = []
    for dex in POSITION_VENUES:
        positions.extend(await gateway.get_open_positions(address, dex=dex))
    return positions


async def fetch_open_orders(gateway: HyperliquidGateway, address: str) -> list[OpenOrder]:
    """A Trader's resting orders across every venue Epigone covers, merged into
    one list — frontendOpenOrders is per-dex exactly like clearinghouseState
    (verified live 2026-07-24, issue #115), so this walks the same
    POSITION_VENUES the positions fetch does.

    Same all-or-raise rule as fetch_open_positions: a partial fetch would read
    an unanswered venue's ladder as cancelled — the order poller would drop
    those known ids and then re-alert the entire ladder when the venue answers
    again, and a display would show a book the wallet never thinned. Any venue
    failure therefore raises GatewayError (the #21/#31 rule)."""
    orders: list[OpenOrder] = []
    for dex in POSITION_VENUES:
        orders.extend(await gateway.get_open_orders(address, dex=dex))
    return orders
