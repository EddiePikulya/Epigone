"""The Hyperliquid websocket seam (issue #157).

The read-side REST gateway has one rule (ADR-0001): all Hyperliquid I/O goes
through an injected interface, tests use a fake, production wires the real
client. The websocket lane obeys the same rule with its own, smaller interface,
because a socket is not a request — it is a long-lived thing that can go quiet,
go away, or lie about being alive, and those are exactly the behaviours the
lane's tests need to stage.

`WebsocketConnection` is deliberately thin: send a message, take the next one
(or learn that none came within a timeout), close. Everything above it — what
to subscribe to, when to resync, when to declare the connection dead — is lane
logic in `epigone.ws.lane` and `epigone.ws.order_lane`, testable without a
socket. This module also holds `RollingMinute`, the allowance ledger both lanes
pace themselves against: the per-IP outbound cap is shared BETWEEN them, so one
implementation of "may I, right now" is the point rather than an accident.

**Subscription vocabulary, verified live against testnet 2026-08-02.**
`allDexsClearinghouseState` exists and is the right subscription: it carries
EVERY dex's clearinghouse state in one message — `clearinghouseStates` as
[dex_name, state] pairs, core under the empty name — where the per-dex
`clearinghouseState` form costs one subscription per venue for the same
coverage. `allDexsOrderUpdates` does NOT exist (the server rejects it as
unparseable); `orderUpdates` takes no `dex` and is account-wide as it stands.

**Corrected 2026-08-03 (issue #168).** This paragraph used to end "so two
subscriptions cover a Trader completely, which is what the per-IP subscription
cap has to be budgeted against". Both halves are wrong. The two subscriptions
cannot share a connection: `allDexsClearinghouseState` messages name their
`user` and `orderUpdates` messages do not, so the order feed is unattributable
wherever more than one Trader is subscribed — it now takes a connection of its
own per Trader (ADR-0007). And the subscription cap is not what binds: an
undocumented per-IP allowance of **15 unique users** does, ~33× sooner.

Note what the all-dex form covers: every venue, including the ones REST polling
deliberately dropped for weight reasons (POSITION_VENUES omits mkts). That is a
REST-side decision about REST costs and is not reintroduced there — so the lane
REDUCES each message to the covered venues before diffing it
(epigone.gateway.on_covered_venue). It has to: the reconnect resync that
anchors the stream is a REST read, which can only see those venues, and diffing
a wider observation against a narrower anchor manufactures a phantom OPEN for
every uncovered coin — then a CLOSE and an OPEN on each reconnect. The
subscription stays the all-dex form regardless, because it is one subscription
where the per-dex forms are one apiece.
"""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from epigone.clock import Clock
from epigone.gateway import GatewayError, Position
from epigone.gateway.http import parse_positions

# The public websocket endpoint. Product data is mainnet, exactly like the REST
# gateway's default; probes against testnet carry their own URL
# (scripts/testnet_ws_probe.py) rather than tempting the product with one.
WS_URL = "wss://api.hyperliquid.xyz/ws"

# Channels the lanes read. POSITIONS_CHANNEL becomes position events on the
# shared connection; ORDERS_CHANNEL becomes order events on a connection of its
# own, one per Trader, because its frames do not say whose orders they are
# (ADR-0007); LIVENESS_CHANNEL is the always-emitting market feed that makes
# silence mean something, and every connection subscribes it (see lane.py).
POSITIONS_CHANNEL = "allDexsClearinghouseState"
ORDERS_CHANNEL = "orderUpdates"
LIVENESS_CHANNEL = "allMids"


class WebsocketClosed(Exception):
    """The peer went away — the connection is finished and must be replaced."""


class WebsocketConnection(Protocol):
    async def send(self, message: Mapping[str, Any]) -> None:
        """Send one JSON message. Raises WebsocketClosed if the peer is gone."""
        ...

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        """The next message, or None if `timeout` seconds passed without one.

        A None is NOT a health verdict — it is one quiet tick, and the lane
        decides what a run of them means. Raises WebsocketClosed when the peer
        closes."""
        ...

    async def close(self) -> None:
        """Close, idempotently and without raising."""
        ...


# How the lane gets a connection. A factory rather than a URL, so a test hands
# back a staged connection — and a staged END of one, which is how a disconnect
# is exercised without a socket.
Connect = Callable[[], Awaitable[WebsocketConnection]]


def subscribe(subscription: Mapping[str, Any]) -> dict[str, Any]:
    return {"method": "subscribe", "subscription": dict(subscription)}


def unsubscribe(subscription: Mapping[str, Any]) -> dict[str, Any]:
    return {"method": "unsubscribe", "subscription": dict(subscription)}


def positions_subscription(address: str) -> dict[str, Any]:
    return {"type": POSITIONS_CHANNEL, "user": address.lower()}


def orders_subscription(address: str) -> dict[str, Any]:
    return {"type": ORDERS_CHANNEL, "user": address.lower()}


def liveness_subscription() -> dict[str, Any]:
    return {"type": LIVENESS_CHANNEL}


def ping() -> dict[str, Any]:
    return {"method": "ping"}


class RollingMinute:
    """A rolling-minute ledger: how many times something has happened in the
    last 60 seconds, and whether it may happen again.

    Not a token bucket and not a pacer — it never sleeps. It answers one
    question ("may I, right now?") and the caller decides what a no means. Two
    callers do, and they mean different things by it: the outbound message cap
    defers a subscription change to the next refresh, and the order lane's rate
    ceiling stops recording a Trader who is plainly a market maker."""

    def __init__(self, clock: Clock, limit: int) -> None:
        self._clock = clock
        self._limit = limit
        self._at: list[datetime] = []

    def take(self) -> bool:
        now = self._clock.now()
        cutoff = now - timedelta(minutes=1)
        self._at = [at for at in self._at if at > cutoff]
        if len(self._at) >= self._limit:
            return False
        self._at.append(now)
        return True


@dataclass(frozen=True)
class OrderUpdate:
    """One entry of an `orderUpdates` frame: an order, and what just happened to
    it.

    Note what is NOT here, because it is not on the wire (measured 2026-08-03):
    no `user` — which is why attribution has to come from the connection an
    update arrived on (ADR-0007) — and no `orderType` / `isTrigger` /
    `triggerPx` / `reduceOnly` / `isPositionTpsl`, so a streamed placement
    cannot say whether it is a stop, a take-profit or a plain resting limit.
    Only the REST resync can read those.

    `size` is what remains to fill and `original_size` what it started as, so a
    partial fill is visible as the two disagreeing."""

    order_id: int
    coin: str
    is_buy: bool
    limit_price: Decimal
    size: Decimal
    original_size: Decimal
    status: str
    placed_at: datetime
    status_at: datetime
    cloid: str | None = None


def parse_orders_message(data: Any) -> list[OrderUpdate]:
    """An `orderUpdates` payload as order updates, PERP ORDERS ONLY.

    Spot rows are dropped by the same convention `parse_open_orders` uses
    (spot coins are `@N`-indexed or `BASE/QUOTE`-named) — the feed carries them
    (an `@156` order was observed live) and Epigone is perp-only, so keeping
    them would put a meaningless ticker for an uncovered asset class into an
    execution seam.

    An unrecognized `side` fails loudly rather than reading as a sell, exactly
    as the REST parser does: silently flipping a mirrored order's direction is
    the worst failure this module could have. Raises GatewayError on any shape
    surprise, so a half-read frame never reaches the diff — the lane logs it and
    keeps the connection, since one odd frame must not cost the dataset."""
    try:
        updates: list[OrderUpdate] = []
        for entry in data:
            raw = entry["order"]
            coin = str(raw["coin"])
            if coin.startswith("@") or "/" in coin:
                continue  # a spot order: out of product scope
            side = str(raw["side"])
            if side not in ("A", "B"):
                raise ValueError(f"unknown order side {side!r}")
            cloid = raw.get("cloid")
            updates.append(
                OrderUpdate(
                    order_id=int(raw["oid"]),
                    coin=coin,
                    is_buy=side == "B",
                    limit_price=Decimal(raw["limitPx"]),
                    size=Decimal(raw["sz"]),
                    original_size=Decimal(raw["origSz"]),
                    status=str(entry["status"]),
                    placed_at=_at(raw["timestamp"]),
                    status_at=_at(entry["statusTimestamp"]),
                    cloid=str(cloid) if cloid is not None else None,
                )
            )
        return updates
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected {ORDERS_CHANNEL} payload shape: {exc!r}") from exc


def _at(milliseconds: Any) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def parse_positions_message(data: Mapping[str, Any]) -> tuple[str, list[Position]]:
    """An `allDexsClearinghouseState` payload as (address, all venues' positions).

    The message body is `{"user": …, "clearinghouseStates": [[dex, state], …]}`
    and each `state` is byte-identical in shape to what the REST
    `clearinghouseState` endpoint returns — so it is parsed by the REST parser,
    on purpose. Two parsers for one payload shape would be two things to keep
    in step, and the whole value of this lane is that its events are comparable
    to the poller's; a divergence in how a position is READ would show up as a
    transport difference and mean nothing.

    The empty dex name is core, which maps to the REST parser's `dex=None` and
    leaves those coins bare; a builder dex namespaces its coins `dex:COIN`
    exactly as the REST path does, so both lanes key snapshots the same way.

    Raises GatewayError on a shape surprise, like every other parser — a
    half-read position list would read as closed positions and manufacture
    false CLOSE events."""
    try:
        user = str(data["user"]).lower()
        positions: list[Position] = []
        for dex, state in data["clearinghouseStates"]:
            positions.extend(parse_positions(state, str(dex) or None))
    except (KeyError, TypeError, ValueError) as exc:
        raise GatewayError(f"unexpected {POSITIONS_CHANNEL} payload shape: {exc!r}") from exc
    return user, positions
