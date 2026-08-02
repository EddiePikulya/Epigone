"""The Hyperliquid websocket seam (issue #157).

The read-side REST gateway has one rule (ADR-0001): all Hyperliquid I/O goes
through an injected interface, tests use a fake, production wires the real
client. The websocket lane obeys the same rule with its own, smaller interface,
because a socket is not a request — it is a long-lived thing that can go quiet,
go away, or lie about being alive, and those are exactly the behaviours the
lane's tests need to stage.

`WebsocketConnection` is deliberately thin: send a message, take the next one
(or learn that none came within a timeout), close. Everything above it — what
to subscribe to, when to resync, when to declare the connection dead — is
lane logic in `epigone.ws.lane`, testable without a socket.

**Subscription vocabulary, verified live against testnet 2026-08-02.**
`allDexsClearinghouseState` exists and is the right subscription: it carries
EVERY dex's clearinghouse state in one message — `clearinghouseStates` as
[dex_name, state] pairs, core under the empty name — where the per-dex
`clearinghouseState` form costs one subscription per venue for the same
coverage. `allDexsOrderUpdates` does NOT exist (the server rejects it as
unparseable); `orderUpdates` takes no `dex` and is account-wide as it stands.
So two subscriptions cover a Trader completely, which is what the per-IP
subscription cap has to be budgeted against.

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
from typing import Any, Protocol

from epigone.gateway import GatewayError, Position
from epigone.gateway.http import parse_positions

# The public websocket endpoint. Product data is mainnet, exactly like the REST
# gateway's default; probes against testnet carry their own URL
# (scripts/testnet_ws_probe.py) rather than tempting the product with one.
WS_URL = "wss://api.hyperliquid.xyz/ws"

# Channels the lane reads. POSITIONS_CHANNEL is the one that becomes events;
# ORDERS_CHANNEL is subscribed and measured but deliberately not persisted (the
# order-event seam is undesigned — ADR-0006 §Scope, and #157's scope
# correction); LIVENESS_CHANNEL is the always-emitting market feed that makes
# silence mean something (see lane.py).
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
