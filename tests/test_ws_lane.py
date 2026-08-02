"""The websocket shadow lane (issue #157): a second producer of position
events that nothing consumes.

The lane's value is entirely comparative — it exists so the cutover ticket
(#158) can hold two descriptions of the same reality side by side — so the
tests are built around the four things that would make that comparison a lie:

1. **Semantics parity.** If the websocket lane's idea of a scale-in differs
   from the poller's by so much as a threshold, the comparison measures two
   drifting copies of a rule rather than two transports. The centrepiece test
   walks one scenario through BOTH lanes and demands the event rows match.
2. **Reconnect resync.** A websocket delivers from the moment you subscribe, so
   a gap is silent data loss unless absolute state is re-established first.
   A change that happens while the lane is disconnected must produce exactly
   one event afterwards — not zero, and not two.
3. **Liveness.** A Trader who isn't trading and a subscription that died look
   identical from inside the lane. Only the first must keep the heartbeat
   fresh.
4. **Allowances.** Subscription and connection counts stay inside the per-IP
   caps as the tracked set changes.

Seam per the house convention: fake gateway, fake clock, staged websocket, real
Postgres. And one test guards the promise the whole ticket rests on — that
Position Alerts and the poller are untouched by any of it.
"""

from decimal import Decimal
from typing import Any

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.position_events import POLL_SOURCE, WS_SOURCE
from epigone.safety.heartbeat import last_beat
from epigone.stream.poller import run_poll_pass
from epigone.ws import (
    LIVENESS_CHANNEL,
    ORDERS_CHANNEL,
    POSITIONS_CHANNEL,
    WebsocketClosed,
    lane,
)
from epigone.ws.lane import (
    LIVENESS_TIMEOUT_SECONDS,
    MAX_SUBSCRIBED_TRADERS,
    PING_INTERVAL_SECONDS,
    RECEIVE_TICK_SECONDS,
    RECONNECT_MIN_SECONDS,
    SUBSCRIPTION_LIMIT,
    WS_LANE_PROCESS,
    LaneSilent,
    OutboundAllowance,
    run_connection,
    run_lane,
)
from tests.support.clock import FakeClock
from tests.support.websocket import FakeWebsocket, connecting

WIDE_OPEN_BUDGET = 1_000_000

TRADER = "0xaaa"
OTHER = "0xbbb"
FOLLOWER = 42

# Quiet ticks needed to cross each deadline, given what one costs.
TICKS_TO_SILENCE = int(LIVENESS_TIMEOUT_SECONDS / RECEIVE_TICK_SECONDS) + 1
TICKS_TO_PING = int(PING_INTERVAL_SECONDS / RECEIVE_TICK_SECONDS) + 1


def position(
    coin: str = "BTC",
    side: Side = Side.LONG,
    size_usd: str = "10000",
    leverage: str = "5",
    entry_price: str = "100",
    unrealized_pnl: str = "0",
    size_coin: str = "100",
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal(leverage),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal(unrealized_pnl),
        size_coin=Decimal(size_coin),
    )


def wire(pos: Position) -> dict[str, Any]:
    """One position as clearinghouseState carries it on the wire.

    Built from the SAME Position the fake gateway hands the poller, so the two
    lanes in the parity test are fed one scenario rather than two hand-written
    ones that could quietly disagree."""
    size_coin = pos.size_coin or Decimal(0)
    return {
        "position": {
            "coin": pos.coin,
            "szi": str(size_coin if pos.side is Side.LONG else -size_coin),
            "positionValue": str(pos.size_usd),
            "leverage": {"value": str(pos.leverage)},
            "entryPx": str(pos.entry_price),
            "unrealizedPnl": str(pos.unrealized_pnl),
        }
    }


def positions_message(address: str, positions: list[Position]) -> dict[str, Any]:
    """An `allDexsClearinghouseState` push: every venue's state in one message,
    core under the empty dex name (the shape recorded live 2026-08-02)."""
    return {
        "channel": POSITIONS_CHANNEL,
        "data": {
            "user": address,
            "clearinghouseStates": [["", {"assetPositions": [wire(p) for p in positions]}]],
        },
    }


def liveness_message() -> dict[str, Any]:
    return {"channel": LIVENESS_CHANNEL, "data": {"mids": {"BTC": "100"}}}


def order_message(address: str) -> dict[str, Any]:
    return {
        "channel": ORDERS_CHANNEL,
        "data": [{"order": {"coin": "BTC", "oid": 1, "user": address}, "status": "open"}],
    }


async def track(pool: asyncpg.Pool, clock: FakeClock, address: str, *user_ids: int) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        clock.now(),
    )
    for user_id in user_ids:
        await pool.execute(
            "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )
        await pool.execute(
            "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)",
            user_id,
            address,
        )


async def run_lane_once(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    socket: FakeWebsocket,
) -> BaseException:
    """Drive one connection to its end and return why it ended. Every staged
    connection ends — that is what makes the test terminate — so the reason is
    the assertion, not an accident."""
    with pytest.raises((WebsocketClosed, LaneSilent)) as caught:
        await run_connection(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            connecting(socket),
            clock,
        )
    return caught.value


async def events(pool: asyncpg.Pool, source: str) -> list[asyncpg.Record]:
    return await pool.fetch(
        "SELECT * FROM position_events WHERE source = $1 ORDER BY id", source
    )


def comparable(row: asyncpg.Record) -> tuple[Any, ...]:
    """One event as the cutover comparison will read it: what happened, to
    which position, at what sizes. `id` and the timestamps are excluded — the
    lanes observe the same change at different moments, which is the LATENCY
    the comparison measures, not a disagreement about the event."""
    return (
        row["kind"],
        row["coin"],
        row["side"],
        row["size_usd"],
        row["size_coin"],
        row["prev_size_usd"],
        row["prev_size_coin"],
        row["leverage"],
        row["entry_price"],
        row["prev_side"],
        row["realized_pnl"],
        row["pct_return"],
    )


# The scenario both lanes walk in the parity test: a baseline that must emit
# nothing, then every kind the diff can produce, then a sub-threshold drift
# that must stay silent.
SCENARIO = [
    [position(size_coin="100")],
    [position(size_coin="100"), position(coin="ETH", size_usd="8000", size_coin="5")],
    [
        position(size_coin="200", size_usd="20000", unrealized_pnl="500"),
        position(coin="ETH", size_usd="8000", size_coin="5"),
    ],
    [
        position(size_coin="60", side=Side.SHORT, size_usd="15000", entry_price="250"),
        position(coin="ETH", size_usd="8000", size_coin="5"),
    ],
    [
        position(size_coin="20", side=Side.SHORT, size_usd="5000", entry_price="250"),
        position(coin="ETH", size_usd="8000", size_coin="5"),
    ],
    # Sub-threshold drift on ETH: 5% is ordinary noise, not news.
    [
        position(size_coin="20", side=Side.SHORT, size_usd="5000", entry_price="250"),
        position(coin="ETH", size_usd="8400", size_coin="5"),
    ],
    [position(coin="ETH", size_usd="8400", size_coin="5")],
]


async def test_websocket_events_match_the_pollers_for_the_same_changes(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The comparison the whole ticket feeds: one scenario, two transports,
    identical events. Same kinds in the same order, a flip as ONE row carrying
    both legs, the same scale threshold, and first-observation silence."""
    await track(pool, clock, TRADER, FOLLOWER)

    # The REST lane walks the scenario one poll at a time.
    for state in SCENARIO:
        gateway.set_positions(TRADER, state)
        await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
        clock.advance(10)
    poll_events = [comparable(row) for row in await events(pool, POLL_SOURCE)]

    # The websocket lane starts from the same first observation — its resync
    # reads it over REST — and is then streamed the rest.
    gateway.set_positions(TRADER, SCENARIO[0])
    socket = FakeWebsocket(
        clock,
        [liveness_message()] + [positions_message(TRADER, state) for state in SCENARIO[1:]],
    )
    await run_lane_once(pool, gateway, clock, socket)
    ws_events = [comparable(row) for row in await events(pool, WS_SOURCE)]

    assert ws_events == poll_events
    assert [row[0] for row in ws_events] == [
        "open",
        "scale_in",
        "flip",
        "scale_out",
        "close",
    ]


async def test_a_traders_first_observation_emits_nothing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A position that predates the lane's first look is not something anyone
    could have watched open — the poller's baseline rule, which the lane owes
    or every newly tracked wallet would produce phantom opens."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(clock, [positions_message(TRADER, [position()])])
    await run_lane_once(pool, gateway, clock, socket)

    assert await events(pool, WS_SOURCE) == []
    assert await pool.fetchval("SELECT count(*) FROM ws_position_snapshots") == 1


async def test_a_change_during_a_disconnect_yields_exactly_one_event(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The single most important requirement: a reconnect re-establishes
    absolute state BEFORE streaming resumes, so the gap is neither lost
    silently nor replayed."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    # First connection: baseline, then it dies.
    await run_lane_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    assert await events(pool, WS_SOURCE) == []

    # While the lane is down the Trader opens ETH. Nothing streams it.
    grown = [position(), position(coin="ETH", size_usd="8000", size_coin="5")]
    gateway.set_positions(TRADER, grown)
    clock.advance(30)

    # Second connection: the resync sees the gap, and the subscription's own
    # opening push then repeats the same state — which must add nothing.
    socket = FakeWebsocket(clock, [liveness_message(), positions_message(TRADER, grown)])
    await run_lane_once(pool, gateway, clock, socket)

    rows = await events(pool, WS_SOURCE)
    assert [(row["kind"], row["coin"]) for row in rows] == [("open", "ETH")]


class JournallingGateway(FakeHyperliquidGateway):
    """Records its reads into a journal shared with the socket, so a test can
    assert what happened BEFORE what."""

    def __init__(self, journal: list[str]) -> None:
        super().__init__()
        self._journal = journal

    async def get_open_positions(
        self, address: str, dex: str | None = None
    ) -> list[Position]:
        self._journal.append(f"resync:{address}")
        return await super().get_open_positions(address, dex=dex)


class JournallingWebsocket(FakeWebsocket):
    def __init__(
        self, clock: FakeClock, script: list[dict[str, Any] | None], journal: list[str]
    ) -> None:
        super().__init__(clock, script)
        self._journal = journal

    async def send(self, message: Any) -> None:
        await super().send(message)
        if message.get("method") == "subscribe":
            self._journal.append(f"subscribe:{message['subscription'].get('user', '')}")


async def test_the_resync_happens_before_the_subscription(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Order is the correctness argument, not a detail: subscribing first would
    let a push land while the REST read was in flight, and the staler answer
    would then overwrite it and manufacture a phantom event. So the assertion
    is the ORDER, not merely that both happened."""
    journal: list[str] = []
    gateway = JournallingGateway(journal)
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = JournallingWebsocket(clock, [liveness_message()], journal)
    await run_lane_once(pool, gateway, clock, socket)

    assert journal.index(f"resync:{TRADER}") < journal.index(f"subscribe:{TRADER}")
    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER]
    resynced_at = await pool.fetchval(
        "SELECT resynced_at FROM ws_lane_state WHERE trader_address = $1", TRADER
    )
    assert resynced_at is not None


async def test_a_venue_the_resync_cannot_see_is_not_diffed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The all-dex subscription carries every venue; the REST resync that
    anchors it covers only POSITION_VENUES. Diffing the wider observation
    against the narrower anchor would invent an OPEN for every uncovered coin,
    and then a CLOSE and an OPEN on each reconnect — poisoning exactly the
    dataset this lane exists to produce."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])  # REST sees the core venue only
    streamed = [position(), position(coin="mkts:FOO", size_usd="4000", size_coin="7")]

    for _ in range(2):  # baseline, then a reconnect that resyncs again
        await run_lane_once(
            pool,
            gateway,
            clock,
            FakeWebsocket(clock, [liveness_message(), positions_message(TRADER, streamed)]),
        )
        assert await events(pool, WS_SOURCE) == []


async def test_a_quiet_trader_keeps_the_heartbeat_fresh(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A Trader who isn't trading produces no position messages — and that must
    not read as a dead lane, because market data is still arriving."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    # Market data throughout, not one position message, well past the deadline.
    script: list[dict[str, Any] | None] = []
    for _ in range(TICKS_TO_SILENCE + 10):
        script.extend([liveness_message(), None])
    ended = await run_lane_once(pool, gateway, clock, FakeWebsocket(clock, script))

    assert isinstance(ended, WebsocketClosed)  # the script ran out, not silence
    assert await last_beat(pool, WS_LANE_PROCESS) is not None
    assert await events(pool, WS_SOURCE) == []


async def test_a_dead_connection_is_detected_though_nothing_reports_it(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The other half of the same question: a connection that stops delivering
    market data is dead even though it never closed and never errored."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    silent: list[dict[str, Any] | None] = [None] * TICKS_TO_SILENCE
    ended = await run_lane_once(pool, gateway, clock, FakeWebsocket(clock, silent))

    assert isinstance(ended, LaneSilent)


async def test_the_lanes_own_pongs_do_not_pass_for_liveness(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The subtle failure this guards: if any inbound message counted as
    liveness, a lane whose subscriptions had all died would keep itself looking
    healthy by talking to itself through the keepalive."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    script: list[dict[str, Any] | None] = []
    for _ in range(TICKS_TO_SILENCE + 5):
        script.extend([None, {"channel": "pong"}])
    ended = await run_lane_once(pool, gateway, clock, FakeWebsocket(clock, script))

    assert isinstance(ended, LaneSilent)


async def test_the_lane_pings_inside_the_servers_idle_timeout(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Hyperliquid closes a connection idle in both directions at 60s (measured
    on testnet 2026-08-02). The lane keeps its own socket open rather than
    depending on a market feed to do it."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    script: list[dict[str, Any] | None] = []
    for _ in range(TICKS_TO_PING):
        script.extend([None, liveness_message()])
    socket = FakeWebsocket(clock, script)
    await run_lane_once(pool, gateway, clock, socket)

    assert socket.pings >= 1


async def test_each_trader_costs_two_all_dex_subscriptions(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The per-IP subscription cap is what sets the Trader ceiling, so what a
    Trader costs is load-bearing: the all-dex position feed plus the
    account-wide order feed, and nothing per venue."""
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_positions(OTHER, [position()])

    socket = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, socket)

    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER, OTHER]
    assert socket.subscriptions(ORDERS_CHANNEL) == [TRADER, OTHER]
    assert socket.subscriptions(LIVENESS_CHANNEL) == [""]
    assert socket.subscription_count == 2 * 2 + 1
    assert socket.subscription_count <= SUBSCRIPTION_LIMIT


async def test_an_untracked_trader_is_unsubscribed_and_forgotten(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Subscriptions follow the tracked set as it changes, and a wallet that
    leaves is forgotten — so a re-follow re-baselines silently instead of
    diffing against months-old memory."""
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_positions(OTHER, [position()])

    # One refresh subscribes both; the unfollow lands; the next refresh drops one.
    script: list[dict[str, Any] | None] = [liveness_message()]
    socket = FakeWebsocket(clock, script)
    with pytest.raises((WebsocketClosed, LaneSilent)):
        await run_connection(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            connecting(socket),
            clock,
        )
    assert socket.subscription_count == 5

    await pool.execute("DELETE FROM tracks WHERE trader_address = $1", OTHER)
    second = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, second)

    assert second.subscriptions(POSITIONS_CHANNEL) == [TRADER]
    assert await pool.fetchval(
        "SELECT count(*) FROM ws_lane_state WHERE trader_address = $1", OTHER
    ) == 0


async def test_the_subscription_cap_bounds_the_shadowed_set() -> None:
    """The ceiling is arithmetic, and it is stated so the day it binds is a
    logged refusal rather than the server quietly rejecting subscriptions."""
    assert MAX_SUBSCRIBED_TRADERS == 499
    assert MAX_SUBSCRIBED_TRADERS * 2 + 1 <= SUBSCRIPTION_LIMIT


async def test_a_poll_set_past_the_cap_is_truncated_not_overrun(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the cap the lane shadows what it can and says so, rather than
    letting the server start rejecting subscriptions — at which point the lane
    would be silently partial with no signal that it was."""
    monkeypatch.setattr(lane, "MAX_SUBSCRIBED_TRADERS", 1)
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_positions(OTHER, [position()])

    socket = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, socket)

    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER]
    assert socket.subscription_count == 2 + 1


async def test_the_outbound_ledger_holds_a_rolling_minute(clock: FakeClock) -> None:
    """The per-IP outbound cap is a per-MINUTE one, so the ledger has to forget:
    a lane that counted forever would stop subscribing after its first busy
    minute and never resume."""
    allowance = OutboundAllowance(clock, limit=2)

    assert allowance.take() and allowance.take()
    assert not allowance.take()

    clock.advance(61)
    assert allowance.take()


async def test_an_exhausted_outbound_allowance_defers_rather_than_overruns(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wallet whose subscribe could not be sent must not be recorded as
    subscribed: the lane would then diff streamed messages it never anchored,
    and would never retry the subscription. Deferring costs latency; pretending
    costs correctness."""
    monkeypatch.setattr(lane, "OUTBOUND_BUDGET_PER_MINUTE", 1)  # the liveness sub spends it
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(
        clock, [liveness_message(), positions_message(TRADER, [position(coin="ETH")])]
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert socket.subscriptions(POSITIONS_CHANNEL) == []
    assert await events(pool, WS_SOURCE) == []  # nothing anchored, so nothing diffed


async def test_order_messages_are_measured_and_never_persisted(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """orderUpdates is subscribed — it costs a subscription and serves the
    liveness/measurement question — but the order-event seam is a design
    decision this ticket does not make (ADR-0006 §Scope). Nothing durable."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(
        clock, [liveness_message(), order_message(TRADER), order_message(TRADER)]
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert await events(pool, WS_SOURCE) == []
    tables = await pool.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '%order%'"
    )
    assert {row["tablename"] for row in tables} == {
        "order_snapshots",
        "order_poll_state",
        "order_alerts",
    }


async def test_the_lane_writes_no_alerts_and_leaves_the_pollers_state_alone(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The promise the ticket rests on: a User sees nothing, and the poller's
    own diff memory is untouched — so even a badly wrong lane cannot change
    what gets alerted."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            positions_message(TRADER, [position(), position(coin="ETH", size_coin="5")]),
        ],
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert await events(pool, WS_SOURCE) != []  # the lane did produce a signal
    assert await pool.fetchval("SELECT count(*) FROM position_alerts") == 0
    assert await pool.fetchval("SELECT count(*) FROM position_snapshots") == 0
    assert await pool.fetchval("SELECT count(*) FROM position_poll_state") == 0
    assert await events(pool, POLL_SOURCE) == []


async def test_a_message_for_an_unsubscribed_trader_is_ignored(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A push for a wallet this connection has not resynced would be diffed
    against memory the stream is not yet trusted against — exactly the
    gap-losing behaviour the resync exists to prevent."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(
        clock, [liveness_message(), positions_message(OTHER, [position(coin="ETH")])]
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert await events(pool, WS_SOURCE) == []
    assert await pool.fetchval(
        "SELECT count(*) FROM ws_position_snapshots WHERE trader_address = $1", OTHER
    ) == 0


class SlowGateway(FakeHyperliquidGateway):
    """A gateway whose reads cost real time, like the network's do."""

    def __init__(self, clock: FakeClock, seconds: float) -> None:
        super().__init__()
        self._clock = clock
        self._seconds = seconds

    async def get_open_positions(
        self, address: str, dex: str | None = None
    ) -> list[Position]:
        self._clock.advance(self._seconds)
        return await super().get_open_positions(address, dex=dex)


async def test_a_slow_resync_is_not_mistaken_for_a_dead_connection(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """A connection's first refresh resyncs EVERY tracked wallet over REST, and
    at a large poll set that outlasts the liveness deadline. Charging that time
    to the deadline would declare a healthy connection dead before it had read
    one message — and it would do it again on every reconnect, so the lane
    would never start."""
    slow = SlowGateway(clock, seconds=LIVENESS_TIMEOUT_SECONDS)
    await track(pool, clock, TRADER, FOLLOWER)
    slow.set_positions(TRADER, [position()])

    socket = FakeWebsocket(clock, [liveness_message()])
    ended = await run_lane_once(pool, slow, clock, socket)

    assert isinstance(ended, WebsocketClosed)  # it read the script, not LaneSilent
    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER]


class StopLoop(Exception):
    """Ends the otherwise-endless supervisor from inside its own backoff."""


class CountedClock(FakeClock):
    """A clock that stops the world after a given number of sleeps, so the
    reconnect supervisor — which by design never returns — can be tested."""

    def __init__(self, sleeps: int) -> None:
        super().__init__()
        self._remaining = sleeps

    async def sleep(self, seconds: float) -> None:
        await super().sleep(seconds)
        self._remaining -= 1
        if self._remaining <= 0:
            raise StopLoop


async def test_the_supervisor_reconnects_and_never_propagates_a_failure(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway
) -> None:
    """The structural claim behind "this cannot break alerting": the lane's
    process survives anything one connection does. It runs alone (ADR-0002), so
    a crash here costs the dataset and nothing else — but a crash that ended
    the process would silently stop collecting it, which is why every failure
    is caught, backed off, and retried."""
    clock = CountedClock(sleeps=3)
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    healthy = FakeWebsocket(clock, [liveness_message()])

    class Exploding(FakeWebsocket):
        async def receive(self, timeout: float) -> dict[str, Any] | None:
            raise RuntimeError("something nobody predicted")

    exploding = Exploding(clock, [])

    with pytest.raises(StopLoop):
        await run_lane(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            connecting(healthy, exploding),
            clock,
        )

    # Both connections were used and closed: the unexpected RuntimeError was
    # caught exactly like a clean disconnect, and the lane came back for more.
    assert healthy.closed and exploding.closed
    assert clock.slept[0] >= RECONNECT_MIN_SECONDS
    assert await last_beat(pool, WS_LANE_PROCESS) is not None  # start was stamped


async def test_an_unreadable_message_does_not_end_the_connection(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A shape surprise is logged and skipped: half-reading a position list
    would manufacture false CLOSE events, and killing the connection over one
    odd frame would cost the dataset the lane exists to collect."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            {"channel": POSITIONS_CHANNEL, "data": {"user": TRADER}},
            positions_message(TRADER, [position(), position(coin="ETH", size_coin="5")]),
        ],
    )
    ended = await run_lane_once(pool, gateway, clock, socket)

    assert isinstance(ended, WebsocketClosed)  # ran to the end of the script
    assert [row["kind"] for row in await events(pool, WS_SOURCE)] == ["open"]
