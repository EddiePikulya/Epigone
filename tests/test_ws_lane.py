"""The websocket position lane (issues #157, #158).

It began as a second producer of position events that nothing consumed, and the
tests below still pin the four things that would have made THAT comparison a
lie — because they are the same four things that would make the lane, now the
producer everything acts on, quietly wrong:

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
Postgres.

The final section is the cutover itself (#158): what changes when this lane
owns event production — it alerts, through the same seam the poller uses — and
what deliberately does not, since a lane must behave identically whether or not
anyone is listening, or the moment ownership moves would be a moment its output
changes shape.
"""

from decimal import Decimal
from typing import Any

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.lane_authority import POLL_OWNER, WS_OWNER, read_authority, take_ownership
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
    outbound_allowance,
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
    both legs, the same scale threshold, and first-observation silence.

    The websocket half is staged the way the real feed behaves — each state
    pushed, then pushed again after a few seconds of quiet, because the
    positions feed re-sends absolute state on a ~5s cadence whether or not
    anything changed. That matters since the cutover: an entry-side scale is
    held for a moment so a burst of fills becomes one event (ADR-0009), and it
    emits on the next push. Parity is therefore a claim about what the two
    lanes SAY, observation to observation — not about the instant they say it,
    which is the latency the cutover was measured on."""
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
    quiet: list[Any] = [None] * (int(lane.WS_COALESCE_WINDOW_SECONDS / RECEIVE_TICK_SECONDS) + 1)
    socket = FakeWebsocket(
        clock,
        [liveness_message()]
        + [
            staged
            for state in SCENARIO[1:]
            for staged in (positions_message(TRADER, state), *quiet,
                           positions_message(TRADER, state), *quiet)
        ],
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


async def test_each_trader_costs_one_all_dex_subscription(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """What a Trader costs this connection: the all-dex position feed, and
    nothing per venue. The account-wide ORDER feed used to be subscribed here
    too — issue #168 moved it to a connection of its own, because its frames
    carry no `user` and cannot be attributed on a shared connection."""
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_positions(OTHER, [position()])

    socket = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, socket)

    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER, OTHER]
    assert socket.subscriptions(ORDERS_CHANNEL) == []
    assert socket.subscriptions(LIVENESS_CHANNEL) == [""]
    assert socket.subscription_count == 2 + 1
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
    assert socket.subscription_count == 3

    await pool.execute("DELETE FROM tracks WHERE trader_address = $1", OTHER)
    second = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, second)

    assert second.subscriptions(POSITIONS_CHANNEL) == [TRADER]
    assert await pool.fetchval(
        "SELECT count(*) FROM ws_lane_state WHERE trader_address = $1", OTHER
    ) == 0


async def test_the_unique_user_cap_bounds_the_shadowed_set() -> None:
    """The ceiling is MEASURED, not derived, and that is the correction issue
    #168 forced: the binding limit is an undocumented per-IP allowance of 15
    unique users, not the 1000-subscription cap ADR-0006 computed 499 from. It
    is stated here so the day it binds is a logged refusal rather than the
    server quietly rejecting subscriptions — which is what it had been doing."""
    assert MAX_SUBSCRIBED_TRADERS == 15
    assert MAX_SUBSCRIBED_TRADERS + 1 <= SUBSCRIPTION_LIMIT  # subscriptions are not the problem


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
    assert socket.subscription_count == 1 + 1


async def test_the_outbound_ledger_holds_a_rolling_minute(clock: FakeClock) -> None:
    """The per-IP outbound cap is a per-MINUTE one, so the ledger has to forget:
    a lane that counted forever would stop subscribing after its first busy
    minute and never resume."""
    allowance = outbound_allowance(clock, limit=2)

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


async def test_this_connection_no_longer_subscribes_the_order_feed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Issue #168's correction. `orderUpdates` frames carry no `user`, so on a
    connection serving every Trader they are anonymous and could never be
    persisted — counting them measured nothing per Trader, and they are a large
    share of this connection's inbound traffic. The order lane subscribes them
    one Trader per connection instead (epigone.ws.order_lane).

    A stray order frame arriving here is therefore ignored rather than acted
    on: this connection has no way to know whose it is."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])

    stray = {"channel": ORDERS_CHANNEL, "data": [{"order": {"coin": "BTC"}, "status": "open"}]}
    socket = FakeWebsocket(clock, [liveness_message(), stray])
    ended = await run_lane_once(pool, gateway, clock, socket)

    assert isinstance(ended, WebsocketClosed)  # it read the frame and carried on
    assert socket.subscriptions(ORDERS_CHANNEL) == []
    assert await events(pool, WS_SOURCE) == []
    assert await pool.fetchval("SELECT count(*) FROM order_events") == 0


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


# --- the cutover: this lane becomes the one anyone listens to (issue #158) ---
#
# Everything above holds whether or not the lane owns event production, and
# that is deliberate — the lane's behaviour must not depend on its status, or
# the moment ownership moves would be a moment its output changes shape. What
# follows is the part that DOES depend on it.


async def websocket_owns(pool: asyncpg.Pool, clock: FakeClock) -> None:
    async with pool.acquire() as conn, conn.transaction():
        await take_ownership(conn, WS_OWNER, "test: cutover complete", clock.now())


async def test_the_authoritative_lane_alerts_the_traders_followers(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Position Alerts continue uninterrupted across the cutover — from the
    other lane. A User must not be able to tell which transport saw their
    Trader open a position, which is why both lanes fan out through one seam
    (epigone.position_publish) rather than each owning a copy of the rule."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await websocket_owns(pool, clock)

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            positions_message(TRADER, [position(), position(coin="ETH", size_coin="5")]),
        ],
    )
    await run_lane_once(pool, gateway, clock, socket)

    rows = await events(pool, WS_SOURCE)
    assert [(row["kind"], row["coin"], row["authoritative"]) for row in rows] == [
        ("open", "ETH", True)
    ]
    alerted = await pool.fetch("SELECT * FROM position_alerts")
    assert [(row["user_telegram_id"], row["kind"], row["coin"]) for row in alerted] == [
        (FOLLOWER, "open", "ETH")
    ]


async def test_a_burst_of_fills_becomes_one_entry_event(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The granularity decision (ADR-0009). The websocket sees individual fills
    where the poll window coalesced them; producing each one would mirror a
    single entry with three orders, three sets of fees, and three chances of a
    sliver falling under the exchange minimum. The burst is held by freezing
    the anchor, so what finally emits is ONE scale-in measured from where the
    burst began — not three, and not the last leg alone."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_usd="10000", size_coin="100")])
    await websocket_owns(pool, clock)

    burst = [
        positions_message(TRADER, [position(size_usd=size, size_coin=coin)])
        for size, coin in (("13000", "130"), ("16000", "160"), ("20000", "200"))
    ]
    quiet: list[Any] = [None] * (int(lane.WS_COALESCE_WINDOW_SECONDS / RECEIVE_TICK_SECONDS) + 1)
    socket = FakeWebsocket(
        clock,
        [liveness_message(), *burst, *quiet, positions_message(
            TRADER, [position(size_usd="20000", size_coin="200")]
        )],
    )
    await run_lane_once(pool, gateway, clock, socket)

    rows = await events(pool, WS_SOURCE)
    assert [(row["kind"], row["prev_size_usd"], row["size_usd"]) for row in rows] == [
        ("scale_in", Decimal("10000"), Decimal("20000"))
    ]


async def test_an_exit_inside_the_coalescing_window_is_never_delayed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The guardrail on the whole decision: only entries may debounce. A close
    landing mid-burst is produced at the first observation that shows it, with
    no quiet tick in between — closing late is the one failure the copy path
    treats as unacceptable (ADR-0007's direction asymmetry)."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_usd="10000", size_coin="100")])
    await websocket_owns(pool, clock)

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            positions_message(TRADER, [position(size_usd="13000", size_coin="130")]),
            positions_message(TRADER, []),
        ],
    )
    await run_lane_once(pool, gateway, clock, socket)

    rows = await events(pool, WS_SOURCE)
    assert [row["kind"] for row in rows] == ["close"]


async def test_a_scale_out_inside_the_window_is_never_delayed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Same guardrail, the other exit. A trim is money coming off the table."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_usd="10000", size_coin="100")])
    await websocket_owns(pool, clock)

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            positions_message(TRADER, [position(size_usd="13000", size_coin="130")]),
            positions_message(TRADER, [position(size_usd="6000", size_coin="60")]),
        ],
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert [row["kind"] for row in await events(pool, WS_SOURCE)] == ["scale_out"]


async def test_a_held_entry_that_falls_back_emits_nothing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A Trader who adds and immediately unwinds the add ended where they
    started. The held anchor makes that a non-event rather than a scale-in
    followed by a scale-out, which is the same reading the 10s poll has always
    given the same behaviour."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_usd="10000", size_coin="100")])
    await websocket_owns(pool, clock)

    quiet: list[Any] = [None] * (int(lane.WS_COALESCE_WINDOW_SECONDS / RECEIVE_TICK_SECONDS) + 1)
    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            positions_message(TRADER, [position(size_usd="13000", size_coin="130")]),
            *quiet,
            positions_message(TRADER, [position(size_usd="10100", size_coin="101")]),
        ],
    )
    await run_lane_once(pool, gateway, clock, socket)

    assert await events(pool, WS_SOURCE) == []


async def test_losing_production_re_establishes_absolute_state_in_place(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The handback's precondition, earned rather than assumed. The poller
    escalates precisely when it suspects this lane of missing changes, so
    "the connection never dropped" is not evidence of anything: the lane
    re-reads absolute state for every wallet it holds and stamps that, and the
    ownership decision waits for the stamp."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await websocket_owns(pool, clock)

    async def poller_escalates() -> None:
        clock.advance(5)
        async with pool.acquire() as conn, conn.transaction():
            await take_ownership(conn, POLL_OWNER, "test: drift", clock.now())

    refresh_ticks = int(lane.TRACKED_REFRESH_SECONDS / RECEIVE_TICK_SECONDS) + 1
    socket = FakeWebsocket(
        clock,
        [liveness_message(), poller_escalates, *([None] * refresh_ticks)],
    )
    await run_lane_once(pool, gateway, clock, socket)

    since = (await read_authority(pool)).since
    resynced_at = await pool.fetchval(
        "SELECT resynced_at FROM ws_lane_state WHERE trader_address = $1", TRADER
    )
    assert resynced_at >= since
    # And exactly one subscription: the wallet was re-anchored where it stood,
    # never unsubscribed and resubscribed.
    assert socket.subscriptions(POSITIONS_CHANNEL) == [TRADER]


async def test_a_lane_that_does_not_own_production_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The shadow half of the same rule, and the one that keeps the comparison
    alive after the cutover: a lane that is not authoritative still records
    everything it sees, and none of it reaches a User or a consumer."""
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

    rows = await events(pool, WS_SOURCE)
    assert [(row["kind"], row["authoritative"]) for row in rows] == [("open", False)]
    assert await pool.fetchval("SELECT count(*) FROM position_alerts") == 0


async def test_the_scarce_websocket_slots_go_to_copy_enabled_leaders(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Which wallets a transport that can hold 15 covers is a DECISION (#158,
    2026-08-04: "selection needs to be deliberate, not alphabetical"). An
    address-sorted prefix decides it by leading hex digit; a Leader whose wallet
    happens to start with 0xf would lose its slot to fifteen strangers."""
    leader = "0xffff"
    await track(pool, clock, leader, FOLLOWER)
    for index in range(MAX_SUBSCRIBED_TRADERS):
        await track(pool, clock, f"0xa{index:03x}", FOLLOWER)
    await pool.execute(
        """
        INSERT INTO copy_subs
            (operator_id, leader_address, sub_name, allocation_usd, base_stake_usd,
             copy_mode, enabled, created_at)
        VALUES (7, $1, 'copy-1', 1000, 100, 'default', TRUE, $2)
        """,
        leader,
        clock.now(),
    )

    socket = FakeWebsocket(clock, [liveness_message()])
    await run_lane_once(pool, gateway, clock, socket)

    subscribed = socket.subscriptions(POSITIONS_CHANNEL)
    assert subscribed[0] == leader
    assert len(subscribed) == MAX_SUBSCRIBED_TRADERS
