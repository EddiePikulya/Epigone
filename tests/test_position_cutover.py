"""The REST poll pass as a WARM STANDBY (issue #158, ADR-0009).

After the cutover the poller keeps doing everything it did except the one thing
that matters most: it reads every wallet, diffs it, records what it saw, and
writes NO authoritative events while the websocket is healthy. It exists in
that state to do two jobs no heartbeat can do — to be the failover path that is
exercised every single day rather than only during incidents, and to catch the
websocket LYING: connected, delivering, and silently missing a change.

These tests drive the poller seam through that whole life:

- steady state: the poller reads, agrees, and stays quiet;
- drift: the poller sees a change the websocket never produced, raises it as an
  incident, takes production back, and only THEN writes the event;
- failover: what the websocket already produced is not produced a second time;
- exclusivity: two producers racing for one (Trader, coin) yield ONE
  authoritative event — demonstrated by running them concurrently, not asserted.
"""

import asyncio
from datetime import datetime
from decimal import Decimal

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.lane_authority import (
    POLL_OWNER,
    RECONCILE_GRACE_SECONDS,
    WS_HEARTBEAT_STALE_SECONDS,
    WS_OWNER,
    WS_RECOVERY_SECONDS,
    evaluate_authority,
    read_authority,
    take_ownership,
)
from epigone.position_events import POLL_SOURCE, WS_SOURCE, PositionEvent, record_events
from epigone.position_publish import publish
from epigone.safety.heartbeat import beat
from epigone.stream.main import (
    STANDBY_POLL_INTERVAL_SECONDS,
    StandbyState,
    run_position_cycle,
)
from epigone.stream.poller import POLL_INTERVAL_SECONDS, run_poll_pass
from epigone.ws import MAX_SUBSCRIBED_TRADERS, WS_LANE_PROCESS
from epigone.ws.lane import POSITIONS_PUSH_INTERVAL_SECONDS, WS_COALESCE_WINDOW_SECONDS
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000

TRADER = "0xaaa"
LEADER = "0xbbb"
FOLLOWER = 42
OPERATOR = 7


def position(
    coin: str = "BTC",
    side: Side = Side.LONG,
    size_usd: str = "10000",
    size_coin: str = "100",
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal("5"),
        entry_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        size_coin=Decimal(size_coin),
    )


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


async def poll(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)


async def websocket_owns(pool: asyncpg.Pool, clock: FakeClock) -> None:
    async with pool.acquire() as conn, conn.transaction():
        await take_ownership(conn, WS_OWNER, "test: cutover complete", clock.now())


async def websocket_produced(
    pool: asyncpg.Pool, clock: FakeClock, address: str, event: PositionEvent
) -> None:
    """The websocket lane, having produced an event authoritatively — what the
    poller's reconciliation is comparing its own diff against."""
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn, address, [event], clock.now(), source=WS_SOURCE, authoritative=True
        )


async def events(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM position_events ORDER BY id")


async def alerts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM position_alerts ORDER BY id")


@pytest.fixture
async def baselined(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """One tracked Trader holding one position, both lanes' memory established
    and the websocket authoritative — the world as it is after the cutover."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await poll(pool, gateway, clock)
    await websocket_owns(pool, clock)


async def test_a_healthy_websocket_leaves_the_poller_reading_and_silent(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The steady state. The poller sees the same close the websocket saw,
    records what it observed for the comparison, and produces nothing: no
    authoritative event for the copy path, no alert for the follower — the
    websocket already told them both."""
    clock.advance(60)
    gateway.set_positions(TRADER, [])
    await websocket_produced(pool, clock, TRADER, PositionEvent(kind="close", coin="BTC",
                                                                prev_side="long"))

    await poll(pool, gateway, clock)

    written = await events(pool)
    assert [(row["source"], row["authoritative"]) for row in written] == [
        (WS_SOURCE, True),
        (POLL_SOURCE, False),
    ]
    assert await alerts(pool) == []
    assert (await read_authority(pool)).owner == WS_OWNER


async def test_the_poller_keeps_recording_what_it_saw_for_the_comparison(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The shadow dataset does not end at the cutover — it changes sides. The
    four days that justified the cutover are thin for tail behaviour, and the
    lane that is not producing is exactly the one worth measuring."""
    clock.advance(60)
    gateway.set_positions(TRADER, [position(size_usd="20000")])
    await websocket_produced(
        pool,
        clock,
        TRADER,
        PositionEvent(kind="scale_in", coin="BTC", side="long",
                      size_usd=Decimal("20000"), prev_size_usd=Decimal("10000")),
    )

    await poll(pool, gateway, clock)

    shadow = [row for row in await events(pool) if row["source"] == POLL_SOURCE]
    assert len(shadow) == 1
    assert shadow[0]["kind"] == "scale_in"
    assert shadow[0]["authoritative"] is False


async def test_a_change_the_websocket_never_produced_escalates_and_is_then_written(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """Drift is an incident. The websocket was connected and delivering, and a
    change still never arrived — the failure mode a heartbeat cannot see. The
    poller must not quietly write it (that would put two writers on one Trader);
    it takes production back, and the event is then produced by the lane that
    now owns it.

    Two looks, because the first is not evidence: a lane that is merely a few
    seconds behind looks identical to a lane that missed the change, and only
    one of them is still silent a moment later (see the reconciliation-patience
    tests below)."""
    clock.advance(60)
    gateway.set_positions(TRADER, [])

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    authority = await read_authority(pool)
    assert authority.owner == POLL_OWNER
    assert "drift" in authority.reason
    assert TRADER in authority.reason and "BTC" in authority.reason
    written = await events(pool)
    assert [(row["kind"], row["source"], row["authoritative"]) for row in written] == [
        ("close", POLL_SOURCE, True)
    ]
    assert [row["kind"] for row in await alerts(pool)] == ["close"]


async def test_the_escalated_poller_does_not_re_produce_what_the_websocket_produced(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The failover's other half, and the one that costs real money if it is
    wrong. A change straddling the transfer was already produced by the
    websocket and already copied; the poller inheriting production must not
    produce it again, or the copy doubles."""
    clock.advance(30)
    gateway.set_positions(TRADER, [position(coin="ETH"), position()])
    await websocket_produced(
        pool, clock, TRADER, PositionEvent(kind="open", coin="ETH", side="long")
    )
    clock.advance(30)
    gateway.set_positions(TRADER, [position(coin="ETH"), position(), position(coin="SOL")])

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == POLL_OWNER
    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [(row["source"], row["coin"]) for row in authoritative] == [
        (WS_SOURCE, "ETH"),
        (POLL_SOURCE, "SOL"),
    ]
    assert [row["coin"] for row in await alerts(pool)] == ["SOL"]


async def test_a_websocket_shadow_row_never_suppresses_the_poller(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The mirror image: while the POLLER owns production the websocket lane
    keeps writing everything it sees, and none of it is authoritative. Those
    rows must not read as 'already produced' — that would be the shadow lane
    silencing the live one."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [])
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn,
            TRADER,
            [PositionEvent(kind="close", coin="BTC", prev_side="long")],
            clock.now(),
            source=WS_SOURCE,
            authoritative=False,
        )

    await poll(pool, gateway, clock)

    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [(row["source"], row["kind"]) for row in authoritative] == [(POLL_SOURCE, "close")]
    assert [row["kind"] for row in await alerts(pool)] == ["close"]


async def test_two_producers_racing_one_trader_produce_exactly_one_event(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The rule the whole cutover rests on, demonstrated rather than asserted:
    a websocket write and a poller escalation for the same (Trader, coin),
    committed concurrently, leave exactly one authoritative event.

    The websocket's write takes the authority row FOR SHARE and holds it for
    the length of its transaction; the poller's transfer needs it FOR UPDATE
    and therefore cannot interleave. Whichever order they land in, the loser
    sees the ownership it no longer has (or never had) and writes a shadow row.
    """
    clock.advance(60)
    gateway.set_positions(TRADER, [])

    async def websocket_write() -> None:
        async with pool.acquire() as conn, conn.transaction():
            await publish(
                conn,
                TRADER,
                [PositionEvent(kind="close", coin="BTC", prev_side="long")],
                clock.now(),
                source=WS_SOURCE,
            )

    await asyncio.gather(websocket_write(), poll(pool, gateway, clock))

    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [row["coin"] for row in authoritative] == ["BTC"]
    assert len(await alerts(pool)) == 1


async def test_copy_enabled_leaders_are_polled_first_in_degraded_mode(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Degraded mode prioritises the wallets that move money. Ordering is the
    whole mechanism: the pass is paced by the shared weight budget, so a set
    too large for the escalated cadence stretches at its TAIL — and a Leader
    must never be in the tail."""
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, LEADER, FOLLOWER)
    await pool.execute(
        """
        INSERT INTO copy_subs
            (operator_id, leader_address, sub_name, allocation_usd, base_stake_usd,
             copy_mode, enabled, created_at)
        VALUES ($1, $2, 'copy-1', 1000, 100, 'default', TRUE, $3)
        """,
        OPERATOR,
        LEADER,
        clock.now(),
    )

    await poll(pool, gateway, clock)

    polled = [address for address, _dex in gateway.positions_calls]
    assert polled[0] == LEADER


# --- the standby cadence (issue #158) -----------------------------------------
#
# Two clocks live in the position loop and they are deliberately different: how
# often ownership is DECIDED, and how often wallets are POLLED. Deciding stays
# punctual so failover is bounded; polling drops to the low cadence that returns
# most of the shared budget to ingest while the websocket is healthy.


async def ws_beating(pool: asyncpg.Pool, when: datetime) -> None:
    await beat(pool, WS_LANE_PROCESS, when)


async def healthy_websocket(pool: asyncpg.Pool, clock: FakeClock) -> None:
    """A websocket that has earned production: beating, beating for long
    enough, and holding a fresh anchor for every wallet in the poll set."""
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)
    await pool.execute(
        """
        INSERT INTO ws_lane_state (trader_address, baselined_at, resynced_at)
        SELECT trader_address, $1, $1 FROM tracks
        ON CONFLICT (trader_address) DO UPDATE SET resynced_at = EXCLUDED.resynced_at
        """,
        clock.now(),
    )
    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    assert (await evaluate_authority(pool, clock)).owner == WS_OWNER


async def test_a_healthy_websocket_drops_the_poller_to_the_standby_cadence(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Warm, not idle. The poller keeps running — the failover path is the same
    path used every day — but at a cadence that hands most of the shared weight
    budget back to ingest, which is the cutover's other dividend."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await healthy_websocket(pool, clock)
    state = StandbyState()

    polls = 0
    for _tick in range(int(STANDBY_POLL_INTERVAL_SECONDS / POLL_INTERVAL_SECONDS)):
        await ws_beating(pool, clock.now())  # the lane is alive throughout
        if await run_position_cycle(
            pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
        ):
            polls += 1
        clock.advance(POLL_INTERVAL_SECONDS)

    assert polls == 1


async def test_ownership_is_re_decided_every_tick_however_slowly_the_poller_polls(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """What bounds the failover. If ownership were only re-decided on the passes
    themselves, a dead websocket would go unnoticed for a whole standby interval
    on top of the staleness window. Deciding every tick keeps the transfer
    inside the documented budget: staleness + one tick + the pass."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await healthy_websocket(pool, clock)
    state = StandbyState()
    await run_position_cycle(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state)

    # The lane dies. One tick past the staleness window, production moves and
    # the poller polls at once rather than waiting out the standby interval.
    clock.advance(WS_HEARTBEAT_STALE_SECONDS + POLL_INTERVAL_SECONDS)
    result = await run_position_cycle(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
    )

    assert result is not None and result.polled == 1
    assert (await read_authority(pool)).owner == POLL_OWNER


async def test_the_reconciliation_grace_outlasts_a_coalesced_entry(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A constant relation, pinned because breaking it turns the two lanes'
    agreement into a false incident: an entry the websocket is legitimately
    holding (ADR-0009's burst coalescing) emits one push later, and the poller
    must not have called it a miss by then."""
    assert (
        RECONCILE_GRACE_SECONDS
        > WS_COALESCE_WINDOW_SECONDS + POSITIONS_PUSH_INTERVAL_SECONDS
    )


async def test_a_poll_set_outgrowing_the_websocket_takes_production_back(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """One IP streams 15 unique users (ADR-0008). A tracked set that grows past
    that leaves wallets the websocket never watches — and a lane authoritative
    for wallets it cannot see is a silent hole in alerting and in the copy path,
    which is worse than a degraded lane. The ceiling is checked while the
    websocket HOLDS production, not only when it asks for it: the poll set grows
    under the operator's hand, at a moment nobody is looking at ownership."""
    await track(pool, clock, TRADER, FOLLOWER)
    await healthy_websocket(pool, clock)

    for index in range(MAX_SUBSCRIBED_TRADERS):
        await track(pool, clock, f"0xf{index:039x}", FOLLOWER)
    await ws_beating(pool, clock.now())
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == POLL_OWNER
    assert "15" in authority.reason


async def test_withdrawal_detection_survives_the_standby_cadence(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A regression the cutover could have introduced silently. Withdrawal
    Alerts are inferred from two equity observations and refuse to judge across
    a gap longer than a few poll intervals — so slowing the poller to the
    standby cadence without widening that gate would have turned the alert off
    altogether, with nothing failing and nobody told (#171's own note: it would
    go quiet SILENTLY)."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_account_value(TRADER, Decimal("100000"))
    await healthy_websocket(pool, clock)
    await poll(pool, gateway, clock)

    # The interval is a floor, not a promise: a tick fires once at least that
    # long has passed, and the pass itself takes time on top.
    clock.advance(STANDBY_POLL_INTERVAL_SECONDS + 3)
    gateway.set_account_value(TRADER, Decimal("40000"))  # $60k walks out
    await poll(pool, gateway, clock)

    alerted = await pool.fetch("SELECT * FROM withdrawal_alerts")
    assert [row["amount_usd"] for row in alerted] == [Decimal("60000")]


# --- crying wolf, and not crying at all (issue #158) ---------------------------
#
# Reconciliation has to survive two facts about the lanes it compares. The
# websocket can be BEHIND the poller for a few seconds — it holds entry bursts,
# and it re-sends state on a ~5s cadence — so a change the poller sees first is
# not yet evidence of a miss. And a coin the websocket reported SOMETHING about
# is not a coin it reported EVERYTHING about.


async def test_a_change_the_websocket_has_not_produced_yet_is_not_an_incident(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The websocket holds entry bursts by design (ADR-0009) and re-sends state
    on its own cadence, so a change in the seconds before a poll is routinely
    seen here first. Escalating on the first sighting would make the incident
    routine — and an incident that fires every day is one nobody reads."""
    clock.advance(60)
    gateway.set_positions(TRADER, [position(size_usd="20000")])

    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == WS_OWNER
    assert [row["authoritative"] for row in await events(pool)] == []


async def test_the_websocket_producing_it_late_settles_the_doubt(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The other half of the same patience: the held entry lands, the second
    look finds it, and nothing was ever an incident."""
    clock.advance(60)
    gateway.set_positions(TRADER, [position(size_usd="20000")])
    await poll(pool, gateway, clock)

    clock.advance(POLL_INTERVAL_SECONDS)
    await websocket_produced(
        pool,
        clock,
        TRADER,
        PositionEvent(kind="scale_in", coin="BTC", side="long",
                      size_usd=Decimal("20000"), prev_size_usd=Decimal("10000")),
    )
    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == WS_OWNER
    assert [row["authoritative"] for row in await events(pool)] == [True, False]


async def test_a_deferred_verdict_makes_the_poller_look_again_at_once(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Patience must not cost a standby interval. A poller that suspects the
    websocket stops trusting the slow cadence and looks again on the next tick,
    so a real miss is produced ~10s late rather than ~60s late."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await healthy_websocket(pool, clock)
    state = StandbyState()
    await run_position_cycle(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state)

    gateway.set_positions(TRADER, [position(size_usd="20000")])
    clock.advance(STANDBY_POLL_INTERVAL_SECONDS)
    await ws_beating(pool, clock.now())
    deferring = await run_position_cycle(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
    )
    clock.advance(POLL_INTERVAL_SECONDS)  # one TICK, far short of the standby interval
    await ws_beating(pool, clock.now())
    deciding = await run_position_cycle(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
    )

    assert deferring is not None and deferring.deferred == 1
    assert deciding is not None and deciding.drifted == 1


async def test_an_exit_the_websocket_missed_is_drift_even_on_a_coin_it_reported(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """A coin the websocket said SOMETHING about is not a coin it said
    EVERYTHING about. Matching on the coin alone would let an entry the lane did
    produce vouch for an exit it did not — and an exit nobody produces is a copy
    position that never closes, the worst outcome this system can reach."""
    clock.advance(30)
    await websocket_produced(
        pool,
        clock,
        TRADER,
        PositionEvent(kind="scale_in", coin="BTC", side="long",
                      size_usd=Decimal("20000"), prev_size_usd=Decimal("10000")),
    )
    clock.advance(30)
    gateway.set_positions(TRADER, [])  # the Leader is out; the websocket never said so
    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == POLL_OWNER
    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [(row["source"], row["kind"]) for row in authoritative] == [
        (WS_SOURCE, "scale_in"),
        (POLL_SOURCE, "close"),
    ]


async def test_alerts_survive_a_cutover_a_failover_and_a_recovery(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The acceptance criterion a User would notice: "Position Alerts continue
    uninterrupted across a cutover, a failover and a recovery."

    Walked end to end, one Trader, one follower, four changes — one under each
    regime the cutover can be in. The follower gets four alerts and no
    duplicates, and could not tell from them which transport was watching.
    """
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    await poll(pool, gateway, clock)  # pre-cutover: the poller owns and alerts
    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_positions(TRADER, [position(), position(coin="ETH")])
    await poll(pool, gateway, clock)

    # Cutover. The websocket produces; the poller reads and stays quiet.
    await healthy_websocket(pool, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    async with pool.acquire() as conn, conn.transaction():
        await publish(
            conn,
            TRADER,
            [PositionEvent(kind="open", coin="SOL", side="long", size_usd=Decimal("1"))],
            clock.now(),
            source=WS_SOURCE,
        )
    gateway.set_positions(TRADER, [position(), position(coin="ETH"), position(coin="SOL")])
    await poll(pool, gateway, clock)

    # Failover: the lane goes silent, the poller escalates and alerts again.
    clock.advance(WS_HEARTBEAT_STALE_SECONDS + 1)
    assert (await evaluate_authority(pool, clock)).owner == POLL_OWNER
    gateway.set_positions(TRADER, [position(), position(coin="SOL")])  # ETH closes
    await poll(pool, gateway, clock)

    # Recovery: sustained health plus a fresh anchor, and the websocket alerts.
    await healthy_websocket(pool, clock)
    async with pool.acquire() as conn, conn.transaction():
        await publish(
            conn,
            TRADER,
            [PositionEvent(kind="close", coin="SOL", prev_side="long")],
            clock.now(),
            source=WS_SOURCE,
        )

    assert [(row["kind"], row["coin"]) for row in await alerts(pool)] == [
        ("open", "ETH"),
        ("open", "SOL"),
        ("close", "ETH"),
        ("close", "SOL"),
    ]
