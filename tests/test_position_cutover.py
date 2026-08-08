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
from epigone.gateway import GatewayError, Position, RateLimitedError, Side
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
from epigone.position_snapshots import WS_SNAPSHOTS, remember
from epigone.safety.heartbeat import beat
from epigone.stream.main import (
    STANDBY_POLL_INTERVAL_SECONDS,
    StandbyState,
    run_position_cycle,
)
from epigone.stream.poller import (
    MAX_CONSECUTIVE_FAILURES,
    POLL_INTERVAL_SECONDS,
    run_poll_pass,
)
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


async def pending_coins(pool: asyncpg.Pool, address: str) -> list[str]:
    """The coins whose verdict the poller is holding over for another look."""
    held = await pool.fetchval(
        "SELECT reconcile_pending FROM position_poll_state WHERE trader_address = $1", address
    )
    return sorted(held or ())


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


async def test_the_reconciliation_grace_outlasts_a_straddled_handover(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The grace's other relation, and the one the swallow hole was measured in
    (issue #200). The coalescing relation above says the grace outlasts a
    change the websocket is deliberately holding; this one says it outlasts a
    change an ownership TRANSFER caught mid-flight.

    The arithmetic the handover presents: the websocket observes a straddler up
    to one poll interval before the poller's next look — the poller polls every
    tick while it owns production, so that is how long a change can sit
    unlooked-at — and the doubt it raises is confirmed one tick after that.
    Both are spent out of the grace, and what is left over is the margin. It
    was never written down, so nothing said how much of it there was.

    A relation, not a number: any of the three may be retuned, and this fails
    only when the retune eats the margin."""
    straddle = POLL_INTERVAL_SECONDS  # a change can sit one look unnoticed...
    confirm = POLL_INTERVAL_SECONDS  # ...and the doubt is judged a tick later
    assert RECONCILE_GRACE_SECONDS > straddle + confirm
    # And the leftover is not a rounding error: a whole feed cadence still
    # fits, which is the smallest unit any of this is observed in.
    assert (
        RECONCILE_GRACE_SECONDS - (straddle + confirm) >= POSITIONS_PUSH_INTERVAL_SECONDS
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


async def test_a_held_doubt_keeps_its_window_without_holding_the_wallets_freshness(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """What a pass that WITHHOLDS a verdict still records (issue #200).

    Reconciliation withholds two things on a doubt — the verdict and the
    anchor — and #200 adds a third, the window the doubt is judged in. It
    deliberately does not withhold a fourth. `last_polled_at` goes on meaning
    when this pass last READ this wallet, and the freshness the Withdrawal
    Alert's staleness gate is measured in goes on being the equity
    observation's own (`trader_equity.observed_at`), which lands
    unconditionally on every pass.

    That distinction is the reason the doubt gets its own column instead of
    freezing the poll cursor: a frozen cursor would be a poll pass claiming not
    to have polled, and every reading of "how long since we looked at this
    wallet" would quietly change with it. Withdrawal detection is the reading
    that matters — it refuses to judge across a gap it did not watch, so
    widening the apparent gap turns the alert off with nothing failing."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    gateway.set_account_value(TRADER, Decimal("100000"))
    await healthy_websocket(pool, clock)
    await poll(pool, gateway, clock)
    baselined_at = clock.now()

    # A doubt is raised and held: the lane's anchor is where the position used
    # to be, so by its own account it owes an event it has not produced.
    clock.advance(STANDBY_POLL_INTERVAL_SECONDS)
    await websocket_watching(pool, clock, TRADER, [position()])
    gateway.set_positions(TRADER, [position(size_usd="20000")])
    await poll(pool, gateway, clock)
    held_at = clock.now()

    state = await pool.fetchrow(
        "SELECT * FROM position_poll_state WHERE trader_address = $1", TRADER
    )
    assert state is not None
    assert await pending_coins(pool, TRADER) == ["BTC"]
    assert state["reconcile_since"] == baselined_at  # the doubt keeps its window
    assert state["last_polled_at"] == held_at  # the wallet was still read

    # $60k walks out while the doubt stands. The pass that confirms the doubt
    # is an ordinary look at the account, and says so.
    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_account_value(TRADER, Decimal("40000"))
    await poll(pool, gateway, clock)

    alerted = await pool.fetch("SELECT * FROM withdrawal_alerts")
    assert [row["amount_usd"] for row in alerted] == [Decimal("60000")]
    assert (await read_authority(pool)).owner == POLL_OWNER  # and the doubt confirmed


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


# --- the benign divergence class (issue #158, 2026-08-02 comment) -------------
#
# "The scale-significance threshold measures against the last observation, and
# the lanes observe at different cadences — a gradual size change can cross 25%
# in one poll yet never in any single ws push. The comparison should classify
# these as expected, not as lane errors."
#
# The same sentence, one cutover later, is a rule about ownership: a change the
# websocket's own rules never make an event of is not a change it MISSED, and
# escalating on it would hand the operator an incident channel to learn to
# ignore.


async def websocket_watching(
    pool: asyncpg.Pool, clock: FakeClock, address: str, positions: list[Position]
) -> None:
    """The websocket lane's memory of a Trader: baselined, and anchored on
    exactly these positions. What the poller reads to ask whether the lane
    still owes an event — never writes (ADR-0002: the lanes meet in Postgres,
    and each lane's diff memory has exactly one writer)."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            INSERT INTO ws_lane_state (trader_address, baselined_at, resynced_at)
            VALUES ($1, $2, $2)
            ON CONFLICT (trader_address) DO UPDATE SET resynced_at = EXCLUDED.resynced_at
            """,
            address,
            clock.now(),
        )
        await conn.execute(
            "DELETE FROM ws_position_snapshots WHERE trader_address = $1", address
        )
        for pos in positions:
            await remember(
                conn, WS_SNAPSHOTS, address, pos, opened_at=clock.now(), updated_at=clock.now()
            )


async def test_a_gradual_change_the_websocket_never_called_an_event_is_not_drift(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The class the spec names. A position that grows ~8% per push crosses the
    25% threshold against the poller's 60s-old anchor and never against any
    single websocket observation — so the lane emitted nothing, and its memory
    is nonetheless completely current. It missed nothing. Treating that as a
    miss would thrash ownership daily on lanes that agree about reality."""
    clock.advance(60)
    grown = [position(size_usd="20000")]
    gateway.set_positions(TRADER, grown)
    await websocket_watching(pool, clock, TRADER, grown)

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == WS_OWNER
    assert [(row["kind"], row["authoritative"]) for row in await events(pool)] == [
        ("scale_in", False)
    ]
    assert await alerts(pool) == []


async def test_a_change_the_websockets_own_memory_still_owes_is_drift(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The discriminator's other side, and the reason it is the right one: the
    websocket lane's anchor is where the position USED to be, so by its own
    rules it owes an event it has not produced. That is the silent-miss failure
    mode the warm standby exists for — and here the lane's own memory says so.
    """
    clock.advance(60)
    await websocket_watching(pool, clock, TRADER, [position()])  # anchored on the OLD size
    gateway.set_positions(TRADER, [position(size_usd="20000")])

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    authority = await read_authority(pool)
    assert authority.owner == POLL_OWNER and "drift" in authority.reason
    assert [row["kind"] for row in await alerts(pool)] == ["scale_in"]


async def test_a_wallet_the_websocket_is_not_watching_vouches_for_nothing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """A lane with no memory of a Trader agrees with reality about a flat
    wallet the way an empty room agrees with an empty room. Absence of memory
    is absence of watching, never evidence of currency — so it vouches for
    nothing and an unproduced change stays drift."""
    clock.advance(60)
    gateway.set_positions(TRADER, [])  # the position closed; the lane never said so

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    assert (await read_authority(pool)).owner == POLL_OWNER
    assert [row["kind"] for row in await alerts(pool)] == ["close"]


async def test_a_pass_with_nothing_to_report_clears_a_held_doubt(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """A held doubt is about ONE change, and it must not outlive it. The Leader
    trims back before the second look, so the re-diff is a sub-threshold
    update — nothing to report, nothing to hold. A doubt left standing here
    would sit in the wallet's row for days and then escalate the first
    unrelated change that ever raced the websocket, with no patience at all."""
    clock.advance(60)
    await websocket_watching(pool, clock, TRADER, [position()])
    gateway.set_positions(TRADER, [position(size_usd="20000")])
    await poll(pool, gateway, clock)
    assert await pending_coins(pool, TRADER) == ["BTC"]

    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_positions(TRADER, [position(size_usd="10500")])  # back to ~where it was
    await poll(pool, gateway, clock)

    assert await pending_coins(pool, TRADER) == []
    assert (await read_authority(pool)).owner == WS_OWNER


async def test_one_held_doubt_does_not_escalate_a_different_coin(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """Patience is per change, not per wallet. BTC has now been doubted twice
    and is drift; ETH is being seen for the first time in the same pass and is
    not evidence of anything. The incident must name the coin that earned it —
    blaming ETH would send an operator looking at the wrong position.

    Both are produced once the escalation lands, because by then the poller
    owns production and there is nothing left to defer to."""
    clock.advance(60)
    await websocket_watching(pool, clock, TRADER, [position()])
    gateway.set_positions(TRADER, [position(size_usd="20000")])
    await poll(pool, gateway, clock)
    assert await pending_coins(pool, TRADER) == ["BTC"]

    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_positions(TRADER, [position(size_usd="20000"), position(coin="ETH")])
    await poll(pool, gateway, clock)

    authority = await read_authority(pool)
    assert authority.owner == POLL_OWNER
    assert "BTC" in authority.reason and "ETH" not in authority.reason
    assert await pending_coins(pool, TRADER) == []
    assert sorted(row["coin"] for row in await alerts(pool)) == ["BTC", "ETH"]


async def websocket_shadow_wrote(
    pool: asyncpg.Pool, clock: FakeClock, address: str, event: PositionEvent
) -> None:
    """The websocket lane having OBSERVED a change while the poller owned
    production: the row lands unconsumed (`authoritative = FALSE`) and the
    lane's anchor advances past it all the same."""
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn, address, [event], clock.now(), source=WS_SOURCE, authoritative=False
        )


async def test_a_change_stranded_by_an_ownership_transfer_is_produced_not_swallowed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The hole an anchor-only discriminator opens, and the reason the events
    table has to be consulted too.

    A Leader closes in the seconds before a handback. The websocket sees it,
    but it does not own production yet, so the row lands unconsumed — and its
    anchor advances anyway. The poller's last poll-owned pass was moments
    earlier, so it never diffed the close; by the time it does, the websocket
    owns production and its memory is flat.

    "Anchor current" would vouch that away, and nobody would ever produce the
    close: no event, no alert, no incident, and a copy position that never
    closes. An unconsumed row moving the same direction is exactly the evidence
    that distinguishes this from the benign class — there, the lane's rules
    made the change a non-event and there is no row at all."""
    clock.advance(30)
    gateway.set_positions(TRADER, [])
    await websocket_shadow_wrote(
        pool, clock, TRADER, PositionEvent(kind="close", coin="BTC", prev_side="long")
    )
    await websocket_watching(pool, clock, TRADER, [])  # the lane's anchor moved on

    clock.advance(30)
    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    authority = await read_authority(pool)
    assert authority.owner == POLL_OWNER
    assert "BTC" in authority.reason
    assert [row["kind"] for row in await alerts(pool)] == ["close"]
    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [(row["source"], row["kind"]) for row in authoritative] == [(POLL_SOURCE, "close")]


async def test_an_incident_says_whether_the_lane_missed_it_or_ownership_did(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """Two very different diagnoses reach the operator through one alert, and
    the sentence has to tell them apart: a lane that dropped a change is a lane
    to investigate, while a change that fell between two owners is the transfer
    doing what transfers do. Sending someone to read websocket logs for the
    second one wastes the incident."""
    clock.advance(30)
    gateway.set_positions(TRADER, [])
    await websocket_shadow_wrote(
        pool, clock, TRADER, PositionEvent(kind="close", coin="BTC", prev_side="long")
    )
    await websocket_watching(pool, clock, TRADER, [])

    clock.advance(30)
    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    assert "ownership" in (await read_authority(pool)).reason


async def test_a_lane_that_still_owes_the_change_is_named_as_the_miss_it_is(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """...and the genuine miss keeps its own words. The lane's anchor is where
    the position used to be and no row exists at all: it owes an event by its
    own account and never delivered one."""
    clock.advance(60)
    await websocket_watching(pool, clock, TRADER, [position()])
    gateway.set_positions(TRADER, [])

    await poll(pool, gateway, clock)
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    reason = (await read_authority(pool)).reason
    assert "never arrived on the websocket" in reason and "ownership" not in reason


async def test_a_doubt_is_confirmed_against_the_window_that_raised_it(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The straddler whose hold lands late, and the last remnant of the swallow
    hole (issue #200).

    Every other guard here bounds how LONG the hold takes to arrive. This one
    says it does not matter. The transfer-tick pass — the one thing keeping the
    hold within a tick of the handover — is exactly the pass that can skip a
    wallet: a `RateLimitedError` costs that wallet its look and nothing else,
    and the wallet then waits out a whole standby interval. If the confirm
    look's window were re-derived from the pass that HELD the doubt, it would
    by then start after the unconsumed row that proved the change was
    stranded, the coin would be reclassified benign, the anchor would advance,
    and an exit nobody produced would be swallowed for good.

    The doubt's window is the doubt's own, so the evidence cannot age out of
    it — however late the hold arrives, and however long the confirm waits."""
    # The Leader closes in the seconds before a handback. The websocket sees it
    # while it does not yet own production: the row lands unconsumed and its
    # anchor moves on regardless.
    clock.advance(5)
    gateway.set_positions(TRADER, [])
    await websocket_shadow_wrote(
        pool, clock, TRADER, PositionEvent(kind="close", coin="BTC", prev_side="long")
    )
    await websocket_watching(pool, clock, TRADER, [])

    # The pass that would have caught it within a tick is rate limited on this
    # wallet — pacing, not an outage, so the pass carries on and this wallet
    # simply keeps its `since` and polls again next time.
    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.positions_errors[TRADER] = RateLimitedError("still 429 after retries")
    await poll(pool, gateway, clock)
    assert await pending_coins(pool, TRADER) == []

    # Next time is a whole standby interval later. The doubt is raised there...
    clock.advance(STANDBY_POLL_INTERVAL_SECONDS)
    del gateway.positions_errors[TRADER]
    await poll(pool, gateway, clock)
    assert await pending_coins(pool, TRADER) == ["BTC"]

    # ...and confirmed on the next tick, against the evidence that raised it.
    clock.advance(POLL_INTERVAL_SECONDS)
    await poll(pool, gateway, clock)

    authority = await read_authority(pool)
    assert authority.owner == POLL_OWNER
    assert "BTC" in authority.reason and "ownership" in authority.reason
    assert [row["kind"] for row in await alerts(pool)] == ["close"]
    authoritative = [row for row in await events(pool) if row["authoritative"]]
    assert [(row["source"], row["kind"]) for row in authoritative] == [(POLL_SOURCE, "close")]


async def test_a_pass_that_aborts_leaves_the_tail_of_the_set_where_it_was(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The other way the pass above can skip a wallet, and the reason the fix
    is the same one (issue #200). A sustained failure streak means Hyperliquid
    is down, so the pass gives up and resumes next cycle — and every wallet in
    the tail of `leaders_first` keeps its `since` untouched, exactly as a rate
    limited one does.

    Which is what makes the repair complete rather than route-specific: a
    wallet is skipped by having no `_apply_poll` at all, so a doubt raised on
    it later is still raised against the last look that actually happened."""
    baselined_at = clock.now()
    for index in range(MAX_CONSECUTIVE_FAILURES):
        # Sorted ahead of TRADER ("0xaaa"), so the abort lands before it: ties
        # in the poll set break alphabetically (`epigone.poll_set`).
        down = f"0x1{index:039x}"
        await track(pool, clock, down, FOLLOWER)
        gateway.positions_errors[down] = GatewayError("hyperliquid is down")

    clock.advance(STANDBY_POLL_INTERVAL_SECONDS)
    gateway.set_positions(TRADER, [])
    await poll(pool, gateway, clock)

    polled_at = await pool.fetchval(
        "SELECT last_polled_at FROM position_poll_state WHERE trader_address = $1", TRADER
    )
    assert polled_at == baselined_at


async def test_the_reached_back_window_belongs_to_the_doubt_not_to_the_wallet(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, baselined: None
) -> None:
    """The other half of #200: reaching back is a privilege of the coin that
    earned it.

    A doubt's window reaches back to the look the doubt was raised against, and
    a wallet holding one is usually holding other positions too. If that reach
    were the WALLET's — one cursor for every coin on it — an old websocket row
    would vouch for a brand new change on a different coin, which is the same
    swallow this ticket exists to close, arriving through the repair itself.

    Here BTC's doubt reaches back a whole standby interval while ETH is judged
    from this pass's own previous look, which the lane's old ETH entry falls
    outside of. One wallet, one pass, two windows."""
    # The Leader opens ETH and the websocket produces it. The poller does not
    # look until a standby interval later, by which time BTC has grown too and
    # the lane's memory still owes that one.
    clock.advance(5)
    gateway.set_positions(TRADER, [position(), position(coin="ETH")])
    await websocket_produced(
        pool, clock, TRADER, PositionEvent(kind="open", coin="ETH", side="long")
    )

    clock.advance(STANDBY_POLL_INTERVAL_SECONDS)
    gateway.set_positions(TRADER, [position(size_usd="20000"), position(coin="ETH")])
    await websocket_watching(pool, clock, TRADER, [position(), position(coin="ETH")])
    await poll(pool, gateway, clock)
    assert await pending_coins(pool, TRADER) == ["BTC"]

    # The lane was simply late on BTC and produces it. ETH scales in at the
    # same moment and the lane owes THAT — its only ETH row is the entry from
    # a minute ago, which is inside BTC's window and nowhere near ETH's.
    clock.advance(5)
    await websocket_produced(
        pool,
        clock,
        TRADER,
        PositionEvent(kind="scale_in", coin="BTC", side="long",
                      size_usd=Decimal("20000"), prev_size_usd=Decimal("10000")),
    )
    clock.advance(5)
    grown = [position(size_usd="20000"), position(coin="ETH", size_usd="20000")]
    gateway.set_positions(TRADER, grown)
    await websocket_watching(
        pool, clock, TRADER, [position(size_usd="20000"), position(coin="ETH")]
    )
    await poll(pool, gateway, clock)

    assert await pending_coins(pool, TRADER) == ["ETH"]
    assert (await read_authority(pool)).owner == WS_OWNER
    assert [row for row in await events(pool) if row["authoritative"]
            and row["source"] == POLL_SOURCE] == []


async def test_every_ownership_transfer_is_followed_by_a_pass_at_once(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The tick that moves ownership polls, however recently the last pass ran.

    A latency property with a correctness history. Until #200 the confirm
    look's lookback was re-derived from the pass that HELD the doubt, so a
    first post-handback pass arriving a standby interval late would confirm
    against a window that no longer reached the unconsumed row proving the
    straddle — reclassified benign, and swallowed. The window is now the
    doubt's own (`position_poll_state.reconcile_since`), so this pass is back
    to buying what it looks like it buys: a straddler is found within a tick
    of the handover rather than within a standby interval.

    Staged so the assertion can only pass because of the transfer: the poller
    polls, ownership moves ten seconds later, and ten seconds is nothing like
    the standby interval that would otherwise authorise a pass."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position()])
    state = StandbyState()

    # The websocket serves its probation while the poller owns and polls: it is
    # beating throughout, but has not re-established absolute state, so
    # ownership cannot return yet.
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)
    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    polled_before_handover = await run_position_cycle(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
    )
    assert polled_before_handover is not None
    assert (await read_authority(pool)).owner == POLL_OWNER

    # The lane finishes re-anchoring, and the very next tick hands ownership
    # back — one poll interval after a pass, six times short of the standby
    # cadence the new owner implies.
    await websocket_watching(pool, clock, TRADER, [position()])
    clock.advance(POLL_INTERVAL_SECONDS)
    await ws_beating(pool, clock.now())
    result = await run_position_cycle(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock, state
    )

    assert (await read_authority(pool)).owner == WS_OWNER
    assert result is not None and result.polled == 1
