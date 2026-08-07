"""The durable position-event seam (issue #156, ADR-0006).

The poll pass already decides what a Trader did — open, close, flip, scale in,
scale out. Until now that decision only survived as per-follower Position Alert
rows. These tests pin the second, durable record beside them: `position_events`,
written in the same transaction that advances the snapshot, and claimed exactly
once by each consumer that reads it.

Seam per the house convention: fake HyperliquidGateway, fake clock, real
Postgres. Nothing here builds a consumer — the copy executor (#136) is where
that arrives; what is under test is the seam it will read.
"""

import asyncio
from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone import position_publish
from epigone.budget import WeightBudget
from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.position_events import (
    POSITION_EVENT_RETENTION,
    claim_event,
    outstanding_events,
)
from epigone.stream.poller import run_poll_pass
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000

TRADER = "0xaaa"
FOLLOWER = 42


def position(
    coin: str = "BTC",
    side: Side = Side.LONG,
    size_usd: str = "10000",
    leverage: str = "5",
    entry_price: str = "100",
    unrealized_pnl: str = "0",
    size_coin: str | None = None,
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal(leverage),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal(unrealized_pnl),
        size_coin=Decimal(size_coin) if size_coin is not None else None,
    )


async def track(pool: asyncpg.Pool, clock: FakeClock, address: str, *user_ids: int) -> None:
    """A Trader in the Universe, tracked by each given User."""
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


async def poll(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> int:
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
    return result.events


async def events(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM position_events ORDER BY id")


async def alerts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM position_alerts ORDER BY id")


# The one scenario both golden tests below read, walked one poll at a time: a
# baseline, then every kind the diff can emit, then a sub-threshold drift that
# must stay silent. Ten seconds apart, the real poll interval.
async def walk_every_kind(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    eth = position(coin="ETH", size_coin="5", size_usd="8000", leverage="2")

    async def step(*positions: Position) -> None:
        gateway.set_positions(TRADER, list(positions))
        await poll(pool, gateway, clock)
        clock.advance(10)

    # Baseline: BTC long already open when Epigone first looks.
    await step(position(size_coin="100"))
    # OPEN — ETH appears.
    await step(position(size_coin="100"), eth)
    # SCALE-IN — BTC doubles.
    await step(position(size_coin="200", size_usd="20000", unrealized_pnl="500"), eth)
    # FLIP — BTC turns short.
    await step(
        position(size_coin="60", side=Side.SHORT, size_usd="15000", entry_price="250"), eth
    )
    # SCALE-OUT — the short is trimmed to a third.
    await step(
        position(
            size_coin="20",
            side=Side.SHORT,
            size_usd="5000",
            entry_price="250",
            unrealized_pnl="-300",
        ),
        eth,
    )
    # CLOSE — BTC is gone.
    await step(eth)
    # Silent drift: ETH grows 5%, below SCALE_SIGNIFICANCE_THRESHOLD.
    await step(position(coin="ETH", size_coin="5.25", size_usd="8400", leverage="2"))


async def test_every_change_records_exactly_one_event_and_the_first_look_records_none(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """One event per change, in the order the Trader made them — and nothing at
    all for the positions that predated Epigone's first look at the wallet."""
    await track(pool, clock, TRADER, FOLLOWER)

    await walk_every_kind(pool, gateway, clock)

    rows = await events(pool)
    assert [(r["kind"], r["coin"]) for r in rows] == [
        ("open", "ETH"),
        ("scale_in", "BTC"),
        ("flip", "BTC"),
        ("scale_out", "BTC"),
        ("close", "BTC"),
    ]
    assert all(r["trader_address"] == TRADER for r in rows)
    assert all(r["source"] == "poll" for r in rows)


async def test_an_event_says_what_the_position_alert_for_the_same_change_says(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The acceptance criterion, read literally: every field the alert and the
    event share carries the same value, and the event's observed_at is the
    alert's created_at. Two records of one thing, never a second opinion."""
    await track(pool, clock, TRADER, FOLLOWER)

    await walk_every_kind(pool, gateway, clock)

    shared = (
        "kind",
        "coin",
        "side",
        "size_usd",
        "prev_size_usd",
        "leverage",
        "entry_price",
        "prev_side",
        "realized_pnl",
        "pct_return",
        "opened_at",
    )
    event_rows = await events(pool)
    alert_rows = await alerts(pool)
    assert len(event_rows) == len(alert_rows) == 5
    for event, alert in zip(event_rows, alert_rows, strict=True):
        assert {k: event[k] for k in shared} == {k: alert[k] for k in shared}
        assert event["observed_at"] == alert["created_at"]


async def test_a_flip_records_one_event_carrying_both_legs(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """ADR-0006 declines to split a flip into a close and an open: one row means
    the two legs are atomically co-visible and can never be reordered. The
    executor reads it as one instruction — close what you hold, open the other
    side."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(side=Side.LONG, size_coin="100")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(
        TRADER,
        [
            position(
                side=Side.SHORT,
                size_usd="15000",
                size_coin="60",
                entry_price="250",
                unrealized_pnl="0",
            )
        ],
    )
    assert await poll(pool, gateway, clock) == 1

    rows = await events(pool)
    assert len(rows) == 1
    flip = rows[0]
    assert flip["kind"] == "flip"
    # The closed leg...
    assert flip["prev_side"] == "long"
    assert flip["realized_pnl"] == Decimal("0")
    # ...and the new one, in the same row.
    assert flip["side"] == "short"
    assert flip["size_usd"] == Decimal("15000")
    assert flip["size_coin"] == Decimal("60")
    assert flip["entry_price"] == Decimal("250")


async def test_an_event_carries_the_position_in_coin_units(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The reason #155 shipped first: an executor cannot place an order from a
    dollar notional. Every kind carries the units it acts on — a scale carries
    both the size it reached and the size it came from, and a close carries what
    the closed position last measured, because a live fetch would show nothing."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [position(size_coin="200", size_usd="20000")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [])
    await poll(pool, gateway, clock)

    scale, close = await events(pool)
    assert (scale["kind"], scale["size_coin"], scale["prev_size_coin"]) == (
        "scale_in",
        Decimal("200"),
        Decimal("100"),
    )
    assert (close["kind"], close["size_coin"]) == ("close", Decimal("200"))


async def test_a_position_snapshotted_before_the_coin_size_column_closes_as_unmirrorable(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Migration 0028's one lasting gap, recorded honestly rather than guessed:
    a position that predates the column and closes before its next poll yields
    an event with NULL units. Consumers read NULL as "size not mirrorable"."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin=None)])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [])
    await poll(pool, gateway, clock)

    (close,) = await events(pool)
    assert close["kind"] == "close"
    assert close["size_coin"] is None
    assert close["size_usd"] == Decimal("10000")


# Captured from the poll pass as it behaved BEFORE position_events existed, and
# frozen here on purpose: "Position Alerts are byte-for-byte unchanged" is an
# acceptance criterion of #156, so it is proved against a record of the old
# behaviour rather than re-derived from the new code. Every column the alert
# table has except `id`, for every alert the scenario above produces — the id is
# a sequence value the shared fixture's TRUNCATE deliberately does not reset, so
# it says nothing about behaviour.
ALERTS_BEFORE_POSITION_EVENTS = [
    {
        "user_telegram_id": 42,
        "trader_address": "0xaaa",
        "kind": "open",
        "coin": "ETH",
        "side": "long",
        "size_usd": Decimal("8000"),
        "prev_size_usd": None,
        "leverage": Decimal("2"),
        "entry_price": Decimal("100"),
        "prev_side": None,
        "realized_pnl": None,
        "pct_return": None,
        "opened_at": None,
        "created_at": timedelta(seconds=10),
        "delivered_at": None,
        "attempts": 0,
        "telegram_message_id": None,
        "scale_arrows": None,
        "tpsl_line": None,
    },
    {
        "user_telegram_id": 42,
        "trader_address": "0xaaa",
        "kind": "scale_in",
        "coin": "BTC",
        "side": "long",
        "size_usd": Decimal("2E+4"),
        "prev_size_usd": Decimal("1E+4"),
        "leverage": Decimal("5"),
        "entry_price": Decimal("100"),
        "prev_side": None,
        "realized_pnl": None,
        "pct_return": Decimal("0.125"),
        "opened_at": timedelta(0),
        "created_at": timedelta(seconds=20),
        "delivered_at": None,
        "attempts": 0,
        "telegram_message_id": None,
        "scale_arrows": None,
        "tpsl_line": None,
    },
    {
        "user_telegram_id": 42,
        "trader_address": "0xaaa",
        "kind": "flip",
        "coin": "BTC",
        "side": "short",
        "size_usd": Decimal("15000"),
        "prev_size_usd": None,
        "leverage": Decimal("5"),
        "entry_price": Decimal("250"),
        "prev_side": "long",
        "realized_pnl": Decimal("500"),
        "pct_return": Decimal("0.125"),
        "opened_at": timedelta(0),
        "created_at": timedelta(seconds=30),
        "delivered_at": None,
        "attempts": 0,
        "telegram_message_id": None,
        "scale_arrows": None,
        "tpsl_line": None,
    },
    {
        "user_telegram_id": 42,
        "trader_address": "0xaaa",
        "kind": "scale_out",
        "coin": "BTC",
        "side": "short",
        "size_usd": Decimal("5000"),
        "prev_size_usd": Decimal("15000"),
        "leverage": Decimal("5"),
        "entry_price": Decimal("250"),
        "prev_side": None,
        "realized_pnl": None,
        "pct_return": Decimal("-0.3"),
        "opened_at": timedelta(seconds=30),
        "created_at": timedelta(seconds=40),
        "delivered_at": None,
        "attempts": 0,
        "telegram_message_id": None,
        "scale_arrows": None,
        "tpsl_line": None,
    },
    {
        "user_telegram_id": 42,
        "trader_address": "0xaaa",
        "kind": "close",
        "coin": "BTC",
        "side": None,
        "size_usd": Decimal("5000"),
        "prev_size_usd": None,
        "leverage": None,
        "entry_price": None,
        "prev_side": "short",
        "realized_pnl": Decimal("-300"),
        "pct_return": Decimal("-0.3"),
        "opened_at": timedelta(seconds=30),
        "created_at": timedelta(seconds=50),
        "delivered_at": None,
        "attempts": 0,
        "telegram_message_id": None,
        "scale_arrows": None,
        "tpsl_line": None,
    },
]


async def test_position_alerts_are_byte_for_byte_what_they_were_before_events_existed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """No User-visible difference: the alert rows this scenario produces must
    equal, column for column, the ones the pass produced before it wrote a
    single event. Timestamps in the golden are offsets from the clock's start,
    so the comparison pins the timing too without pinning the fake clock's epoch."""
    await track(pool, clock, TRADER, FOLLOWER)
    started_at = clock.now()

    await walk_every_kind(pool, gateway, clock)

    expected = [
        {
            key: started_at + value if isinstance(value, timedelta) else value
            for key, value in row.items()
        }
        for row in ALERTS_BEFORE_POSITION_EVENTS
    ]
    actual = [{k: v for k, v in row.items() if k != "id"} for row in await alerts(pool)]
    assert actual == expected


async def test_a_wallet_in_the_poll_set_with_no_followers_still_records_events(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A wallet polled only because a User linked it as their own (#121) has no
    followers, so it queues no alerts — and records events all the same. The
    seam records what the pass diffed; it is emphatically not filtered by who
    would be notified, which is the very coupling ADR-0006 exists to prevent.
    Consumers select the wallets they care about; retention clears the rest."""
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ('0xown', $1, $1) ON CONFLICT (address) DO NOTHING
        """,
        clock.now(),
    )
    await pool.execute("INSERT INTO users (telegram_id, linked_wallet) VALUES (42, '0xown')")
    gateway.set_positions("0xown", [position(coin="SOL", size_coin="50")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions("0xown", [position(coin="SOL", size_usd="20000", size_coin="100")])
    await poll(pool, gateway, clock)

    assert [(r["trader_address"], r["kind"]) for r in await events(pool)] == [
        ("0xown", "scale_in")
    ]
    assert await alerts(pool) == []


# --- Atomicity: one transaction, demonstrated rather than asserted -----------
#
# The event insert joins the per-trader transaction the snapshot upsert and the
# alert fan-out already share. There is no way to observe that from outside
# except by interrupting the pass partway through and looking at what survived,
# so these two tests do exactly that — once on each side of the event write.


async def snapshot_sizes(pool: asyncpg.Pool) -> dict[str, Decimal]:
    return {
        r["coin"]: r["size_usd"]
        for r in await pool.fetch("SELECT coin, size_usd FROM position_snapshots")
    }


def interrupt(monkeypatch: pytest.MonkeyPatch, target: str) -> None:
    """Make the pass die at `target`, the way a killed process would.

    Patched on `epigone.position_publish` since the cutover (#158): recording
    an event and fanning it out moved there, so that both lanes publish
    identically. The property under test did not move — the pass must still
    leave both writes or neither."""

    async def die(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("interrupted mid-pass")

    monkeypatch.setattr(position_publish, target, die)


async def test_an_interrupted_pass_leaves_neither_the_event_nor_the_snapshot(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Killed between advancing the snapshot and writing the event, the pass
    must leave the snapshot where it was — otherwise the change is lost, seen by
    nobody and diffed away. The next pass then re-detects it exactly once."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [])  # BTC closes
    with monkeypatch.context() as interrupted:
        interrupt(interrupted, "record_events")
        with pytest.raises(RuntimeError):
            await poll(pool, gateway, clock)

    assert await snapshot_sizes(pool) == {"BTC": Decimal("10000")}
    assert await events(pool) == []
    assert await alerts(pool) == []

    clock.advance(10)
    assert await poll(pool, gateway, clock) == 1
    assert [(r["kind"], r["coin"]) for r in await events(pool)] == [("close", "BTC")]
    assert len(await alerts(pool)) == 1


async def test_an_event_never_commits_without_the_snapshot_that_produced_it(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other side of the same window, and the reason the write must not open
    a transaction of its own: killed AFTER the event insert, the pass leaves no
    event either. An event that outlived its snapshot advance would be replayed
    by every later pass — the poller would re-diff the same change forever, and
    the next pass here proves it re-detects it once instead."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [position(size_coin="200", size_usd="20000")])
    with monkeypatch.context() as interrupted:
        interrupt(interrupted, "queue_alerts")
        with pytest.raises(RuntimeError):
            await poll(pool, gateway, clock)

    assert await events(pool) == []
    assert await snapshot_sizes(pool) == {"BTC": Decimal("10000")}

    clock.advance(10)
    assert await poll(pool, gateway, clock) == 1
    assert [(r["kind"], r["size_usd"]) for r in await events(pool)] == [
        ("scale_in", Decimal("20000"))
    ]


async def test_one_event_is_recorded_however_many_followers_the_alert_fans_out_to(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The two records are counted differently on purpose: an alert row is one
    per follower, an event is one per thing that happened. This is the whole
    reason a consumer cannot dedupe alert rows back into events."""
    await track(pool, clock, TRADER, 42, 43, 44)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)

    clock.advance(10)
    gateway.set_positions(TRADER, [position(size_coin="200", size_usd="20000")])
    await poll(pool, gateway, clock)

    assert len(await events(pool)) == 1
    assert sorted(r["user_telegram_id"] for r in await alerts(pool)) == [42, 43, 44]


# --- Claims: a consumer handles each event exactly once ----------------------

CONSUMER = "copy-executor"


async def one_event(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> int:
    """Produce a single real event through the poll pass; return its id."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)
    clock.advance(10)
    gateway.set_positions(TRADER, [position(size_coin="200", size_usd="20000")])
    assert await poll(pool, gateway, clock) == 1
    (row,) = await events(pool)
    return int(row["id"])


async def test_an_unclaimed_event_is_the_consumers_backlog_and_a_claimed_one_is_not(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    event_id = await one_event(pool, gateway, clock)

    async with pool.acquire() as conn:
        backlog = await outstanding_events(conn, CONSUMER)
        assert [e.id for e in backlog] == [event_id]
        assert backlog[0].trader_address == TRADER
        assert backlog[0].source == "poll"
        assert backlog[0].event.kind == "scale_in"
        assert backlog[0].event.size_coin == Decimal("200")
        assert backlog[0].event.prev_size_coin == Decimal("100")

        assert await claim_event(conn, event_id, CONSUMER, clock.now()) is True
        assert await outstanding_events(conn, CONSUMER) == []


async def test_two_claimers_racing_for_one_event_produce_exactly_one_winner(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Two instances of the same consumer — a restart overlapping its
    predecessor, say — reach for the same event at the same moment. Exactly one
    is told to act on it; the loser must not, and finds out from the claim
    itself rather than from a read it raced."""
    event_id = await one_event(pool, gateway, clock)

    async def attempt() -> bool:
        async with pool.acquire() as conn, conn.transaction():
            return await claim_event(conn, event_id, CONSUMER, clock.now())

    outcomes = await asyncio.gather(attempt(), attempt())

    assert sorted(outcomes) == [False, True]
    claims = await pool.fetchval("SELECT count(*) FROM position_event_claims")
    assert claims == 1


async def test_each_consumer_claims_the_same_event_for_itself(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Progress is per consumer, not a shared cursor: one consumer taking an
    event says nothing about anyone else's backlog. That is what lets a second
    consumer join later without the first noticing."""
    event_id = await one_event(pool, gateway, clock)

    async with pool.acquire() as conn:
        assert await claim_event(conn, event_id, CONSUMER, clock.now()) is True

        assert [e.id for e in await outstanding_events(conn, "audit-tap")] == [event_id]
        assert await claim_event(conn, event_id, "audit-tap", clock.now()) is True
        assert await outstanding_events(conn, "audit-tap") == []


async def test_a_crash_before_the_claim_commits_leaves_the_event_outstanding(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The consumer claims in the same transaction that durably records what it
    is about to do. Die before that commits and nothing happened at all: the
    event is still there to be picked up, never silently skipped."""
    event_id = await one_event(pool, gateway, clock)

    with pytest.raises(RuntimeError):
        async with pool.acquire() as conn, conn.transaction():
            assert await claim_event(conn, event_id, CONSUMER, clock.now()) is True
            raise RuntimeError("killed between the claim and the wire")

    async with pool.acquire() as conn:
        assert [e.id for e in await outstanding_events(conn, CONSUMER)] == [event_id]


async def test_a_crash_after_the_claim_commits_never_reprocesses_the_event(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Once the claim is committed the event is handled, whatever became of the
    consumer next. This is ADR-0006's trade, taken deliberately: a claim that
    never reached the wire is a missed copy, which reconciliation surfaces and
    the leader's next event re-syncs — chosen over a doubled position with real
    money behind it."""
    event_id = await one_event(pool, gateway, clock)

    async with pool.acquire() as conn, conn.transaction():
        assert await claim_event(conn, event_id, CONSUMER, clock.now()) is True
    # ...and now the process dies. A fresh one starts and reads its backlog.
    async with pool.acquire() as conn:
        assert await outstanding_events(conn, CONSUMER) == []
        assert await claim_event(conn, event_id, CONSUMER, clock.now()) is False


# --- Retention ---------------------------------------------------------------


async def test_events_past_the_retention_window_are_pruned_and_their_claims_with_them(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Pruned in the pass that writes, the record_rate_limit way — no sweeper.
    A week is a safety bound, not housekeeping: a consumer that far behind is
    broken, and replaying week-old copy signals mirrors an expired thesis."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_positions(TRADER, [position(size_coin="100")])
    await poll(pool, gateway, clock)

    async def scale_to(size_usd: str, size_coin: str) -> None:
        gateway.set_positions(TRADER, [position(size_usd=size_usd, size_coin=size_coin)])
        assert await poll(pool, gateway, clock) == 1

    clock.advance(10)
    await scale_to("20000", "200")  # the event that ages out
    async with pool.acquire() as conn:
        (aged_out,) = await outstanding_events(conn, CONSUMER)
        assert await claim_event(conn, aged_out.id, CONSUMER, clock.now()) is True

    clock.advance((POSITION_EVENT_RETENTION - timedelta(days=1)).total_seconds())
    await scale_to("40000", "400")  # still inside the window when the last pass runs

    clock.advance(timedelta(days=2).total_seconds())
    await scale_to("80000", "800")

    surviving = await events(pool)
    assert [r["size_usd"] for r in surviving] == [Decimal("40000"), Decimal("80000")]
    assert aged_out.id not in {r["id"] for r in surviving}
    # The claim went with the event it claimed — no orphan rows to reason about.
    assert await pool.fetchval("SELECT count(*) FROM position_event_claims") == 0
