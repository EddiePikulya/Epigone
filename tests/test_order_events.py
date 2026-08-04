"""The durable order-event seam (issue #168, ADR-0008).

An Order Event is what happened to one of a Trader's resting orders — placed,
filled, cancelled, rejected, triggered — recorded as the thing that happened,
independently of whether any User was notified. `order_alerts` is not that: it
is one batched row per follower per wallet per cycle (issue #115's noise rule),
a delivery record with the batch stored as rendered JSONB.

What these tests pin is the seam itself: the vocabulary, the raw status kept
beside it, the retention that a firehose feed forces, and the claim protocol.
Plus the guarantee #168 asks for by name — that nothing in the POSITION-event
consumer path can pick these rows up, which is a property of them being a
different table rather than a promise anyone has to keep.

Seam per the house convention: real Postgres, fake clock. Nothing here builds a
consumer; what is under test is the seam one would read.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone.order_events import (
    ORDER_EVENT_RETENTION,
    RESYNC_ORIGIN,
    STREAM_ORIGIN,
    OrderEvent,
    claim_order_event,
    classify,
    outstanding_order_events,
    record_order_events,
)
from epigone.position_events import PositionEvent, outstanding_events, record_events

TRADER = "0xaaa"
NOW = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)


def event(
    kind: str = "placed",
    order_id: int = 1,
    status: str | None = "open",
    origin: str = STREAM_ORIGIN,
    **extra: object,
) -> OrderEvent:
    return OrderEvent(
        kind=kind,
        order_id=order_id,
        coin="BTC",
        is_buy=True,
        origin=origin,
        status=status,
        limit_price=Decimal("63791"),
        size=Decimal("3.06617"),
        original_size=Decimal("3.06617"),
        placed_at=NOW,
        status_at=NOW,
        **extra,  # type: ignore[arg-type]
    )


async def record(pool: asyncpg.Pool, *events: OrderEvent, at: datetime = NOW) -> None:
    async with pool.acquire() as conn, conn.transaction():
        await record_order_events(conn, TRADER, list(events), at)


# -- the vocabulary -----------------------------------------------------------


def test_the_exchanges_statuses_map_onto_epigones_vocabulary() -> None:
    """The four statuses seen live on 2026-08-03 plus the documented family.
    `iocCancelRejected` is the one that matters: it names BOTH a cancel and a
    rejection, so the order the rules are applied in is load-bearing, not an
    implementation detail."""
    assert classify("open") == "placed"
    assert classify("filled") == "filled"
    assert classify("triggered") == "triggered"
    assert classify("canceled") == "canceled"
    assert classify("marginCanceled") == "canceled"
    assert classify("selfTradeCanceled") == "canceled"
    assert classify("badAloPxRejected") == "rejected"
    assert classify("iocCancelRejected") == "rejected"


def test_a_status_nobody_has_seen_is_named_not_guessed() -> None:
    """The statuses are open-ended and four of the observed ones would not have
    been guessed. An unknown one must never be bucketed into a kind that a copy
    consumer would act on — it becomes 'other' and keeps its raw string, the
    OpenOrder.tpsl precedent: self-describing beats silently wrong."""
    assert classify("someStatusFromTheFuture") == "other"


async def test_an_unknown_status_survives_the_round_trip_verbatim(pool: asyncpg.Pool) -> None:
    """Classification is lossy on purpose, so the lossless copy has to be kept:
    a consumer meeting `other` can read what the exchange actually said."""
    await record(pool, event(kind="other", status="someStatusFromTheFuture"))

    row = await pool.fetchrow("SELECT kind, status FROM order_events")
    assert (row["kind"], row["status"]) == ("other", "someStatusFromTheFuture")


async def test_a_streamed_event_must_carry_the_status_it_was_classified_from(
    pool: asyncpg.Pool,
) -> None:
    """A streamed row without a status would be a classification with nothing to
    check it against. Only a resync-derived row may omit one — no status ever
    described it."""
    with pytest.raises(asyncpg.IntegrityConstraintViolationError):
        await record(pool, event(status=None, origin=STREAM_ORIGIN))


async def test_a_resync_derived_event_needs_no_status(pool: asyncpg.Pool) -> None:
    await record(pool, event(kind="gone", status=None, origin=RESYNC_ORIGIN))

    row = await pool.fetchrow("SELECT kind, status, origin FROM order_events")
    assert (row["kind"], row["status"], row["origin"]) == ("gone", None, RESYNC_ORIGIN)


async def test_what_the_stream_cannot_observe_is_null_rather_than_defaulted(
    pool: asyncpg.Pool,
) -> None:
    """WsBasicOrder carries no orderType/isTrigger/reduceOnly, so a streamed
    placement genuinely does not know whether it is a stop or a plain limit.
    NULL says that. Defaulting to 'plain limit' would tell a mirroring consumer
    a confident lie about an order that closes a position."""
    await record(pool, event())

    row = await pool.fetchrow("SELECT order_type, is_trigger, reduce_only FROM order_events")
    assert (row["order_type"], row["is_trigger"], row["reduce_only"]) == (None, None, None)


# -- retention ----------------------------------------------------------------


async def test_events_are_pruned_past_the_retention_window(pool: asyncpg.Pool) -> None:
    """Pruned by the write itself (the record_rate_limit precedent), and the
    window is 24h rather than position events' 7 days: a resting order from
    yesterday has filled or been cancelled, so replaying it would place an order
    against a book that has moved on."""
    old = NOW - ORDER_EVENT_RETENTION - timedelta(minutes=1)
    await record(pool, event(order_id=1), at=old)
    assert await pool.fetchval("SELECT count(*) FROM order_events") == 1

    await record(pool, event(order_id=2), at=NOW)

    remaining = await pool.fetch("SELECT order_id FROM order_events")
    assert [row["order_id"] for row in remaining] == [2]


async def test_retention_is_shorter_than_the_position_seams() -> None:
    """Stated as a relationship, not two constants that could drift apart: the
    staleness argument for orders is strictly stronger than for positions."""
    from epigone.position_events import POSITION_EVENT_RETENTION

    assert ORDER_EVENT_RETENTION < POSITION_EVENT_RETENTION


# -- the claim protocol -------------------------------------------------------


async def test_a_consumers_backlog_is_what_it_has_not_claimed(pool: asyncpg.Pool) -> None:
    await record(pool, event(order_id=1), event(order_id=2))

    async with pool.acquire() as conn:
        backlog = await outstanding_order_events(conn, "executor")
        assert [claimable.event.order_id for claimable in backlog] == [1, 2]
        assert backlog[0].trader_address == TRADER

        async with conn.transaction():
            assert await claim_order_event(conn, backlog[0].id, "executor", NOW) is True
        assert [c.event.order_id for c in await outstanding_order_events(conn, "executor")] == [2]
        # Another consumer's backlog is untouched: claims are per consumer.
        assert len(await outstanding_order_events(conn, "risk")) == 2


async def test_two_claimers_racing_the_same_event_produce_one_winner(
    pool: asyncpg.Pool,
) -> None:
    """Exactly-once is the primary key doing the work, not a read-then-write."""
    await record(pool, event())
    event_id = await pool.fetchval("SELECT id FROM order_events")

    async with pool.acquire() as conn, conn.transaction():
        assert await claim_order_event(conn, event_id, "executor", NOW) is True
        assert await claim_order_event(conn, event_id, "executor", NOW) is False


async def test_claims_cascade_away_with_the_events_they_claimed(pool: asyncpg.Pool) -> None:
    """Otherwise retention would leave claim rows pointing at nothing, and the
    table would grow without bound behind a table that does not."""
    await record(pool, event(), at=NOW - ORDER_EVENT_RETENTION - timedelta(minutes=1))
    event_id = await pool.fetchval("SELECT id FROM order_events")
    async with pool.acquire() as conn, conn.transaction():
        await claim_order_event(conn, event_id, "executor", NOW)

    await record(pool, event(order_id=2), at=NOW)

    assert await pool.fetchval("SELECT count(*) FROM order_event_claims") == 0


# -- the seam boundary #168 asks for by name ----------------------------------


async def test_the_position_event_consumer_cannot_see_order_events(
    pool: asyncpg.Pool,
) -> None:
    """#166's review noted that `outstanding_events`/`claim_event` do not filter
    on `source`, so any seam sharing those tables would be silently picked up by
    the position-event consumer path. A separate table closes that structurally:
    there is no filter to forget."""
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        TRADER,
        NOW,
    )
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn, TRADER, [PositionEvent(kind="open", coin="BTC", side="long")], NOW
        )
    await record(pool, event(), event(order_id=2))

    async with pool.acquire() as conn:
        positions = await outstanding_events(conn, "executor")
        assert [claimable.event.kind for claimable in positions] == ["open"]
        orders = await outstanding_order_events(conn, "executor")
        assert [claimable.event.kind for claimable in orders] == ["placed", "placed"]


async def test_claiming_an_order_event_leaves_the_position_backlog_alone(
    pool: asyncpg.Pool,
) -> None:
    """The two claim tables are keyed on ids from two different identity
    sequences, so a shared claim table would collide by construction. They are
    not shared, and this is the test that says so."""
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        TRADER,
        NOW,
    )
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn, TRADER, [PositionEvent(kind="open", coin="BTC", side="long")], NOW
        )
    await record(pool, event())

    async with pool.acquire() as conn, conn.transaction():
        order_id = (await outstanding_order_events(conn, "executor"))[0].id
        await claim_order_event(conn, order_id, "executor", NOW)

    async with pool.acquire() as conn:
        assert len(await outstanding_events(conn, "executor")) == 1
        assert await pool.fetchval("SELECT count(*) FROM position_event_claims") == 0
