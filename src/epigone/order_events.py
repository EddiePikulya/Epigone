"""The durable order-event seam (issue #168, ADR-0007).

An Order Event is what happened to one of a Trader's resting orders — placed,
filled, cancelled, rejected, triggered. An order is a Trader's *plan*; a
position is what the plan did. ADR-0006 built the position seam and deferred
this one deliberately, because the only durable trace of an order change was
`order_alerts` — one batched row per follower per wallet per cycle (issue #115's
noise rule) carrying rendered JSONB, which is a delivery record and not an
event — and because REST could never poll orders fast enough for the seam to
mean anything.

The websocket can. What it cannot do is say WHOSE order an update is: an
`orderUpdates` frame carries no `user` field (measured 2026-08-03), unlike the
positions feed. Attribution is therefore structural — one connection per
shadowed Trader — and it lives in `epigone.ws.order_lane`, not here.

This module is the same two halves as `position_events`, and they are asymmetric
for the same reasons:

**Producers write in someone else's transaction.** `record_order_events` takes a
connection, never a pool, because the event rows must commit with the memory
advance they were diffed from. A crash between the two would either lose the
event or replay it forever.

**Consumers claim before acting**, per consumer rather than against a shared
cursor: identity values are allocated at INSERT and published at COMMIT, so a
cursor can advance past a lower id that has not appeared yet and skip it
forever.

Two things differ from the position seam, and both are forced by what was
measured rather than chosen:

- **`status` rides beside `kind`.** The exchange's status vocabulary is
  open-ended — one hour of live traffic produced `badAloPxRejected` and
  `iocCancelRejected` beside the obvious ones — so classification is lossy and
  the lossless copy is kept. An unrecognised status becomes `other` rather than
  being forced into a kind a copy consumer would act on.
- **Retention is 24h, not 7 days.** A single active address was measured at 1471
  order updates a minute. Order events are not rare, and a day-old resting order
  has either filled or been cancelled — replaying it would place an order
  against a book that has moved on.

This module builds no consumer, exactly as `position_events` shipped without
one. A seam with a write side and no read side is half a seam.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg

# Epigone's order vocabulary. `gone` is the one that has no wire status behind
# it: a resting order that left the book while the lane was disconnected is
# known to have ended and not known how, because absolute state cannot say
# whether it filled or was cancelled. Naming that ambiguity beats guessing
# between `filled` and `canceled`, and beats staying silent about it.
PLACED = "placed"
FILLED = "filled"
CANCELED = "canceled"
REJECTED = "rejected"
TRIGGERED = "triggered"
GONE = "gone"
OTHER = "other"

# How Epigone learned of the event. `stream` is an `orderUpdates` push, carrying
# the exchange's own account of what happened; `resync` is inferred by diffing a
# point-in-time `frontendOpenOrders` read against the lane's memory, which can
# see THAT something changed during a gap but not always what or when. A
# consumer judging how far to trust a row reads this column.
STREAM_ORIGIN = "stream"
RESYNC_ORIGIN = "resync"

# Pruned by the write, the `record_rate_limit` precedent ADR-0006 also followed.
# Far shorter than POSITION_EVENT_RETENTION, and deliberately so: ADR-0006's
# staleness argument ("replaying week-old copy signals mirrors a thesis that has
# expired") is strictly stronger for a resting order, which is a live intention
# about a live book rather than a record of a completed act.
ORDER_EVENT_RETENTION = timedelta(hours=24)


def classify(status: str) -> str:
    """One exchange status as Epigone's vocabulary.

    Substring rules rather than an enumeration, because the status set is
    open-ended: the `*Canceled` family alone covers margin, self-trade,
    reduce-only, sibling-filled, delisted, liquidated and open-interest-cap
    variants, and new ones can ship at any time. Matching on the family keeps
    those classified correctly the day they first appear.

    **Rejection is tested before cancellation**, and that order is the whole
    subtlety: `iocCancelRejected` — observed live — names both. It is a
    rejection (the order never rested), and reading it as a cancel would tell a
    consumer that a resting order it believed in had been pulled, when there was
    never an order at all.

    Anything unrecognised is `other`, never a nearest guess. The raw status is
    stored beside the classification, so `other` is a pointer to the truth
    rather than a loss of it."""
    if status == "open":
        return PLACED
    if status == "filled":
        return FILLED
    if status == "triggered":
        return TRIGGERED
    if "eject" in status:
        return REJECTED
    if "ancel" in status:
        return CANCELED
    return OTHER


@dataclass(frozen=True)
class OrderEvent:
    """One update to one resting order, as the wire delivered it.

    The trigger fields — `order_type`, `is_trigger`, `trigger_price`,
    `reduce_only`, `is_position_tpsl` — are None on anything the STREAM
    observed, because `WsBasicOrder` does not carry them: a streamed placement
    knows its price and size but not whether it is a stop, a take-profit or a
    plain resting limit. A resync reads them over REST and fills them in. None
    means "not observed", never a default — the `size_coin` convention of
    ADR-0006. A consumer mirroring an order treats an unknown type as
    not-mirrorable rather than assuming a plain limit, which would be a
    confident lie about an order that might be closing a position.

    `status` is None only for a resync-derived event, which no status ever
    described; the table's CHECK enforces that."""

    kind: str  # 'placed' | 'filled' | 'canceled' | 'rejected' | 'triggered' | 'gone' | 'other'
    order_id: int  # the exchange's oid — the thing a cancel names
    coin: str  # venue-namespaced, exactly as positions and orders are elsewhere
    is_buy: bool
    origin: str  # 'stream' | 'resync'
    status: str | None = None  # the exchange's own word for it, verbatim
    limit_price: Decimal | None = None
    size: Decimal | None = None  # what was left to fill at this update
    original_size: Decimal | None = None
    cloid: str | None = None  # opaque client order id, stored as given
    order_type: str | None = None  # resync-only, see above
    is_trigger: bool | None = None
    trigger_price: Decimal | None = None
    reduce_only: bool | None = None
    is_position_tpsl: bool | None = None
    placed_at: datetime | None = None  # the order's own timestamp
    status_at: datetime | None = None  # the exchange's statusTimestamp


@dataclass(frozen=True)
class ClaimableOrderEvent:
    """An outstanding event as a consumer sees it: what happened, whose order it
    was, when it was observed, and the id to claim it by."""

    id: int
    trader_address: str
    observed_at: datetime
    event: OrderEvent


async def record_order_events(
    conn: asyncpg.Connection,
    trader_address: str,
    events: list[OrderEvent],
    observed_at: datetime,
) -> None:
    """Persist one Trader's order events **inside the caller's open
    transaction**.

    A connection rather than a pool, for `record_events`' reason: these rows
    must commit with the `ws_order_state` advance they were diffed from, or the
    lane's memory and its record of what it saw can disagree across a crash.

    `trader_address` comes from the caller — which knows it because the update
    arrived on that Trader's own connection — and never from the payload, which
    does not carry it."""
    await conn.executemany(
        """
        INSERT INTO order_events
            (trader_address, order_id, coin, kind, status, origin, is_buy, limit_price,
             size, original_size, cloid, order_type, is_trigger, trigger_price,
             reduce_only, is_position_tpsl, placed_at, status_at, observed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19)
        """,
        [
            (
                trader_address,
                event.order_id,
                event.coin,
                event.kind,
                event.status,
                event.origin,
                event.is_buy,
                event.limit_price,
                event.size,
                event.original_size,
                event.cloid,
                event.order_type,
                event.is_trigger,
                event.trigger_price,
                event.reduce_only,
                event.is_position_tpsl,
                event.placed_at,
                event.status_at,
                observed_at,
            )
            for event in events
        ],
    )
    await conn.execute(
        "DELETE FROM order_events WHERE observed_at < $1",
        observed_at - ORDER_EVENT_RETENTION,
    )


async def outstanding_order_events(
    conn: asyncpg.Connection, consumer: str
) -> list[ClaimableOrderEvent]:
    """This consumer's backlog: every order event it has not claimed, oldest
    first.

    Bounded by retention rather than by a LIMIT, exactly as the position seam
    is — but retention here is doing much more work, because the feed is a
    firehose and the lane's rate ceiling is what keeps a market maker from
    filling the backlog with requotes."""
    rows = await conn.fetch(
        """
        SELECT e.*
        FROM order_events e
        LEFT JOIN order_event_claims c ON c.event_id = e.id AND c.consumer = $1
        WHERE c.event_id IS NULL
        ORDER BY e.id
        """,
        consumer,
    )
    return [
        ClaimableOrderEvent(
            id=row["id"],
            trader_address=row["trader_address"],
            observed_at=row["observed_at"],
            event=OrderEvent(
                kind=row["kind"],
                order_id=row["order_id"],
                coin=row["coin"],
                is_buy=row["is_buy"],
                origin=row["origin"],
                status=row["status"],
                limit_price=row["limit_price"],
                size=row["size"],
                original_size=row["original_size"],
                cloid=row["cloid"],
                order_type=row["order_type"],
                is_trigger=row["is_trigger"],
                trigger_price=row["trigger_price"],
                reduce_only=row["reduce_only"],
                is_position_tpsl=row["is_position_tpsl"],
                placed_at=row["placed_at"],
                status_at=row["status_at"],
            ),
        )
        for row in rows
    ]


async def claim_order_event(
    conn: asyncpg.Connection, event_id: int, consumer: str, claimed_at: datetime
) -> bool:
    """Take this event off `consumer`'s backlog. True if this caller won it.

    The primary key decides the race, not a read-then-write: two claimers both
    INSERT, one conflicts, and only the winner gets True. A consumer that gets
    False must not act — it, or another instance of itself, already has."""
    claimed = await conn.fetchval(
        """
        INSERT INTO order_event_claims (event_id, consumer, claimed_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id, consumer) DO NOTHING
        RETURNING TRUE
        """,
        event_id,
        consumer,
        claimed_at,
    )
    return claimed is True
