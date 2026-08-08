"""The durable position-event seam (issue #156, ADR-0006).

A position event is what a tracked Trader *did* — opened, closed, flipped,
scaled in, scaled out. The poll pass has always worked this out by diffing
`position_snapshots`; until now the only durable trace was the per-follower
Position Alert rows it fanned out to. This module writes the event itself.

Two halves, and they are deliberately not symmetric:

**Producers write in someone else's transaction.** `record_events` takes a
connection, never a pool, because the event insert must join the transaction
that advances the snapshot it was diffed from — the poll pass's already-open
per-trader transaction. There is no second transaction and no write outside
one. A crash between the two writes would otherwise either lose the event
(snapshot advanced, no record) or replay it forever (event written, snapshot
not advanced), and the poller's exactly-once property is exactly that
atomicity. Same write-ahead discipline as `safety/audit.py`: the durable
record lands with the effect, never after it.

**Consumers claim before acting.** `outstanding_events` is the backlog for one
named consumer — the events carrying no claim row of its own — and
`claim_event` takes an event off that backlog. Claims are per-consumer rather
than a shared cursor because identity values are allocated at INSERT and
published at COMMIT, so two producers committing concurrently can publish out
of id order; a cursor that advanced past the higher id would skip the lower one
forever. A WebSocket lane joining as a second producer is the whole reason the
table exists, so a scheme correct only under one producer would defeat it.

Ordering is total per (trader_address, coin) by id and deliberately unguaranteed
across them; exclusivity is a property of CONSUMED events, not written ones. In
steady state one producer writes per (Trader, coin), but a later ticket runs
poll and WS side by side on purpose to compare them — a dual-written position is
that comparison working, not a bug, and a consumer that must not see both
filters on `source`.

A claim means *handled*, not *traded*: an event the risk policy declines is
still claimed, with an audit row recording the decision, or the backlog never
drains. And a claim is written before the wire, in the same transaction as the
attempt — so a crash mid-flight leaves a claimed event with no outcome, which
is a missed copy. ADR-0006 chooses that over the alternative: an unclaimed but
sent event is a doubled position with real money behind it.

This module builds no consumer. The copy executor (#136) is the first one.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg

# The producer that observed the event. 'poll' is the REST poll pass; 'ws' is
# the WebSocket shadow lane (issue #157), which writes these same rows through
# this same seam without the consumer changing — ADR-0006's payoff, collected.
# A consumer that must not see both filters on this column; today nothing
# reads 'ws' rows at all, which is what makes the lane unable to affect
# alerting even when it is wrong.
POLL_SOURCE = "poll"
WS_SOURCE = "ws"

# Events are pruned past a week, and that is a safety property rather than
# housekeeping: a consumer seven days behind is not behind, it is broken, and
# replaying week-old copy signals mirrors a thesis that has expired — actively
# worse than doing nothing. Consumers pair this with a far tighter staleness
# guard of their own (#136 picks one, on the order of minutes).
POSITION_EVENT_RETENTION = timedelta(days=7)


@dataclass(frozen=True)
class PositionEvent:
    """One thing a Trader did to one position, as the poll pass diffed it.

    `side`, `size_usd`, `size_coin`, `leverage` and `entry_price` describe the
    NEW leg (open, flip, scale); `prev_side`, `realized_pnl`, `pct_return` and
    `opened_at` the CLOSED leg (close, flip) and a scale's live return;
    `prev_size_usd`/`prev_size_coin` the size a scale grew or shrank from. A
    close carries the closed position's last observed size in `size_usd` /
    `size_coin` — the position it closed, not a null — which is also what a
    minimum-size floor judges it by in the alert layer.

    A flip is one event carrying both legs, never a close followed by an open:
    one record makes the pair atomically co-visible and unreorderable for free.

    Coin units may be None for a position snapshotted before migration 0028;
    consumers read that as "size not mirrorable" rather than inventing units
    from the notional, which would be wrong by exactly the unrealized move.
    """

    kind: str  # 'open' | 'close' | 'flip' | 'scale_in' | 'scale_out'
    coin: str  # venue-namespaced, exactly as the snapshot key is
    side: str | None = None  # new leg
    size_usd: Decimal | None = None  # the position notional the event is about
    size_coin: Decimal | None = None  # the same position in coin units (#155)
    prev_size_usd: Decimal | None = None  # size before a scale
    prev_size_coin: Decimal | None = None  # ...in coin units
    leverage: Decimal | None = None
    entry_price: Decimal | None = None
    prev_side: str | None = None  # closed leg
    realized_pnl: Decimal | None = None
    pct_return: Decimal | None = None
    opened_at: datetime | None = None


@dataclass(frozen=True)
class ClaimableEvent:
    """An outstanding event as a consumer sees it: what happened, who it
    happened to, when it was observed, and the id to claim it by."""

    id: int
    trader_address: str
    observed_at: datetime
    source: str
    event: PositionEvent


async def record_events(
    conn: asyncpg.Connection,
    trader_address: str,
    events: list[PositionEvent],
    observed_at: datetime,
    *,
    source: str = POLL_SOURCE,
    authoritative: bool,
) -> None:
    """Persist one Trader's events **inside the caller's open transaction**.

    Takes a connection rather than a pool for that reason alone: the rows must
    commit with the snapshot advance they were diffed from, or not at all.

    `authoritative` says whether the lane that observed these OWNED production
    at that instant (issue #158, ADR-0009) — the column consumers filter on.
    Both lanes record everything they see, always; the one that does not own
    production writes rows nothing consumes, which is how the shadow comparison
    survives the cutover instead of ending at it. Producers do not decide this
    for themselves: `epigone.position_publish.publish` reads it off the
    authority row under a lock, and is the seam both lanes write through.

    It has no default, deliberately (issue #199). Every value it could default
    to is a wrong answer for some caller, and the wrong answer in the TRUE
    direction is the dangerous one: rows the copy executor drains. A caller that
    has not thought about ownership must be made to, and the type checker is
    where that happens.

    Retention is applied here, in the pass that wrote, rather than by a sweeper
    — the `record_rate_limit` precedent (`epigone.budget` prunes stale
    rate_limit_events as it inserts one). Events are rare, so the table stays
    small enough that the sweep costs nothing and never needs an index
    strategy. Claims cascade away with the events they claimed.

    It departs from that precedent in one way, deliberately: `record_rate_limit`
    prunes best-effort on its own connection, because a failed health signal
    must never disturb the pass it rides on. This prune runs inside the caller's
    transaction and is allowed to abort it, because the cost of doing so is
    nil — the snapshot rolls back with it and the next pass re-diffs the same
    change — while a second connection per Trader per pass is not."""
    await conn.executemany(
        """
        INSERT INTO position_events
            (trader_address, coin, kind, side, size_usd, size_coin, prev_size_usd,
             prev_size_coin, leverage, entry_price, prev_side, realized_pnl, pct_return,
             opened_at, observed_at, source, authoritative)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """,
        [
            (
                trader_address,
                event.coin,
                event.kind,
                event.side,
                event.size_usd,
                event.size_coin,
                event.prev_size_usd,
                event.prev_size_coin,
                event.leverage,
                event.entry_price,
                event.prev_side,
                event.realized_pnl,
                event.pct_return,
                event.opened_at,
                observed_at,
                source,
                authoritative,
            )
            for event in events
        ],
    )
    await conn.execute(
        "DELETE FROM position_events WHERE observed_at < $1",
        observed_at - POSITION_EVENT_RETENTION,
    )


async def outstanding_events(
    conn: asyncpg.Connection,
    consumer: str,
    *,
    source: str | None = None,
    authoritative: bool | None = None,
    traders: list[str] | None = None,
) -> list[ClaimableEvent]:
    """This consumer's backlog: every event it has not claimed, oldest first.

    Ordering by id is the guarantee ADR-0006 states — total per (Trader, coin),
    and nothing across them, which no consumer needs. Unbounded and unindexed
    by design, both for the same reason: retention keeps the table to a few
    thousand rows. If it ever outgrows that, an index strategy and a batch
    bound are the things to revisit, not the claims model.

    `authoritative` is the filter ADR-0007 decision 4 makes MANDATORY for the
    copy executor rather than optional, and it lives here — in the query —
    because that is where the ADR puts it. Both lanes dual-write every (trader,
    coin) they observe, so an unfiltered executor would copy every trade TWICE.

    The ADR (and #158's own checklist) originally said to filter on `source`
    and flip it from 'poll' to 'ws' at the cutover. ADR-0009 supersedes that:
    a source filter cannot survive failover in either position — pinned to
    'poll' the executor goes blind the moment the websocket takes over, pinned
    to 'ws' it goes blind the moment the poller takes it back. `authoritative`
    is the same exclusivity expressed against the thing that actually moves,
    and it is the producers, under a lock, that decide it. `source` stays
    available for readers doing lane comparison rather than consumption.

    `traders` narrows the backlog to the copy-enabled Leaders. It is a filter,
    not a skip: an event for a wallet nobody copies must never be CLAIMED,
    because claim-means-handled applies to events that qualified, and claiming
    them would mean a later /copy silently inherits a drained backlog. Events
    that never qualify simply stay outstanding until retention prunes them."""
    rows = await conn.fetch(
        """
        SELECT e.*
        FROM position_events e
        LEFT JOIN position_event_claims c ON c.event_id = e.id AND c.consumer = $1
        WHERE c.event_id IS NULL
          AND ($2::text IS NULL OR e.source = $2)
          AND ($3::boolean IS NULL OR e.authoritative = $3)
          AND ($4::text[] IS NULL OR e.trader_address = ANY($4))
        ORDER BY e.id
        """,
        consumer,
        source,
        authoritative,
        traders,
    )
    return [
        ClaimableEvent(
            id=row["id"],
            trader_address=row["trader_address"],
            observed_at=row["observed_at"],
            source=row["source"],
            event=PositionEvent(
                kind=row["kind"],
                coin=row["coin"],
                side=row["side"],
                size_usd=row["size_usd"],
                size_coin=row["size_coin"],
                prev_size_usd=row["prev_size_usd"],
                prev_size_coin=row["prev_size_coin"],
                leverage=row["leverage"],
                entry_price=row["entry_price"],
                prev_side=row["prev_side"],
                realized_pnl=row["realized_pnl"],
                pct_return=row["pct_return"],
                opened_at=row["opened_at"],
            ),
        )
        for row in rows
    ]


async def claim_event(
    conn: asyncpg.Connection, event_id: int, consumer: str, claimed_at: datetime
) -> bool:
    """Take this event off `consumer`'s backlog. True if this caller won it.

    Exactly-once is the primary key doing the work, not a read-then-write: two
    claimers racing for the same event both INSERT, one conflicts, and only the
    winner gets True. A consumer that gets False has already handled the event
    (or another instance of itself has) and must not act on it.

    Called with the caller's transaction connection, so the claim commits with
    whatever the consumer durably records about acting on it."""
    claimed = await conn.fetchval(
        """
        INSERT INTO position_event_claims (event_id, consumer, claimed_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id, consumer) DO NOTHING
        RETURNING TRUE
        """,
        event_id,
        consumer,
        claimed_at,
    )
    return claimed is True
