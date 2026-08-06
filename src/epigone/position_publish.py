"""What producing a position event MEANS, for whichever lane is producing
(issue #158, ADR-0009).

Before the cutover this lived inside the poll pass, because the poll pass was
the only producer: it recorded the event, fanned an alert out to every follower,
and nudged the fine pass when a round trip closed. After the cutover the
websocket does all three while it is healthy and the poller does all three when
it is not — and the one thing that must never happen is the two lanes doing
them DIFFERENTLY. A Position Alert that arrives with a different shape, or
stops arriving at all, depending on which transport is currently authoritative
would make the failover visible to Users, which is precisely what the warm
standby exists to avoid.

So publication is one function, called by both, and it answers the ownership
question itself rather than trusting its caller:

    authoritative = await owns(conn, source)

read under a share lock inside the caller's own event-writing transaction. A
lane that does not own production still RECORDS everything it observed — with
`authoritative = FALSE`, which nothing consumes — because the two-producer
comparison that justified the cutover is worth more after it than before, and
because the poller's shadow rows are the raw material of reconciliation.
"""

import logging
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.ingest.fine import mark_due_now
from epigone.lane_authority import owns
from epigone.position_events import PositionEvent, record_events

log = logging.getLogger(__name__)


async def publish(
    conn: asyncpg.Connection,
    trader_address: str,
    events: list[PositionEvent],
    now: datetime,
    *,
    source: str,
) -> bool:
    """Record `events` and, if `source` owns production, act on them: alert the
    Trader's followers and mark a closed round trip due. Returns whether they
    were authoritative.

    Inside the caller's transaction, always — the events must commit with the
    snapshot advance they were diffed from (ADR-0006), and the ownership read
    must be part of the same transaction as the write it authorises or it could
    go stale between the two."""
    authoritative = await owns(conn, source)
    await record_events(
        conn, trader_address, events, now, source=source, authoritative=authoritative
    )
    if not authoritative:
        log.debug(
            "%s lane: recorded %d shadow event(s) for %s — another lane owns production",
            source,
            len(events),
            trader_address,
        )
        return False
    await queue_alerts(conn, trader_address, events, now)
    # A close or flip mints a round trip; bump the wallet due-now so the fine
    # pass folds it in within minutes and Recent trades / track record match the
    # alert by the time the User taps through (issue #129). Opens and scales
    # don't — nothing lands in fine_trades that matters at alert-read time (the
    # open shows via live positions). One bump per publication regardless of how
    # many coins closed; the freshness guard makes any repeat a harmless no-op.
    if any(event.kind in ("close", "flip") for event in events):
        await mark_due_now(conn, trader_address, now)
    return True


async def queue_alerts(
    conn: asyncpg.Connection, address: str, events: list[PositionEvent], now: datetime
) -> None:
    """Fan out each event to this Trader's followers, honouring each Track's
    alert controls (issue #10): a muted Track gets nothing, and an effective
    min-size floor (per-Track override, else the User's global floor) drops
    events for positions smaller than it. Filtering here — at queue time —
    means a suppressed event is never stored, so unmuting never backfills.

    Fan-out reads `tracks`, so a wallet in the poll set only because a User
    linked it as their own (#121) is diffed and recorded like any other and
    alerts nobody."""
    followers = await conn.fetch(
        """
        SELECT t.user_telegram_id, t.muted,
               coalesce(t.min_size_usd, u.min_size_usd) AS min_size
        FROM tracks t
        JOIN users u ON u.telegram_id = t.user_telegram_id
        WHERE t.trader_address = $1
        """,
        address,
    )
    rows = [
        (
            follower["user_telegram_id"],
            address,
            event.kind,
            event.coin,
            event.side,
            event.size_usd,
            event.prev_size_usd,
            event.leverage,
            event.entry_price,
            event.prev_side,
            event.realized_pnl,
            event.pct_return,
            event.opened_at,
            now,
        )
        for follower in followers
        if not follower["muted"]
        for event in events
        if not _below_floor(event, follower["min_size"])
    ]
    await conn.executemany(
        """
        INSERT INTO position_alerts
            (user_telegram_id, trader_address, kind, coin, side, size_usd, prev_size_usd,
             leverage, entry_price, prev_side, realized_pnl, pct_return, opened_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """,
        rows,
    )


def _below_floor(event: PositionEvent, floor: Decimal | None) -> bool:
    """Whether a min-size floor suppresses this event. A floor judges every
    alert kind by the position notional it carries (event.size_usd); an event
    with no notional (should not happen) is never suppressed."""
    return floor is not None and event.size_usd is not None and event.size_usd < floor
