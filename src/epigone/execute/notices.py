"""The executor's own messages to the operator's chat (ADR-0007 decision 11).

The operator runs this product from Telegram. The audit trail is a record,
not a notification channel: nobody watches a table. So every copy action,
every skip with its reason, and every pager case is queued here for the bot
process to deliver.

TWO PROPERTIES ARE LOAD-BEARING, both from ADR-0006's separation and both
holding in BOTH directions:

- execution never reads `position_alerts`, so no alert preference — a mute,
  a per-Track size floor — can change what gets traded;
- copy status is never written onto alert rows, so a copy report can never be
  suppressed by one either. Hence a queue of its own rather than a `kind` on
  the existing one.

FULL VERBOSITY IS THE POINT, not an oversight. Events are rare and the reader
is one person who acts manually on what they see — a skipped copy they never
hear about is indistinguishable, from the chat, from a copy that never had an
event. Filtering can come when there is someone to filter for.

Writes take a connection, not a pool, wherever the notice must commit with
the decision it describes — the same discipline `position_events` and the
audit trail use: the durable record lands with the effect, never after it.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

log = logging.getLogger(__name__)

# What the operator is being told. `pager` rides the 🚨 monitor path on top of
# this queue — decision 11 puts unfilled closes, liquidations and
# unclassifiable divergences there rather than letting them drown in the
# ordinary stream.
ACTION = "action"
SKIP = "skip"
PAGER = "pager"
PROVISIONING = "provisioning"

# Delivered notices are pruned past a fortnight — long enough to reconstruct
# an incident from the chat side, short enough that the table stays a queue
# rather than a log. The audit trail is the durable record; this is the
# doorbell. Pruned by the writer as it inserts, the `record_rate_limit` /
# `record_events` precedent, so no sweeper process has to exist for it.
NOTICE_RETENTION = timedelta(days=14)


@dataclass(frozen=True)
class CopyNotice:
    id: int
    user_telegram_id: int
    created_at: datetime
    kind: str
    body: str


async def notify(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    operator_id: int,
    kind: str,
    body: str,
    now: datetime,
) -> None:
    """Queue one message for the operator.

    Rendered at WRITE time rather than at delivery: the executor is the only
    thing that knows what it just did, the bot process would have to re-derive
    it from state that has already moved on, and decision 11 explicitly calls
    message formatting an implementation detail. It also keeps the bot from
    needing to understand copy semantics at all."""
    await conn.execute(
        """
        INSERT INTO copy_notices (user_telegram_id, created_at, kind, body)
        VALUES ($1, $2, $3, $4)
        """,
        operator_id,
        now,
        kind,
        body,
    )
    await conn.execute(
        "DELETE FROM copy_notices WHERE delivered_at IS NOT NULL AND created_at < $1",
        now - NOTICE_RETENTION,
    )


async def pending_notices(pool: asyncpg.Pool) -> list[CopyNotice]:
    """Undelivered notices, oldest first — for tests and for the operator's
    own inspection. The bot's drain reads the raw rows through the shared
    outbox helper instead, because that helper owns the attempts column."""
    rows = await pool.fetch(
        "SELECT * FROM copy_notices WHERE delivered_at IS NULL ORDER BY id"
    )
    return [
        CopyNotice(
            id=row["id"],
            user_telegram_id=row["user_telegram_id"],
            created_at=row["created_at"],
            kind=row["kind"],
            body=row["body"],
        )
        for row in rows
    ]
