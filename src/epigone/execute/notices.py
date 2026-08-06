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

The one exception is a BACKLOG DRAIN (issue #190, amendment D-7): "events are
rare" is a statement about live operation, and a cycle that empties weeks of
retained backlog in one pass is the case it does not describe. `SkipDigest`
below coalesces such a cycle's skips into a single summary. It changes chat
delivery ONLY — the audit trail stays one row per event, verbatim, in both
regimes, and only skips are ever coalesced.

Writes take a connection, not a pool, wherever the notice must commit with
the decision it describes — the same discipline `position_events` and the
audit trail use: the durable record lands with the effect, never after it.
"""

import logging
from collections import Counter
from dataclasses import dataclass, field
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


# How many skips one cycle must produce before the operator reads a summary
# instead of the sentences. NOT an operator knob, deliberately: it is not a
# preference about verbosity, it is the line between the two regimes the chat
# has — live trickle and backlog drain — and moving it changes what the chat
# MEANS rather than how loud it is.
#
# Five, because the two regimes are nowhere near each other. Live, the poller
# hands the executor one Leader's one or two events per cycle and a busy pass
# is three; a drain is the whole retained backlog at once — measured at ~20 on
# the first /copy of a long-tracked Leader (2026-08-06), and unbounded above
# that on a restart after downtime. Anything from five to a dozen would divide
# them identically, so the value is chosen at the low end: on the rare cycle
# that lands between the two, a summary of six costs the operator a click into
# the audit trail, where a storm of twenty costs them the rest of the chat.
SKIP_DIGEST_THRESHOLD = 5


@dataclass(frozen=True)
class _PendingSkip:
    leader: str
    category: str
    body: str
    # When the SKIP WAS DECIDED, not when it was delivered. A drain can take
    # minutes, and a notice stamped at flush time would tell the operator that
    # thirty events were decided in the same instant.
    decided_at: datetime


@dataclass
class SkipDigest:
    """One executor cycle's skip notices, held back until the cycle knows how
    many of them there were.

    Held in memory rather than written-then-collapsed, because the collapse
    would otherwise race the bot's own drain: a long cycle can have its first
    notices delivered to Telegram before its last event is handled, and
    deleting those would leave the operator with five sentences AND a summary
    that counts them again.

    What that costs is the one thing to know about this class: a skip notice no
    longer commits in the same transaction as the claim and the audit row it
    describes, so a crash mid-cycle loses the CHAT LINES for events already
    claimed. It does not lose the record — the audit row is still written with
    the claim, and `notify`'s docstring calls this table the doorbell rather
    than the trail. Trading the doorbell for the storm is the trade issue #190
    asks for; trading the trail would not be."""

    entries: list[_PendingSkip] = field(default_factory=list)

    def add(self, *, leader: str, category: str, body: str, now: datetime) -> None:
        """Hold one skip. `leader` is rendered as the operator reads it, and
        `category` is the coarse bucket a summary counts — the per-event
        sentence in `body` is what an under-threshold cycle sends verbatim."""
        self.entries.append(
            _PendingSkip(leader=leader, category=category, body=body, decided_at=now)
        )

    async def flush(
        self,
        conn: asyncpg.Pool | asyncpg.Connection,
        *,
        operator_id: int,
        now: datetime,
    ) -> None:
        """Deliver the cycle's skips and forget them: individually while the
        cycle looks like live operation, as one summary once it looks like a
        drain."""
        if not self.entries:
            return
        if len(self.entries) > SKIP_DIGEST_THRESHOLD:
            body = _summarise(self.entries)
            await notify(conn, operator_id=operator_id, kind=SKIP, body=body, now=now)
            self.entries = []
            return
        # Dropped one at a time, AFTER its own write lands. The obvious
        # `held, self.entries = self.entries, []` loses the tail if the third
        # of five writes hits a dead connection — and these events are already
        # claimed, so nothing would ever offer them again. Here a failed write
        # leaves the rest held, and `_drain_backlog`'s `finally` is not the
        # last chance: the next cycle's flush finds them still queued.
        while self.entries:
            entry = self.entries[0]
            await notify(
                conn,
                operator_id=operator_id,
                kind=SKIP,
                body=entry.body,
                now=entry.decided_at,
            )
            self.entries.pop(0)


def _summarise(held: list[_PendingSkip]) -> str:
    """The drain, in one message: how many died and why, per Leader.

    Ordered by count rather than by name, because the operator's question on
    reading it is "what happened to most of them" — and ties break by name so
    two identical cycles read identically."""
    # Plain text, no markup: these bodies are sent with no parse_mode, so a
    # backtick around `copy_skipped` would reach the operator as a backtick
    # (the lesson of #185, and the reason /limits renders as written).
    lines = [
        f"⏭ {len(held)} leader events skipped, summarised — a backlog drain, "
        f"not that many separate problems. Each one has its own row on the "
        f"audit trail, action copy_skipped, carrying its own full reason."
    ]
    per_leader = Counter(entry.leader for entry in held)
    for leader, total in sorted(per_leader.items(), key=lambda item: (-item[1], item[0])):
        reasons = Counter(entry.category for entry in held if entry.leader == leader)
        breakdown = ", ".join(
            f"{count} {category}"
            for category, count in sorted(reasons.items(), key=lambda item: (-item[1], item[0]))
        )
        lines.append(f"• {leader} — {total}: {breakdown}")
    return "\n".join(lines)


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
