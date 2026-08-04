"""Copy Episodes: one position's life inside a Copy Sub-account (issue #136).

A Copy Episode runs from a copied open to whatever ends it — the Leader's
exit, a bracket trigger, the operator's own hand, or a liquidation. It exists
because two questions have no other honest answer:

**"Is this Leader event about a position we actually hold?"** Decision 8's
residual: an entry skipped as stale is never opened, so the Leader's later
scale and eventual close refer to nothing. Without an episode the executor
would happily "trim 30% of" a position it does not have.

**"Did this position end because the LEADER exited, or because OUR bracket
fired?"** Decision 6's episode rule g1 makes those two endings behave
differently: a bracket exit ENDS the episode, and every later Leader event on
that position is skipped until they fully close and freshly re-open. A
position that simply vanished from the exchange cannot tell you which
happened; the episode plus the bracket rows can.

`size_coin` here is BOOKKEEPING, never authority. Decision 10's self-damping
principle says a relative operation applies to the size the EXCHANGE reports:
"trim 30%" sells 30% of what we really hold, so a divergence converges instead
of compounding. This column exists to be compared against reality and adopted
from it — `adopt_size` is the only writer that matters — and never to size an
order.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

# How an episode ended. The distinctions are the ones ADR-0007 acts on, not a
# taxonomy for its own sake: `bracket` triggers the no-re-entry rule (g1),
# `liquidated` and `unclassified` page, and `leader_close` is the normal one.
ENDED_LEADER_CLOSE = "leader_close"
ENDED_LEADER_FLIP = "leader_flip"
ENDED_BRACKET = "bracket"
ENDED_OPERATOR = "operator_closed"
ENDED_LIQUIDATED = "liquidated"


@dataclass(frozen=True)
class CopyEpisode:
    id: int
    sub_id: int
    coin: str
    side: str
    opened_at: datetime
    opened_event_id: int | None
    entry_price: Decimal
    size_coin: Decimal
    ended_at: datetime | None
    ended_reason: str | None


@dataclass(frozen=True)
class BracketOrder:
    """One resting trigger leg of a bracket, by exchange order id."""

    episode_id: int
    order_id: int
    tpsl: str  # 'tp' | 'sl'
    placed_at: datetime


async def open_episode(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sub_id: int,
    coin: str,
    side: str,
    entry_price: Decimal,
    size_coin: Decimal,
    opened_at: datetime,
    opened_event_id: int | None,
) -> CopyEpisode:
    """Start an episode from OUR fill — our average price and the units we
    actually got, never the Leader's. The unique partial index refuses a
    second live episode on the same (sub, coin), which is a bug turned into a
    failed write rather than a doubled position."""
    row = await conn.fetchrow(
        """
        INSERT INTO copy_episodes
            (sub_id, coin, side, opened_at, opened_event_id, entry_price, size_coin)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        """,
        sub_id,
        coin,
        side,
        opened_at,
        opened_event_id,
        entry_price,
        size_coin,
    )
    assert row is not None
    return _episode(row)


async def live_episode(
    conn: asyncpg.Pool | asyncpg.Connection, *, sub_id: int, coin: str
) -> CopyEpisode | None:
    row = await conn.fetchrow(
        """
        SELECT * FROM copy_episodes
        WHERE sub_id = $1 AND coin = $2 AND ended_at IS NULL
        """,
        sub_id,
        coin,
    )
    return None if row is None else _episode(row)


async def live_episodes(
    conn: asyncpg.Pool | asyncpg.Connection, sub_id: int
) -> list[CopyEpisode]:
    rows = await conn.fetch(
        "SELECT * FROM copy_episodes WHERE sub_id = $1 AND ended_at IS NULL ORDER BY id",
        sub_id,
    )
    return [_episode(row) for row in rows]


async def last_ended_episode(
    conn: asyncpg.Pool | asyncpg.Connection, *, sub_id: int, coin: str
) -> CopyEpisode | None:
    """The most recently ENDED episode on this (sub, coin) — what decision 6's
    episode rule reads to explain a skip. "The bracket already took us out"
    and "we never opened this one" are different sentences to the operator,
    and both are skips."""
    row = await conn.fetchrow(
        """
        SELECT * FROM copy_episodes
        WHERE sub_id = $1 AND coin = $2 AND ended_at IS NOT NULL
        ORDER BY ended_at DESC, id DESC
        LIMIT 1
        """,
        sub_id,
        coin,
    )
    return None if row is None else _episode(row)


async def adopt_size(
    conn: asyncpg.Pool | asyncpg.Connection, episode_id: int, size_coin: Decimal
) -> None:
    """Set the bookkept size to what the exchange actually reports (decision
    10: adopt actual as the new baseline). The only sanctioned way this column
    changes after an open."""
    await conn.execute(
        "UPDATE copy_episodes SET size_coin = $2 WHERE id = $1", episode_id, size_coin
    )


async def end_episode(
    conn: asyncpg.Pool | asyncpg.Connection,
    episode_id: int,
    *,
    reason: str,
    ended_at: datetime,
) -> None:
    """Close an episode's books. Idempotent by the WHERE clause: whichever
    path noticed the ending first owns the reason, and a later, vaguer one
    (the reconcile's "gone") must not overwrite a precise one (the executor's
    "the Leader closed it")."""
    await conn.execute(
        """
        UPDATE copy_episodes SET ended_at = $2, ended_reason = $3
        WHERE id = $1 AND ended_at IS NULL
        """,
        episode_id,
        ended_at,
        reason,
    )


async def record_bracket(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    episode_id: int,
    order_id: int,
    tpsl: str,
    placed_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO copy_bracket_orders (episode_id, order_id, tpsl, placed_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (episode_id, order_id) DO NOTHING
        """,
        episode_id,
        order_id,
        tpsl,
        placed_at,
    )


async def bracket_orders(
    conn: asyncpg.Pool | asyncpg.Connection, episode_id: int
) -> list[BracketOrder]:
    rows = await conn.fetch(
        "SELECT * FROM copy_bracket_orders WHERE episode_id = $1 ORDER BY order_id",
        episode_id,
    )
    return [
        BracketOrder(
            episode_id=row["episode_id"],
            order_id=row["order_id"],
            tpsl=row["tpsl"],
            placed_at=row["placed_at"],
        )
        for row in rows
    ]


async def forget_brackets(conn: asyncpg.Pool | asyncpg.Connection, episode_id: int) -> None:
    """Drop this episode's bracket rows — used when the legs are cancelled or
    replaced, so the table never claims a trigger that is no longer on the
    book."""
    await conn.execute("DELETE FROM copy_bracket_orders WHERE episode_id = $1", episode_id)


def _episode(row: asyncpg.Record) -> CopyEpisode:
    return CopyEpisode(
        id=row["id"],
        sub_id=row["sub_id"],
        coin=row["coin"],
        side=row["side"],
        opened_at=row["opened_at"],
        opened_event_id=row["opened_event_id"],
        entry_price=row["entry_price"],
        size_coin=row["size_coin"],
        ended_at=row["ended_at"],
        ended_reason=row["ended_reason"],
    )
