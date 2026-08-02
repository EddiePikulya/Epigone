"""Where a lane keeps its memory of a Trader's positions.

`epigone.position_diff` decides WHAT happened; this module is how a lane
remembers what it last saw, so the next decision has something to diff against.
The split matters because there are now two lanes with two tables — the REST
poll pass in `position_snapshots`, the websocket shadow lane in
`ws_position_snapshots` (migration 0030) — and they must stay separate storage
with identical behaviour.

Separate storage is not a preference: a lane that advanced the other's memory
would make the other diff against state it never observed, and the change it
was about to report would vanish. Identical behaviour is equally load-bearing,
because the whole point of running both is comparing their events — and a
difference in how a snapshot is WRITTEN (a preserved `opened_at`, a backfilled
`size_coin`) shows up as a difference in the events, indistinguishable from a
difference in the transports. Extracting the decision into `position_diff` and
leaving two copies of applying it would have left exactly that gap open.

The table name is a parameter, and it is interpolated into the SQL rather than
bound — parameters cannot name relations. It is safe because the only values
that reach it are the two module constants below; nothing user-supplied, and
nothing computed. Callers pass one of those constants, never a string.
"""

from collections.abc import Sequence
from datetime import datetime

import asyncpg

from epigone.gateway import Position
from epigone.position_diff import CoinChange, SnapshotState

# The two lanes' memory. Same columns under the same names, deliberately, so
# one reader and one writer serve both.
POLL_SNAPSHOTS = "position_snapshots"
WS_SNAPSHOTS = "ws_position_snapshots"


async def read_snapshots(
    conn: asyncpg.Connection, table: str, address: str
) -> dict[str, SnapshotState]:
    """One Trader's remembered positions, by coin — the diff's `previous`."""
    rows = await conn.fetch(
        f"SELECT * FROM {table} WHERE trader_address = $1",  # noqa: S608 — see module docstring
        address,
    )
    return {row["coin"]: SnapshotState.from_row(row) for row in rows}


async def apply_changes(
    conn: asyncpg.Connection,
    table: str,
    address: str,
    changes: Sequence[CoinChange],
    updated_at: datetime,
) -> None:
    """Advance memory to match what the diff decided.

    A closed coin is forgotten entirely; everything else is rewritten, carrying
    the `opened_at` the change chose — `now` for an open or a flip (the
    position's clock restarts), the previously remembered one for a scale or a
    silent sub-threshold update (holding time is continuous through a resize).

    Called inside the caller's open transaction, always: this must commit with
    the events that were diffed from it, or an interrupted pass would either
    lose an event (memory advanced, nothing recorded) or replay it forever
    (recorded, memory not advanced). That atomicity is the whole exactly-once
    property, and it belongs to the caller's transaction, not to this call."""
    for change in changes:
        if change.position is None:
            await conn.execute(
                f"DELETE FROM {table} WHERE trader_address = $1 AND coin = $2",  # noqa: S608
                address,
                change.coin,
            )
        else:
            assert change.opened_at is not None  # a remembered position always has one
            await remember(
                conn,
                table,
                address,
                change.position,
                opened_at=change.opened_at,
                updated_at=updated_at,
            )


async def remember(
    conn: asyncpg.Connection,
    table: str,
    address: str,
    position: Position,
    *,
    opened_at: datetime,
    updated_at: datetime,
) -> None:
    """Write one position down, replacing whatever was remembered for that coin.

    Every column is rewritten on every observation, which is also what
    backfills the rows written before a column existed — `size_coin` closed its
    own gap that way within one poll interval (migration 0028)."""
    await conn.execute(
        f"""
        INSERT INTO {table}
            (trader_address, coin, side, size_usd, leverage, entry_price,
             unrealized_pnl, size_coin, opened_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (trader_address, coin) DO UPDATE
            SET side = EXCLUDED.side,
                size_usd = EXCLUDED.size_usd,
                leverage = EXCLUDED.leverage,
                entry_price = EXCLUDED.entry_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                size_coin = EXCLUDED.size_coin,
                opened_at = EXCLUDED.opened_at,
                updated_at = EXCLUDED.updated_at
        """,  # noqa: S608 — the table name is a module constant; see the docstring
        address,
        position.coin,
        position.side.value,
        position.size_usd,
        position.leverage,
        position.entry_price,
        position.unrealized_pnl,
        position.size_coin,
        opened_at,
        updated_at,
    )
