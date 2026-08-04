"""What a tracked Trader's account is worth, as of the last time Epigone looked
(issue #170).

The poll pass has always fetched this and dropped it: `clearinghouseState`
answers with `marginSummary.accountValue` beside the positions the diff reads.
This module writes it down — one row per Trader, the latest observation,
overwritten in place.

**Not an event, and that is why it is not a table like position_events.** An
open or a close is something a Trader DID, once, and the record of it is the
only trace that will ever exist. An equity observation is what was true when
Epigone looked, re-observed every ten seconds whether or not anything changed;
kept as history it would be thousands of rows a day per Trader describing,
almost always, nothing happening.

**The producer writes in someone else's transaction.** `record_equity` takes a
connection, never a pool, for the reason `position_events.record_events` does:
the write joins the poll pass's already-open per-Trader transaction, so equity
and snapshots advance together or not at all. An interrupted pass leaves a
Trader's recorded equity exactly as consistent with their recorded positions as
the snapshots are.

**The observation it replaces is its return value.** Withdrawal alerts (#171,
ADR-0007 "Out of A4 scope") are a delta between consecutive observations, and
the previous one has to survive long enough to be subtracted from the new one.
Rather than keep history for it, `record_equity` hands back what it overwrote,
inside the transaction doing the overwriting — so a consumer sees both figures
at the one moment both exist, and cannot read a "previous" value that a
concurrent pass has already moved on from.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg


@dataclass(frozen=True)
class EquityObservation:
    """One look at a Trader's covered-venue equity.

    `account_value` is the sum across POSITION_VENUES — each venue holds its own
    collateral (epigone.gateway.fetch_account_state) — and `observed_at` is when
    Epigone saw it, its own clock rather than either venue's."""

    account_value: Decimal
    observed_at: datetime


async def record_equity(
    conn: asyncpg.Connection, address: str, account_value: Decimal, now: datetime
) -> EquityObservation | None:
    """Store this pass's equity observation for one Trader and return the one it
    replaced — None the first time Epigone ever priced this wallet.

    Read-then-write rather than a single upsert with a clever RETURNING clause,
    because the previous value is the point: this runs inside the caller's
    transaction, so the read and the write are one atomic step regardless, and
    the plain form says what it does."""
    previous = await conn.fetchrow(
        "SELECT account_value, observed_at FROM trader_equity WHERE trader_address = $1", address
    )
    await conn.execute(
        """
        INSERT INTO trader_equity (trader_address, account_value, observed_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (trader_address) DO UPDATE
            SET account_value = EXCLUDED.account_value, observed_at = EXCLUDED.observed_at
        """,
        address,
        account_value,
        now,
    )
    if previous is None:
        return None
    return EquityObservation(
        account_value=previous["account_value"], observed_at=previous["observed_at"]
    )
