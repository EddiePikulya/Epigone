"""First-fine-data notice queue, DB seam (issue #83).

The two writers meet only in Postgres (ADR-0002): the bot records a (user,
wallet) pair's notice state on Follow (`record_follow_notice_state`); the ingest
fine pass flips waiting pairs to deliverable when it persists a wallet's metrics
(`mark_first_data_ready`); the bot delivery loop (epigone.bot.first_data_notice)
drains the 'ready' rows. Neither process calls the other.

Correctness rests on one invariant: the (user, wallet) row's *existence* is the
whole record of "this pair is already handled". A pair is settled at Follow —
either 'suppressed' (data was already there) or 'pending' (waiting) — and only
ever moves pending → ready. Nothing else re-inserts or resets a row, so
restarts, unfollow+refollow, and the 0008-style fine wipe→reseed never
re-notify. Deliberately no backfill: a User already tracking a wallet before
this shipped has no row and gets no notice, exactly like #71's presets.
"""

from datetime import datetime

import asyncpg


async def record_follow_notice_state(
    conn: asyncpg.Connection, user_telegram_id: int, address: str, now: datetime
) -> None:
    """Settle a fresh Follow's notice state, once. A wallet that already has real
    track-record data becomes 'suppressed' (the data is simply there when they
    look — no notice); a wallet without it becomes 'pending' (notify when its
    first data lands). "Has data" is fine_checkpoint_at — the newest observed
    fill, set only by a fine scan that saw at least one fill (NULL for a
    never-scanned wallet *and* for one whose scan came back empty), so an empty
    scan doesn't count as "full track-record data" (issue #83). This is the same
    predicate mark_first_data_ready's flip waits on, kept in lockstep. ON
    CONFLICT DO NOTHING is the dedup: a pair ever recorded — suppressed, pending,
    or already notified — is never reset, so unfollow + refollow cannot
    re-notify. Call only on a genuinely new Track (track_address already returns
    early for a re-follow)."""
    await conn.execute(
        """
        INSERT INTO first_data_notices (user_telegram_id, trader_address, status, created_at)
        VALUES (
            $1, $2,
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM traders
                    WHERE address = $2 AND fine_checkpoint_at IS NOT NULL
                )
                THEN 'suppressed' ELSE 'pending'
            END,
            $3
        )
        ON CONFLICT (user_telegram_id, trader_address) DO NOTHING
        """,
        user_telegram_id,
        address,
        now,
    )


async def mark_first_data_ready(conn: asyncpg.Connection, address: str) -> None:
    """Real track-record data for `address` just landed: queue a one-time notice
    for every tracker still waiting on it by flipping their 'pending' rows to
    'ready'. The caller only invokes this once the wallet actually has fills (an
    empty scan is not "full track-record data"), so the flip and the follow-time
    'suppressed' check stay on the one predicate. Idempotent and re-run on every
    qualifying refresh, not only the first — a pair whose Follow committed just
    after the very first scan (a narrow race) is a 'pending' row the next refresh
    sweeps up. 'suppressed' and already-'ready'/delivered rows are untouched,
    which is what keeps a wipe→reseed from re-notifying trackers who had already
    seen the data. Must run in the same transaction that writes the metrics, so
    'first data landed' and 'notices queued' commit together (restart-safe, like
    #4's outbox)."""
    await conn.execute(
        "UPDATE first_data_notices SET status = 'ready' "
        "WHERE trader_address = $1 AND status = 'pending'",
        address,
    )
