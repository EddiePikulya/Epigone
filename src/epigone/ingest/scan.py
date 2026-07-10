"""Universe seed + coarse Metric Library, both from one leaderboard download.

The leaderboard rows already carry every coarse number (account value plus
per-window pnl/roi/volume), so seeding populates `coarse_metrics` for the whole
Universe in the same pass that seeds the Traders — at zero per-account API cost
(issue #26, retiring the old ~33h portfolio scan). "Refreshing" coarse metrics
is just re-seeding, which is idempotent (upsert).

The refresh-tier constants and failure-streak cap live here because the fine
pass (epigone.ingest.fine) still scans per Trader and shares them.
"""

import logging
from datetime import timedelta

import asyncpg

from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, LeaderboardEntry, Window

log = logging.getLogger(__name__)

# spec-defaults refresh tiers: active traders hourly-daily, dormant swept weekly.
ACTIVE_REFRESH_INTERVAL = timedelta(days=1)
DORMANT_REFRESH_INTERVAL = timedelta(days=7)

# A sustained failure streak means the source is down, not that traders are odd:
# stop burning budget and let the next cycle resume from the bookkeeping.
MAX_CONSECUTIVE_FAILURES = 5


async def seed_universe(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> int | None:
    """Upsert the leaderboard into the Universe and its coarse metrics, in one
    pass. Returns the entry count, or None when the leaderboard source failed
    (logged; existing Universe intact). Idempotent: re-seeding upserts, never
    duplicates."""
    try:
        entries = await gateway.get_leaderboard()
    except GatewayError:
        log.exception("universe seed skipped: leaderboard source failed; existing Universe intact")
        return None
    now = clock.now()
    async with pool.acquire() as conn, conn.transaction():
        await conn.executemany(
            """
            INSERT INTO traders (address, display_name, refresh_tier, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $4, $4)
            ON CONFLICT (address) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    refresh_tier = EXCLUDED.refresh_tier,
                    last_seen_at = EXCLUDED.last_seen_at
            """,
            [(e.address.lower(), e.display_name, _classify_tier(e), now) for e in entries],
        )
        await conn.executemany(
            """
            INSERT INTO coarse_metrics
                (address, time_window, pnl, roi, volume, account_value, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (address, time_window) DO UPDATE
                SET pnl = EXCLUDED.pnl,
                    roi = EXCLUDED.roi,
                    volume = EXCLUDED.volume,
                    account_value = EXCLUDED.account_value,
                    computed_at = EXCLUDED.computed_at
            """,
            [
                (e.address.lower(), w.value, win.pnl, win.roi, win.volume, e.account_value, now)
                for e in entries
                for w, win in e.windows.items()
            ],
        )
    log.info("universe seeded: %d leaderboard entries upserted with coarse metrics", len(entries))
    return len(entries)


def _classify_tier(entry: LeaderboardEntry) -> str:
    """Traded this week -> active (drives the fine pass's daily cadence); silent
    this week -> dormant (weekly sweep). Sourced from the leaderboard's own week
    volume (issue #26)."""
    week = entry.windows.get(Window.WEEK)
    if week is not None and week.volume > 0:
        return "active"
    return "dormant"
