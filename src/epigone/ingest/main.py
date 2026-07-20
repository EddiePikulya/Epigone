"""Ingest process: Universe seed + coarse metrics from the leaderboard download
(issues #5, #26) + fine metric pass with Bot vetting (issue #8).

Each cycle reseeds from the leaderboard on a configurable cadence — hourly by
default (issue #50), operator-tunable via SEED_INTERVAL_MINUTES. That single
free CDN download lands the whole Universe's coarse Metric Library at zero
per-account cost and discovers new wallets, so a frequent re-seed keeps windowed
stats and fine-eligibility current within the hour without touching the per-IP
rate budget. The fine pass (survivors and tracked Traders) then spends whatever
the shared weight budget (epigone.budget, issue #28) has left above the stream's
reserve. Interruptions are safe: fine-pass progress lives in per-Trader
bookkeeping, so a restart resumes where the scan stopped.
"""

import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp
import asyncpg

from epigone.budget import STREAM_RESERVE_WEIGHT, Budget, SharedWeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway import HyperliquidGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.ingest.fine import run_fine_pass
from epigone.ingest.scan import seed_universe

CYCLE_PAUSE_SECONDS = 60

log = logging.getLogger(__name__)


async def run(
    pool_url: str, clock: Clock, seed_interval: timedelta, chunk_size: int
) -> None:
    pool = await create_pool(pool_url)
    await migrate(pool)
    # Ingest is the background spender: it draws the shared budget (issue #28)
    # only above the stream's reserve, so Position Alerts always poll first.
    budget = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session, clock)
        await ingest_loop(pool, gateway, budget, clock, seed_interval, chunk_size=chunk_size)


async def ingest_loop(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    clock: Clock,
    seed_interval: timedelta,
    *,
    max_cycles: int | None = None,
    chunk_size: int | None = None,
) -> None:
    """Seed → fine-pass each cycle, re-seeding once `seed_interval` has elapsed
    since the last successful seed. Each cycle's fine pass is bounded to
    `chunk_size` due Traders (issue #66), so under a backlog control returns to
    this loop between chunks — the seed keeps its cadence and the due queue is
    re-read (ordering #65, freshly due wallets) every chunk instead of once per
    multi-hour pass. `None` runs the whole due queue in one pass (pre-chunking
    behavior); production passes the configured size. `max_cycles` bounds the
    otherwise-infinite loop so tests can drive a finite number of cycles against
    the injected clock; production leaves it None."""
    last_seeded: datetime | None = None
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        if last_seeded is None or clock.now() - last_seeded >= seed_interval:
            seeded = await seed_universe(pool, gateway, clock)
            if seeded is not None:
                last_seeded = clock.now()
        fine = await run_fine_pass(pool, gateway, budget, clock, chunk_size=chunk_size)
        log.info(
            "ingest cycle: fine %d refreshed / %d failed%s",
            fine.refreshed,
            fine.failed,
            " (aborted)" if fine.aborted else "",
        )
        await clock.sleep(CYCLE_PAUSE_SECONDS)
        cycles += 1


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    await run(
        settings.database_url,
        SystemClock(),
        timedelta(minutes=settings.seed_interval_minutes),
        settings.fine_chunk_size,
    )


if __name__ == "__main__":
    asyncio.run(main())
