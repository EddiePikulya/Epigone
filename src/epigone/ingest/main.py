"""Ingest process: Universe seed + coarse metrics from the leaderboard download
(issues #5, #26) + fine metric pass with Bot vetting (issue #8).

Each cycle reseeds from the leaderboard at most daily — that single download
also lands the whole Universe's coarse Metric Library, at zero per-account cost.
The full ingest weight budget then goes to the fine pass (survivors and tracked
Traders). Interruptions are safe: fine-pass progress lives in per-Trader
bookkeeping, so a restart resumes where the scan stopped.
"""

import asyncio
import logging
from datetime import datetime, timedelta

import aiohttp

from epigone.budget import WeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import apply_schema, create_pool
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.ingest.budget import INGEST_WEIGHT_PER_MINUTE
from epigone.ingest.fine import run_fine_pass
from epigone.ingest.scan import seed_universe

SEED_INTERVAL = timedelta(days=1)
CYCLE_PAUSE_SECONDS = 60

log = logging.getLogger(__name__)


async def run(pool_url: str, clock: Clock) -> None:
    pool = await create_pool(pool_url)
    await apply_schema(pool)
    budget = WeightBudget(INGEST_WEIGHT_PER_MINUTE, clock)
    last_seeded: datetime | None = None
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session)
        while True:
            if last_seeded is None or clock.now() - last_seeded >= SEED_INTERVAL:
                seeded = await seed_universe(pool, gateway, clock)
                if seeded is not None:
                    last_seeded = clock.now()
            fine = await run_fine_pass(pool, gateway, budget, clock)
            log.info(
                "ingest cycle: fine %d refreshed / %d failed%s",
                fine.refreshed,
                fine.failed,
                " (aborted)" if fine.aborted else "",
            )
            await clock.sleep(CYCLE_PAUSE_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    await run(settings.database_url, SystemClock())


if __name__ == "__main__":
    asyncio.run(main())
