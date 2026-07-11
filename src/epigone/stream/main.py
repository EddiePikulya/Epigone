"""Stream process: the ~30s tracked-wallet position poller (issue #4).

Each cycle runs one poll pass over every distinct tracked Trader, then sleeps
whatever remains of the 30s interval; when pacing or wallet count stretches a
pass past the interval, the next one starts immediately. Alerts land in
position_alerts for the bot process to deliver (ADR-0002).
"""

import asyncio
import logging

import aiohttp

from epigone.budget import SharedWeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.stream.poller import POLL_INTERVAL_SECONDS, run_poll_pass

log = logging.getLogger(__name__)


async def run(pool_url: str, clock: Clock) -> None:
    pool = await create_pool(pool_url)
    await migrate(pool)
    # The stream spends the shared budget (issue #28) with no reserve to leave:
    # it has priority — ingest is the one that must keep clear of its floor.
    budget = SharedWeightBudget(pool, clock)
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session, clock)
        while True:
            started = clock.now()
            result = await run_poll_pass(pool, gateway, budget, clock)
            log.debug(
                "stream cycle: %d polled, %d events, %d failed%s",
                result.polled,
                result.events,
                result.failed,
                " (aborted)" if result.aborted else "",
            )
            elapsed = (clock.now() - started).total_seconds()
            await clock.sleep(max(0.0, POLL_INTERVAL_SECONDS - elapsed))


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    await run(settings.database_url, SystemClock())


if __name__ == "__main__":
    asyncio.run(main())
