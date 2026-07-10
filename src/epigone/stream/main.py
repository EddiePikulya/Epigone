"""Stream process: the ~30s tracked-wallet position poller (issue #4).

Each cycle runs one poll pass over every distinct tracked Trader, then sleeps
whatever remains of the 30s interval; when pacing or wallet count stretches a
pass past the interval, the next one starts immediately. Alerts land in
position_alerts for the bot process to deliver (ADR-0002).
"""

import asyncio
import logging

import aiohttp

from epigone.budget import WeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import apply_schema, create_pool
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.stream.poller import (
    POLL_INTERVAL_SECONDS,
    STREAM_WEIGHT_PER_MINUTE,
    run_poll_pass,
)

log = logging.getLogger(__name__)


async def run(pool_url: str, clock: Clock) -> None:
    pool = await create_pool(pool_url)
    await apply_schema(pool)
    budget = WeightBudget(STREAM_WEIGHT_PER_MINUTE, clock)
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session)
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
