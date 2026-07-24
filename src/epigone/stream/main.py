"""Stream process: the ~30s tracked-wallet position poller (issue #4) plus the
slower resting-order poller (issue #115).

Two loops share the process. Each cycle runs one pass over every distinct
tracked Trader, then sleeps whatever remains of its interval; when pacing or
wallet count stretches a pass past the interval, the next one starts
immediately. Alerts land in position_alerts / order_alerts for the bot process
to deliver (ADR-0002).

Position polling keeps priority (#115's budget rule): the position loop spends
with no reserve to leave, while the order loop spends like ingest — behind
STREAM_RESERVE_WEIGHT — so order polling can never draw the shared bucket
below the floor that guarantees position polls their instant claim. A slow
order pass therefore stretches only its own cadence, never Position Alert
latency.
"""

import asyncio
import logging

import aiohttp
import asyncpg

from epigone.budget import STREAM_RESERVE_WEIGHT, Budget, SharedWeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway import HyperliquidGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.stream.orders import run_order_poll_pass
from epigone.stream.poller import POLL_INTERVAL_SECONDS, run_poll_pass

log = logging.getLogger(__name__)


async def run(pool_url: str, clock: Clock, order_poll_interval_seconds: int) -> None:
    pool = await create_pool(pool_url)
    await migrate(pool)
    # The position poller spends the shared budget (issue #28) with no reserve
    # to leave: it has priority — ingest and the order loop are the ones that
    # must keep clear of its floor.
    position_budget = SharedWeightBudget(pool, clock)
    order_budget = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session, clock)
        await asyncio.gather(
            _position_loop(pool, gateway, position_budget, clock),
            _order_loop(pool, gateway, order_budget, clock, order_poll_interval_seconds),
        )


async def _position_loop(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> None:
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


async def _order_loop(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    clock: Clock,
    interval_seconds: int,
) -> None:
    while True:
        started = clock.now()
        result = await run_order_poll_pass(pool, gateway, budget, clock)
        log.debug(
            "order cycle: %d polled, %d new orders, %d failed%s",
            result.polled,
            result.new_orders,
            result.failed,
            " (aborted)" if result.aborted else "",
        )
        elapsed = (clock.now() - started).total_seconds()
        await clock.sleep(max(0.0, interval_seconds - elapsed))


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    await run(settings.database_url, SystemClock(), settings.order_poll_interval_seconds)


if __name__ == "__main__":
    asyncio.run(main())
