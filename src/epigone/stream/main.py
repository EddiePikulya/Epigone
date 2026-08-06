"""Stream process: the tracked-wallet position poller (issue #4) plus the
slower resting-order poller (issue #115).

Two loops share the process. Each cycle runs one pass over every distinct
tracked Trader, then sleeps whatever remains of its interval; when pacing or
wallet count stretches a pass past the interval, the next one starts
immediately. Alerts land in position_alerts / order_alerts for the bot process
to deliver (ADR-0002).

**The position loop keeps two cadences since the cutover (issue #158,
ADR-0009), and the split is the whole point.** Every tick — 10s, unchanged — it
re-decides who owns event production by reading the websocket lane's heartbeat
age. How often it POLLS depends on that answer: at the escalated cadence when
it owns production, at STANDBY_POLL_INTERVAL_SECONDS when the websocket does.

Deciding punctually while polling slowly is what bounds the failover. Were
ownership re-decided only on the passes themselves, a dead lane would go
unnoticed for a standby interval on top of the staleness window; deciding every
tick keeps the transfer inside staleness + one tick + the pass — under two
minutes, and the number the ticket asks to be documented.

The standby cadence is not idleness. The poller still reads every wallet, still
diffs, still records what it saw, and still watches for changes the websocket
never produced — the reconciliation that catches a lane which is connected,
delivering, and silently missing changes. What it stops doing is producing
events anyone acts on. Meanwhile the ~6× cadence drop hands most of its share
of the weight budget back to ingest, which is the cutover's quieter dividend.

Position polling keeps priority (#115's budget rule): the position loop spends
behind only the execution lane's floor (issue #133 — signed orders outrank
everything), while the order loop spends like ingest — additionally behind
STREAM_RESERVE_WEIGHT — so order polling can never draw the shared bucket
below the floor that guarantees position polls their instant claim. A slow
order pass therefore stretches only its own cadence, never Position Alert
latency.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime

import aiohttp
import asyncpg

from epigone.budget import (
    EXECUTION_RESERVE_WEIGHT,
    STREAM_RESERVE_WEIGHT,
    Budget,
    SharedWeightBudget,
)
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway import HyperliquidGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.lane_authority import POLL_OWNER, evaluate_authority
from epigone.stream.orders import run_order_poll_pass
from epigone.stream.poller import (
    POLL_INTERVAL_SECONDS,
    STANDBY_POLL_INTERVAL_SECONDS,
    PollResult,
    run_poll_pass,
)

log = logging.getLogger(__name__)


@dataclass
class StandbyState:
    """What the position loop remembers between ticks: when it last actually
    polled. Deliberately not a timer — the loop re-decides from the clock every
    tick, so a restart, a long pass or a cadence change resolves on the next
    tick rather than leaving a schedule behind."""

    last_pass_at: datetime | None = None


async def run_position_cycle(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    clock: Clock,
    state: StandbyState,
    *,
    ws_authoritative: bool = True,
) -> PollResult | None:
    """One tick of the position loop: decide who owns production, then poll if
    this tick's cadence says so. Returns the pass's result, or None on a tick
    that only re-decided.

    Extracted from the loop so the cadence — the thing that changes under
    failover, and the thing whose punctuality bounds it — is testable without
    an infinite loop or a real clock."""
    authority = await evaluate_authority(pool, clock, enabled=ws_authoritative)
    cadence = (
        POLL_INTERVAL_SECONDS
        if authority.owner == POLL_OWNER
        else STANDBY_POLL_INTERVAL_SECONDS
    )
    now = clock.now()
    if (
        state.last_pass_at is not None
        and (now - state.last_pass_at).total_seconds() < cadence
    ):
        return None
    state.last_pass_at = now
    result = await run_poll_pass(pool, gateway, budget, clock)
    if result.drifted:
        log.error(
            "position lane: %d wallet(s) drifted from the websocket's production; "
            "the poller has taken it back",
            result.drifted,
        )
    return result


async def run(
    pool_url: str, clock: Clock, order_poll_interval_seconds: int, *, ws_authoritative: bool = True
) -> None:
    pool = await create_pool(pool_url)
    await migrate(pool)
    # The position poller spends the shared budget (issue #28) above only the
    # execution lane's floor (issue #133): signed orders outrank everything,
    # then position polls — ingest and the order loop are the ones that must
    # also keep clear of the stream's floor.
    position_budget = SharedWeightBudget(pool, clock, reserve=EXECUTION_RESERVE_WEIGHT)
    order_budget = SharedWeightBudget(
        pool, clock, reserve=EXECUTION_RESERVE_WEIGHT + STREAM_RESERVE_WEIGHT
    )
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session, clock)
        await asyncio.gather(
            _position_loop(pool, gateway, position_budget, clock, ws_authoritative),
            _order_loop(pool, gateway, order_budget, clock, order_poll_interval_seconds),
        )


async def _position_loop(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    clock: Clock,
    ws_authoritative: bool,
) -> None:
    state = StandbyState()
    while True:
        started = clock.now()
        result = await run_position_cycle(
            pool, gateway, budget, clock, state, ws_authoritative=ws_authoritative
        )
        if result is not None:
            log.debug(
                "stream cycle: %d polled, %d events (%d produced), %d failed%s",
                result.polled,
                result.events,
                result.produced,
                result.failed,
                " (aborted)" if result.aborted else "",
            )
        elapsed = (clock.now() - started).total_seconds()
        # The TICK is fixed whatever the polling cadence is: ownership is
        # re-decided on every one of them, and that is what bounds the failover.
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
    await run(
        settings.database_url,
        SystemClock(),
        settings.order_poll_interval_seconds,
        ws_authoritative=settings.ws_authoritative,
    )


if __name__ == "__main__":
    asyncio.run(main())
