"""The websocket shadow lane process (issue #157).

Its own process, not a third loop inside `stream`, and that is a safety
decision rather than a packaging one. The lane's central claim is that it
cannot break alerting: nothing reads its rows, so the only way it could reach
Position Alerts is by taking down the process that produces them. Sharing the
stream process would leave exactly that path open — an unhandled failure in a
gathered task ends the process, and the pollers with it. A separate service
closes it structurally: the lane can crash, wedge, or be stopped by the
operator (`docker compose stop ws`) and the poll pass never notices.

Budget: the resync reads are the lanes' only REST spend, and they carry the
ingest-style reserve — EXECUTION_RESERVE_WEIGHT plus STREAM_RESERVE_WEIGHT —
so they are the FIRST thing starved when the shared bucket tightens. A
reconnect storm can therefore slow these lanes and ingest, and can never draw
the bucket below the floor that guarantees position polling its instant claim
(epigone.stream.main's rule, applied to lanes that rank below both). The order
lanes' resync is the heavier of the two — `frontendOpenOrders` is weight 20 per
venue against `clearinghouseState`'s 2 — which is another reason they are
bounded at MAX_ORDER_LANES rather than following the whole tracked set.
"""

import asyncio
import logging

import aiohttp

from epigone.budget import EXECUTION_RESERVE_WEIGHT, STREAM_RESERVE_WEIGHT, SharedWeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.ws import WS_URL, WebsocketConnection
from epigone.ws.lane import run_lane
from epigone.ws.order_lane import run_order_lanes
from epigone.ws.transport import connect

log = logging.getLogger(__name__)


async def run(pool_url: str, clock: Clock, ws_url: str = WS_URL) -> None:
    pool = await create_pool(pool_url)
    await migrate(pool)
    budget = SharedWeightBudget(
        pool, clock, reserve=EXECUTION_RESERVE_WEIGHT + STREAM_RESERVE_WEIGHT
    )
    async with aiohttp.ClientSession() as session:
        gateway = HttpHyperliquidGateway(session, clock)

        async def open_connection() -> WebsocketConnection:
            return await connect(session, ws_url)

        # Two lanes, one process, and deliberately no coupling between them:
        # the position lane multiplexes every Trader onto one connection, and
        # the order lanes take one connection per Trader because `orderUpdates`
        # frames do not say whose they are (ADR-0008). Both swallow their own
        # failures and reconnect, so `gather` here is composition rather than
        # supervision — neither can end the other.
        await asyncio.gather(
            run_lane(pool, gateway, budget, open_connection, clock),
            run_order_lanes(pool, gateway, budget, open_connection, clock),
        )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    await run(settings.database_url, SystemClock())


if __name__ == "__main__":
    asyncio.run(main())
