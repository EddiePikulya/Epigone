"""A real gateway pointed at endpoints that never answer (issue #163).

A request that exhausts REQUEST_TIMEOUT raises the builtin TimeoutError (what
asyncio.TimeoutError aliases), which is NOT an aiohttp.ClientError — so it only
becomes a GatewayError if the gateway names it explicitly. That wrap lives in
the production gateway, so testing it means the production gateway against a
real stalled socket; a fake can only replay a decision its caller already
handles. The timeout is monkeypatched down from the production 30s so a test
that must actually wait it out costs milliseconds.
"""

import asyncio
from collections.abc import AsyncGenerator, Container
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from epigone.gateway import http as gateway_http
from epigone.gateway.http import HttpHyperliquidGateway
from tests.support.clock import FakeClock

# Short enough that a stalled request is a fast test, long enough that a
# healthy one on the same server still answers well inside it.
HANG_TIMEOUT = aiohttp.ClientTimeout(total=0.05)


@asynccontextmanager
async def hanging_gateway(
    monkeypatch: pytest.MonkeyPatch, *, hangs: Container[str] | None = None
) -> AsyncGenerator[HttpHyperliquidGateway, None]:
    """The production gateway whose endpoints — info and the leaderboard
    download — stall past a deliberately tiny REQUEST_TIMEOUT.

    `hangs` narrows the stall to those wallets (matched on the info body's
    `user`), answering "no open positions" for every other address, so a pass
    over several wallets can time out on one of them. None stalls everything.
    """
    monkeypatch.setattr(gateway_http, "REQUEST_TIMEOUT", HANG_TIMEOUT)

    async def stall(request: web.Request) -> web.Response:
        if hangs is not None and request.method == "POST":
            body = await request.json()
            if body["user"] not in hangs:
                return web.json_response({"assetPositions": []})
        await asyncio.sleep(1)  # the client gives up long before this returns
        raise AssertionError("the client should have timed out long before this")

    app = web.Application()
    app.router.add_post("/info", stall)
    app.router.add_get("/leaderboard", stall)
    server = TestServer(app)
    await server.start_server()
    # get_leaderboard has no constructor seam (it is one fixed URL, not a
    # per-consumer one like info_url), so the module global is the seam.
    monkeypatch.setattr(gateway_http, "LEADERBOARD_URL", str(server.make_url("/leaderboard")))
    session = aiohttp.ClientSession()
    try:
        yield HttpHyperliquidGateway(session, FakeClock(), info_url=str(server.make_url("/info")))
    finally:
        await session.close()
        await server.close()
