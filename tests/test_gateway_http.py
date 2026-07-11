"""Recorded-response tests for the real gateway's positions call (ticket #3)
and its 429 backoff-and-retry behavior (issue #28).

The fixture is a verbatim `clearinghouseState` response recorded from the
public info API on 2026-07-10 for a known whale address. A local HTTP server
replays it so the real gateway code runs an actual request/response cycle.
"""

import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import epigone.gateway.http as gateway_http
from epigone.gateway import GatewayError, RateLimitedError, Side
from epigone.gateway.http import (
    RATE_LIMIT_MAX_TRIES,
    HttpHyperliquidGateway,
    parse_positions,
)
from tests.support.clock import FakeClock

WHALE = "0xAF0FDD39E5d92499B0eD9F68693DA99C0ec1e92e"

RECORDED: dict[str, Any] = json.loads(
    (Path(__file__).parent / "fixtures" / "clearinghouse_state_whale.json").read_text()
)


@asynccontextmanager
async def replaying_gateway(
    payload: dict[str, Any],
    *,
    clock: FakeClock | None = None,
    rng: Callable[[], float] = lambda: 1.0,
    rate_limited: int = 0,
    retry_after: str | None = None,
) -> AsyncGenerator[tuple[HttpHyperliquidGateway, list[Any]], None]:
    """A real gateway whose INFO_URL points at a local server replaying `payload`,
    after answering the first `rate_limited` requests with a 429."""
    received: list[Any] = []
    remaining_429s = [rate_limited]

    async def info(request: web.Request) -> web.Response:
        received.append(await request.json())
        if remaining_429s[0] > 0:
            remaining_429s[0] -= 1
            headers = {"Retry-After": retry_after} if retry_after is not None else {}
            return web.Response(status=429, headers=headers)
        return web.json_response(payload)

    app = web.Application()
    app.router.add_post("/info", info)
    server = TestServer(app)
    await server.start_server()
    original_url = gateway_http.INFO_URL
    gateway_http.INFO_URL = str(server.make_url("/info"))
    session = aiohttp.ClientSession()
    try:
        yield HttpHyperliquidGateway(session, clock or FakeClock(), rng=rng), received
    finally:
        gateway_http.INFO_URL = original_url
        await session.close()
        await server.close()


async def test_parses_all_positions_from_the_recorded_response() -> None:
    async with replaying_gateway(RECORDED) as (gateway, _):
        positions = await gateway.get_open_positions(WHALE)

    assert [p.coin for p in positions] == ["ETH", "SOL", "AAVE", "NEAR", "JTO", "HYPE"]

    eth = positions[0]
    assert eth.side is Side.SHORT  # szi is -1500.0
    assert eth.size_usd == Decimal("2625150.0")
    assert eth.leverage == Decimal("20")
    assert eth.entry_price == Decimal("1677.9")
    assert eth.unrealized_pnl == Decimal("-108299.961306")
    # marginUsed / returnOnEquity ride the same call (issue #35) — the exact
    # money at risk and its return, taken straight from the payload.
    assert eth.margin_used == Decimal("131257.5")
    assert eth.margin == Decimal("131257.5")
    assert eth.return_on_equity == Decimal("-0.8605992383")
    assert eth.return_on_margin == Decimal("-0.8605992383")

    sol = positions[1]
    assert sol.side is Side.LONG  # szi is positive
    assert sol.entry_price == Decimal("73.2257")


async def test_sends_the_documented_clearinghouse_state_request() -> None:
    async with replaying_gateway(RECORDED) as (gateway, received):
        await gateway.get_open_positions(WHALE)

    assert received == [{"type": "clearinghouseState", "user": WHALE.lower()}]


async def test_a_dex_poll_adds_the_dex_field_to_the_request() -> None:
    async with replaying_gateway({"assetPositions": []}) as (gateway, received):
        await gateway.get_open_positions(WHALE, dex="xyz")

    assert received == [{"type": "clearinghouseState", "user": WHALE.lower(), "dex": "xyz"}]


async def test_parse_positions_namespaces_bare_dex_coins() -> None:
    # Defensive: even if the API returned a bare coin under a dex, the parser
    # namespaces it so it never collides with a core coin (issue #21).
    payload = {
        "assetPositions": [
            {
                "position": {
                    "coin": "META",
                    "szi": "-10",
                    "positionValue": "8000",
                    "leverage": {"value": "3"},
                    "entryPx": "800",
                    "unrealizedPnl": "120",
                }
            }
        ]
    }

    (bare,) = parse_positions(payload, dex="xyz")
    assert bare.coin == "xyz:META"
    assert bare.side is Side.SHORT

    # Already-namespaced coins (what the live API actually returns) pass through.
    payload["assetPositions"][0]["position"]["coin"] = "xyz:META"
    (namespaced,) = parse_positions(payload, dex="xyz")
    assert namespaced.coin == "xyz:META"


async def test_trader_with_no_positions_yields_empty_list() -> None:
    empty = {**RECORDED, "assetPositions": []}
    async with replaying_gateway(empty) as (gateway, _):
        assert await gateway.get_open_positions(WHALE) == []


def test_parse_positions_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_positions({"positions": []})


async def test_a_429_sleeps_the_retry_after_and_retries() -> None:
    clock = FakeClock()
    async with replaying_gateway(RECORDED, clock=clock, rate_limited=1, retry_after="7") as (
        gateway,
        received,
    ):
        positions = await gateway.get_open_positions(WHALE)

    assert positions[0].coin == "ETH"  # the retry succeeded and parsed normally
    assert len(received) == 2
    assert clock.slept == [7.0]


async def test_a_429_without_retry_after_backs_off_exponentially() -> None:
    clock = FakeClock()
    async with replaying_gateway(RECORDED, clock=clock, rate_limited=3, rng=lambda: 1.0) as (
        gateway,
        received,
    ):
        await gateway.get_open_positions(WHALE)

    assert clock.slept == [1.0, 2.0, 4.0]  # the full window at rng()=1.0
    assert len(received) == 4


async def test_backoff_jitter_scales_the_window_but_never_to_zero() -> None:
    clock = FakeClock()
    async with replaying_gateway(RECORDED, clock=clock, rate_limited=2, rng=lambda: 0.0) as (
        gateway,
        _,
    ):
        await gateway.get_open_positions(WHALE)

    assert clock.slept == [0.5, 1.0]  # rng()=0.0 bottoms out at half the window


async def test_an_unparseable_retry_after_falls_back_to_backoff() -> None:
    clock = FakeClock()
    async with replaying_gateway(
        RECORDED, clock=clock, rate_limited=1, retry_after="Fri, 10 Jul 2026 12:01:00 GMT"
    ) as (gateway, _):
        await gateway.get_open_positions(WHALE)

    assert clock.slept == [1.0]


async def test_persistent_429s_surface_as_rate_limited_after_bounded_retries() -> None:
    clock = FakeClock()
    async with replaying_gateway(RECORDED, clock=clock, rate_limited=99) as (gateway, received):
        with pytest.raises(RateLimitedError):
            await gateway.get_fills(WHALE)

    assert len(received) == RATE_LIMIT_MAX_TRIES
    # A GatewayError subclass: callers that degrade gracefully keep doing so.
    assert issubclass(RateLimitedError, GatewayError)
