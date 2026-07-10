"""Recorded-response tests for the real gateway's positions call (ticket #3).

The fixture is a verbatim `clearinghouseState` response recorded from the
public info API on 2026-07-10 for a known whale address. A local HTTP server
replays it so the real gateway code runs an actual request/response cycle.
"""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import epigone.gateway.http as gateway_http
from epigone.gateway import GatewayError, Side
from epigone.gateway.http import HttpHyperliquidGateway, parse_positions

WHALE = "0xAF0FDD39E5d92499B0eD9F68693DA99C0ec1e92e"

RECORDED: dict[str, Any] = json.loads(
    (Path(__file__).parent / "fixtures" / "clearinghouse_state_whale.json").read_text()
)


@asynccontextmanager
async def replaying_gateway(
    payload: dict[str, Any],
) -> AsyncGenerator[tuple[HttpHyperliquidGateway, list[Any]], None]:
    """A real gateway whose INFO_URL points at a local server replaying `payload`."""
    received: list[Any] = []

    async def info(request: web.Request) -> web.Response:
        received.append(await request.json())
        return web.json_response(payload)

    app = web.Application()
    app.router.add_post("/info", info)
    server = TestServer(app)
    await server.start_server()
    original_url = gateway_http.INFO_URL
    gateway_http.INFO_URL = str(server.make_url("/info"))
    session = aiohttp.ClientSession()
    try:
        yield HttpHyperliquidGateway(session), received
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
