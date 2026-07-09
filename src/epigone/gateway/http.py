"""The real HyperliquidGateway: direct calls to the public info API (ADR-0001).

`clearinghouseState` costs weight 2 against the 1200/min per-IP budget.
"""

from decimal import Decimal
from typing import Any

import aiohttp

from epigone.gateway import Position, Side

MAINNET_INFO_URL = "https://api.hyperliquid.xyz/info"


class HttpHyperliquidGateway:
    def __init__(self, api_url: str = MAINNET_INFO_URL) -> None:
        self._api_url = api_url
        self._session: aiohttp.ClientSession | None = None

    async def get_open_positions(self, address: str) -> list[Position]:
        state = await self._post({"type": "clearinghouseState", "user": address.lower()})
        return _parse_positions(state)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _post(self, body: dict[str, Any]) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        async with self._session.post(self._api_url, json=body) as response:
            response.raise_for_status()
            return await response.json()


def _parse_positions(state: Any) -> list[Position]:
    positions: list[Position] = []
    for entry in state["assetPositions"]:
        raw = entry["position"]
        size_in_coin = Decimal(raw["szi"])  # signed: negative means short
        if size_in_coin == 0:
            continue
        positions.append(
            Position(
                coin=raw["coin"],
                side=Side.LONG if size_in_coin > 0 else Side.SHORT,
                size_usd=Decimal(raw["positionValue"]),
                leverage=Decimal(raw["leverage"]["value"]),
                entry_price=Decimal(raw["entryPx"]),
                unrealized_pnl=Decimal(raw["unrealizedPnl"]),
            )
        )
    return positions
