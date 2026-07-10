"""The production HyperliquidGateway: direct Hyperliquid endpoints (ADR-0001).

The undocumented stats-data leaderboard is quarantined to Universe seeding —
every failure surfaces as GatewayError so callers can degrade gracefully. The
single leaderboard download also carries the coarse Metric Library, so no
per-account call feeds coarse (issue #26). `clearinghouseState` costs weight 2,
`userFills` weight 20, against the 1200/min per-IP budget.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from epigone.gateway import (
    Fill,
    GatewayError,
    LeaderboardEntry,
    LeaderboardWindow,
    Position,
    Side,
    Window,
)

LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
INFO_URL = "https://api.hyperliquid.xyz/info"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)


class HttpHyperliquidGateway:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        try:
            async with self._session.get(LEADERBOARD_URL, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                payload = await response.json()
        except aiohttp.ClientError as exc:
            raise GatewayError(f"leaderboard request failed: {exc}") from exc
        return parse_leaderboard(payload)

    async def get_fills(self, address: str) -> list[Fill]:
        try:
            async with self._session.post(
                INFO_URL,
                json={"type": "userFills", "user": address.lower()},
                timeout=REQUEST_TIMEOUT,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except aiohttp.ClientError as exc:
            raise GatewayError(f"userFills request failed for {address}: {exc}") from exc
        return parse_fills(payload)

    async def get_open_positions(self, address: str, dex: str | None = None) -> list[Position]:
        body: dict[str, str] = {"type": "clearinghouseState", "user": address.lower()}
        if dex is not None:
            body["dex"] = dex  # a HIP-3 builder-deployed perp DEX (e.g. "xyz", issue #21)
        try:
            async with self._session.post(
                INFO_URL, json=body, timeout=REQUEST_TIMEOUT
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except aiohttp.ClientError as exc:
            raise GatewayError(f"clearinghouseState request failed for {address}: {exc}") from exc
        return parse_positions(payload, dex)


def parse_leaderboard(payload: Any) -> list[LeaderboardEntry]:
    known = {window.value: window for window in Window}
    try:
        return [
            LeaderboardEntry(
                address=str(row["ethAddress"]).lower(),
                display_name=row["displayName"],
                account_value=Decimal(row["accountValue"]),
                windows={
                    known[name]: LeaderboardWindow(
                        pnl=Decimal(perf["pnl"]),
                        roi=Decimal(perf["roi"]),
                        volume=Decimal(perf["vlm"]),
                    )
                    for name, perf in row["windowPerformances"]
                    if name in known  # perpDay/perpWeek/... — combined windows only in V1
                },
            )
            for row in payload["leaderboardRows"]
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected leaderboard payload shape: {exc!r}") from exc


def parse_fills(payload: Any) -> list[Fill]:
    try:
        return [
            Fill(
                coin=str(fill["coin"]),
                price=Decimal(fill["px"]),
                size=Decimal(fill["sz"]),
                direction=str(fill["dir"]),
                closed_pnl=Decimal(fill["closedPnl"]),
                start_position=Decimal(fill["startPosition"]),
                crossed=bool(fill["crossed"]),
                order_id=int(fill["oid"]),
                time=datetime.fromtimestamp(fill["time"] / 1000, tz=UTC),
            )
            for fill in payload
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected userFills payload shape: {exc!r}") from exc


def parse_positions(payload: Any, dex: str | None = None) -> list[Position]:
    try:
        positions: list[Position] = []
        for entry in payload["assetPositions"]:
            raw = entry["position"]
            size_in_coin = Decimal(raw["szi"])  # signed: negative means short
            if size_in_coin == 0:
                continue
            positions.append(
                Position(
                    coin=_namespaced_coin(str(raw["coin"]), dex),
                    side=Side.LONG if size_in_coin > 0 else Side.SHORT,
                    size_usd=Decimal(raw["positionValue"]),
                    leverage=Decimal(raw["leverage"]["value"]),
                    entry_price=Decimal(raw["entryPx"]),
                    unrealized_pnl=Decimal(raw["unrealizedPnl"]),
                )
            )
        return positions
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected clearinghouseState payload shape: {exc!r}") from exc


def _namespaced_coin(coin: str, dex: str | None) -> str:
    """Builder-DEX coins are `dex:COIN` (e.g. `xyz:META`) so they never collide
    with core coins in the (trader, coin) snapshot key (issue #21). The live API
    already returns them namespaced; prefixing is idempotent, so this stays
    correct if that ever changes."""
    if dex is None or coin.startswith(f"{dex}:"):
        return coin
    return f"{dex}:{coin}"
