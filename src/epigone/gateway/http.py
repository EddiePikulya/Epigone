"""The production HyperliquidGateway: direct Hyperliquid endpoints (ADR-0001).

The undocumented stats-data leaderboard is quarantined to Universe seeding —
every failure surfaces as GatewayError so callers can degrade gracefully.
`clearinghouseState` costs weight 2, `portfolio` weight 20, against the
1200/min per-IP budget.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from epigone.gateway import (
    GatewayError,
    LeaderboardEntry,
    PortfolioWindow,
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

    async def get_portfolio(self, address: str) -> dict[Window, PortfolioWindow]:
        try:
            async with self._session.post(
                INFO_URL, json={"type": "portfolio", "user": address}, timeout=REQUEST_TIMEOUT
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except aiohttp.ClientError as exc:
            raise GatewayError(f"portfolio request failed for {address}: {exc}") from exc
        return parse_portfolio(payload)

    async def get_open_positions(self, address: str) -> list[Position]:
        try:
            async with self._session.post(
                INFO_URL,
                json={"type": "clearinghouseState", "user": address.lower()},
                timeout=REQUEST_TIMEOUT,
            ) as response:
                response.raise_for_status()
                payload = await response.json()
        except aiohttp.ClientError as exc:
            raise GatewayError(f"clearinghouseState request failed for {address}: {exc}") from exc
        return parse_positions(payload)


def parse_leaderboard(payload: Any) -> list[LeaderboardEntry]:
    try:
        rows = payload["leaderboardRows"]
        return [
            LeaderboardEntry(
                address=str(row["ethAddress"]).lower(),
                display_name=row["displayName"],
            )
            for row in rows
        ]
    except (KeyError, TypeError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected leaderboard payload shape: {exc!r}") from exc


def parse_portfolio(payload: Any) -> dict[Window, PortfolioWindow]:
    known = {window.value: window for window in Window}
    try:
        windows: dict[Window, PortfolioWindow] = {}
        for name, stats in payload:
            window = known.get(name)
            if window is None:  # perpDay/perpWeek/... — combined windows only in V1
                continue
            values = [Decimal(v) for _, v in stats["accountValueHistory"]]
            pnls = [Decimal(v) for _, v in stats["pnlHistory"]]
            windows[window] = PortfolioWindow(
                pnl=pnls[-1] if pnls else Decimal(0),
                volume=Decimal(stats["vlm"]),
                account_value=values[-1] if values else Decimal(0),
                starting_account_value=values[0] if values else Decimal(0),
            )
        return windows
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected portfolio payload shape: {exc!r}") from exc


def parse_positions(payload: Any) -> list[Position]:
    try:
        positions: list[Position] = []
        for entry in payload["assetPositions"]:
            raw = entry["position"]
            size_in_coin = Decimal(raw["szi"])  # signed: negative means short
            if size_in_coin == 0:
                continue
            positions.append(
                Position(
                    coin=str(raw["coin"]),
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
