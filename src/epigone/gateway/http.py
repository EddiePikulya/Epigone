"""The production HyperliquidGateway: direct Hyperliquid endpoints (ADR-0001).

The undocumented stats-data leaderboard is quarantined to Universe seeding —
every failure surfaces as GatewayError so callers can degrade gracefully. The
single leaderboard download also carries the coarse Metric Library, so no
per-account call feeds coarse (issue #26). `clearinghouseState` costs weight 2;
`userFills` weight 20 plus weight per 20 fills returned (issue #41), against
the shared weight budget (epigone.budget) — callers settle the response-size
surcharge once they see the payload.

A 429 backs off and retries here rather than surfacing (issue #28): sleep the
server's Retry-After when present, else an exponential window with jitter.
Only a persistent streak escapes, as RateLimitedError, which callers treat as
pacing — retry the item later, never abort a whole pass over it.
"""

import logging
import random
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from epigone.clock import Clock
from epigone.gateway import (
    Fill,
    GatewayError,
    LeaderboardEntry,
    LeaderboardWindow,
    Position,
    RateLimitedError,
    Side,
    Window,
)

log = logging.getLogger(__name__)

LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
INFO_URL = "https://api.hyperliquid.xyz/info"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Bounded 429 retries: 6 tries = up to 5 sleeps (1+2+4+8+16s at full jitter),
# ~30s worst case — long enough to ride out a blip, short enough that a pass
# under sustained limiting still moves on and resumes next cycle.
RATE_LIMIT_MAX_TRIES = 6
RATE_LIMIT_BACKOFF_BASE_SECONDS = 1.0
RATE_LIMIT_BACKOFF_CAP_SECONDS = 30.0


class HttpHyperliquidGateway:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        clock: Clock,
        *,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self._session = session
        self._clock = clock
        self._rng = rng

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        try:
            payload = await self._request_json("GET", LEADERBOARD_URL)
        except aiohttp.ClientError as exc:
            raise GatewayError(f"leaderboard request failed: {exc}") from exc
        return parse_leaderboard(payload)

    async def get_fills(self, address: str) -> list[Fill]:
        try:
            payload = await self._request_json(
                "POST", INFO_URL, json_body={"type": "userFills", "user": address.lower()}
            )
        except aiohttp.ClientError as exc:
            raise GatewayError(f"userFills request failed for {address}: {exc}") from exc
        return parse_fills(payload)

    async def get_fills_since(self, address: str, start: datetime) -> list[Fill]:
        # userFillsByTime is inclusive on startTime (ms); the pass passes the ms
        # just past its checkpoint so a fill is never re-folded (issue #11).
        start_ms = int(start.timestamp() * 1000)
        try:
            payload = await self._request_json(
                "POST",
                INFO_URL,
                json_body={
                    "type": "userFillsByTime",
                    "user": address.lower(),
                    "startTime": start_ms,
                },
            )
        except aiohttp.ClientError as exc:
            raise GatewayError(f"userFillsByTime request failed for {address}: {exc}") from exc
        return parse_fills(payload)

    async def get_open_positions(self, address: str, dex: str | None = None) -> list[Position]:
        body: dict[str, str] = {"type": "clearinghouseState", "user": address.lower()}
        if dex is not None:
            body["dex"] = dex  # a HIP-3 builder-deployed perp DEX (e.g. "xyz", issue #21)
        try:
            payload = await self._request_json("POST", INFO_URL, json_body=body)
        except aiohttp.ClientError as exc:
            raise GatewayError(f"clearinghouseState request failed for {address}: {exc}") from exc
        return parse_positions(payload, dex)

    async def _request_json(
        self, method: str, url: str, *, json_body: dict[str, Any] | None = None
    ) -> Any:
        """One request with 429 backoff-and-retry; other failures raise untouched
        (aiohttp errors, wrapped per-endpoint by the callers)."""
        for attempt in range(RATE_LIMIT_MAX_TRIES):
            async with self._session.request(
                method, url, json=json_body, timeout=REQUEST_TIMEOUT
            ) as response:
                if response.status != 429:
                    response.raise_for_status()
                    return await response.json()
                delay = _parse_retry_after(response.headers.get("Retry-After"))
                if delay is None:
                    delay = self._backoff_delay(attempt)
            if attempt + 1 < RATE_LIMIT_MAX_TRIES:
                log.warning("429 from %s: backing off %.1fs (try %d)", url, delay, attempt + 1)
                await self._clock.sleep(delay)
        raise RateLimitedError(f"still 429 from {url} after {RATE_LIMIT_MAX_TRIES} tries")

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential window with equal jitter: 50–100% of base * 2^attempt."""
        window = min(RATE_LIMIT_BACKOFF_CAP_SECONDS, RATE_LIMIT_BACKOFF_BASE_SECONDS * 2.0**attempt)
        return window * (0.5 + 0.5 * self._rng())


def _parse_retry_after(value: str | None) -> float | None:
    """Retry-After as delta-seconds; the HTTP-date form (or garbage) falls back
    to our own backoff rather than trusting a parse of the server's clock."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return max(0.0, seconds)


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
                    # marginUsed / returnOnEquity ride the same call (issue #35);
                    # absent or null falls back to notional/leverage in Position.
                    margin_used=_opt_decimal(raw.get("marginUsed")),
                    return_on_equity=_opt_decimal(raw.get("returnOnEquity")),
                )
            )
        return positions
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise GatewayError(f"unexpected clearinghouseState payload shape: {exc!r}") from exc


def _opt_decimal(value: Any) -> Decimal | None:
    """A Decimal for a present numeric field, None when the API omits it or
    sends null (e.g. marginUsed on an exotic position) — the caller derives a
    fallback (issue #35)."""
    return None if value is None else Decimal(value)


def _namespaced_coin(coin: str, dex: str | None) -> str:
    """Builder-DEX coins are `dex:COIN` (e.g. `xyz:META`) so they never collide
    with core coins in the (trader, coin) snapshot key (issue #21). The live API
    already returns them namespaced; prefixing is idempotent, so this stays
    correct if that ever changes."""
    if dex is None or coin.startswith(f"{dex}:"):
        return coin
    return f"{dex}:{coin}"
