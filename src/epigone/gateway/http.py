"""The production HyperliquidGateway: direct Hyperliquid endpoints (ADR-0001).

The undocumented stats-data leaderboard is quarantined to Universe seeding —
every failure surfaces as GatewayError so callers can degrade gracefully. The
single leaderboard download also carries the coarse Metric Library, so no
per-account call feeds coarse (issue #26). `clearinghouseState` costs weight 2;
each fills endpoint weight 20 plus weight per 20 fills returned (issue #41),
against the shared weight budget (epigone.budget) — and a fill fetch hits
FILL_ENDPOINTS of them (userFills plus userTwapSliceFills, issue #63), so
callers bill one base weight per endpoint and settle the response-size
surcharge once they see the payload.

A 429 backs off and retries here rather than surfacing (issue #28): sleep the
server's Retry-After when present, else an exponential window with jitter.
Only a persistent streak escapes, as RateLimitedError, which callers treat as
pacing — retry the item later, never abort a whole pass over it.
"""

import heapq
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
        # userFills and userTwapSliceFills both serve newest-first, and
        # same-order / same-block fills all share one millisecond timestamp —
        # array position is the only within-ms execution-order signal, and the
        # round-trip engine (#58) mis-reconstructs positions without it.
        # Reverse each whole response to the protocol's execution order
        # (verified live against the position-continuity invariant `end ==
        # next start`: ~0 violations reversed vs ~100% as served for userFills
        # and ~60% for the TWAP endpoint). The ByTime variants differ — see
        # get_fills_since.
        regular = list(reversed(parse_fills(await self._info_json("userFills", address))))
        twap = list(
            reversed(parse_twap_fills(await self._info_json("userTwapSliceFills", address)))
        )
        return _merge_execution_order(regular, twap)

    async def get_fills_since(self, address: str, start: datetime) -> list[Fill]:
        # Both ByTime endpoints are inclusive on startTime (ms); the pass sends
        # the ms just past its checkpoint so no fill from either source is ever
        # re-folded (issue #11). Unlike their full-history counterparts, they
        # serve OLDEST-first — already the protocol's execution order,
        # within-ms included (verified live: ~0 continuity violations as
        # served, ~100% reversed). Served order is kept; reversing here would
        # corrupt the incremental path.
        start_ms = int(start.timestamp() * 1000)
        regular = parse_fills(
            await self._info_json("userFillsByTime", address, startTime=start_ms)
        )
        twap = parse_twap_fills(
            await self._info_json("userTwapSliceFillsByTime", address, startTime=start_ms)
        )
        return _merge_execution_order(regular, twap)

    async def _info_json(self, request_type: str, address: str, **extra: Any) -> Any:
        try:
            return await self._request_json(
                "POST",
                INFO_URL,
                json_body={"type": request_type, "user": address.lower(), **extra},
            )
        except aiohttp.ClientError as exc:
            raise GatewayError(f"{request_type} request failed for {address}: {exc}") from exc

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


def _merge_execution_order(regular: list[Fill], twap: list[Fill]) -> list[Fill]:
    """Merge two already-execution-ordered fill streams into one by timestamp.

    The merge is stable — a within-ms tie across the two sources keeps regular
    fills first. No signal orders a regular fill against a TWAP slice inside
    one millisecond (they live in separate arrays); if that guess is ever
    wrong, the engine's startPosition continuity guard (#63) demotes the
    affected episode rather than let it corrupt the round-trip metrics."""
    return list(heapq.merge(regular, twap, key=lambda f: f.time))


def parse_fills(payload: Any) -> list[Fill]:
    """Map a userFills/userFillsByTime payload to Fills, PRESERVING the array
    order — the payload's order is the only within-millisecond execution-order
    signal (same-order fills share a timestamp), and the two endpoints serve
    opposite directions, so each caller normalizes to execution order itself
    (see get_fills / get_fills_since)."""
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


def parse_twap_fills(payload: Any) -> list[Fill]:
    """Map a userTwapSliceFills(ByTime) payload to Fills, preserving array
    order like parse_fills. The TWAP endpoints nest each execution as
    {"fill": {...}, "twapId": N}; the inner object is a regular fill row."""
    try:
        nested = [item["fill"] for item in payload]
    except (KeyError, TypeError) as exc:
        raise GatewayError(f"unexpected userTwapSliceFills payload shape: {exc!r}") from exc
    return parse_fills(nested)


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
