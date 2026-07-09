"""Universe seed + coarse metric pass (issue #5, two-stage scan stage 1).

Every Trader refreshed is committed independently, so a killed process resumes
from the database bookkeeping rather than restarting the scan.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg

from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, PortfolioWindow, Window
from epigone.ingest.budget import PORTFOLIO_WEIGHT, WeightBudget

log = logging.getLogger(__name__)

# spec-defaults refresh tiers: active traders hourly-daily, dormant swept weekly.
ACTIVE_REFRESH_INTERVAL = timedelta(days=1)
DORMANT_REFRESH_INTERVAL = timedelta(days=7)

# A sustained failure streak means the source is down, not that traders are odd:
# stop burning budget and let the next cycle resume from the bookkeeping.
MAX_CONSECUTIVE_FAILURES = 5


@dataclass(frozen=True)
class CoarseScanResult:
    refreshed: int
    failed: int
    aborted: bool


async def seed_universe(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> int | None:
    """Upsert the leaderboard into the Universe. Returns the entry count, or
    None when the leaderboard source failed (logged; existing Universe intact)."""
    try:
        entries = await gateway.get_leaderboard()
    except GatewayError:
        log.exception("universe seed skipped: leaderboard source failed; existing Universe intact")
        return None
    now = clock.now()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (address) DO UPDATE
                SET display_name = EXCLUDED.display_name,
                    last_seen_at = EXCLUDED.last_seen_at
            """,
            [(e.address.lower(), e.display_name, now) for e in entries],
        )
    log.info("universe seeded: %d leaderboard entries upserted", len(entries))
    return len(entries)


async def run_coarse_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: WeightBudget, clock: Clock
) -> CoarseScanResult:
    """One portfolio call per due Trader, stale-first, paced by the weight budget."""
    due = await _due_addresses(pool, clock.now())
    refreshed = failed = consecutive_failures = 0
    for address in due:
        await budget.spend(PORTFOLIO_WEIGHT)
        try:
            portfolio = await gateway.get_portfolio(address)
        except GatewayError:
            log.warning("coarse scan: portfolio fetch failed for %s", address, exc_info=True)
            failed += 1
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error(
                    "coarse scan aborted after %d consecutive failures; "
                    "%d refreshed so far, resuming next cycle",
                    consecutive_failures,
                    refreshed,
                )
                return CoarseScanResult(refreshed=refreshed, failed=failed, aborted=True)
            continue
        consecutive_failures = 0
        await _store_coarse_metrics(pool, address, portfolio, clock.now())
        refreshed += 1
    if refreshed or failed:
        log.info("coarse pass done: %d refreshed, %d failed", refreshed, failed)
    return CoarseScanResult(refreshed=refreshed, failed=failed, aborted=False)


async def _due_addresses(pool: asyncpg.Pool, now: datetime) -> list[str]:
    rows = await pool.fetch(
        """
        SELECT address FROM traders
        WHERE coarse_refreshed_at IS NULL
           OR (refresh_tier = 'active' AND coarse_refreshed_at <= $1)
           OR (refresh_tier = 'dormant' AND coarse_refreshed_at <= $2)
        ORDER BY coarse_refreshed_at ASC NULLS FIRST, address
        """,
        now - ACTIVE_REFRESH_INTERVAL,
        now - DORMANT_REFRESH_INTERVAL,
    )
    return [row["address"] for row in rows]


async def _store_coarse_metrics(
    pool: asyncpg.Pool,
    address: str,
    portfolio: dict[Window, PortfolioWindow],
    computed_at: datetime,
) -> None:
    tier = _classify_tier(portfolio)
    async with pool.acquire() as conn, conn.transaction():
        await conn.executemany(
            """
            INSERT INTO coarse_metrics
                (address, time_window, pnl, roi, volume, account_value, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (address, time_window) DO UPDATE
                SET pnl = EXCLUDED.pnl,
                    roi = EXCLUDED.roi,
                    volume = EXCLUDED.volume,
                    account_value = EXCLUDED.account_value,
                    computed_at = EXCLUDED.computed_at
            """,
            [
                (address, w.value, p.pnl, _roi(p), p.volume, p.account_value, computed_at)
                for w, p in portfolio.items()
            ],
        )
        await conn.execute(
            "UPDATE traders SET refresh_tier = $2, coarse_refreshed_at = $3 WHERE address = $1",
            address,
            tier,
            computed_at,
        )


def _roi(window: PortfolioWindow) -> Decimal:
    """Return on the stack the window started with; zero when it started empty."""
    if window.starting_account_value > 0:
        return window.pnl / window.starting_account_value
    return Decimal(0)


def _classify_tier(portfolio: dict[Window, PortfolioWindow]) -> str:
    """Traded this week -> active (daily refresh); silent -> dormant (weekly sweep)."""
    week = portfolio.get(Window.WEEK)
    if week is not None and week.volume > 0:
        return "active"
    return "dormant"
