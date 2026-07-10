"""Fine metric pass (issue #8, two-stage scan stage 2).

One userFills call per eligible Trader: coarse-pass survivors (profitable,
active month — the default gate, tunable as thresholds firm up) plus every
tracked Trader. Metrics come from the pure engine (epigone.metrics.fine);
Bot vetting (epigone.metrics.bots) runs on the same fetch. Structure mirrors
the coarse pass: per-Trader commits, stale-first order, failure-streak abort.
Rate limiting is the exception (issue #28): a RateLimitedError counts as a
failure but never toward the abort streak — the gateway already backed off,
and a 429 under load is pacing, not an outage.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, RateLimitedError
from epigone.ingest.scan import (
    ACTIVE_REFRESH_INTERVAL,
    DORMANT_REFRESH_INTERVAL,
    MAX_CONSECUTIVE_FAILURES,
)
from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import FineMetrics, compute_fine_metrics

log = logging.getLogger(__name__)

FILLS_WEIGHT = 20  # one userFills call, against the shared budget (epigone.budget)


@dataclass(frozen=True)
class FineScanResult:
    refreshed: int
    failed: int
    aborted: bool


@dataclass(frozen=True)
class _DueTrader:
    address: str
    account_value: Decimal | None  # from the coarse month window, when scanned
    month_pnl: Decimal | None


async def run_fine_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> FineScanResult:
    """One fills call per due eligible Trader, stale-first, paced by the budget."""
    due = await _due_traders(pool, clock.now())
    refreshed = failed = consecutive_failures = 0
    for trader in due:
        await budget.spend(FILLS_WEIGHT)
        try:
            fills = await gateway.get_fills(trader.address)
        except RateLimitedError:
            # Pacing, not an outage (issue #28): the gateway already backed off
            # and retried, so just rotate the Trader to the back and move on —
            # a 429 streak must never abort the pass.
            log.warning("fine pass: rate limited fetching fills for %s", trader.address)
            await _stamp_attempt(pool, trader.address, clock.now())
            failed += 1
            continue
        except GatewayError:
            log.warning("fine pass: fills fetch failed for %s", trader.address, exc_info=True)
            await _stamp_attempt(pool, trader.address, clock.now())
            failed += 1
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error(
                    "fine pass aborted after %d consecutive failures; "
                    "%d refreshed so far, resuming next cycle",
                    consecutive_failures,
                    refreshed,
                )
                return FineScanResult(refreshed=refreshed, failed=failed, aborted=True)
            continue
        consecutive_failures = 0
        metrics = compute_fine_metrics(fills, account_value=trader.account_value)
        bot_reason = classify_bot(metrics, month_pnl=trader.month_pnl)
        await _store_fine_metrics(pool, trader.address, metrics, bot_reason, clock.now())
        refreshed += 1
    if refreshed or failed:
        log.info("fine pass done: %d refreshed, %d failed", refreshed, failed)
    return FineScanResult(refreshed=refreshed, failed=failed, aborted=False)


async def _stamp_attempt(pool: asyncpg.Pool, address: str, now: datetime) -> None:
    await pool.execute("UPDATE traders SET fine_attempted_at = $2 WHERE address = $1", address, now)


async def _due_traders(pool: asyncpg.Pool, now: datetime) -> list[_DueTrader]:
    # Same rotation as the coarse pass: least-recently-attempted first.
    # Tracked Traders may predate their first coarse scan (NULL tier/coarse
    # row); they refresh on the active cadence.
    rows = await pool.fetch(
        """
        SELECT t.address, cm.account_value, cm.pnl AS month_pnl
        FROM traders t
        LEFT JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = 'month'
        WHERE (
            EXISTS (SELECT 1 FROM tracks WHERE trader_address = t.address)
            OR (cm.pnl > 0 AND cm.volume > 0)
        )
        AND (
            t.fine_refreshed_at IS NULL
            OR t.fine_refreshed_at <=
                CASE WHEN t.refresh_tier = 'dormant'
                     THEN $2::timestamptz ELSE $1::timestamptz END
        )
        ORDER BY t.fine_attempted_at ASC NULLS FIRST, t.address
        """,
        now - ACTIVE_REFRESH_INTERVAL,
        now - DORMANT_REFRESH_INTERVAL,
    )
    return [
        _DueTrader(
            address=row["address"],
            account_value=row["account_value"],
            month_pnl=row["month_pnl"],
        )
        for row in rows
    ]


async def _store_fine_metrics(
    pool: asyncpg.Pool,
    address: str,
    metrics: FineMetrics,
    bot_reason: str | None,
    computed_at: datetime,
) -> None:
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            INSERT INTO fine_metrics
                (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
                 avg_leverage, maker_share, realized_pnl, window_start, window_end, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (address) DO UPDATE
                SET trade_count = EXCLUDED.trade_count,
                    win_rate = EXCLUDED.win_rate,
                    avg_win = EXCLUDED.avg_win,
                    avg_loss = EXCLUDED.avg_loss,
                    sharpe = EXCLUDED.sharpe,
                    max_drawdown = EXCLUDED.max_drawdown,
                    avg_leverage = EXCLUDED.avg_leverage,
                    maker_share = EXCLUDED.maker_share,
                    realized_pnl = EXCLUDED.realized_pnl,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    computed_at = EXCLUDED.computed_at
            """,
            address,
            metrics.trade_count,
            metrics.win_rate,
            metrics.avg_win,
            metrics.avg_loss,
            metrics.sharpe,
            metrics.max_drawdown,
            metrics.avg_leverage,
            metrics.maker_share,
            metrics.realized_pnl,
            metrics.window_start,
            metrics.window_end,
            computed_at,
        )
        # A flag keeps its original timestamp while the reason stays fresh; a
        # profile that stops matching the heuristics returns to the screener.
        await conn.execute(
            """
            UPDATE traders
            SET fine_refreshed_at = $2,
                fine_attempted_at = $2,
                bot_flagged_at = CASE
                    WHEN $3::text IS NULL THEN NULL
                    ELSE coalesce(bot_flagged_at, $2)
                END,
                bot_reason = $3
            WHERE address = $1
            """,
            address,
            computed_at,
            bot_reason,
        )
