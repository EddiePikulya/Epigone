"""Fine metric pass (issue #8, two-stage scan stage 2).

One fills call per eligible Trader: coarse-pass survivors (profitable, active
month — the default gate, tunable as thresholds firm up) plus every tracked
Trader. The first refresh pulls the account's full fill history; every later
one is **incremental** (issue #11) — it fetches only the fills since the
Trader's checkpoint (fine_checkpoint_at) and folds them into the persisted
trade store (epigone.metrics.fine, fine_trades), so a fast-tier refresh is
cheap and history accumulates past the ~2000-fill API cap. Metrics come from
the pure engine reducing the folded state; Bot vetting (epigone.metrics.bots)
runs on it too. Structure mirrors the coarse pass: per-Trader commits,
stale-first order, failure-streak abort. Rate limiting is the exception (issue
#28): a RateLimitedError counts as a failure but never toward the abort streak
— the gateway already backed off, and a 429 under load is pacing, not an outage.
"""

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg

from epigone.budget import Budget, record_rate_limit
from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, RateLimitedError
from epigone.ingest.scan import (
    ACTIVE_REFRESH_INTERVAL,
    DORMANT_REFRESH_INTERVAL,
    MAX_CONSECUTIVE_FAILURES,
)
from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import (
    EMPTY_STATE,
    ClosedTrade,
    FineMetrics,
    FineState,
    extract_state,
    fold_states,
    metrics_from_state,
)

log = logging.getLogger(__name__)

FILLS_WEIGHT = 20  # one userFills call, against the shared budget (epigone.budget)

# The incremental pass fetches fills strictly after the checkpoint. userFillsByTime
# is inclusive on startTime, so start one fill-timestamp tick (1ms, the API's
# resolution) past the last folded fill — never re-folding it (issue #11).
CHECKPOINT_STEP = timedelta(milliseconds=1)

# userFills really costs its base 20 *plus* weight per 20 fills returned
# (Hyperliquid rate-limit docs; issue #41) — up to ~+100 on a full ~2000-fill
# response. The surcharge is only known once the response arrives, so the pass
# settles it post-hoc against the budget; billing it flat at 20 was why "under
# budget" load still tripped a steady trickle of 429s. Ceil is the conservative
# read of "per 20 items"; recalibrate here if production metering disagrees.
FILLS_PER_SURCHARGE_WEIGHT = 20


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
    checkpoint: datetime | None  # fine_checkpoint_at: None means "never folded, full pull"


async def run_fine_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> FineScanResult:
    """One fills call per due eligible Trader, stale-first, paced by the budget."""
    due = await _due_traders(pool, clock.now())
    refreshed = failed = consecutive_failures = 0
    for trader in due:
        checkpoint = trader.checkpoint
        full_pull = checkpoint is None  # no checkpoint yet: seed from a full history
        # Load the prior fold state before spending, so a fetch failure costs
        # only the base weight (no wasted read); the load is a cheap DB read.
        prior = None if full_pull else await _load_fine_state(pool, trader.address, checkpoint)
        await budget.spend(FILLS_WEIGHT)
        try:
            if checkpoint is None:
                fetched = await gateway.get_fills(trader.address)
            else:
                fetched = await gateway.get_fills_since(
                    trader.address, checkpoint + CHECKPOINT_STEP
                )
        except RateLimitedError:
            # Pacing, not an outage (issue #28): the gateway already backed off
            # and retried, so just rotate the Trader to the back and move on —
            # a 429 streak must never abort the pass.
            log.warning("fine pass: rate limited fetching fills for %s", trader.address)
            await _stamp_attempt(pool, trader.address, clock.now())
            # Surface the sustained-limiting signal for the health monitor (#54):
            # this 429 streak survived the gateway's backoff, so it is real
            # limiting worth alerting on, not the normal single-429 pacing.
            await record_rate_limit(pool, clock.now())
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
        # The surcharge bills only the fills actually returned — an incremental
        # pull of a few new fills settles far less than a full ~2000-fill pull.
        surcharge = math.ceil(len(fetched) / FILLS_PER_SURCHARGE_WEIGHT)
        if surcharge:
            await budget.settle(surcharge)
        delta = extract_state(fetched)  # reduce the batch once; reuse for fold + upsert
        state = fold_states(prior or EMPTY_STATE, delta)
        metrics = metrics_from_state(state, account_value=trader.account_value)
        bot_reason = classify_bot(metrics, month_pnl=trader.month_pnl)
        await _store_fine_refresh(
            pool, trader.address, metrics, state, delta.trades, bot_reason, clock.now(),
            reseed=full_pull,
        )
        refreshed += 1
    if refreshed or failed:
        log.info("fine pass done: %d refreshed, %d failed", refreshed, failed)
    return FineScanResult(refreshed=refreshed, failed=failed, aborted=False)


async def _stamp_attempt(pool: asyncpg.Pool, address: str, now: datetime) -> None:
    await pool.execute("UPDATE traders SET fine_attempted_at = $2 WHERE address = $1", address, now)


# The fine-pass eligibility predicate, single-sourced so the health monitor
# (issue #52) can count *due* Traders with the exact same rule the pass rotates
# on — "idle because caught up" must read as healthy, not as a stuck pass. Reads
# `traders t LEFT JOIN coarse_metrics cm (month)`; $1 = active cutoff, $2 =
# dormant cutoff. Tracked Traders may predate their first coarse scan (NULL
# tier/coarse row); they refresh on the active cadence.
DUE_ELIGIBILITY = """
    (
        EXISTS (SELECT 1 FROM tracks WHERE trader_address = t.address)
        OR (cm.pnl > 0 AND cm.volume > 0)
    )
    AND (
        t.fine_refreshed_at IS NULL
        OR t.fine_refreshed_at <=
            CASE WHEN t.refresh_tier = 'dormant'
                 THEN $2::timestamptz ELSE $1::timestamptz END
    )
"""


async def count_due_traders(pool: asyncpg.Pool, now: datetime) -> int:
    """How many eligible Traders are due a fine refresh right now — the same
    predicate `_due_traders` rotates on (issue #52's ingest-progress check)."""
    count = await pool.fetchval(
        f"""
        SELECT count(*)
        FROM traders t
        LEFT JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = 'month'
        WHERE {DUE_ELIGIBILITY}
        """,
        now - ACTIVE_REFRESH_INTERVAL,
        now - DORMANT_REFRESH_INTERVAL,
    )
    return int(count)


async def _due_traders(pool: asyncpg.Pool, now: datetime) -> list[_DueTrader]:
    # Same rotation as the coarse pass: least-recently-attempted first.
    rows = await pool.fetch(
        f"""
        SELECT t.address, t.fine_checkpoint_at, cm.account_value, cm.pnl AS month_pnl
        FROM traders t
        LEFT JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = 'month'
        WHERE {DUE_ELIGIBILITY}
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
            checkpoint=row["fine_checkpoint_at"],
        )
        for row in rows
    ]


async def _load_fine_state(
    pool: asyncpg.Pool, address: str, checkpoint: datetime | None
) -> FineState:
    """Rebuild the fold state from storage for an incremental refresh (issue
    #11): the persisted closed trades plus the maker/perp accumulators and the
    fill window, with `checkpoint` (fine_checkpoint_at) as `last_fill_at`."""
    counters = await pool.fetchrow(
        "SELECT maker_fill_count, perp_fill_count, window_start, window_end, "
        "hold_seconds_sum, hold_episode_count "
        "FROM fine_metrics WHERE address = $1",
        address,
    )
    trade_rows = await pool.fetch(
        "SELECT order_id, pnl, peak_notional, closed_at FROM fine_trades WHERE address = $1",
        address,
    )
    trades = tuple(
        ClosedTrade(
            order_id=r["order_id"],
            pnl=r["pnl"],
            peak_notional=r["peak_notional"],
            closed_at=r["closed_at"],
        )
        for r in trade_rows
    )
    # The open episodes (issue #48): each coin held non-flat at the checkpoint,
    # so a close arriving in the next batch resolves against its stored open-time.
    episode_rows = await pool.fetch(
        "SELECT coin, opened_at FROM fine_open_episodes WHERE address = $1", address
    )
    return FineState(
        trades=trades,
        maker_fill_count=counters["maker_fill_count"] if counters else 0,
        perp_fill_count=counters["perp_fill_count"] if counters else 0,
        window_start=counters["window_start"] if counters else None,
        window_end=counters["window_end"] if counters else None,
        last_fill_at=checkpoint,
        hold_seconds_sum=counters["hold_seconds_sum"] if counters else 0,
        hold_episode_count=counters["hold_episode_count"] if counters else 0,
        open_episodes=tuple((r["coin"], r["opened_at"]) for r in episode_rows),
    )


async def _store_fine_refresh(
    pool: asyncpg.Pool,
    address: str,
    metrics: FineMetrics,
    state: FineState,
    delta_trades: tuple[ClosedTrade, ...],
    bot_reason: str | None,
    computed_at: datetime,
    *,
    reseed: bool,
) -> None:
    """Persist a refresh: the reduced metrics plus the fold state that feeds the
    next incremental pass. `reseed` (a full pull, checkpoint was NULL) rebuilds
    the trade store from scratch; otherwise only this batch's `delta_trades` are
    upserted, keeping a fast-tier refresh a small write (issue #11)."""
    async with pool.acquire() as conn, conn.transaction():
        if reseed:
            await conn.execute("DELETE FROM fine_trades WHERE address = $1", address)
        # The open episodes are the whole current set (a Trader holds few coins),
        # so rewrite them wholesale rather than diff (issue #48).
        await conn.execute("DELETE FROM fine_open_episodes WHERE address = $1", address)
        if state.open_episodes:
            await conn.executemany(
                "INSERT INTO fine_open_episodes (address, coin, opened_at) VALUES ($1, $2, $3)",
                [(address, coin, opened_at) for coin, opened_at in state.open_episodes],
            )
        if delta_trades:
            await conn.executemany(
                """
                INSERT INTO fine_trades (address, order_id, pnl, peak_notional, closed_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (address, order_id) DO UPDATE
                    SET pnl = EXCLUDED.pnl,
                        peak_notional = EXCLUDED.peak_notional,
                        closed_at = EXCLUDED.closed_at
                """,
                [(address, t.order_id, t.pnl, t.peak_notional, t.closed_at) for t in delta_trades],
            )
        await conn.execute(
            """
            INSERT INTO fine_metrics
                (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
                 avg_leverage, maker_share, avg_hold_seconds, realized_pnl,
                 window_start, window_end, maker_fill_count, perp_fill_count,
                 hold_seconds_sum, hold_episode_count, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (address) DO UPDATE
                SET trade_count = EXCLUDED.trade_count,
                    win_rate = EXCLUDED.win_rate,
                    avg_win = EXCLUDED.avg_win,
                    avg_loss = EXCLUDED.avg_loss,
                    sharpe = EXCLUDED.sharpe,
                    max_drawdown = EXCLUDED.max_drawdown,
                    avg_leverage = EXCLUDED.avg_leverage,
                    maker_share = EXCLUDED.maker_share,
                    avg_hold_seconds = EXCLUDED.avg_hold_seconds,
                    realized_pnl = EXCLUDED.realized_pnl,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    maker_fill_count = EXCLUDED.maker_fill_count,
                    perp_fill_count = EXCLUDED.perp_fill_count,
                    hold_seconds_sum = EXCLUDED.hold_seconds_sum,
                    hold_episode_count = EXCLUDED.hold_episode_count,
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
            metrics.avg_hold_seconds,
            metrics.realized_pnl,
            metrics.window_start,
            metrics.window_end,
            state.maker_fill_count,
            state.perp_fill_count,
            state.hold_seconds_sum,
            state.hold_episode_count,
            computed_at,
        )
        # A flag keeps its original timestamp while the reason stays fresh; a
        # profile that stops matching the heuristics returns to the screener.
        # fine_checkpoint_at advances to the newest folded fill so the next pass
        # fetches only what is new (NULL only when the Trader had no fills at all,
        # which keeps the next pass a cheap full pull).
        await conn.execute(
            """
            UPDATE traders
            SET fine_refreshed_at = $2,
                fine_attempted_at = $2,
                fine_checkpoint_at = $4,
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
            state.last_fill_at,
        )
