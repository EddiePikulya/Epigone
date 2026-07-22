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
from epigone.first_data_notice import mark_first_data_ready
from epigone.gateway import FILL_ENDPOINTS, GatewayError, HyperliquidGateway, RateLimitedError
from epigone.ingest.scan import (
    ACTIVE_REFRESH_INTERVAL,
    DORMANT_REFRESH_INTERVAL,
    MAX_CONSECUTIVE_FAILURES,
)
from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import (
    EMPTY_STATE,
    FineMetrics,
    FineState,
    OpenEpisode,
    RoundTrip,
    extract_state,
    fold_states,
    metrics_from_state,
)

log = logging.getLogger(__name__)

# One fill fetch hits FILL_ENDPOINTS info endpoints (userFills plus
# userTwapSliceFills, issue #63) at base weight 20 each, against the shared
# budget (epigone.budget) — billed per endpoint so the accounting can never
# drift from the calls the gateway actually makes.
FILLS_WEIGHT = 20 * FILL_ENDPOINTS

# The incremental pass fetches fills strictly after the checkpoint. userFillsByTime
# is inclusive on startTime, so start one fill-timestamp tick (1ms, the API's
# resolution) past the last folded fill — never re-folding it (issue #11).
CHECKPOINT_STEP = timedelta(milliseconds=1)

# A fills endpoint really costs its base 20 *plus* weight per 20 fills
# returned (Hyperliquid rate-limit docs; issue #41) — up to ~+100 on a full
# ~2000-fill response. The surcharge is only known once the response arrives,
# so the pass settles it post-hoc against the budget; billing it flat was why
# "under budget" load still tripped a steady trickle of 429s. The gateway
# hands back the two endpoints' responses already merged, so the per-endpoint
# split is unknown: the settle bills the worst-case split (each endpoint's
# ceil can round up once), staying conservative rather than under-billing by
# up to FILL_ENDPOINTS − 1. Recalibrate if production metering disagrees.
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
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    clock: Clock,
    *,
    chunk_size: int | None = None,
) -> FineScanResult:
    """One fills call per due eligible Trader, stale-first, paced by the budget.

    `chunk_size` bounds the pass to the leading `chunk_size` of the due queue and
    returns, so `ingest_loop` re-seeds and re-reads the queue between chunks
    (issue #66) — the failure-streak abort is scoped to the chunk (it resets
    per pass) and a persistent storm still surfaces via the success-starvation
    check (#61). `None` (the default) is the pre-chunking whole-queue pass; a
    chunk at least the due count is likewise one full pass, unchanged."""
    due = await _due_traders(pool, clock.now(), chunk_size)
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
        if fetched:
            surcharge = (
                math.ceil(len(fetched) / FILLS_PER_SURCHARGE_WEIGHT) + FILL_ENDPOINTS - 1
            )
            await budget.settle(surcharge)
        prior_state = prior or EMPTY_STATE
        state = fold_states(prior_state, extract_state(fetched))
        metrics = metrics_from_state(state, account_value=trader.account_value)
        bot_reason = classify_bot(metrics, month_pnl=trader.month_pnl)
        # The fold can complete a round-trip the batch alone couldn't (a close
        # resolving a carried open episode), so the incremental upsert is "new
        # since the prior state", not the batch's own trades.
        prior_keys = {(t.coin, t.closed_at, t.seq) for t in prior_state.round_trips}
        new_trips = tuple(
            t for t in state.round_trips if (t.coin, t.closed_at, t.seq) not in prior_keys
        )
        await _store_fine_refresh(
            pool, trader.address, metrics, state, new_trips, bot_reason, clock.now(),
            reseed=full_pull,
        )
        refreshed += 1
    if refreshed or failed:
        log.info("fine pass done: %d refreshed, %d failed", refreshed, failed)
    return FineScanResult(refreshed=refreshed, failed=failed, aborted=False)


async def _stamp_attempt(pool: asyncpg.Pool, address: str, now: datetime) -> None:
    await pool.execute("UPDATE traders SET fine_attempted_at = $2 WHERE address = $1", address, now)


# A Follow marks its wallet due-now for an immediate fine refresh (issue #82) —
# unless the fine data was refreshed this recently. The floor is anti-spam (a
# follow→unfollow→follow loop can't force pointless refresh spend) and a
# guard against redundant work (a wallet another User already tracks is on the
# active cadence, freshly scanned, so already current).
FOLLOW_REFRESH_FRESHNESS = timedelta(minutes=15)


async def mark_due_on_follow(
    executor: asyncpg.Pool | asyncpg.Connection, address: str, now: datetime
) -> bool:
    """Bump `address` to the front of the fine-refresh queue on a Follow, unless
    its fine data is still fresh. Clears fine_refreshed_at (making it due per
    DUE_ELIGIBILITY) and fine_attempted_at (sorting it first per `_due_traders`'
    ORDER BY), so the chunked, tracked-first pass (#65/#66) refreshes it within
    minutes — no restart, no Hyperliquid call here (ADR-0002: processes meet in
    Postgres). Skips a wallet refreshed within FOLLOW_REFRESH_FRESHNESS; returns
    whether it bumped."""
    status = await executor.execute(
        """
        UPDATE traders
        SET fine_refreshed_at = NULL, fine_attempted_at = NULL
        WHERE address = $1
          AND (fine_refreshed_at IS NULL OR fine_refreshed_at <= $2)
        """,
        address,
        now - FOLLOW_REFRESH_FRESHNESS,
    )
    return bool(status != "UPDATE 0")


# Whether Trader `t` is tracked by a User — single-sourced because it gates both
# eligibility (below) and due-queue priority (issue #65's ORDER BY), which must
# stay in lockstep on what "tracked" means.
IS_TRACKED = "EXISTS (SELECT 1 FROM tracks WHERE trader_address = t.address)"

# The fine-pass eligibility predicate, single-sourced so the health monitor
# (issue #52) can count *due* Traders with the exact same rule the pass rotates
# on — "idle because caught up" must read as healthy, not as a stuck pass. Reads
# `traders t LEFT JOIN coarse_metrics cm (month)`; $1 = active cutoff, $2 =
# dormant cutoff. Tracked Traders may predate their first coarse scan (NULL
# tier/coarse row); they refresh on the active cadence.
DUE_ELIGIBILITY = f"""
    (
        {IS_TRACKED}
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


async def _due_traders(
    pool: asyncpg.Pool, now: datetime, chunk_size: int | None
) -> list[_DueTrader]:
    # Drain a backlog best-first without breaking rotation fairness (issue #65):
    # tracked Traders lead (formalizing the post-wipe hand-seed), then least-
    # recently-attempted (rotation still dominates — a low-PnL wallet's timestamp
    # keeps aging until it reaches the front, so nothing starves), and coarse
    # month PnL only tiebreaks among equals — in practice the never-attempted
    # pile, whose NULL timestamps would otherwise drain in address-hex order.
    # PnL over ROI deliberately: ROI crowns tiny lucky accounts.
    #
    # `chunk_size` caps the queue to that most-due prefix (issue #66); Postgres
    # treats LIMIT NULL as no limit, so `None` returns the whole due list.
    rows = await pool.fetch(
        f"""
        SELECT t.address, t.fine_checkpoint_at, cm.account_value, cm.pnl AS month_pnl
        FROM traders t
        LEFT JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = 'month'
        WHERE {DUE_ELIGIBILITY}
        ORDER BY
            {IS_TRACKED} DESC,
            t.fine_attempted_at ASC NULLS FIRST,
            cm.pnl DESC NULLS LAST,
            t.address
        LIMIT $3
        """,
        now - ACTIVE_REFRESH_INTERVAL,
        now - DORMANT_REFRESH_INTERVAL,
        chunk_size,
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
    #11): the persisted round-trips plus the maker/perp/realized accumulators
    and the fill window, with `checkpoint` (fine_checkpoint_at) as
    `last_fill_at`."""
    counters = await pool.fetchrow(
        "SELECT maker_fill_count, perp_fill_count, realized_pnl, window_start, window_end "
        "FROM fine_metrics WHERE address = $1",
        address,
    )
    trade_rows = await pool.fetch(
        "SELECT coin, pnl, peak_notional, opened_at, closed_at, seq "
        "FROM fine_trades WHERE address = $1 ORDER BY closed_at, coin, seq",
        address,
    )
    trades = tuple(
        RoundTrip(
            coin=r["coin"],
            pnl=r["pnl"],
            peak_notional=r["peak_notional"],
            opened_at=r["opened_at"],
            closed_at=r["closed_at"],
            seq=r["seq"],
        )
        for r in trade_rows
    )
    # The open episodes (issues #48/#58): each coin held non-flat at the
    # checkpoint, with the net PnL and peak notional its trims have realized so
    # far, so a close arriving in a later batch completes the whole round-trip —
    # plus the walked net position the next batch's continuity guard verifies
    # against (issue #63).
    episode_rows = await pool.fetch(
        "SELECT coin, opened_at, pnl, peak_notional, net_position "
        "FROM fine_open_episodes WHERE address = $1",
        address,
    )
    return FineState(
        round_trips=trades,
        maker_fill_count=counters["maker_fill_count"] if counters else 0,
        perp_fill_count=counters["perp_fill_count"] if counters else 0,
        realized_pnl=counters["realized_pnl"] if counters else Decimal(0),
        window_start=counters["window_start"] if counters else None,
        window_end=counters["window_end"] if counters else None,
        last_fill_at=checkpoint,
        open_episodes=tuple(
            OpenEpisode(
                coin=r["coin"],
                opened_at=r["opened_at"],
                pnl=r["pnl"],
                peak_notional=r["peak_notional"],
                net_position=r["net_position"],
            )
            for r in episode_rows
        ),
    )


async def _store_fine_refresh(
    pool: asyncpg.Pool,
    address: str,
    metrics: FineMetrics,
    state: FineState,
    new_trips: tuple[RoundTrip, ...],
    bot_reason: str | None,
    computed_at: datetime,
    *,
    reseed: bool,
) -> None:
    """Persist a refresh: the reduced metrics plus the fold state that feeds the
    next incremental pass. `reseed` (a full pull, checkpoint was NULL) rebuilds
    the trade store from scratch; otherwise only the round-trips new since the
    prior state (`new_trips`) are upserted, keeping a fast-tier refresh a small
    write (issue #11)."""
    async with pool.acquire() as conn, conn.transaction():
        if reseed:
            await conn.execute("DELETE FROM fine_trades WHERE address = $1", address)
        # The open episodes are the whole current set (a Trader holds few coins),
        # so rewrite them wholesale rather than diff (issue #48).
        await conn.execute("DELETE FROM fine_open_episodes WHERE address = $1", address)
        if state.open_episodes:
            await conn.executemany(
                "INSERT INTO fine_open_episodes "
                "(address, coin, opened_at, pnl, peak_notional, net_position) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                [
                    (address, e.coin, e.opened_at, e.pnl, e.peak_notional, e.net_position)
                    for e in state.open_episodes
                ],
            )
        if new_trips:
            await conn.executemany(
                """
                INSERT INTO fine_trades
                    (address, coin, pnl, peak_notional, opened_at, closed_at, seq)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (address, coin, closed_at, seq) DO UPDATE
                    SET pnl = EXCLUDED.pnl,
                        peak_notional = EXCLUDED.peak_notional,
                        opened_at = EXCLUDED.opened_at
                """,
                [
                    (address, t.coin, t.pnl, t.peak_notional, t.opened_at, t.closed_at, t.seq)
                    for t in new_trips
                ],
            )
        await conn.execute(
            """
            INSERT INTO fine_metrics
                (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
                 avg_leverage, maker_share, avg_hold_seconds, effective_coins, realized_pnl,
                 window_start, window_end, maker_fill_count, perp_fill_count, computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
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
                    effective_coins = EXCLUDED.effective_coins,
                    realized_pnl = EXCLUDED.realized_pnl,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    maker_fill_count = EXCLUDED.maker_fill_count,
                    perp_fill_count = EXCLUDED.perp_fill_count,
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
            metrics.effective_coins,
            metrics.realized_pnl,
            metrics.window_start,
            metrics.window_end,
            state.maker_fill_count,
            state.perp_fill_count,
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
        # First real fine data for this wallet has now landed: queue the one-time
        # notice (issue #83) for any tracker still waiting on it. Gated on the
        # wallet actually having fills (last_fill_at, == fine_checkpoint_at) —
        # an empty scan writes a fine_metrics row but isn't "full track-record
        # data", so it leaves waiting trackers 'pending' until real fills arrive.
        # In the same transaction as the metrics write so "data landed" and
        # "notices queued" commit together (restart-safe); a no-op for a wallet
        # with no waiting trackers, which is every routine refresh.
        if state.last_fill_at is not None:
            await mark_first_data_ready(conn, address)
