"""Bot vetting: flags Traders whose profile is market-making, not copyable skill.

A Bot (CONTEXT.md) stays in the database but never reaches a screener result.
Heuristics and thresholds come from the ansem-bullpen vetting R&D, calibrated
against its real exclusions and the 15 vetted wallets (docs/metrics.md), and
re-based on round-trip trades (issue #58): every vetted human still clears
each threshold with wide margin (their round-trip maxima: 57 trades, ~2.5
trades/day; perfect win rates appear only over tiny samples the min-exits
guard forgives)."""

from decimal import Decimal

from epigone.metrics.fine import FineMetrics

# ~100% win rate sustained over many exits means losses are never realized —
# the 0x8af700ba exclusion ran 100% over 637 exits. Exits are completed
# round-trips now (#58): a market-maker cycles flat constantly, so its perfect
# exits stay numerous, while no vetted human tops 57 round-trips in view.
BOT_WIN_RATE = Decimal("0.98")
BOT_WIN_RATE_MIN_EXITS = 100

# Exit cadence no human sustains: the excluded market-makers cycled flat ~440
# times a day; the busiest vetted human completes ~2.5 round-trips per day.
BOT_EXITS_PER_DAY = 200

# Big monthly PnL with almost no trading activity: the money is made by
# holding, not trading — nothing there to copy. Activity is judged by fills
# seen, not completed round-trips (#58): a long-hold human whose opens predate
# our fill window shows few round-trips yet 1600+ fills across the vetted
# wallets, while the excluded $13M/month whale (a handful of closing orders)
# barely filled at all.
STATIC_HOLDINGS_MAX_FILLS = 50
STATIC_HOLDINGS_MIN_MONTH_PNL = Decimal("100000")


def classify_bot(metrics: FineMetrics, month_pnl: Decimal | None) -> str | None:
    """The Bot reason for this profile, or None for a copyable Trader.
    `month_pnl` is the coarse month window (None when the coarse pass hasn't
    reached this Trader); only the static-holdings check needs it."""
    if (
        metrics.win_rate is not None
        and metrics.win_rate >= BOT_WIN_RATE
        and metrics.trade_count >= BOT_WIN_RATE_MIN_EXITS
    ):
        return f"{metrics.win_rate:.0%} win rate over {metrics.trade_count} exits"
    if metrics.window_start is not None and metrics.window_end is not None:
        span_days = (metrics.window_end - metrics.window_start).total_seconds() / 86_400
        exits_per_day = metrics.trade_count / max(span_days, 1)  # a burst is judged per day
        if exits_per_day >= BOT_EXITS_PER_DAY:
            return f"{exits_per_day:.0f} exits per day"
    if (
        month_pnl is not None
        and abs(month_pnl) >= STATIC_HOLDINGS_MIN_MONTH_PNL
        and metrics.perp_fill_count <= STATIC_HOLDINGS_MAX_FILLS
    ):
        return f"${month_pnl:,.0f} month PnL from static holdings ({metrics.perp_fill_count} fills)"
    return None
