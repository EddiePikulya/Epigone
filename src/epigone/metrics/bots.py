"""Bot vetting: flags Traders whose profile is market-making, not copyable skill.

A Bot (CONTEXT.md) stays in the database but never reaches a screener result.
Heuristics and thresholds come from the ansem-bullpen vetting R&D, calibrated
against its real exclusions and the 15 vetted wallets (docs/metrics.md):
every vetted human clears each threshold with wide margin (max observed:
95.6% win rate, 25 exits/day)."""

from decimal import Decimal

from epigone.metrics.fine import FineMetrics

# ~100% win rate sustained over many exits means losses are never realized —
# the 0x8af700ba exclusion ran 100% over 637 exits.
BOT_WIN_RATE = Decimal("0.98")
BOT_WIN_RATE_MIN_EXITS = 100

# Exit cadence no human sustains: the excluded market-makers ran ~440/day.
BOT_EXITS_PER_DAY = 200

# Big monthly PnL with almost no closed trades: the money is made by holding,
# not trading — nothing there to copy.
STATIC_HOLDINGS_MAX_TRADES = 5
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
        and metrics.trade_count <= STATIC_HOLDINGS_MAX_TRADES
    ):
        return f"${month_pnl:,.0f} month PnL from static holdings ({metrics.trade_count} exits)"
    return None
