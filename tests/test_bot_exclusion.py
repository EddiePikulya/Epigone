"""Bot vetting (issue #8): a Bot is an excluded Trader whose profile is
market-making, not copyable skill (CONTEXT.md). Heuristics and thresholds are
calibrated so every real excluded account from the ansem-bullpen R&D is caught
while all 15 vetted wallets pass (test_golden_wallets.py proves the latter).
Exit counts are completed round-trips (issue #58); the static-holdings check
judges activity by fills seen, so a long-hold human with few visible
round-trips is never mistaken for a holdings whale.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import FineMetrics

T0 = datetime(2026, 7, 1, tzinfo=UTC)


def metrics(
    trade_count: int = 50,
    win_rate: str | None = "0.7",
    days: int = 14,
    realized_pnl: str = "10000",
    perp_fill_count: int = 2000,
) -> FineMetrics:
    return FineMetrics(
        trade_count=trade_count,
        win_rate=Decimal(win_rate) if win_rate is not None else None,
        avg_win=Decimal("100"),
        avg_loss=Decimal("50"),
        sharpe=Decimal("2"),
        max_drawdown=Decimal("500"),
        avg_leverage=Decimal("3"),
        maker_share=Decimal("0.5"),
        avg_hold_seconds=3600,
        realized_pnl=Decimal(realized_pnl),
        perp_fill_count=perp_fill_count,
        window_start=T0,
        window_end=T0 + timedelta(days=days),
    )


def test_a_healthy_profile_is_not_a_bot() -> None:
    assert classify_bot(metrics(), month_pnl=Decimal("50000")) is None


def test_near_perfect_win_rate_over_many_exits_is_a_bot() -> None:
    # The 0x8af700ba exclusion: 100% WR over 637 exits — never realizes a loss.
    reason = classify_bot(metrics(trade_count=637, win_rate="1"), month_pnl=None)
    assert reason is not None and "win rate" in reason


def test_near_perfect_win_rate_over_a_small_sample_is_forgiven() -> None:
    # Small perfect streaks happen to patient humans — under the round-trip
    # basis a vetted wallet runs 100% over its 5 visible round-trips.
    assert classify_bot(metrics(trade_count=45, win_rate="0.99"), month_pnl=None) is None


def test_extreme_exit_frequency_is_a_bot() -> None:
    # The 881-exits-in-2-days exclusion; the busiest vetted human completes
    # ~2.5 round-trips per day.
    reason = classify_bot(metrics(trade_count=881, win_rate="0.6", days=2), month_pnl=None)
    assert reason is not None and "exits per day" in reason


def test_a_burst_within_one_day_is_measured_against_a_full_day() -> None:
    # 100 exits in two hours is a busy afternoon, not 1200 exits/day.
    assert classify_bot(metrics(trade_count=100, days=0), month_pnl=None) is None


def test_large_month_pnl_with_almost_no_activity_is_a_static_holder() -> None:
    # The $13M/month whale whose PnL came from holdings, not trading: a handful
    # of fills in its whole visible history.
    reason = classify_bot(
        metrics(trade_count=3, win_rate="1", realized_pnl="1200", perp_fill_count=12),
        month_pnl=Decimal("13000000"),
    )
    assert reason is not None and "static holdings" in reason


def test_a_long_hold_trader_with_few_round_trips_is_not_a_static_holder() -> None:
    # The #58 regression case: a whale whose opens predate our fill window has
    # zero completed round-trips but thousands of fills — trading, not holding.
    assert (
        classify_bot(
            metrics(trade_count=0, win_rate=None, perp_fill_count=2000),
            month_pnl=Decimal("3000000"),
        )
        is None
    )


def test_low_activity_with_modest_pnl_is_just_a_quiet_trader() -> None:
    assert (
        classify_bot(
            metrics(trade_count=3, win_rate="1", perp_fill_count=12), month_pnl=Decimal("9000")
        )
        is None
    )


def test_static_holdings_check_needs_coarse_data() -> None:
    assert (
        classify_bot(metrics(trade_count=3, win_rate="1", perp_fill_count=12), month_pnl=None)
        is None
    )


def test_no_trades_and_no_month_pnl_is_not_a_bot() -> None:
    empty = FineMetrics(
        trade_count=0,
        win_rate=None,
        avg_win=None,
        avg_loss=None,
        sharpe=None,
        max_drawdown=Decimal("0"),
        avg_leverage=None,
        maker_share=None,
        avg_hold_seconds=None,
        realized_pnl=Decimal("0"),
        perp_fill_count=0,
        window_start=None,
        window_end=None,
    )
    assert classify_bot(empty, month_pnl=None) is None
