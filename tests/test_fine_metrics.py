"""The fine-metric engine: fills in, Metric Library values out (issue #8).

Closed-trade grouping is the prior-art vetting rule (ansem-bullpen WALLETS.md):
a trade is all closing fills sharing one closing order, its PnL the sum of
their closedPnl. Golden fixtures cross-check the whole engine in
test_golden_wallets.py; here each metric earns its definition on synthetic
fills small enough to verify by hand.
"""

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from epigone.gateway import Fill
from epigone.metrics.fine import (
    FineMetrics,
    compute_fine_metrics,
    extract_state,
    fold_state,
    metrics_from_state,
)
from tests.support.fills import T0, fill


def compute(fills: list[Fill], account_value: Decimal | None = None) -> FineMetrics:
    return compute_fine_metrics(fills, account_value=account_value)


def test_closing_fills_sharing_an_order_form_one_trade() -> None:
    metrics = compute(
        [
            fill(pnl="30", order_id=7),
            fill(pnl="-10", order_id=7),  # partial fills of the same closing order
            fill(pnl="-5", order_id=8, at=T0 + timedelta(hours=1)),
        ]
    )
    assert metrics.trade_count == 2  # order 7 (+20) and order 8 (-5)
    assert metrics.win_rate == Decimal("0.5")
    assert metrics.realized_pnl == Decimal("15")


def test_opens_and_spot_fills_never_count_as_trades() -> None:
    metrics = compute(
        [
            fill("Open Long", order_id=1),
            fill("Buy", coin="PURR/USDC", order_id=2),
            fill("Spot Dust Conversion", coin="@151", order_id=3),
            fill("Sell", coin="@107", order_id=4),
        ]
    )
    assert metrics.trade_count == 0
    assert metrics.win_rate is None


def test_flips_liquidations_and_settlements_close_trades() -> None:
    metrics = compute(
        [
            fill("Long > Short", pnl="100", order_id=1),
            fill("Liquidated Isolated Long", pnl="-40", order_id=2, at=T0 + timedelta(hours=1)),
            fill("Settlement", pnl="60", order_id=3, at=T0 + timedelta(hours=2), coin="#7501"),
        ]
    )
    assert metrics.trade_count == 3
    assert metrics.win_rate == Decimal(2) / Decimal(3)


def test_a_breakeven_trade_is_not_a_win() -> None:
    metrics = compute([fill(pnl="0", order_id=1), fill(pnl="10", order_id=2)])
    assert metrics.win_rate == Decimal("0.5")


def test_avg_win_and_avg_loss_average_their_own_sides() -> None:
    metrics = compute(
        [
            fill(pnl="100", order_id=1),
            fill(pnl="50", order_id=2),
            fill(pnl="-30", order_id=3),
            fill(pnl="0", order_id=4),  # breakeven joins neither average
        ]
    )
    assert metrics.avg_win == Decimal("75")
    assert metrics.avg_loss == Decimal("30")  # reported as a positive magnitude


def test_avg_win_is_none_without_wins_and_avg_loss_none_without_losses() -> None:
    all_wins = compute([fill(pnl="10", order_id=1)])
    assert all_wins.avg_loss is None
    all_losses = compute([fill(pnl="-10", order_id=1)])
    assert all_losses.avg_win is None


def test_max_drawdown_is_the_deepest_fall_of_the_realized_pnl_curve() -> None:
    pnls = ["100", "-50", "-30", "200"]  # peak 100 -> trough 20 before recovering
    in_time_order = [
        fill(pnl=pnl, order_id=i, at=T0 + timedelta(hours=i)) for i, pnl in enumerate(pnls, start=1)
    ]
    # The API serves newest first; the curve must be walked in time order regardless.
    metrics = compute(list(reversed(in_time_order)))
    assert metrics.max_drawdown == Decimal("80")


def test_max_drawdown_is_zero_when_pnl_only_climbs() -> None:
    metrics = compute(
        [
            fill(pnl="10", order_id=1),
            fill(pnl="20", order_id=2, at=T0 + timedelta(hours=1)),
        ]
    )
    assert metrics.max_drawdown == Decimal("0")


def test_sharpe_rewards_steady_daily_pnl() -> None:
    # Three UTC days realizing 100, 150, 110: mean 120, sample std ~26.46.
    metrics = compute(
        [
            fill(pnl="100", order_id=1, at=T0),
            fill(pnl="150", order_id=2, at=T0 + timedelta(days=1)),
            fill(pnl="110", order_id=3, at=T0 + timedelta(days=2)),
        ]
    )
    assert metrics.sharpe is not None
    assert float(metrics.sharpe) == pytest.approx(120 / 26.457513 * 365**0.5, rel=1e-5)


def test_sharpe_counts_quiet_days_between_trades_as_zero() -> None:
    # 100 on day one, nothing on day two, 100 on day three: the quiet day
    # spreads the same profit thinner, so Sharpe must drop below the
    # every-day-100 case (which has zero variance and no Sharpe at all).
    metrics = compute(
        [
            fill(pnl="100", order_id=1, at=T0),
            fill(pnl="100", order_id=2, at=T0 + timedelta(days=2)),
        ]
    )
    assert metrics.sharpe is not None
    daily = [100, 0, 100]
    mean = sum(daily) / 3
    std = (sum((x - mean) ** 2 for x in daily) / 2) ** 0.5
    assert float(metrics.sharpe) == pytest.approx(mean / std * 365**0.5, rel=1e-5)


def test_sharpe_is_none_when_variance_vanishes_or_history_is_one_day() -> None:
    one_day = compute([fill(pnl="10", order_id=1), fill(pnl="20", order_id=2)])
    assert one_day.sharpe is None
    flat = compute(
        [
            fill(pnl="10", order_id=1, at=T0),
            fill(pnl="10", order_id=2, at=T0 + timedelta(days=1)),
        ]
    )
    assert flat.sharpe is None


def test_maker_share_counts_resting_perp_fills() -> None:
    metrics = compute(
        [
            fill("Open Long", order_id=1, crossed=False),
            fill(pnl="5", order_id=2, crossed=False),
            fill(pnl="5", order_id=3, crossed=True),
            fill("Buy", coin="PURR/USDC", order_id=4, crossed=False),  # spot: ignored
        ]
    )
    assert metrics.maker_share == Decimal(2) / Decimal(3)


def test_avg_leverage_relates_peak_trade_notional_to_account_value() -> None:
    # Trade 1 peaks at |start_position| 100 x price 10 = 1000 notional;
    # trade 2 at 50 x 10 = 500. Against a $250 account: (4x + 2x) / 2 = 3x.
    metrics = compute(
        [
            fill(pnl="1", order_id=1, start_position="40", price="10"),
            fill(pnl="1", order_id=1, start_position="100", price="10"),
            fill(pnl="1", order_id=2, start_position="50", price="10"),
        ],
        account_value=Decimal("250"),
    )
    assert metrics.avg_leverage == Decimal("3")


def test_avg_leverage_is_none_without_account_value() -> None:
    fills = [fill(pnl="1", order_id=1, start_position="10")]
    assert compute(fills, account_value=None).avg_leverage is None
    assert compute(fills, account_value=Decimal("0")).avg_leverage is None


def test_window_spans_perp_fills_only() -> None:
    metrics = compute(
        [
            fill("Buy", coin="PURR/USDC", order_id=1, at=T0 - timedelta(days=300)),
            fill("Open Long", order_id=2, at=T0),
            fill(pnl="5", order_id=3, at=T0 + timedelta(days=2)),
        ]
    )
    assert metrics.window_start == T0
    assert metrics.window_end == T0 + timedelta(days=2)


def test_no_perp_fills_yields_an_empty_but_present_result() -> None:
    metrics = compute([fill("Buy", coin="PURR/USDC", order_id=1)])
    assert metrics.trade_count == 0
    assert metrics.win_rate is None
    assert metrics.avg_win is None
    assert metrics.avg_loss is None
    assert metrics.sharpe is None
    assert metrics.max_drawdown == Decimal("0")
    assert metrics.avg_leverage is None
    assert metrics.maker_share is None
    assert metrics.avg_hold_seconds is None
    assert metrics.realized_pnl == Decimal("0")
    assert metrics.window_start is None
    assert metrics.window_end is None


# --- Average holding time (issue #48) ----------------------------------------
# An episode is the span a coin is non-flat: opens when the signed position
# leaves 0, closes when it returns to 0. avg_hold_seconds is the mean duration
# over completed episodes; an episode still open at window end is excluded.


def _open(coin: str = "HYPE", *, at: datetime, order_id: int = 1) -> Fill:
    return fill("Open Long", coin=coin, at=at, order_id=order_id, start_position="0")


def _add(coin: str = "HYPE", *, at: datetime, order_id: int = 1) -> Fill:
    # A same-side scale-in: already long, adding more, so it never crosses 0.
    return fill("Open Long", coin=coin, at=at, order_id=order_id, start_position="1")


def _close(coin: str = "HYPE", *, at: datetime, start: str = "1", order_id: int = 1) -> Fill:
    return fill("Close Long", coin=coin, at=at, order_id=order_id, start_position=start)


def test_a_single_open_then_close_is_one_episode() -> None:
    metrics = compute([_open(at=T0), _close(at=T0 + timedelta(hours=2))])
    assert metrics.avg_hold_seconds == 2 * 3600


def test_scaling_in_stays_one_episode_until_flat() -> None:
    metrics = compute(
        [
            _open(at=T0),
            _add(at=T0 + timedelta(hours=1)),  # more size, still non-flat
            _close(at=T0 + timedelta(hours=4), start="2"),  # start 2, size 1 -> 1, still open
            _close(at=T0 + timedelta(hours=6), start="1"),  # start 1, size 1 -> 0, closes
        ]
    )
    # One episode, T0 -> T0+6h; the interim partial close never ended it.
    assert metrics.avg_hold_seconds == 6 * 3600


def test_the_mean_averages_completed_episodes() -> None:
    metrics = compute(
        [
            _open(at=T0, order_id=1),
            _close(at=T0 + timedelta(hours=2), order_id=1),  # 2h
            _open(at=T0 + timedelta(hours=5), order_id=2),
            _close(at=T0 + timedelta(hours=9), order_id=2),  # 4h
        ]
    )
    assert metrics.avg_hold_seconds == 3 * 3600  # (2h + 4h) / 2


def test_a_flip_closes_one_episode_and_opens_the_next() -> None:
    metrics = compute(
        [
            _open(at=T0),
            # Long > Short: start 1, size 3 -> -2, crosses 0 (closes the long at +2h,
            # opens a short there).
            fill("Long > Short", at=T0 + timedelta(hours=2), start_position="1", size="3"),
            # Close Short: start -2, size 2 -> 0, closes the short at +5h.
            fill("Close Short", at=T0 + timedelta(hours=5), start_position="-2", size="2"),
        ]
    )
    # Two 2h/3h episodes split at the flip: (2h + 3h) / 2.
    assert metrics.avg_hold_seconds == (2 * 3600 + 3 * 3600) // 2


def test_an_episode_still_open_at_window_end_is_excluded() -> None:
    only_open = compute([_open(at=T0)])
    assert only_open.avg_hold_seconds is None  # never closed, no completed episode

    one_closed_one_open = compute(
        [
            _open(at=T0, coin="HYPE", order_id=1),
            _close(at=T0 + timedelta(hours=3), coin="HYPE", order_id=1),
            _open(at=T0 + timedelta(hours=1), coin="BTC", order_id=2),  # still open at end
        ]
    )
    assert one_closed_one_open.avg_hold_seconds == 3 * 3600  # only the closed one counts


def test_episodes_are_tracked_per_coin_independently() -> None:
    metrics = compute(
        [
            _open(at=T0, coin="HYPE", order_id=1),
            _open(at=T0 + timedelta(hours=1), coin="BTC", order_id=2),
            _close(at=T0 + timedelta(hours=2), coin="BTC", order_id=2),  # BTC: 1h
            _close(at=T0 + timedelta(hours=4), coin="HYPE", order_id=1),  # HYPE: 4h
        ]
    )
    assert metrics.avg_hold_seconds == (3600 + 4 * 3600) // 2


def test_an_open_predating_the_window_is_not_counted() -> None:
    # The first fill is already non-flat (start 1): its open is off the front of
    # the pull, so closing it yields no duration — same truncation caveat as #11.
    metrics = compute([_close(at=T0 + timedelta(hours=2), start="1")])
    assert metrics.avg_hold_seconds is None


def test_holding_time_folds_across_a_checkpoint() -> None:
    # A position opened before the checkpoint, closed after it: the open-time
    # rode the fold in open_episodes, so the close resolves to a real duration.
    early = [_open(at=T0)]
    late = [_close(at=T0 + timedelta(hours=3))]  # first late fill is already non-flat
    folded = metrics_from_state(fold_state(extract_state(early), late), None)
    assert folded.avg_hold_seconds == 3 * 3600
    # And it equals computing the union in one shot.
    assert folded.avg_hold_seconds == compute(early + late).avg_hold_seconds


# --- Incremental folding (issue #11) -----------------------------------------
# The foldable state is what persists between incremental refreshes: extract a
# batch, fold in the fills since the checkpoint, then reduce to metrics. Folding
# disjoint batches must equal computing the whole history at once.


def test_folding_the_fills_since_a_checkpoint_equals_computing_the_union() -> None:
    early = [
        fill(pnl="100", order_id=1, at=T0, crossed=False, start_position="50"),
        fill(pnl="-40", order_id=2, at=T0 + timedelta(days=1)),
    ]
    late = [
        fill("Open Long", order_id=3, at=T0 + timedelta(days=2), start_position="0"),
        fill(pnl="60", order_id=3, at=T0 + timedelta(days=2, hours=1)),
    ]
    account = Decimal("1000")
    folded = metrics_from_state(fold_state(extract_state(early), late), account)
    whole = compute_fine_metrics(early + late, account_value=account)
    assert folded == whole


def test_folding_retains_trades_from_before_the_new_batch() -> None:
    # The point of the fold: history accumulates past any single pull's window,
    # so nothing is lost to the ~2000-fill cap a full re-pull would hit (#11).
    prior = extract_state(
        [fill(pnl="10", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 6)]
    )
    folded = fold_state(prior, [fill(pnl="10", order_id=99, at=T0 + timedelta(days=1))])
    assert len(folded.trades) == 6
    assert metrics_from_state(folded, None).realized_pnl == Decimal("60")


def test_folding_keeps_the_earliest_window_and_advances_the_checkpoint() -> None:
    prior = extract_state([fill(order_id=1, at=T0)])
    folded = fold_state(prior, [fill(order_id=2, at=T0 + timedelta(days=5))])
    assert folded.window_start == T0  # earliest perp fill is preserved
    assert folded.window_end == T0 + timedelta(days=5)
    assert folded.last_fill_at == T0 + timedelta(days=5)  # checkpoint moves forward


def test_folding_an_empty_batch_leaves_the_state_unchanged() -> None:
    prior = extract_state(
        [fill(pnl="30", order_id=1, at=T0, crossed=False), fill(pnl="-5", order_id=2, at=T0)]
    )
    assert fold_state(prior, []) == prior
