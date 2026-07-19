"""The fine-metric engine: fills in, Metric Library values out (issue #8).

A *trade* is a completed position round-trip (issue #58): from the fill that
takes a coin off flat to the fill that returns it to flat, with net PnL the sum
of the episode's closing fills' closedPnl. Partial trims realize PnL inside one
trade, never as trades of their own, and a round-trip only counts when both its
open and its full close are in captured history. Golden fixtures cross-check
the whole engine in test_golden_wallets.py; here each metric earns its
definition on synthetic fills small enough to verify by hand.
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


def _open(coin: str = "HYPE", *, at: datetime, size: str = "1", crossed: bool = True) -> Fill:
    return fill("Open Long", coin=coin, at=at, start_position="0", size=size, crossed=crossed)


def _add(coin: str = "HYPE", *, at: datetime, start: str = "1", size: str = "1") -> Fill:
    # A same-side scale-in: already long, adding more, so it never crosses 0.
    return fill("Open Long", coin=coin, at=at, start_position=start, size=size)


def _close(
    coin: str = "HYPE",
    *,
    at: datetime,
    pnl: str = "0",
    start: str = "1",
    size: str = "1",
    price: str = "10",
    crossed: bool = True,
) -> Fill:
    return fill(
        "Close Long",
        coin=coin,
        at=at,
        pnl=pnl,
        start_position=start,
        size=size,
        price=price,
        crossed=crossed,
    )


def trip(
    pnl: str = "0", *, at: datetime = T0, coin: str = "HYPE", hold: timedelta = timedelta(hours=1)
) -> list[Fill]:
    """One completed round-trip: open at `at`, full close `hold` later."""
    return [_open(coin=coin, at=at), _close(coin=coin, at=at + hold, pnl=pnl)]


# --- The round-trip trade (issue #58) -----------------------------------------


def test_an_open_and_full_close_form_one_round_trip() -> None:
    metrics = compute(trip(pnl="30"))
    assert metrics.trade_count == 1
    assert metrics.win_rate == Decimal(1)
    assert metrics.realized_pnl == Decimal("30")


def test_partial_trims_stay_inside_one_trade() -> None:
    metrics = compute(
        [
            _open(at=T0, size="3"),
            _close(at=T0 + timedelta(hours=1), pnl="10", start="3"),
            _close(at=T0 + timedelta(hours=2), pnl="10", start="2"),
            _close(at=T0 + timedelta(hours=3), pnl="10", start="1"),
        ]
    )
    assert metrics.trade_count == 1  # three trims, one decision
    assert metrics.win_rate == Decimal(1)
    assert metrics.realized_pnl == Decimal("30")


def test_trimming_a_never_closed_position_yields_no_trades() -> None:
    # The 0xaf0f…e92e wallet (issue #58): 78 profitable fills, every one a trim
    # of a single still-open position whose open predates the window. Banked
    # money, zero completed trades — never 78 fake wins.
    metrics = compute(
        [_close(at=T0 + timedelta(hours=i), pnl="50", start=str(100 - i)) for i in range(1, 79)]
    )
    assert metrics.trade_count == 0
    assert metrics.win_rate is None
    assert metrics.avg_win is None
    assert metrics.sharpe is None
    assert metrics.realized_pnl == Decimal(50 * 78)


def test_an_in_window_position_still_open_at_the_end_is_not_a_trade_yet() -> None:
    metrics = compute(
        [
            _open(at=T0, size="2"),
            _close(at=T0 + timedelta(hours=1), pnl="25", start="2"),  # trim, still open
        ]
    )
    assert metrics.trade_count == 0
    assert metrics.win_rate is None
    assert metrics.realized_pnl == Decimal("25")


def test_a_close_whose_open_predates_the_window_is_excluded() -> None:
    # A full close, but the open is off the front of the pull: excluded from
    # the trade metrics rather than given partial credit.
    metrics = compute([_close(at=T0, pnl="50", start="1")])
    assert metrics.trade_count == 0
    assert metrics.win_rate is None
    assert metrics.realized_pnl == Decimal("50")


def test_a_trade_trimmed_in_profit_but_closed_at_a_net_loss_is_a_loss() -> None:
    metrics = compute(
        [
            _open(at=T0, size="2"),
            _close(at=T0 + timedelta(hours=1), pnl="20", start="2"),
            _close(at=T0 + timedelta(hours=2), pnl="-50", start="1"),
        ]
    )
    assert metrics.trade_count == 1
    assert metrics.win_rate == Decimal(0)
    assert metrics.avg_win is None
    assert metrics.avg_loss == Decimal("30")  # the net outcome, not the trim
    assert metrics.realized_pnl == Decimal("-30")


def test_win_rate_and_averages_reduce_over_round_trips() -> None:
    metrics = compute(
        [
            *trip(pnl="100", at=T0),
            *trip(pnl="-40", at=T0 + timedelta(hours=3)),
            *trip(pnl="60", at=T0 + timedelta(hours=6)),
        ]
    )
    assert metrics.trade_count == 3
    assert metrics.win_rate == Decimal(2) / Decimal(3)
    assert metrics.avg_win == Decimal("80")
    assert metrics.avg_loss == Decimal("40")
    assert metrics.realized_pnl == Decimal("120")


def test_a_breakeven_round_trip_is_not_a_win() -> None:
    metrics = compute([*trip(pnl="0", at=T0), *trip(pnl="10", at=T0 + timedelta(hours=2))])
    assert metrics.win_rate == Decimal("0.5")


def test_realized_pnl_banks_money_the_trade_metrics_exclude() -> None:
    # realized_pnl stays comprehensive (issue #58): it may exceed the sum of
    # the counted round-trips' PnLs by exactly the unattributable partials.
    metrics = compute(
        [
            _close(at=T0, pnl="500", start="9", coin="LIT"),  # trim of an unseen open
            *trip(pnl="-40", at=T0 + timedelta(hours=1)),
        ]
    )
    assert metrics.trade_count == 1
    assert metrics.realized_pnl == Decimal("460")


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


def test_flips_liquidations_and_settlements_complete_round_trips() -> None:
    metrics = compute(
        [
            _open(at=T0, coin="ETH"),
            # Long > Short: start 1, size 3 -> -2, through 0 — closes the long
            # (+100, a win) and opens a short in the same fill.
            fill(
                "Long > Short",
                pnl="100",
                at=T0 + timedelta(hours=1),
                coin="ETH",
                start_position="1",
                size="3",
            ),
            # The short from the flip closes at a loss: its own trade.
            fill(
                "Close Short",
                pnl="-40",
                at=T0 + timedelta(hours=2),
                coin="ETH",
                start_position="-2",
                size="2",
            ),
            _open(at=T0, coin="DOGE"),
            fill(
                "Liquidated Isolated Long",
                pnl="-60",
                at=T0 + timedelta(hours=3),
                coin="DOGE",
                start_position="1",
                size="1",
            ),
            _open(at=T0, coin="#7501"),
            fill(
                "Settlement",
                pnl="60",
                at=T0 + timedelta(hours=4),
                coin="#7501",
                start_position="1",
                size="1",
            ),
        ]
    )
    assert metrics.trade_count == 4
    assert metrics.win_rate == Decimal(2) / Decimal(4)


def test_avg_win_is_none_without_wins_and_avg_loss_none_without_losses() -> None:
    all_wins = compute(trip(pnl="10"))
    assert all_wins.avg_loss is None
    all_losses = compute(trip(pnl="-10"))
    assert all_losses.avg_win is None


def test_max_drawdown_is_the_deepest_fall_of_the_realized_pnl_curve() -> None:
    pnls = ["100", "-50", "-30", "200"]  # peak 100 -> trough 20 before recovering
    in_time_order = [
        f
        for i, pnl in enumerate(pnls)
        for f in trip(pnl=pnl, at=T0 + timedelta(hours=2 * i), coin=f"C{i}")
    ]
    # The API serves newest first; the curve must be walked in time order regardless.
    metrics = compute(list(reversed(in_time_order)))
    assert metrics.max_drawdown == Decimal("80")


def test_max_drawdown_is_zero_when_pnl_only_climbs() -> None:
    metrics = compute([*trip(pnl="10", at=T0), *trip(pnl="20", at=T0 + timedelta(hours=2))])
    assert metrics.max_drawdown == Decimal("0")


def test_sharpe_rewards_steady_daily_pnl() -> None:
    # Three UTC days realizing 100, 150, 110: mean 120, sample std ~26.46.
    metrics = compute(
        [
            *trip(pnl="100", at=T0),
            *trip(pnl="150", at=T0 + timedelta(days=1)),
            *trip(pnl="110", at=T0 + timedelta(days=2)),
        ]
    )
    assert metrics.sharpe is not None
    assert float(metrics.sharpe) == pytest.approx(120 / 26.457513 * 365**0.5, rel=1e-5)


def test_sharpe_counts_quiet_days_between_trades_as_zero() -> None:
    # 100 on day one, nothing on day two, 100 on day three: the quiet day
    # spreads the same profit thinner, so Sharpe must drop below the
    # every-day-100 case (which has zero variance and no Sharpe at all).
    metrics = compute([*trip(pnl="100", at=T0), *trip(pnl="100", at=T0 + timedelta(days=2))])
    assert metrics.sharpe is not None
    daily = [100, 0, 100]
    mean = sum(daily) / 3
    std = (sum((x - mean) ** 2 for x in daily) / 2) ** 0.5
    assert float(metrics.sharpe) == pytest.approx(mean / std * 365**0.5, rel=1e-5)


def test_sharpe_is_none_when_variance_vanishes_or_history_is_one_day() -> None:
    one_day = compute([*trip(pnl="10", at=T0), *trip(pnl="20", at=T0 + timedelta(hours=2))])
    assert one_day.sharpe is None
    flat = compute([*trip(pnl="10", at=T0), *trip(pnl="10", at=T0 + timedelta(days=1))])
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
    # Trade 1's trims reveal peaks 1000 and 400 -> 1000; trade 2 closes from
    # 500. Against a $250 account: (4x + 2x) / 2 = 3x.
    metrics = compute(
        [
            _open(at=T0, size="100"),
            _close(at=T0 + timedelta(hours=1), pnl="1", start="100", size="60", price="10"),
            _close(at=T0 + timedelta(hours=2), pnl="1", start="40", size="40", price="10"),
            _open(at=T0 + timedelta(hours=3), size="50"),
            _close(at=T0 + timedelta(hours=4), pnl="1", start="50", size="50", price="10"),
        ],
        account_value=Decimal("250"),
    )
    assert metrics.avg_leverage == Decimal("3")


def test_avg_leverage_is_none_without_account_value() -> None:
    fills = trip(pnl="1")
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
# A round-trip's duration is its holding time; avg_hold_seconds is the mean
# over completed round-trips, so it shares their pre-window exclusion.


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
    assert metrics.trade_count == 1


def test_the_mean_averages_completed_episodes() -> None:
    metrics = compute(
        [
            _open(at=T0),
            _close(at=T0 + timedelta(hours=2)),  # 2h
            _open(at=T0 + timedelta(hours=5)),
            _close(at=T0 + timedelta(hours=9)),  # 4h
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
            _open(at=T0, coin="HYPE"),
            _close(at=T0 + timedelta(hours=3), coin="HYPE"),
            _open(at=T0 + timedelta(hours=1), coin="BTC"),  # still open at end
        ]
    )
    assert one_closed_one_open.avg_hold_seconds == 3 * 3600  # only the closed one counts


def test_episodes_are_tracked_per_coin_independently() -> None:
    metrics = compute(
        [
            _open(at=T0, coin="HYPE"),
            _open(at=T0 + timedelta(hours=1), coin="BTC"),
            _close(at=T0 + timedelta(hours=2), coin="BTC"),  # BTC: 1h
            _close(at=T0 + timedelta(hours=4), coin="HYPE"),  # HYPE: 4h
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
        _open(at=T0, size="2", crossed=False),
        _close(at=T0 + timedelta(hours=1), pnl="100", start="2"),  # trim rides the fold
        *trip(pnl="-40", at=T0 + timedelta(days=1), coin="BTC"),
    ]
    late = [
        _close(at=T0 + timedelta(days=2), pnl="60", start="1"),  # completes the HYPE trade
        *trip(pnl="25", at=T0 + timedelta(days=3), coin="ETH"),
    ]
    account = Decimal("1000")
    folded = metrics_from_state(fold_state(extract_state(early), late), account)
    whole = compute_fine_metrics(early + late, account_value=account)
    assert folded == whole


def test_a_round_trip_accumulates_net_pnl_across_multiple_refreshes() -> None:
    # Opened in one batch, trimmed in the next, fully closed in a third: one
    # trade whose net PnL sums all three batches (issue #58) — the open
    # episode's accumulator survives each fold and never double-counts.
    opened = [_open(at=T0, size="2")]
    trimmed = [_close(at=T0 + timedelta(hours=1), pnl="20", start="2")]
    closed = [_close(at=T0 + timedelta(hours=2), pnl="-50", start="1")]
    state = fold_state(fold_state(extract_state(opened), trimmed), closed)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 1
    assert metrics.win_rate == Decimal(0)  # trimmed in profit, net a loss
    assert metrics.avg_loss == Decimal("30")
    assert metrics.avg_hold_seconds == 2 * 3600
    assert metrics.realized_pnl == Decimal("-30")
    assert state.open_episodes == ()


def test_pre_history_trims_stay_excluded_but_bank_realized_pnl_across_folds() -> None:
    early = [_close(at=T0, pnl="30", start="9")]  # trim of an open we never saw
    late = [_close(at=T0 + timedelta(hours=1), pnl="70", start="8", size="8")]  # its full close
    metrics = metrics_from_state(fold_state(extract_state(early), late), None)
    assert metrics.trade_count == 0  # the open was never captured: no partial credit
    assert metrics.win_rate is None
    assert metrics.realized_pnl == Decimal("100")  # the money still banked


def test_folding_retains_trades_from_before_the_new_batch() -> None:
    # The point of the fold: history accumulates past any single pull's window,
    # so nothing is lost to the ~2000-fill cap a full re-pull would hit (#11).
    prior = extract_state(
        [
            f
            for i in range(1, 6)
            for f in trip(pnl="10", at=T0 + timedelta(hours=2 * i), coin=f"C{i}")
        ]
    )
    folded = fold_state(prior, trip(pnl="10", at=T0 + timedelta(days=1)))
    assert len(folded.round_trips) == 6
    assert metrics_from_state(folded, None).realized_pnl == Decimal("60")


def test_folding_keeps_the_earliest_window_and_advances_the_checkpoint() -> None:
    prior = extract_state([fill(order_id=1, at=T0)])
    folded = fold_state(prior, [fill(order_id=2, at=T0 + timedelta(days=5))])
    assert folded.window_start == T0  # earliest perp fill is preserved
    assert folded.window_end == T0 + timedelta(days=5)
    assert folded.last_fill_at == T0 + timedelta(days=5)  # checkpoint moves forward


def test_folding_an_empty_batch_leaves_the_state_unchanged() -> None:
    prior = extract_state(
        [
            _open(at=T0, size="2", crossed=False),
            _close(at=T0 + timedelta(hours=1), pnl="30", start="2"),  # trim, still open
            *trip(pnl="-5", at=T0 + timedelta(hours=2), coin="BTC"),
        ]
    )
    assert fold_state(prior, []) == prior
