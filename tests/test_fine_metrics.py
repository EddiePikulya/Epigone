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
    FineState,
    OpenEpisode,
    RoundTrip,
    compute_fine_metrics,
    extract_state,
    fold_state,
    metrics_from_state,
    reduce_trips,
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


def _trips_across_coins(counts: dict[str, int]) -> list[Fill]:
    """Round-trips spread across coins by count — `{"SOL": 3, "BTC": 1}` is
    three SOL trips and one BTC trip, each an isolated open→close."""
    fills: list[Fill] = []
    hour = 0
    for coin, n in counts.items():
        for _ in range(n):
            fills += trip(coin=coin, at=T0 + timedelta(hours=hour))
            hour += 1
    return fills


def test_effective_coins_is_one_for_a_single_pair_specialist() -> None:
    metrics = compute(_trips_across_coins({"SOL": 4}))
    assert metrics.effective_coins == Decimal(1)


def test_effective_coins_reads_a_fifty_fifty_two_ticker_specialist_as_two() -> None:
    # The revised metric's whole point (#95): a 50/50 pair is *focused*, so it
    # must read 2.0 — a top-coin share would have called it an ambiguous 50%.
    metrics = compute(_trips_across_coins({"SOL": 5, "ETH": 5}))
    assert metrics.effective_coins == Decimal(2)


def test_effective_coins_shrugs_off_a_single_dust_probe() -> None:
    # One stray trip among fifty must stay ≈1, not read as a genuine second coin
    # (top-coin share would have said 0.98; inverse HHI ≈ 1.04).
    metrics = compute(_trips_across_coins({"SOL": 49, "BTC": 1}))
    assert metrics.effective_coins == Decimal(2500) / Decimal(2402)
    assert abs(metrics.effective_coins - Decimal("1.04")) < Decimal("0.01")


def test_effective_coins_counts_an_even_spread_as_its_coin_count() -> None:
    metrics = compute(_trips_across_coins(dict.fromkeys(["SOL", "BTC", "ETH"], 2)))
    assert metrics.effective_coins == Decimal(3)


def test_effective_coins_is_none_without_completed_round_trips() -> None:
    # An open with no close banks no round-trip, so the spread is undefined.
    metrics = compute([_open(at=T0)])
    assert metrics.trade_count == 0
    assert metrics.effective_coins is None


# --- The anti-deception trio (issue #113) -------------------------------------


def test_median_trade_is_the_typical_pnl_across_wins_and_losses() -> None:
    # Five trips: median is the middle value, wins and losses together — one big
    # winner cannot drag it the way it drags the mean.
    metrics = compute(
        [
            *trip(pnl="-30", at=T0),
            *trip(pnl="-10", at=T0 + timedelta(hours=1)),
            *trip(pnl="20", at=T0 + timedelta(hours=2)),
            *trip(pnl="50", at=T0 + timedelta(hours=3)),
            *trip(pnl="5000", at=T0 + timedelta(hours=4)),
        ]
    )
    assert metrics.median_trade == Decimal("20")


def test_median_trade_is_negative_for_a_coin_flipper() -> None:
    # A losing typical trade reads negative — the point of a median over ALL
    # trips (a positive win rate cannot hide it).
    metrics = compute(
        [*trip(pnl="-40", at=T0), *trip(pnl="-20", at=T0 + timedelta(hours=1))]
    )
    assert metrics.median_trade == Decimal("-30")


def test_median_trade_of_a_single_trade_is_that_trade() -> None:
    metrics = compute(trip(pnl="123"))
    assert metrics.median_trade == Decimal("123")


def test_median_trade_is_none_without_trips() -> None:
    metrics = compute([_open(at=T0)])
    assert metrics.trade_count == 0
    assert metrics.median_trade is None


def test_profit_factor_is_gross_wins_over_gross_losses() -> None:
    # Wins 100 + 50 = 150 gross; losses 40 + 20 = 60 gross → 2.5.
    metrics = compute(
        [
            *trip(pnl="100", at=T0),
            *trip(pnl="50", at=T0 + timedelta(hours=1)),
            *trip(pnl="-40", at=T0 + timedelta(hours=2)),
            *trip(pnl="-20", at=T0 + timedelta(hours=3)),
        ]
    )
    assert metrics.profit_factor == Decimal("150") / Decimal("60")


def test_profit_factor_below_one_exposes_a_win_rate_illusion() -> None:
    # A majority-wins wallet that still loses money: three small wins, one big
    # loss. Win rate 75%, profit factor < 1 — the number that catches it.
    metrics = compute(
        [
            *trip(pnl="10", at=T0),
            *trip(pnl="10", at=T0 + timedelta(hours=1)),
            *trip(pnl="10", at=T0 + timedelta(hours=2)),
            *trip(pnl="-100", at=T0 + timedelta(hours=3)),
        ]
    )
    assert metrics.win_rate == Decimal("0.75")
    assert metrics.profit_factor == Decimal("30") / Decimal("100")
    assert metrics.profit_factor < 1


def test_profit_factor_is_none_without_losses() -> None:
    # No losing dollars means a zero denominator — an unbounded "∞" the screener
    # renders as absent, never a huge number.
    metrics = compute([*trip(pnl="100", at=T0), *trip(pnl="50", at=T0 + timedelta(hours=1))])
    assert metrics.avg_loss is None
    assert metrics.profit_factor is None


def test_profit_factor_is_zero_for_an_all_losses_wallet() -> None:
    # Losses but no winning dollars: gross_win 0 over real losses → a genuine 0,
    # not NULL (the denominator exists).
    metrics = compute([*trip(pnl="-40", at=T0), *trip(pnl="-20", at=T0 + timedelta(hours=1))])
    assert metrics.profit_factor == Decimal("0")


def test_top_trade_share_is_the_best_trip_over_the_total() -> None:
    # Best trip 90 of a 150 total → 0.6 (60%): most of the profit is one trade.
    metrics = compute(
        [
            *trip(pnl="90", at=T0),
            *trip(pnl="40", at=T0 + timedelta(hours=1)),
            *trip(pnl="20", at=T0 + timedelta(hours=2)),
        ]
    )
    assert metrics.top_trade_share == Decimal("90") / Decimal("150")


def test_top_trade_share_of_a_single_winning_trade_is_all_of_it() -> None:
    metrics = compute(trip(pnl="500"))
    assert metrics.top_trade_share == Decimal("1")


def test_top_trade_share_is_none_when_total_pnl_is_negative() -> None:
    # A net-losing record has no profit to concentrate — the ratio is
    # meaningless, so NULL (never a share of a negative total).
    metrics = compute(
        [
            *trip(pnl="30", at=T0),
            *trip(pnl="-40", at=T0 + timedelta(hours=1)),
            *trip(pnl="-50", at=T0 + timedelta(hours=2)),
        ]
    )
    assert metrics.realized_pnl < 0  # -60 total: no profit to concentrate
    assert metrics.top_trade_share is None


def test_top_trade_share_is_none_for_an_all_losses_wallet() -> None:
    metrics = compute([*trip(pnl="-40", at=T0), *trip(pnl="-20", at=T0 + timedelta(hours=1))])
    assert metrics.top_trade_share is None


def test_top_trade_share_is_none_without_trips() -> None:
    metrics = compute([_open(at=T0)])
    assert metrics.top_trade_share is None


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


# --- startPosition continuity guard (issue #63) -------------------------------
# Every fill carries the position size before it executed. When the engine's
# walked net position disagrees with an incoming fill's startPosition beyond
# float dust, executions were missed (a fill source the fetch didn't cover, or
# history truncated by the ~2000 cap): the episode is demoted to untracked —
# the same treatment as a pre-window open — rather than credited as a
# round-trip reconstructed from a walk that skipped executions.


def test_a_gap_in_the_walk_demotes_the_episode_to_untracked() -> None:
    metrics = compute(
        [
            _open(at=T0),  # 0 -> 1
            # The close says the position was 5, not the walked 1: four coins
            # of executions are missing. Banked money, but never a trade.
            _close(at=T0 + timedelta(hours=1), pnl="50", start="5", size="5"),
        ]
    )
    assert metrics.trade_count == 0
    assert metrics.win_rate is None
    assert metrics.avg_hold_seconds is None
    assert metrics.realized_pnl == Decimal("50")


def test_a_clean_trip_after_a_demoted_episode_counts() -> None:
    metrics = compute(
        [
            _open(at=T0),
            _close(at=T0 + timedelta(hours=1), pnl="50", start="5", size="5"),  # demoted
            *trip(pnl="10", at=T0 + timedelta(hours=2)),  # clean open-from-flat: counts
        ]
    )
    assert metrics.trade_count == 1
    assert metrics.win_rate == Decimal(1)
    assert metrics.realized_pnl == Decimal("60")


def test_float_representation_dust_does_not_demote() -> None:
    # The API's own startPosition strings carry ~1e-10 relative dust (a real
    # example: "3472099.9999999998" following a 3472100.0 position); a genuine
    # missed execution is off by a whole fill, so dust must not demote.
    metrics = compute(
        [
            _open(at=T0, size="3472100.0"),
            _close(
                at=T0 + timedelta(hours=1),
                pnl="10",
                start="3472099.9999999998",
                size="3472099.9999999998",
            ),
        ]
    )
    assert metrics.trade_count == 1


def test_missed_closes_demote_the_old_episode_and_the_flat_open_starts_clean() -> None:
    metrics = compute(
        [
            _open(at=T0),  # 0 -> 1
            # A fresh open from flat while the walk says 1: the old episode's
            # closes were missed. It is dropped; this open anchors clean.
            _open(at=T0 + timedelta(hours=1)),
            _close(at=T0 + timedelta(hours=2), pnl="10", start="1"),
        ]
    )
    assert metrics.trade_count == 1
    assert metrics.avg_hold_seconds == 3600  # the clean reopen's hold, not T0's
    assert metrics.realized_pnl == Decimal("10")


def test_after_a_mid_position_gap_the_tail_stays_untracked_until_flat() -> None:
    metrics = compute(
        [
            _open(at=T0, size="2"),  # 0 -> 2
            _close(at=T0 + timedelta(hours=1), pnl="5", start="5"),  # gap: demoted, 5 -> 4
            # The rest of the demoted position's life stays untracked...
            _close(at=T0 + timedelta(hours=2), pnl="20", start="4", size="4"),  # 4 -> flat
            # ...but the next open-from-flat is a clean, counted trip.
            *trip(pnl="7", at=T0 + timedelta(hours=3)),
        ]
    )
    assert metrics.trade_count == 1
    assert metrics.avg_win == Decimal("7")
    assert metrics.realized_pnl == Decimal("32")


def test_an_untracked_flip_reopens_a_tracked_episode() -> None:
    # A flip's far side opens at a position the walk knows exactly, even when
    # the flipped-away episode was demoted — the new episode is clean.
    metrics = compute(
        [
            _open(at=T0),  # 0 -> 1
            # Long > Short from a startPosition of 3 (walked: 1): demoted flip,
            # but it leaves a known -2 short opened here.
            fill(
                "Long > Short",
                pnl="15",
                at=T0 + timedelta(hours=1),
                start_position="3",
                size="5",
            ),
            fill(
                "Close Short",
                pnl="-4",
                at=T0 + timedelta(hours=3),
                start_position="-2",
                size="2",
            ),
        ]
    )
    assert metrics.trade_count == 1  # only the short leg
    assert metrics.avg_loss == Decimal("4")
    assert metrics.avg_hold_seconds == 2 * 3600
    assert metrics.realized_pnl == Decimal("11")


def test_a_checkpoint_gap_between_stored_position_and_next_fill_demotes() -> None:
    # The self-healing path for TWAP-blind stored state (#63): the carried open
    # episode says the position is 2, the next batch's first fill says 5 —
    # executions were missed across the checkpoint, so no round-trip.
    early = [_open(at=T0, size="2")]
    late = [_close(at=T0 + timedelta(hours=1), pnl="50", start="5", size="5")]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 0
    assert metrics.realized_pnl == Decimal("50")
    assert state.open_episodes == ()  # the corrupt episode is dropped, not carried
    # Demoting across the fold equals demoting inside one batch.
    assert metrics == compute(early + late)


def test_a_broken_leading_segment_never_completes_the_carried_episode() -> None:
    early = [_open(at=T0, size="2")]
    late = [
        _close(at=T0 + timedelta(hours=1), pnl="10", start="2"),  # trim 2 -> 1, matches
        _close(at=T0 + timedelta(hours=2), pnl="30", start="3", size="3"),  # gap: demoted
    ]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 0
    assert metrics.realized_pnl == Decimal("40")
    assert state.open_episodes == ()
    assert metrics == compute(early + late)


def test_an_open_episode_carries_its_walked_net_position() -> None:
    # The stored fold state must know where the walk left the position, or the
    # next batch's continuity check has nothing to compare against (#63).
    state = extract_state(
        [
            _open(at=T0, size="3"),
            _close(at=T0 + timedelta(hours=1), pnl="5", start="3"),  # trim 3 -> 2
        ]
    )
    (episode,) = state.open_episodes
    assert episode.net_position == Decimal("2")


def test_a_pre_guard_stored_episode_with_unknown_position_demotes() -> None:
    # Rows persisted before #63 carry net_position 0 (the migration default) —
    # a position the walk never verified. A real continuation always starts
    # non-flat, so the mismatch demotes exactly like any other gap: TWAP-blind
    # stored episodes heal on their next incremental without a data reset.
    stored = FineState(
        round_trips=(),
        maker_fill_count=1,
        perp_fill_count=1,
        realized_pnl=Decimal(0),
        window_start=T0,
        window_end=T0,
        last_fill_at=T0,
        open_episodes=(OpenEpisode("HYPE", T0, Decimal(0), Decimal(0)),),  # net defaults to 0
    )
    late = [_close(at=T0 + timedelta(hours=1), pnl="10", start="1")]
    state = fold_state(stored, late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 0
    assert metrics.realized_pnl == Decimal("10")
    assert state.open_episodes == ()


# --- Batch-boundary reconciliation (#63 adversarial review of PR #64) ---------
# The guard's blind spot was the batch head: a coin's first fill of a batch has
# no walked position to check against, and a stored open episode was only ever
# compared against a Continuation (a non-flat first fill). A batch whose first
# fill starts FLAT while the store holds an open episode is a contradiction —
# either the episode's close was missed, or a cross-source same-millisecond
# interleave was merged in the wrong order at the head — and both must demote
# rather than mint trips from an unverifiable walk.


def test_a_mis_merged_batch_head_millisecond_cannot_mint_a_trip() -> None:
    # True execution order at t1: TWAP close (5 -> 0), regular reopen (0 -> 5).
    # The merge has no cross-source within-ms signal and puts the regular fill
    # first, so the walk sees reopen-then-close — a self-consistent fabrication
    # that used to mint a zero-length trip and strand the stored episode.
    early = [_open(at=T0, size="5")]
    t1 = T0 + timedelta(hours=1)
    late = [
        _open(at=t1, size="5"),  # regular reopen, merged first
        _close(at=t1, pnl="100", start="5", size="5"),  # TWAP close, truly first
    ]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 0  # no trip from an unverifiable head millisecond
    assert metrics.realized_pnl == Decimal("100")  # the close still banks
    assert state.open_episodes == ()  # the stored episode demotes, not survives


def test_the_demoted_batch_head_leaves_no_zombie_episode_to_resurrect() -> None:
    # The stored episode's net_position (5) coincidentally matches the real
    # position after the head millisecond; before the fix it survived the fold
    # and a later close chained it into a chimera trip spanning the wrong open.
    early = [_open(at=T0, size="5")]
    t1 = T0 + timedelta(hours=1)
    late = [
        _open(at=t1, size="5"),
        _close(at=t1, pnl="100", start="5", size="5"),
    ]
    mid = fold_state(extract_state(early), late)
    final = fold_state(mid, [_close(at=T0 + timedelta(hours=2), pnl="7", start="5", size="5")])
    metrics = metrics_from_state(final, None)
    assert metrics.trade_count == 0  # the t2 close matches no carried episode: dropped
    assert metrics.realized_pnl == Decimal("107")
    assert final.open_episodes == ()


def test_a_stored_episode_whose_close_was_missed_demotes_when_the_batch_starts_flat() -> None:
    # No interleave needed: any missed close (2000-cap truncation, a TWAP-blind
    # prior fold) leaves the next batch's first fill flat. The stored episode
    # used to survive as a zombie — stale opened_at/pnl/peak waiting to chain
    # into a chimera; now the flat head demotes it.
    early = [_open(at=T0, size="2")]
    late = [
        _open(at=T0 + timedelta(hours=2)),  # first fill starts flat: contradiction
        _close(at=T0 + timedelta(hours=3), pnl="10", start="1"),
    ]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    # The fresh open-from-flat is API truth (a different millisecond, so no
    # interleave ambiguity): its round-trip counts, with its own open time.
    assert metrics.trade_count == 1
    assert metrics.avg_hold_seconds == 3600
    assert metrics.realized_pnl == Decimal("10")
    assert state.open_episodes == ()  # the stale stored episode is gone
    # Demoting at the fold equals what one batch would have concluded.
    assert metrics == compute(early + late)


def test_a_clean_head_open_still_carries_forward_after_a_missed_close() -> None:
    early = [_open(at=T0, size="2")]  # stored episode, close never seen
    t1 = T0 + timedelta(hours=2)
    late = [_open(at=t1)]  # flat head, position still open at batch end
    state = fold_state(extract_state(early), late)
    # The stale episode demotes; the fresh open is truth and rides forward.
    (episode,) = state.open_episodes
    assert episode.opened_at == t1
    assert episode.net_position == Decimal("1")


def test_a_chimera_head_reopen_is_not_carried_as_an_open_episode() -> None:
    # Head millisecond mints a trip AND leaves a same-ms reopen: the reopen's
    # open time is as unverifiable as the trip (the interleave could have put
    # the close first), so neither survives the fold.
    early = [_open(at=T0, size="1")]
    t1 = T0 + timedelta(hours=1)
    late = [
        _open(at=t1),  # possibly the mis-merged half of a close/reopen pair
        _close(at=t1, pnl="5", start="1"),
        _open(at=t1),  # same-ms reopen
    ]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 0
    assert state.open_episodes == ()
    assert metrics.realized_pnl == Decimal("5")


def test_a_full_close_into_dust_goes_flat_without_a_phantom_episode() -> None:
    # A dusty full close (start 3472099.9999999998, the API's own float dust)
    # leaves end = -2e-10 — numerically non-zero, actually flat. Exact-zero
    # checks used to read that as a flip and persist a phantom dust-short
    # episode; flatness now tolerates the same dust the continuity check does.
    state = extract_state(
        [
            _open(at=T0, size="3472100.0"),
            _close(
                at=T0 + timedelta(hours=1),
                pnl="10",
                start="3472099.9999999998",
                size="3472099.9999999998",
            ),
        ]
    )
    assert len(state.round_trips) == 1
    assert state.open_episodes == ()  # flat, not a phantom -2e-10 short
    # And the dust-flat position anchors a clean next trip.
    folded = fold_state(state, trip(pnl="4", at=T0 + timedelta(hours=2)))
    assert metrics_from_state(folded, None).trade_count == 2


# --- Within-millisecond ordering (issue #58 review) ---------------------------
# Same-order and same-block fills share one millisecond, so timestamps cannot
# order them: the engine's stable sort must honor the list's execution order
# (the gateway contract), and simultaneous completions need distinct identities.


def test_same_millisecond_fills_resolve_in_execution_order() -> None:
    t1 = T0 + timedelta(hours=1)
    metrics = compute(
        [
            _open(at=T0, size="2"),
            _close(at=t1, pnl="10", start="2"),  # trim...
            _close(at=t1, pnl="20", start="1"),  # ...and full close, same ms
            _open(at=t1),  # reopen in the same ms
            _close(at=t1 + timedelta(hours=1), pnl="-5"),
        ]
    )
    assert metrics.trade_count == 2
    assert metrics.win_rate == Decimal("0.5")  # +30 net win, then the -5 loss
    assert metrics.realized_pnl == Decimal("25")


def test_same_ms_close_reopen_close_keeps_both_trades_across_a_fold() -> None:
    # Two round-trips completing on one timestamp must not collide in the
    # fold's keyed upsert — the (coin, closed_at, seq) identity keeps both.
    early = [_open(at=T0)]
    t1 = T0 + timedelta(hours=1)
    late = [
        _close(at=t1, pnl="10"),  # completes the carried episode (seq 0)
        _open(at=t1),  # reopen in the same ms
        _close(at=t1, pnl="-5"),  # and close again in the same ms (seq 1)
    ]
    state = fold_state(extract_state(early), late)
    metrics = metrics_from_state(state, None)
    assert metrics.trade_count == 2
    assert metrics.win_rate == Decimal("0.5")
    assert metrics.realized_pnl == Decimal("5")
    assert [t.seq for t in state.round_trips] == [0, 1]


def _trip(coin: str, pnl: str, *, opened: datetime, closed: datetime, peak: str = "0") -> RoundTrip:
    return RoundTrip(
        coin=coin,
        pnl=Decimal(pnl),
        peak_notional=Decimal(peak),
        opened_at=opened,
        closed_at=closed,
    )


def test_reduce_trips_matches_the_state_reduction_on_the_trip_derived_fields() -> None:
    # The shared reducer is the single definition of the trip-derived metrics:
    # reducing a state's own trips through it must equal metrics_from_state on
    # every field a trip slice can produce (accumulators live only on the state).
    fills = [
        _open(at=T0),
        _close(at=T0 + timedelta(days=1), pnl="30", price="20"),
        _open(coin="SOL", at=T0 + timedelta(days=2)),
        _close(coin="SOL", at=T0 + timedelta(days=3), pnl="-10", price="5"),
    ]
    state = extract_state(fills)
    full = metrics_from_state(state, account_value=Decimal("1000"))
    trip = reduce_trips(list(state.round_trips), account_value=Decimal("1000"))

    assert trip.trade_count == full.trade_count
    assert trip.win_rate == full.win_rate
    assert trip.avg_win == full.avg_win
    assert trip.avg_loss == full.avg_loss
    assert trip.sharpe == full.sharpe
    assert trip.max_drawdown == full.max_drawdown
    assert trip.avg_leverage == full.avg_leverage
    assert trip.avg_hold_seconds == full.avg_hold_seconds
    assert trip.effective_coins == full.effective_coins


def test_reduce_trips_reduces_over_only_the_trips_it_is_given() -> None:
    # Windowing is just filtering the trip list: dropping the loss leaves a
    # perfect win rate over the one remaining trip, reduced by the same formulas.
    win = _trip("HYPE", "30", opened=T0, closed=T0 + timedelta(days=1))
    loss = _trip("SOL", "-10", opened=T0 + timedelta(days=2), closed=T0 + timedelta(days=3))

    both = reduce_trips([win, loss], None)
    assert both.trade_count == 2
    assert both.win_rate == Decimal("0.5")

    recent = reduce_trips([win], None)
    assert recent.trade_count == 1
    assert recent.win_rate == Decimal("1")
    assert recent.avg_loss is None


def test_reduce_trips_over_nothing_is_all_none() -> None:
    empty = reduce_trips([], None)
    assert empty.trade_count == 0
    assert empty.win_rate is None
    assert empty.avg_win is None
    assert empty.avg_hold_seconds is None
    assert empty.effective_coins is None
    assert empty.max_drawdown == Decimal("0")
