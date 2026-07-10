"""Fine-metric engine: a Trader's recent fills in, Metric Library values out.

Pure computation — no I/O, no clock. The closed-trade grouping rule comes from
the ansem-bullpen vetting R&D that produced the golden wallets: a *trade* is
all closing fills sharing one closing order, its PnL the sum of their
closedPnl (before fees). Definitions in plain language: docs/metrics.md.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import stdev

from epigone.gateway import Fill

TRADING_DAYS_PER_YEAR = 365  # perps trade every day; Sharpe annualizes over all of them


@dataclass(frozen=True)
class ClosedTrade:
    """One realized trade: every closing fill of a single closing order."""

    pnl: Decimal
    peak_notional: Decimal  # largest position value the closing fills reveal
    closed_at: datetime  # time of the trade's last closing fill


@dataclass(frozen=True)
class FineMetrics:
    """Fills-derived Metric Library values. None means "not computable from
    this fill history" (no trades, no losses, one active day, …) — the
    screener treats None as absent, never as zero."""

    trade_count: int
    win_rate: Decimal | None
    avg_win: Decimal | None
    avg_loss: Decimal | None  # positive magnitude
    sharpe: Decimal | None
    max_drawdown: Decimal  # USD depth of the worst realized-PnL fall
    avg_leverage: Decimal | None
    maker_share: Decimal | None
    realized_pnl: Decimal
    window_start: datetime | None  # first and last perp fill the metrics saw
    window_end: datetime | None


def compute_fine_metrics(fills: list[Fill], account_value: Decimal | None) -> FineMetrics:
    """Compute the fine Metric Library from a fill history (any order; the
    engine sorts). `account_value` (from the coarse pass) anchors the
    leverage estimate; without it avg_leverage is None."""
    perp = sorted((f for f in fills if f.is_perp), key=lambda f: f.time)
    trades = _close_trades(perp)
    wins = [t.pnl for t in trades if t.pnl > 0]
    losses = [t.pnl for t in trades if t.pnl < 0]
    return FineMetrics(
        trade_count=len(trades),
        win_rate=Decimal(len(wins)) / len(trades) if trades else None,
        avg_win=sum(wins, Decimal(0)) / len(wins) if wins else None,
        avg_loss=-sum(losses, Decimal(0)) / len(losses) if losses else None,
        sharpe=_sharpe(trades),
        max_drawdown=_max_drawdown(trades),
        avg_leverage=_avg_leverage(trades, account_value),
        maker_share=(Decimal(sum(1 for f in perp if not f.crossed)) / len(perp) if perp else None),
        realized_pnl=sum((t.pnl for t in trades), Decimal(0)),
        window_start=perp[0].time if perp else None,
        window_end=perp[-1].time if perp else None,
    )


def _close_trades(perp_fills_in_time_order: list[Fill]) -> list[ClosedTrade]:
    grouped: dict[int, list[Fill]] = defaultdict(list)
    for f in perp_fills_in_time_order:
        if f.closes_position:
            grouped[f.order_id].append(f)
    trades = [
        ClosedTrade(
            pnl=sum((f.closed_pnl for f in group), Decimal(0)),
            peak_notional=max(abs(f.start_position) * f.price for f in group),
            closed_at=group[-1].time,
        )
        for group in grouped.values()
    ]
    trades.sort(key=lambda t: t.closed_at)
    return trades


def _max_drawdown(trades: list[ClosedTrade]) -> Decimal:
    cumulative = peak = worst = Decimal(0)
    for trade in trades:
        cumulative += trade.pnl
        peak = max(peak, cumulative)
        worst = max(worst, peak - cumulative)
    return worst


def _sharpe(trades: list[ClosedTrade]) -> Decimal | None:
    """Annualized Sharpe of daily realized PnL. Days without a close count as
    zero — a trader realizing the same profit in fewer active days is streakier,
    not steadier. Needs two calendar days and nonzero variance."""
    if not trades:
        return None
    first, last = trades[0].closed_at.date(), trades[-1].closed_at.date()
    if first == last:
        return None
    by_day: dict[date, Decimal] = defaultdict(Decimal)
    for trade in trades:
        by_day[trade.closed_at.date()] += trade.pnl
    span = (last - first).days + 1
    daily = [float(by_day[first + timedelta(days=offset)]) for offset in range(span)]
    spread = stdev(daily)
    if spread == 0:
        return None
    mean = sum(daily) / len(daily)
    return Decimal(str(mean / spread * TRADING_DAYS_PER_YEAR**0.5))


def _avg_leverage(trades: list[ClosedTrade], account_value: Decimal | None) -> Decimal | None:
    """Estimated: each trade's peak position value against today's account
    value — fills carry no margin data, so this is the copyability signal,
    not the exchange's leverage setting."""
    if not trades or account_value is None or account_value <= 0:
        return None
    notionals = sum((t.peak_notional for t in trades), Decimal(0))
    return notionals / len(trades) / account_value
