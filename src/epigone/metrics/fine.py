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

    order_id: int  # the closing order, the trade's identity across refreshes (#11)
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
    avg_hold_seconds: int | None  # mean completed-episode duration; None with no closed episodes
    realized_pnl: Decimal
    window_start: datetime | None  # first and last perp fill the metrics saw
    window_end: datetime | None


@dataclass(frozen=True)
class FineState:
    """The foldable accumulation of a Trader's fill history — everything the
    fine metrics reduce from, and exactly what persists between incremental
    refreshes (issue #11). Rebuilt from storage, folded with the fills since
    the last checkpoint, then reduced back to a FineMetrics.

    The trade list carries whole trades (one per closing order); maker_share is
    a ratio over *all* perp fills, so its numerator/denominator accumulate here
    rather than being recoverable from the trades. `last_fill_at` is the
    checkpoint: the newest fill of any kind folded so far.

    Holding time rides along the same disjoint-batch fold (issue #48). A
    *position episode* is the span a coin is non-flat; the mean completed
    episode duration survives folding as a running `hold_seconds_sum` +
    `hold_episode_count` (like maker_fill_count). An episode can straddle a
    checkpoint — opened in one batch, closed in the next — so `open_episodes`
    carries each still-open episode's open-time (coin → opened_at) across
    refreshes; a close arriving later resolves against it. `dangling_closes`
    is a *delta-only* transient: closes of episodes opened before this batch,
    which fold_states matches to the prior's open_episodes; a persisted state
    always has it empty."""

    trades: tuple[ClosedTrade, ...]  # one per closing order, time-sorted
    maker_fill_count: int  # maker (resting) perp fills seen
    perp_fill_count: int  # all perp fills seen (the maker_share denominator)
    window_start: datetime | None  # first / last perp fill across all history
    window_end: datetime | None
    last_fill_at: datetime | None  # newest fill of any kind: the fetch checkpoint
    hold_seconds_sum: int = 0  # summed duration of completed episodes
    hold_episode_count: int = 0  # completed episodes (the mean's denominator)
    open_episodes: tuple[tuple[str, datetime], ...] = ()  # coin → open-time, still open
    dangling_closes: tuple[tuple[str, datetime], ...] = ()  # delta-only; resolved on fold


EMPTY_STATE = FineState(trades=(), maker_fill_count=0, perp_fill_count=0,
                        window_start=None, window_end=None, last_fill_at=None)


def extract_state(fills: list[Fill]) -> FineState:
    """Reduce a raw fill batch (any order; the engine sorts) to a FineState.
    On its own this is a full history; folded onto a prior state it is a
    delta."""
    perp = sorted((f for f in fills if f.is_perp), key=lambda f: f.time)
    hold_sum, hold_count, open_episodes, dangling = _episodes(perp)
    return FineState(
        trades=tuple(_close_trades(perp)),
        maker_fill_count=sum(1 for f in perp if not f.crossed),
        perp_fill_count=len(perp),
        window_start=perp[0].time if perp else None,
        window_end=perp[-1].time if perp else None,
        last_fill_at=max((f.time for f in fills), default=None),
        hold_seconds_sum=hold_sum,
        hold_episode_count=hold_count,
        open_episodes=open_episodes,
        dangling_closes=dangling,
    )


def fold_states(prior: FineState, delta: FineState) -> FineState:
    """Combine a prior state with a delta already reduced from the fills since
    the checkpoint (issue #11).

    The `delta` MUST be disjoint from what `prior` already saw — the ingest
    pass guarantees this by fetching only fills strictly after
    `prior.last_fill_at`, so the counts add cleanly and no fill is
    double-counted. Trades are keyed by closing order: a delta trade replaces
    any same-order prior one, which keeps a boundary re-fetch idempotent (a
    closing order's fills are contemporaneous, so an order lands wholly on one
    side of the checkpoint)."""
    trades = {t.order_id: t for t in prior.trades}
    trades.update((t.order_id, t) for t in delta.trades)
    hold_sum, hold_count, open_episodes = _fold_episodes(prior, delta)
    return FineState(
        trades=tuple(sorted(trades.values(), key=lambda t: t.closed_at)),
        maker_fill_count=prior.maker_fill_count + delta.maker_fill_count,
        perp_fill_count=prior.perp_fill_count + delta.perp_fill_count,
        window_start=_earliest(prior.window_start, delta.window_start),
        window_end=_latest(prior.window_end, delta.window_end),
        last_fill_at=_latest(prior.last_fill_at, delta.last_fill_at),
        hold_seconds_sum=hold_sum,
        hold_episode_count=hold_count,
        open_episodes=open_episodes,
        # The delta's dangling closes are resolved into the accumulators above;
        # only the prior's own (pre-history, unresolvable) ones ride forward.
        dangling_closes=prior.dangling_closes,
    )


def fold_state(prior: FineState, new_fills: list[Fill]) -> FineState:
    """Reduce `new_fills` to a delta and fold it onto `prior` — the convenience
    form for callers holding raw fills (see fold_states for the invariant)."""
    return fold_states(prior, extract_state(new_fills))


def metrics_from_state(state: FineState, account_value: Decimal | None) -> FineMetrics:
    """Reduce an accumulated FineState to Metric Library values. `account_value`
    (from the coarse pass) anchors the leverage estimate; without it
    avg_leverage is None. Recomputed in full each refresh, so leverage always
    reflects today's account value."""
    trades = list(state.trades)
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
        maker_share=(
            Decimal(state.maker_fill_count) / state.perp_fill_count
            if state.perp_fill_count
            else None
        ),
        avg_hold_seconds=(
            state.hold_seconds_sum // state.hold_episode_count
            if state.hold_episode_count
            else None
        ),
        realized_pnl=sum((t.pnl for t in trades), Decimal(0)),
        window_start=state.window_start,
        window_end=state.window_end,
    )


def compute_fine_metrics(fills: list[Fill], account_value: Decimal | None) -> FineMetrics:
    """Compute the fine Metric Library from a full fill history (any order; the
    engine sorts) — the full-pull path and every non-incremental caller."""
    return metrics_from_state(extract_state(fills), account_value)


def _latest(a: datetime | None, b: datetime | None) -> datetime | None:
    return max(a, b) if a is not None and b is not None else (a or b)


def _earliest(a: datetime | None, b: datetime | None) -> datetime | None:
    return min(a, b) if a is not None and b is not None else (a or b)


# A sentinel open-time for the episode a batch inherits from before its window:
# a coin whose first fill is already non-flat continues an episode opened earlier
# (in the prior state, or truncated off the front of a full pull). Its true
# open-time isn't in this batch, so a close of it becomes a dangling_close for
# fold_states to resolve — never a duration computed from a missing open-time.
_CONTINUING = object()


def _episodes(
    perp_fills_in_time_order: list[Fill],
) -> tuple[int, int, tuple[tuple[str, datetime], ...], tuple[tuple[str, datetime], ...]]:
    """Reduce a time-ordered perp batch to holding-time accounting (issue #48).

    A *position episode* per coin opens when the signed position leaves 0 and
    closes when it returns to 0; a flip through 0 closes one episode and opens
    the next. Returns (summed completed duration in seconds, completed episode
    count, open episodes as coin→open-time, dangling closes as coin→close-time).
    Only closing fills can end an episode, and a closing fill's post position is
    `start − sign(start)·size` — so no direction string is parsed here."""
    by_coin: dict[str, list[Fill]] = defaultdict(list)
    for f in perp_fills_in_time_order:  # already time-sorted, so each coin's is too
        by_coin[f.coin].append(f)
    hold_sum = 0
    hold_count = 0
    open_episodes: dict[str, datetime] = {}
    dangling: list[tuple[str, datetime]] = []
    for coin, fills in by_coin.items():
        # A first fill on a non-flat position continues an episode from before.
        state: object | datetime | None = _CONTINUING if fills[0].start_position != 0 else None
        for f in fills:
            start = f.start_position
            if not f.closes_position:
                if start == 0 and state is None:
                    state = f.time  # the position leaves 0: an episode opens
                continue  # a same-side scale-in never crosses 0
            end = start + (f.size if start < 0 else -f.size)  # toward / through 0
            if end != 0 and (end > 0) == (start > 0):
                continue  # a partial close that stays non-flat: episode continues
            # The episode closes here (full close or flip through 0).
            if isinstance(state, datetime):
                hold_sum += int((f.time - state).total_seconds())
                hold_count += 1
            elif state is _CONTINUING:
                dangling.append((coin, f.time))
            # A flip immediately reopens on the far side; a full close goes flat.
            state = f.time if end != 0 else None
        if isinstance(state, datetime):
            open_episodes[coin] = state
        # A _CONTINUING left open needs no record: fold keeps the prior's open-time.
    return (
        hold_sum,
        hold_count,
        tuple(sorted(open_episodes.items())),
        tuple(dangling),
    )


def _fold_episodes(
    prior: "FineState", delta: "FineState"
) -> tuple[int, int, tuple[tuple[str, datetime], ...]]:
    """Combine the prior and delta holding-time accounting (issue #48). The
    delta's dangling closes — closes of episodes opened before its window —
    resolve against the prior's open episodes into completed durations; a
    dangling close with no matching open predates all known history and is
    dropped (the same truncation caveat as #11)."""
    hold_sum = prior.hold_seconds_sum + delta.hold_seconds_sum
    hold_count = prior.hold_episode_count + delta.hold_episode_count
    open_episodes = dict(prior.open_episodes)
    for coin, close_time in delta.dangling_closes:
        opened_at = open_episodes.pop(coin, None)
        if opened_at is not None:
            hold_sum += int((close_time - opened_at).total_seconds())
            hold_count += 1
    open_episodes.update(delta.open_episodes)  # in-batch opens (incl. reopened coins)
    return hold_sum, hold_count, tuple(sorted(open_episodes.items()))


def _close_trades(perp_fills_in_time_order: list[Fill]) -> list[ClosedTrade]:
    grouped: dict[int, list[Fill]] = defaultdict(list)
    for f in perp_fills_in_time_order:
        if f.closes_position:
            grouped[f.order_id].append(f)
    trades = [
        ClosedTrade(
            order_id=order_id,
            pnl=sum((f.closed_pnl for f in group), Decimal(0)),
            peak_notional=max(abs(f.start_position) * f.price for f in group),
            closed_at=group[-1].time,
        )
        for order_id, group in grouped.items()
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
