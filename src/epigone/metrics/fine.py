"""Fine-metric engine: a Trader's recent fills in, Metric Library values out.

Pure computation — no I/O, no clock. A *trade* is a completed position
round-trip (issue #58): from the fill that takes a coin's position off flat to
the fill that returns it to flat, with net PnL the sum of the episode's closing
fills' closedPnl (before fees). Partial trims realize PnL *inside* one trade,
never as trades of their own — so a wallet can't look prolific and accurate by
trimming a single winner many times. A round-trip only counts when both its
open and its full close are in captured history; a position opened before the
fill window is excluded rather than given partial credit. Definitions in plain
language: docs/metrics.md.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import stdev

from epigone.gateway import Fill

TRADING_DAYS_PER_YEAR = 365  # perps trade every day; Sharpe annualizes over all of them


@dataclass(frozen=True)
class RoundTrip:
    """One completed trade: a position's whole life from flat back to flat."""

    coin: str
    pnl: Decimal  # net over the episode: the sum of its closing fills' closedPnl
    peak_notional: Decimal  # largest position value the episode's closing fills reveal
    opened_at: datetime
    closed_at: datetime
    # (coin, closed_at, seq) is the trade's identity across refreshes (#11).
    # seq is the ordinal among the coin's episodes completing in the SAME
    # millisecond — a same-block close→reopen→close makes two trades sharing a
    # closed_at, and without the ordinal one would vanish in the fold's keyed
    # upsert (and the DB primary key). Same-ms groups never straddle a
    # checkpoint (the incremental fetch cuts on millisecond boundaries), so the
    # ordinal is stable across refreshes.
    seq: int = 0

    @property
    def hold_seconds(self) -> int:
        return int((self.closed_at - self.opened_at).total_seconds())


@dataclass(frozen=True)
class OpenEpisode:
    """A coin still held non-flat: the accumulating first half of a possible
    future round-trip. Carries the net PnL its trims have realized so far and
    the peak notional revealed, so the trade's totals are complete when it
    finally closes — possibly many refreshes later (#11/#58)."""

    coin: str
    opened_at: datetime
    pnl: Decimal
    peak_notional: Decimal


@dataclass(frozen=True)
class Continuation:
    """Delta-only transient: a batch's leading segment of an episode that was
    already open when the batch began (its opening fill is not in the batch).
    fold_states resolves it against the prior state's open episode for the
    coin — into a completed RoundTrip when `closed_at` is set, or into merged
    accumulators when the position is still open at batch end. A continuation
    with no matching open episode predates all captured history: excluded from
    the trade metrics, never partial credit (issue #58)."""

    coin: str
    pnl: Decimal
    peak_notional: Decimal
    closed_at: datetime | None  # None: still open at batch end


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
    avg_hold_seconds: int | None  # mean round-trip duration; None with no completed trades
    realized_pnl: Decimal
    perp_fill_count: int  # all perp fills seen — activity evidence for Bot vetting (#58)
    window_start: datetime | None  # first and last perp fill the metrics saw
    window_end: datetime | None


@dataclass(frozen=True)
class FineState:
    """The foldable accumulation of a Trader's fill history — everything the
    fine metrics reduce from, and exactly what persists between incremental
    refreshes (issue #11). Rebuilt from storage, folded with the fills since
    the last checkpoint, then reduced back to a FineMetrics.

    `round_trips` carries whole completed trades (#58). `realized_pnl` is the
    comprehensive banked-money sum — every closing fill's closedPnl, including
    trims of positions whose opens predate captured history — so it can exceed
    the sum of the counted round-trips' PnLs by exactly those unattributable
    partials. maker_share is a ratio over *all* perp fills, so its
    numerator/denominator accumulate here. `last_fill_at` is the checkpoint:
    the newest fill of any kind folded so far.

    An episode can straddle a checkpoint — opened in one batch, trimmed and
    closed across later ones — so `open_episodes` carries each still-open
    episode (open-time plus the net PnL and peak notional accumulated so far)
    across refreshes; the batch that finally closes it completes the trade.
    `continuations` is a *delta-only* transient: the leading segments of
    episodes opened before this batch, which fold_states matches to the
    prior's open_episodes; a persisted state always has it empty."""

    round_trips: tuple[RoundTrip, ...]  # completed trades, time-sorted
    maker_fill_count: int  # maker (resting) perp fills seen
    perp_fill_count: int  # all perp fills seen (the maker_share denominator)
    realized_pnl: Decimal  # all realized closedPnl seen, attributable or not
    window_start: datetime | None  # first / last perp fill across all history
    window_end: datetime | None
    last_fill_at: datetime | None  # newest fill of any kind: the fetch checkpoint
    open_episodes: tuple[OpenEpisode, ...] = ()  # coins held non-flat, with accumulators
    continuations: tuple[Continuation, ...] = ()  # delta-only; resolved on fold


EMPTY_STATE = FineState(round_trips=(), maker_fill_count=0, perp_fill_count=0,
                        realized_pnl=Decimal(0), window_start=None, window_end=None,
                        last_fill_at=None)


def extract_state(fills: list[Fill]) -> FineState:
    """Reduce a raw fill batch to a FineState. On its own this is a full
    history; folded onto a prior state it is a delta.

    Any macro order is fine — the engine sorts stably by time — but
    same-millisecond fills MUST already be in execution order relative to one
    another (the gateway contract): same-order and same-block fills share a
    timestamp, so the sort cannot break those ties, and _episodes reconstructs
    positions from the sequence."""
    perp = sorted((f for f in fills if f.is_perp), key=lambda f: f.time)
    round_trips, open_episodes, continuations = _episodes(perp)
    return FineState(
        round_trips=round_trips,
        maker_fill_count=sum(1 for f in perp if not f.crossed),
        perp_fill_count=len(perp),
        realized_pnl=sum((f.closed_pnl for f in perp if f.closes_position), Decimal(0)),
        window_start=perp[0].time if perp else None,
        window_end=perp[-1].time if perp else None,
        last_fill_at=max((f.time for f in fills), default=None),
        open_episodes=open_episodes,
        continuations=continuations,
    )


def fold_states(prior: FineState, delta: FineState) -> FineState:
    """Combine a prior state with a delta already reduced from the fills since
    the checkpoint (issue #11).

    The `delta` MUST be disjoint from what `prior` already saw — the ingest
    pass guarantees this by fetching only fills strictly after
    `prior.last_fill_at`, so the sums add cleanly and no fill is
    double-counted. The delta's continuations resolve against the prior's open
    episodes (see _fold_episodes); a delta round-trip replaces any prior one
    with the same (coin, closed_at, seq) identity, keeping a boundary re-fetch
    idempotent (the closing fill lands wholly on one side of the checkpoint)."""
    trips = {(t.coin, t.closed_at, t.seq): t for t in prior.round_trips}
    resolved, open_episodes = _fold_episodes(prior, delta)
    for trip in (*resolved, *delta.round_trips):
        trips[(trip.coin, trip.closed_at, trip.seq)] = trip
    return FineState(
        round_trips=tuple(sorted(trips.values(), key=lambda t: (t.closed_at, t.coin, t.seq))),
        maker_fill_count=prior.maker_fill_count + delta.maker_fill_count,
        perp_fill_count=prior.perp_fill_count + delta.perp_fill_count,
        realized_pnl=prior.realized_pnl + delta.realized_pnl,
        window_start=_earliest(prior.window_start, delta.window_start),
        window_end=_latest(prior.window_end, delta.window_end),
        last_fill_at=_latest(prior.last_fill_at, delta.last_fill_at),
        open_episodes=open_episodes,
        # The delta's continuations are resolved above; only the prior's own
        # (pre-history, unresolvable) ones ride forward.
        continuations=prior.continuations,
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
    trips = list(state.round_trips)
    wins = [t.pnl for t in trips if t.pnl > 0]
    losses = [t.pnl for t in trips if t.pnl < 0]
    return FineMetrics(
        trade_count=len(trips),
        win_rate=Decimal(len(wins)) / len(trips) if trips else None,
        avg_win=sum(wins, Decimal(0)) / len(wins) if wins else None,
        avg_loss=-sum(losses, Decimal(0)) / len(losses) if losses else None,
        sharpe=_sharpe(trips),
        max_drawdown=_max_drawdown(trips),
        avg_leverage=_avg_leverage(trips, account_value),
        maker_share=(
            Decimal(state.maker_fill_count) / state.perp_fill_count
            if state.perp_fill_count
            else None
        ),
        avg_hold_seconds=(
            sum(t.hold_seconds for t in trips) // len(trips) if trips else None
        ),
        realized_pnl=state.realized_pnl,
        perp_fill_count=state.perp_fill_count,
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


def _episodes(
    perp_fills_in_time_order: list[Fill],
) -> tuple[tuple[RoundTrip, ...], tuple[OpenEpisode, ...], tuple[Continuation, ...]]:
    """Reduce a time-ordered perp batch to position-episode accounting
    (issues #48/#58).

    A *position episode* per coin opens when the signed position leaves 0 and
    closes when it returns to 0; a flip through 0 closes one episode and opens
    the next. Along the way each episode accumulates the net closedPnl of its
    closing fills and the peak notional they reveal. An episode wholly inside
    the batch is a completed RoundTrip; one still open at batch end is an
    OpenEpisode; the leading segment of an episode already open at batch start
    (its first fill is non-flat) is a Continuation for fold_states to resolve.
    Only closing fills can end an episode, and a closing fill's post position
    is `start − sign(start)·size` — so no direction string is parsed here."""
    by_coin: dict[str, list[Fill]] = defaultdict(list)
    for f in perp_fills_in_time_order:  # already time-sorted, so each coin's is too
        by_coin[f.coin].append(f)
    trips: list[RoundTrip] = []
    open_episodes: list[OpenEpisode] = []
    continuations: list[Continuation] = []
    for coin, fills in by_coin.items():
        # A first fill on a non-flat position continues an episode from before.
        continuing = fills[0].start_position != 0
        opened_at: datetime | None = None
        pnl = Decimal(0)
        peak = Decimal(0)
        # Ordinal per completion millisecond (RoundTrip.seq): a same-block
        # close→reopen→close completes two episodes on one timestamp. The
        # continuation's close — always the coin's first — consumes ordinal 0,
        # which _fold_episodes assigns to the trade it resolves.
        close_seq: dict[datetime, int] = {}
        for f in fills:
            start = f.start_position
            if not f.closes_position:
                if start == 0 and opened_at is None and not continuing:
                    opened_at = f.time  # the position leaves 0: an episode opens
                continue  # a same-side scale-in never crosses 0
            pnl += f.closed_pnl
            peak = max(peak, abs(start) * f.price)
            end = start + (f.size if start < 0 else -f.size)  # toward / through 0
            if end != 0 and (end > 0) == (start > 0):
                continue  # a partial trim that stays non-flat: episode continues
            # The episode closes here (full close or flip through 0).
            seq = close_seq.get(f.time, 0)
            close_seq[f.time] = seq + 1
            if continuing:
                continuations.append(Continuation(coin, pnl, peak, closed_at=f.time))
                continuing = False
            elif opened_at is not None:
                trips.append(RoundTrip(coin, pnl, peak, opened_at, f.time, seq))
            # A flip immediately reopens on the far side; a full close goes flat.
            opened_at = f.time if end != 0 else None
            pnl = Decimal(0)
            peak = Decimal(0)
        if continuing:  # never closed in this batch: the accumulators ride the fold
            continuations.append(Continuation(coin, pnl, peak, closed_at=None))
        elif opened_at is not None:
            open_episodes.append(OpenEpisode(coin, opened_at, pnl, peak))
    trips.sort(key=lambda t: (t.closed_at, t.coin, t.seq))
    open_episodes.sort(key=lambda e: e.coin)
    return tuple(trips), tuple(open_episodes), tuple(continuations)


def _fold_episodes(
    prior: FineState, delta: FineState
) -> tuple[list[RoundTrip], tuple[OpenEpisode, ...]]:
    """Resolve the delta's continuations against the prior's open episodes
    (issues #48/#58). A continuation that closed completes a round-trip whose
    net PnL and peak notional span both sides of the checkpoint; one still
    open merges its accumulators into the carried episode. A continuation with
    no matching open episode predates all known history and is dropped —
    excluded, never partial credit (realized_pnl still banked its fills)."""
    open_eps = {e.coin: e for e in prior.open_episodes}
    resolved: list[RoundTrip] = []
    for cont in delta.continuations:
        episode = open_eps.pop(cont.coin, None)
        if episode is None:
            continue
        pnl = episode.pnl + cont.pnl
        peak = max(episode.peak_notional, cont.peak_notional)
        if cont.closed_at is None:
            open_eps[cont.coin] = OpenEpisode(cont.coin, episode.opened_at, pnl, peak)
        else:
            # seq 0: the continuation's close is by definition the coin's first
            # episode completion in its batch, so it held ordinal 0 there and
            # any same-ms in-batch trades were numbered after it.
            resolved.append(
                RoundTrip(cont.coin, pnl, peak, episode.opened_at, cont.closed_at, seq=0)
            )
    open_eps.update({e.coin: e for e in delta.open_episodes})  # in-batch opens (incl. reopens)
    return resolved, tuple(sorted(open_eps.values(), key=lambda e: e.coin))


def _max_drawdown(trips: list[RoundTrip]) -> Decimal:
    cumulative = peak = worst = Decimal(0)
    for trip in trips:
        cumulative += trip.pnl
        peak = max(peak, cumulative)
        worst = max(worst, peak - cumulative)
    return worst


def _sharpe(trips: list[RoundTrip]) -> Decimal | None:
    """Annualized Sharpe of daily realized PnL. Days without a completed trade
    count as zero — a trader realizing the same profit in fewer active days is
    streakier, not steadier. Needs two calendar days and nonzero variance."""
    if not trips:
        return None
    first, last = trips[0].closed_at.date(), trips[-1].closed_at.date()
    if first == last:
        return None
    by_day: dict[date, Decimal] = defaultdict(Decimal)
    for trip in trips:
        by_day[trip.closed_at.date()] += trip.pnl
    span = (last - first).days + 1
    daily = [float(by_day[first + timedelta(days=offset)]) for offset in range(span)]
    spread = stdev(daily)
    if spread == 0:
        return None
    mean = sum(daily) / len(daily)
    return Decimal(str(mean / spread * TRADING_DAYS_PER_YEAR**0.5))


def _avg_leverage(trips: list[RoundTrip], account_value: Decimal | None) -> Decimal | None:
    """Estimated: each trade's peak position value against today's account
    value — fills carry no margin data, so this is the copyability signal,
    not the exchange's leverage setting."""
    if not trips or account_value is None or account_value <= 0:
        return None
    notionals = sum((t.peak_notional for t in trips), Decimal(0))
    return notionals / len(trips) / account_value
