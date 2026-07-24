"""Fine-metric engine: a Trader's recent fills in, Metric Library values out.

Pure computation — no I/O, no clock. A *trade* is a completed position
round-trip (issue #58): from the fill that takes a coin's position off flat to
the fill that returns it to flat, with net PnL the sum of the episode's closing
fills' closedPnl (before fees). Partial trims realize PnL *inside* one trade,
never as trades of their own — so a wallet can't look prolific and accurate by
trimming a single winner many times. A round-trip only counts when both its
open and its full close are in captured history; a position opened before the
fill window is excluded rather than given partial credit — and the same
demotion applies when the walked net position disagrees with a fill's
startPosition (#63): missed executions never earn a reconstructed trade.
Definitions in plain language: docs/metrics.md.
"""

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from statistics import median, stdev

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
    finally closes — possibly many refreshes later (#11/#58). `net_position`
    is the signed size the walk left the position at, the anchor the next
    batch's continuity guard checks against (#63); 0 (the pre-#63 storage
    default) means "never verified" and can match no real continuation, so
    legacy episodes demote on their next fold instead of trusting a walk that
    may have been TWAP-blind."""

    coin: str
    opened_at: datetime
    pnl: Decimal
    peak_notional: Decimal
    net_position: Decimal = Decimal(0)


@dataclass(frozen=True)
class Continuation:
    """Delta-only transient: a batch's leading segment of an episode that was
    already open when the batch began (its opening fill is not in the batch).
    fold_states resolves it against the prior state's open episode for the
    coin — into a completed RoundTrip when `closed_at` is set, or into merged
    accumulators when the position is still open at batch end. A continuation
    with no matching open episode predates all captured history: excluded from
    the trade metrics, never partial credit (issue #58).

    `start_position` is the position before the segment's first fill; the fold
    only completes the carried episode when it equals the episode's stored
    net_position — anything else means executions were missed across the
    checkpoint, and the episode demotes to untracked (#63). `tracked` is False
    when the segment broke continuity *inside* the batch: the carried episode
    must still be popped (its position walk is dead), but never credited.
    `net_position` is where the walk left the coin when still open at batch
    end — the merged episode's new anchor."""

    coin: str
    pnl: Decimal
    peak_notional: Decimal
    closed_at: datetime | None  # None: still open at batch end
    start_position: Decimal = Decimal(0)
    net_position: Decimal = Decimal(0)
    tracked: bool = True


@dataclass(frozen=True)
class BatchLead:
    """Delta-only transient: where a batch's walk first touched a coin. The
    fold's boundary-continuity evidence (#63 review): a coin whose batch
    starts FLAT produces no Continuation, so without the lead a stored open
    episode was never compared against the batch at all — it survived as a
    zombie (its close was missed, or a cross-source same-ms interleave was
    merged wrong at the batch head) waiting to chain a later continuation
    into a chimera trade. fold_states demotes the stored episode whenever a
    lead arrives for its coin without a matching continuation, and distrusts
    what the walk minted inside that first millisecond (see _fold_episodes)."""

    coin: str
    first_fill_at: datetime


@dataclass(frozen=True)
class TripMetrics:
    """The Metric Library values reducible from a list of completed round-trips
    alone — every fine metric except the whole-history, fill-level accumulators
    (maker_share, realized_pnl). Because these derive purely from the trips, the
    same reducer computes them over ANY slice of the trip list: the whole store
    for the profile's all-time record, or the round-trips closed inside a
    window for the track-record toggle (#102). None means "not computable from
    these trips" — the screener/profile treat None as absent, never as zero."""

    trade_count: int
    win_rate: Decimal | None
    avg_win: Decimal | None
    avg_loss: Decimal | None  # positive magnitude
    sharpe: Decimal | None
    max_drawdown: Decimal  # USD depth of the worst realized-PnL fall
    avg_leverage: Decimal | None
    avg_hold_seconds: int | None  # mean round-trip duration; None with no completed trades
    effective_coins: Decimal | None  # inverse-HHI coin spread of round-trips; None with no trips
    median_trade: Decimal | None  # median PnL over ALL trips (can be negative); None with no trips
    profit_factor: Decimal | None  # gross wins ÷ gross losses; None with no losses (div-by-zero)
    top_trade_share: Decimal | None  # best trip's share of total PnL; None unless total PnL > 0


@dataclass(frozen=True)
class FineMetrics:
    """Fills-derived Metric Library values. None means "not computable from
    this fill history" (no trades, no losses, one active day, …) — the
    screener treats None as absent, never as zero.

    The trip-derived fields mirror TripMetrics one-for-one; the accumulator
    fields (maker_share, realized_pnl, the fill counts and window bounds) are
    the whole-history readings that no trip slice can reconstruct."""

    trade_count: int
    win_rate: Decimal | None
    avg_win: Decimal | None
    avg_loss: Decimal | None  # positive magnitude
    sharpe: Decimal | None
    max_drawdown: Decimal  # USD depth of the worst realized-PnL fall
    avg_leverage: Decimal | None
    maker_share: Decimal | None
    avg_hold_seconds: int | None  # mean round-trip duration; None with no completed trades
    effective_coins: Decimal | None  # inverse-HHI coin spread of round-trips; None with no trips
    median_trade: Decimal | None  # median PnL over ALL trips (can be negative); None with no trips
    profit_factor: Decimal | None  # gross wins ÷ gross losses; None with no losses (div-by-zero)
    top_trade_share: Decimal | None  # best trip's share of total PnL; None unless total PnL > 0
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
    `continuations` and `batch_leads` are *delta-only* transients:
    fold_states matches the continuations (leading segments of episodes
    opened before this batch) to the prior's open_episodes, and reconciles
    the leads (each coin's first touch) against stored episodes the batch
    contradicts (#63 review); a persisted state always has both empty."""

    round_trips: tuple[RoundTrip, ...]  # completed trades, time-sorted
    maker_fill_count: int  # maker (resting) perp fills seen
    perp_fill_count: int  # all perp fills seen (the maker_share denominator)
    realized_pnl: Decimal  # all realized closedPnl seen, attributable or not
    window_start: datetime | None  # first / last perp fill across all history
    window_end: datetime | None
    last_fill_at: datetime | None  # newest fill of any kind: the fetch checkpoint
    open_episodes: tuple[OpenEpisode, ...] = ()  # coins held non-flat, with accumulators
    continuations: tuple[Continuation, ...] = ()  # delta-only; resolved on fold
    batch_leads: tuple[BatchLead, ...] = ()  # delta-only; reconciled on fold


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
    round_trips, open_episodes, continuations, batch_leads = _episodes(perp)
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
        batch_leads=batch_leads,
    )


def fold_states(prior: FineState, delta: FineState) -> FineState:
    """Combine a prior state with a delta already reduced from the fills since
    the checkpoint (issue #11).

    The `delta` MUST be disjoint from what `prior` already saw — the ingest
    pass guarantees this by fetching only fills strictly after
    `prior.last_fill_at`, so the sums add cleanly and no fill is
    double-counted. The delta's continuations resolve against the prior's open
    episodes and its batch leads demote the stored episodes the batch
    contradicts (see _fold_episodes) — trips the delta minted inside a
    contradicted coin's first millisecond are dropped with the episode, never
    upserted. A delta round-trip otherwise replaces any prior one with the
    same (coin, closed_at, seq) identity, keeping a boundary re-fetch
    idempotent (the closing fill lands wholly on one side of the checkpoint)."""
    trips = {(t.coin, t.closed_at, t.seq): t for t in prior.round_trips}
    resolved, open_episodes, demoted_heads = _fold_episodes(prior, delta)
    kept = (t for t in delta.round_trips if demoted_heads.get(t.coin) != t.closed_at)
    for trip in (*resolved, *kept):
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
        # The delta's transients are resolved above; only the prior's own
        # (pre-history, unresolvable) ones ride forward.
        continuations=prior.continuations,
        batch_leads=prior.batch_leads,
    )


def fold_state(prior: FineState, new_fills: list[Fill]) -> FineState:
    """Reduce `new_fills` to a delta and fold it onto `prior` — the convenience
    form for callers holding raw fills (see fold_states for the invariant)."""
    return fold_states(prior, extract_state(new_fills))


def reduce_trips(trips: list[RoundTrip], account_value: Decimal | None) -> TripMetrics:
    """Reduce a list of completed round-trips to the trip-derived Metric Library
    values — the single definition of win rate, avg win/loss, Sharpe, max
    drawdown, avg size, avg hold and effective coins. `account_value` (from the
    coarse pass) anchors the leverage estimate; without it avg_leverage is None.

    The formulas live here and nowhere else: `metrics_from_state` reduces the
    whole store through this for the persisted fine metrics, and the profile's
    track-record toggle (#102) reduces a windowed trip slice through the very
    same function, so a windowed reading can never drift from the engine's
    definitions."""
    wins = [t.pnl for t in trips if t.pnl > 0]
    losses = [t.pnl for t in trips if t.pnl < 0]
    return TripMetrics(
        trade_count=len(trips),
        win_rate=Decimal(len(wins)) / len(trips) if trips else None,
        avg_win=sum(wins, Decimal(0)) / len(wins) if wins else None,
        avg_loss=-sum(losses, Decimal(0)) / len(losses) if losses else None,
        sharpe=_sharpe(trips),
        max_drawdown=_max_drawdown(trips),
        avg_leverage=_avg_leverage(trips, account_value),
        avg_hold_seconds=(
            sum(t.hold_seconds for t in trips) // len(trips) if trips else None
        ),
        effective_coins=_effective_coins(trips),
        median_trade=_median_trade(trips),
        profit_factor=_profit_factor(trips),
        top_trade_share=_top_trade_share(trips),
    )


def metrics_from_state(state: FineState, account_value: Decimal | None) -> FineMetrics:
    """Reduce an accumulated FineState to Metric Library values. `account_value`
    (from the coarse pass) anchors the leverage estimate; without it
    avg_leverage is None. Recomputed in full each refresh, so leverage always
    reflects today's account value.

    The trip-derived metrics come from the shared `reduce_trips`; only the
    fill-level accumulators (maker_share, realized_pnl, the fill counts and the
    fill window) are read off the folded state itself."""
    trip = reduce_trips(list(state.round_trips), account_value)
    return FineMetrics(
        trade_count=trip.trade_count,
        win_rate=trip.win_rate,
        avg_win=trip.avg_win,
        avg_loss=trip.avg_loss,
        sharpe=trip.sharpe,
        max_drawdown=trip.max_drawdown,
        avg_leverage=trip.avg_leverage,
        maker_share=(
            Decimal(state.maker_fill_count) / state.perp_fill_count
            if state.perp_fill_count
            else None
        ),
        avg_hold_seconds=trip.avg_hold_seconds,
        effective_coins=trip.effective_coins,
        median_trade=trip.median_trade,
        profit_factor=trip.profit_factor,
        top_trade_share=trip.top_trade_share,
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


# The API's own startPosition strings carry float-representation dust (~1e-10
# relative, e.g. "3472099.9999999998" after a 3472100.0 position); a genuine
# missed execution is off by a whole fill. 1e-6 relative separates them —
# shared with the golden-wallet continuity check, which asserts through
# _breaks_continuity so the two can never diverge.
POSITION_DUST = Decimal("0.000001")


def _breaks_continuity(walked: Decimal, start: Decimal) -> bool:
    """True when a fill's startPosition disagrees with the walked net position
    beyond float dust — executions between the two were missed (#63)."""
    return abs(walked - start) > max(abs(start), Decimal(1)) * POSITION_DUST


def _is_flat(position: Decimal) -> bool:
    """Flat within dust: a full close from a dusty startPosition leaves a
    numerically non-zero residue (e.g. -2e-10) that is an artifact of the
    API's own float strings, not a real position — exact-zero flatness would
    read it as a flip and persist a phantom dust episode (#63 review). The
    absolute floor matches _breaks_continuity's tolerance near zero."""
    return abs(position) <= POSITION_DUST


def _post_position(f: Fill) -> Decimal:
    """The signed position after a perp fill. A closing fill moves toward (or
    through) 0 by its size; an opening fill moves away on its named side —
    the only place a direction string decides arithmetic."""
    if f.closes_position:
        return f.start_position + (f.size if f.start_position < 0 else -f.size)
    return f.start_position + (f.size if "Long" in f.direction else -f.size)


def _episodes(
    perp_fills_in_time_order: list[Fill],
) -> tuple[
    tuple[RoundTrip, ...],
    tuple[OpenEpisode, ...],
    tuple[Continuation, ...],
    tuple[BatchLead, ...],
]:
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
    is `start − sign(start)·size` (_post_position — which consults the
    direction string only for opens, where no other side signal exists).

    The walk carries each coin's net position fill to fill, and every fill's
    startPosition must agree with it (#63): a disagreement beyond float dust
    means executions were missed (a fill source the fetch didn't cover, or
    history truncated at the ~2000 cap), so the in-flight episode demotes to
    untracked — the pre-window-open treatment: its closes bank realized_pnl,
    but it never completes a RoundTrip. The walk re-anchors at the offending
    fill's startPosition; tracking resumes at the next verified flat (a fresh
    open from 0, or a flip's far side, whose position the walk knows exactly)."""
    by_coin: dict[str, list[Fill]] = defaultdict(list)
    for f in perp_fills_in_time_order:  # already time-sorted, so each coin's is too
        by_coin[f.coin].append(f)
    trips: list[RoundTrip] = []
    open_episodes: list[OpenEpisode] = []
    continuations: list[Continuation] = []
    leads: list[BatchLead] = []
    for coin, fills in by_coin.items():
        leads.append(BatchLead(coin, fills[0].time))
        # A first fill on a non-flat position continues an episode from before.
        continuing = not _is_flat(fills[0].start_position)
        cont_start = fills[0].start_position
        opened_at: datetime | None = None
        tracked = True  # False while walking a position whose episode was demoted
        pnl = Decimal(0)
        peak = Decimal(0)
        walked: Decimal | None = None  # net position after the previous fill
        # Ordinal per completion millisecond (RoundTrip.seq): a same-block
        # close→reopen→close completes two episodes on one timestamp. The
        # continuation's close — always the coin's first — consumes ordinal 0,
        # which _fold_episodes assigns to the trade it resolves.
        close_seq: dict[datetime, int] = {}
        for f in fills:
            start = f.start_position
            if walked is not None and _breaks_continuity(walked, start):
                # Missed executions: demote the in-flight episode (#63). A
                # broken leading segment still rides the fold flagged
                # untracked, so the carried episode is popped but never
                # completed; an in-batch episode just drops.
                if continuing:
                    continuations.append(
                        Continuation(
                            coin, pnl, peak, closed_at=None,
                            start_position=cont_start, tracked=False,
                        )
                    )
                    continuing = False
                opened_at = None
                pnl = Decimal(0)
                peak = Decimal(0)
                tracked = _is_flat(start)  # flat re-anchors clean; mid-position stays demoted
            if not f.closes_position:
                if _is_flat(start) and opened_at is None and not continuing:
                    opened_at = f.time  # the position leaves 0: an episode opens
                walked = _post_position(f)
                continue  # a same-side scale-in never crosses 0
            pnl += f.closed_pnl
            peak = max(peak, abs(start) * f.price)
            end = _post_position(f)  # toward / through 0
            walked = end
            if not _is_flat(end) and (end > 0) == (start > 0):
                continue  # a partial trim that stays non-flat: episode continues
            # The episode closes here (full close or flip through 0).
            seq = close_seq.get(f.time, 0)
            close_seq[f.time] = seq + 1
            if continuing:
                continuations.append(
                    Continuation(coin, pnl, peak, closed_at=f.time, start_position=cont_start)
                )
                continuing = False
            elif tracked and opened_at is not None:
                trips.append(RoundTrip(coin, pnl, peak, opened_at, f.time, seq))
            # A flip immediately reopens on the far side; a full close goes
            # flat — either way the walk is at a verified position again, so
            # tracking resumes even after a demoted episode.
            opened_at = f.time if not _is_flat(end) else None
            tracked = True
            pnl = Decimal(0)
            peak = Decimal(0)
        if continuing:  # never closed in this batch: the accumulators ride the fold
            continuations.append(
                Continuation(
                    coin, pnl, peak, closed_at=None,
                    start_position=cont_start, net_position=walked or Decimal(0),
                )
            )
        elif tracked and opened_at is not None:
            open_episodes.append(
                OpenEpisode(coin, opened_at, pnl, peak, net_position=walked or Decimal(0))
            )
    trips.sort(key=lambda t: (t.closed_at, t.coin, t.seq))
    open_episodes.sort(key=lambda e: e.coin)
    leads.sort(key=lambda lead: lead.coin)
    return tuple(trips), tuple(open_episodes), tuple(continuations), tuple(leads)


def _fold_episodes(
    prior: FineState, delta: FineState
) -> tuple[list[RoundTrip], tuple[OpenEpisode, ...], dict[str, datetime]]:
    """Resolve the delta's continuations against the prior's open episodes
    (issues #48/#58). A continuation that closed completes a round-trip whose
    net PnL and peak notional span both sides of the checkpoint; one still
    open merges its accumulators into the carried episode. A continuation with
    no matching open episode predates all known history and is dropped —
    excluded, never partial credit (realized_pnl still banked its fills).

    The match must also survive the continuity guard (#63): the continuation's
    start_position has to equal the episode's stored net_position, or
    executions were missed across the checkpoint (a TWAP-blind prior fold, a
    truncated fetch) and the episode demotes — dropped like a pre-history
    continuation rather than completed from a walk that skipped executions. A
    segment that broke continuity inside its own batch (tracked=False) demotes
    the same way.

    The delta's batch leads close the guard's boundary blind spot (#63
    review): a stored open episode whose coin the batch touched with a FLAT
    first fill (so no continuation arrived) is contradicted — its close was
    missed, or a cross-source same-ms interleave was merged wrong at the
    batch head — and demotes instead of surviving as a zombie. What the walk
    minted inside that contradicted first millisecond is equally
    unverifiable (the interleave could have put a close first): its trips are
    dropped by fold_states via the returned `demoted_heads` (coin → head
    timestamp), and a same-ms reopen the head also produced is not carried.
    A flat head at a LATER millisecond than any close it contradicts carries
    no such ambiguity — its fills are API truth — so trips and episodes the
    batch built beyond the head millisecond stand. The whole-history walk of
    a mis-merged head cannot see this contradiction (the wrong order is
    self-consistent), so the fold is deliberately better informed here than
    extract_state on the union would be."""
    open_eps = {e.coin: e for e in prior.open_episodes}
    resolved: list[RoundTrip] = []
    cont_coins = {c.coin for c in delta.continuations}
    demoted_heads: dict[str, datetime] = {}
    for lead in delta.batch_leads:
        if lead.coin in cont_coins or lead.coin not in open_eps:
            continue  # a non-flat head reconciles via its continuation below
        del open_eps[lead.coin]
        demoted_heads[lead.coin] = lead.first_fill_at
    for cont in delta.continuations:
        episode = open_eps.pop(cont.coin, None)
        if episode is None:
            continue
        if not cont.tracked or _breaks_continuity(episode.net_position, cont.start_position):
            continue  # missed executions across the checkpoint: demote (#63)
        pnl = episode.pnl + cont.pnl
        peak = max(episode.peak_notional, cont.peak_notional)
        if cont.closed_at is None:
            open_eps[cont.coin] = OpenEpisode(
                cont.coin, episode.opened_at, pnl, peak, net_position=cont.net_position
            )
        else:
            # seq 0: the continuation's close is by definition the coin's first
            # episode completion in its batch, so it held ordinal 0 there and
            # any same-ms in-batch trades were numbered after it.
            resolved.append(
                RoundTrip(cont.coin, pnl, peak, episode.opened_at, cont.closed_at, seq=0)
            )
    # A trip dropped at a demoted head marks its whole millisecond as a
    # possibly mis-merged close/reopen group: a reopen the head also minted is
    # as unverifiable as the trip, so it is not carried. Head opens WITHOUT a
    # same-ms completion carry no interleave ambiguity (a lone flat-start fill
    # is API truth) and ride forward normally.
    chimera_heads = {
        t.coin for t in delta.round_trips if demoted_heads.get(t.coin) == t.closed_at
    }
    open_eps.update(
        {
            e.coin: e
            for e in delta.open_episodes  # in-batch opens (incl. reopens)
            if not (e.coin in chimera_heads and e.opened_at == demoted_heads[e.coin])
        }
    )
    return resolved, tuple(sorted(open_eps.values(), key=lambda e: e.coin)), demoted_heads


def _effective_coins(trips: list[RoundTrip]) -> Decimal | None:
    """How many coins the wallet *effectively* plays: the inverse Herfindahl of
    its completed round-trips per coin (issue #95). With `s_i` each coin's share
    of trips, `1 / sum(s_i**2) == total**2 / sum(trips_i**2)` — one coin reads
    1.0, a 50/50 pair 2.0, ten coins evenly 10.0. Reduced from the per-coin round-trips
    behind Most played (#80). Chosen over top-coin share because it reads a
    two-ticker specialist as focused and shrugs off dust probes (one stray trip
    among fifty barely moves it). None with no trips: undefined over zero
    trades, never 0 or 1."""
    if not trips:
        return None
    by_coin: dict[str, int] = defaultdict(int)
    for trip in trips:
        by_coin[trip.coin] += 1
    total = len(trips)
    sum_sq = sum(count * count for count in by_coin.values())
    return Decimal(total * total) / sum_sq


def _median_trade(trips: list[RoundTrip]) -> Decimal | None:
    """The median net PnL across ALL completed round-trips — wins and losses
    together, so it can be negative (issue #113). Catches the coin-flipper whose
    typical trade earns nothing: a positive median over many trades is nearly
    unfakeable, immune to one lucky moonshot the way the mean is not. None with
    no trips — undefined over zero trades, never 0."""
    if not trips:
        return None
    return median(t.pnl for t in trips)


def _profit_factor(trips: list[RoundTrip]) -> Decimal | None:
    """Gross winning dollars ÷ gross losing dollars (issue #113): the edge a
    win rate hides — below 1 loses money no matter how often the wallet wins.
    NULL when there are no losses (the denominator is zero — an unbounded "∞"
    the screener renders as absent, not a huge number); an all-losses wallet has
    zero gross winnings and reads a real 0."""
    gross_win = sum((t.pnl for t in trips if t.pnl > 0), Decimal(0))
    gross_loss = -sum((t.pnl for t in trips if t.pnl < 0), Decimal(0))
    if gross_loss == 0:
        return None
    return gross_win / gross_loss


def _top_trade_share(trips: list[RoundTrip]) -> Decimal | None:
    """The best single trip's PnL as a fraction of total trip PnL (issue #113):
    a lottery-record detector — one moonshot carrying the whole record reads
    near 1.0, a repeatable edge stays low. Only meaningful when total PnL > 0;
    NULL otherwise (a negative or zero total makes the ratio meaningless, and a
    net-losing wallet has no "profit" to concentrate). Stored as a fraction."""
    if not trips:
        return None
    total = sum((t.pnl for t in trips), Decimal(0))
    if total <= 0:
        return None
    return max(t.pnl for t in trips) / total


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
