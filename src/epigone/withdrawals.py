"""Money leaving a tracked Trader's account, inferred from two observations of
it (issue #171, ADR-0007 "Out of A4 scope").

The 2026-07-29 research finding — 38% of vetted accounts had been substantially
emptied — as a push notification for every tracked wallet rather than a
copy-time gate for Leaders. A Trader who pulls most of their equity out has
stopped being the Trader anyone chose to follow, and that is worth knowing
within a poll interval rather than at the next screener run.

**What is inferred and what is observed.** Epigone never sees a transfer. It
sees what the account was WORTH at two instants (`epigone.trader_equity`) and
what it HELD at those same two instants (the poll pass's snapshots). The gap
between the change in worth and the change PnL accounts for is money that
entered or left. `ledgerUpdates` on the websocket reports transfers directly and
is the honest source for this; it is deliberately not used yet (#158, the WS
cutover) — noted here so the inference is understood as a stand-in, not as the
final design.

**The accounting.** Account value is collateral plus unrealized PnL, so between
two observations:

    net_outflow = pnl_explained − (equity_now − equity_before)

where `pnl_explained` is what those two observations say PnL did:

    pnl_explained = Σ uPnL now − Σ (uPnL before × the fraction still held)

The second term is the whole subtlety. Unrealized PnL that is still unrealized
is still in the equity figure; unrealized PnL on size that has since gone was
REALIZED, which moved it from the unrealized column into collateral and left
the account worth about what it was. Scaling the previous observation's uPnL by
the fraction of the position still open therefore handles, in one expression,
every case the ticket enumerates: a full close and a flip retain nothing, a
partial close retains its remainder, a scale-in retains all of it (the added
size enters at its own entry price with no PnL of its own), and a position
merely marking up or down retains all of it while `Σ uPnL now` moves.

Worked, for the cases that must not fire:

    pure drawdown   equity −300k, uPnL −300k  → explained −300k, outflow 0
    full close      equity flat, uPnL −50k realized
                    → retained 0, explained 0, outflow 0
    half a loser    equity flat, uPnL −120k → −60k, half the size gone
                    → retained −60k, explained 0, outflow 0

and for the case that must:

    a transfer      equity −300k, positions untouched
                    → explained 0, outflow 300k

**Where this is approximate, and what is done about it.** The realized PnL on
size that left was `(fill price − entry) × size`; what is subtracted above is
`(last observed mark − entry) × size`. The difference is the price move between
the last look and the fill, on the size that left — PnL that genuinely happened
and that neither observation can see, because by the second one the position is
gone and there is nothing left to read a price from. Note the direction: a
position closing into a falling market realized MORE loss than the last mark
said, so the drop looks unexplained, and an unexplained drop is what this module
calls a withdrawal. That is the false alert the ticket most wants not to exist —
"a losing trader is not a leaving trader" — and it is worst exactly when it is
most misleading, on a leveraged position force-closed during a violent move.

So departed size buys an allowance: the outflow must exceed what an ordinary
one-interval price move on the size that left could have realized
(`UNSEEN_MOVE_FRACTION`), or the pass says nothing. A pass where nothing closed
buys no allowance and is judged on the thresholds alone, which is the common
shape of a real transfer — a Trader who closes out and then withdraws does the
two things in different ten-second windows, and the withdrawing one has no
departed size to pay for.

Two residues remain named rather than modelled. **Funding and fees** leave the
account in ways neither observation describes; over ten seconds a $10M position
pays single-dollar funding and even a whole-account taker close pays basis
points, against thresholds measured in quarters. **A position opened and closed
entirely between two passes** leaves no trace in either observation and no
departed size to buy an allowance with; the poll interval is the only bound on
it, and the websocket's `ledgerUpdates` (#158) is the fix, not more arithmetic.

**Direction.** Only outflows are alerted. A deposit is the same arithmetic with
the opposite sign and is simply not news of this kind.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from epigone.gateway import Position
from epigone.position_diff import SnapshotState
from epigone.trader_equity import EquityObservation

# A net outflow alerts when it is BOTH a big share of the account and a real
# amount of money. Two gates rather than one because either alone misreads half
# the Universe: a percentage alone turns a $600 account emptying into a push
# notification, and a dollar figure alone says nothing about a whale routinely
# moving $2M between their own venues.
#
# 25% of prior equity (the ticket's starting proposal, kept): a quarter of an
# account leaving in one ten-second window is a decision, never noise. It is
# also comfortably above everything this module does not model — fees on even a
# whole-account taker close are basis points, funding over ten seconds is
# smaller still — so the threshold absorbs them rather than the arithmetic
# having to.
WITHDRAWAL_FRACTION_THRESHOLD = Decimal("0.25")

# $1,000 absolute floor: below it the alert cannot be worth a follower's
# attention whatever percentage it represents, because no one copies a $2k
# account's sizing. Deliberately not scaled to the follower's min-size floor
# (#10) — that floor judges a POSITION's notional, and a withdrawal has none.
WITHDRAWAL_FLOOR_USD = Decimal("1000")

# How old the previous observation may be for the delta to mean anything, in
# seconds — six poll intervals (epigone.stream.poller.POLL_INTERVAL_SECONDS is
# 10s; imported as a literal to keep this module free of the poller it feeds).
#
# The attribution above reads PnL from two snapshots of the book. Across a gap
# where nothing was watched, a position could have opened, run and closed
# entirely between them, realizing a loss no observation here can see — and an
# unexplained drop is exactly what this module calls a withdrawal. So a stale
# previous observation is not a baseline: the pass records the new equity, says
# nothing, and the NEXT pass judges against a figure from a moment Epigone was
# actually looking. Six intervals rather than one because the interval is a
# floor, not a promise: a saturated budget makes passes run back-to-back
# (docs/spec-defaults.md), and at weight 4 per wallet against the 20 weight/s
# send gate a pass costs ~0.2s per wallet — seconds at the accepted operating
# point, a minute only past ~300 wallets, which is an order of magnitude beyond
# the poll set this system is sized for. If a poll set ever did grow past that,
# this gate would go quiet rather than fire wrongly, which is the direction to
# fail in — but it would go quiet SILENTLY, so it is the thing to check first if
# withdrawal alerts ever stop arriving.
MAX_OBSERVATION_GAP_SECONDS = 60

# How far a coin may plausibly move between two poll passes, as a fraction of
# notional — the size of the PnL a close can have realized without either
# observation seeing it (see "Where this is approximate" above). Size that left
# is charged this much against the outflow before it counts as a withdrawal, so
# a position force-closed into a fast market cannot be reported as a transfer.
#
# 5% over ten seconds is deliberately generous. The house precedent is
# SCALE_SIGNIFICANCE_THRESHOLD's note that "over a 10s poll a real coin never
# moves 25% on price alone"; a fifth of that is a move Epigone has never
# observed in an interval and still leaves an ordinary close — a few tenths of a
# percent — charged almost nothing. It is a bound on the ORDINARY case by
# design: a genuine cascade beats it, and there this module goes quiet, which is
# the direction a notification about someone else's money should fail in.
UNSEEN_MOVE_FRACTION = Decimal("0.05")


@dataclass(frozen=True)
class Withdrawal:
    """Money that left a Trader's account between two observations of it.

    `amount_usd` is the net outflow — the drop in account value with the pass's
    own PnL taken out — never the raw drop. `prior_equity` and `equity_usd` are
    the two observations it sits between, so the alert can say what share of the
    account went and what is left; `observed_at` is when the earlier of the two
    was taken, which is the far end of the window the money left during."""

    amount_usd: Decimal
    prior_equity: Decimal
    equity_usd: Decimal
    observed_at: datetime


@dataclass(frozen=True)
class _PriorBook:
    """What the previous observation's positions still account for, after this
    one: the unrealized PnL still standing, and the notional that has gone.

    Two figures from one walk of the same snapshots, because they are answers to
    the same question — how much of each position is still there — and computing
    them apart would let the two drift over what counts as departed."""

    retained_pnl: Decimal
    departed_notional: Decimal


def detect_withdrawal(
    previous: EquityObservation | None,
    equity_now: Decimal,
    snapshots: Mapping[str, SnapshotState],
    positions: Sequence[Position],
    now: datetime,
) -> Withdrawal | None:
    """Whether this pass caught money leaving, judged from the observation it
    replaced and the two looks at the book that bracket it.

    `previous` is `record_equity`'s return value — None on a Trader's first
    ever pass, when there is no delta to take. `snapshots` are the diff's
    remembered positions (before this pass's changes are applied) and
    `positions` the fresh observation, i.e. exactly the pair
    `epigone.position_diff` decides events from: one look, one judgement, no
    second fetch that could disagree with the first.

    Pure, and deliberately so — it reads no database and writes none, so the
    thresholds can be exercised without a poll pass and the poller keeps
    ownership of the transaction."""
    if previous is None:
        return None
    if (now - previous.observed_at).total_seconds() > MAX_OBSERVATION_GAP_SECONDS:
        return None
    prior_equity = previous.account_value
    if prior_equity <= 0:
        # No denominator, so no "share of the account": an account worth
        # nothing has nothing to pull out of, and a negative one (bad debt) is
        # not a state to reason about a percentage from.
        return None
    book = _read_prior_book(snapshots, positions)
    now_pnl = sum((position.unrealized_pnl for position in positions), Decimal(0))
    explained = now_pnl - book.retained_pnl
    outflow = explained - (equity_now - prior_equity)
    if outflow < WITHDRAWAL_FLOOR_USD:
        return None
    if outflow / prior_equity < WITHDRAWAL_FRACTION_THRESHOLD:
        return None
    if outflow <= book.departed_notional * UNSEEN_MOVE_FRACTION:
        # Size left the book this interval, and a close prices itself at a
        # moment neither observation saw. An outflow no bigger than an ordinary
        # price move on that size is not distinguishable from the trade that
        # took it, so it is not reported as a transfer.
        return None
    return Withdrawal(
        amount_usd=outflow,
        prior_equity=prior_equity,
        equity_usd=equity_now,
        observed_at=previous.observed_at,
    )


def _read_prior_book(
    snapshots: Mapping[str, SnapshotState], positions: Sequence[Position]
) -> _PriorBook:
    """Walk the previous observation's positions against this one, asking of
    each how much of it is still there — the fraction that answers both what
    unrealized PnL is still unrealized and what notional has gone."""
    current = {position.coin: position for position in positions}
    retained_pnl = Decimal(0)
    departed_notional = Decimal(0)
    for coin, snapshot in snapshots.items():
        held = _fraction_still_held(snapshot, current.get(coin))
        retained_pnl += snapshot.unrealized_pnl * held
        departed_notional += snapshot.size_usd * (Decimal(1) - held)
    return _PriorBook(retained_pnl=retained_pnl, departed_notional=departed_notional)


def _fraction_still_held(snapshot: SnapshotState, position: Position | None) -> Decimal:
    """How much of one coin's previously observed position is still open, 0 to 1.

    Gone entirely, or flipped to the other side, holds nothing: the whole prior
    leg closed. A bigger position holds all of it — size added enters at its own
    price carrying no PnL of its own, and nothing departed. A smaller one holds
    the fraction still open, measured in COIN UNITS (#155, ADR-0006), because
    notional would fold the price move into the size change and read a mark-down
    as a partial close. A snapshot written before migration 0028 has no coin
    units to measure against and notional is then the only ratio available; the
    error it carries is one interval's price move, the same residual the
    departed-size allowance exists to cover."""
    if position is None or snapshot.side != position.side.value:
        return Decimal(0)
    if snapshot.size_coin is not None and position.size_coin is not None:
        before, after = snapshot.size_coin, position.size_coin
    else:
        before, after = snapshot.size_usd, position.size_usd
    if before <= 0:
        return Decimal(1)
    return min(Decimal(1), after / before)
