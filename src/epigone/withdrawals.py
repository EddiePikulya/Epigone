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
the account worth exactly what it was. Scaling the previous observation's uPnL
by the fraction of the position still open therefore covers, in one expression,
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

**Funding and fees are unaccounted noise**, by decision. Both leave the account
in ways neither observation names, and over a single ten-second interval both
are orders of magnitude below the thresholds below — a $10M position pays
single-dollar funding in ten seconds, and even a whole-account taker close pays
fees measured in basis points against a threshold measured in quarters. They
are absorbed, not modelled.

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

    @property
    def fraction(self) -> Decimal:
        """The share of the prior account that left. Only ever built by
        `detect_withdrawal`, which never admits a non-positive prior equity."""
        return self.amount_usd / self.prior_equity


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
    outflow = _pnl_explained(snapshots, positions) - (equity_now - prior_equity)
    if outflow < WITHDRAWAL_FLOOR_USD:
        return None
    if outflow / prior_equity < WITHDRAWAL_FRACTION_THRESHOLD:
        return None
    return Withdrawal(
        amount_usd=outflow,
        prior_equity=prior_equity,
        equity_usd=equity_now,
        observed_at=previous.observed_at,
    )


def _pnl_explained(
    snapshots: Mapping[str, SnapshotState], positions: Sequence[Position]
) -> Decimal:
    """How much of the change in account value these two looks at the book
    account for: the unrealized PnL standing now, minus the part of the
    previous observation's unrealized PnL that is still standing. See the
    module docstring for why the second term is a fraction rather than the
    whole."""
    current = {position.coin: position for position in positions}
    now_pnl = sum((position.unrealized_pnl for position in positions), Decimal(0))
    retained = sum(
        (_retained_pnl(snapshot, current.get(coin)) for coin, snapshot in snapshots.items()),
        Decimal(0),
    )
    return now_pnl - retained


def _retained_pnl(snapshot: SnapshotState, position: Position | None) -> Decimal:
    """The part of one coin's previously observed unrealized PnL that is still
    unrealized — the rest was realized by the size that left, which is why it
    no longer shows up as a change in account value.

    Gone entirely, or flipped to the other side, retains nothing: the whole
    prior leg closed. A bigger position retains all of it, since size added
    enters at its own price carrying no PnL. A smaller one retains the fraction
    still open, measured in COIN UNITS (#155, ADR-0006) — notional would fold
    the price move into the size change and call a mark-down a partial close.
    A snapshot written before migration 0028 has no coin units to measure
    against, and there notional is the only ratio available; the error it
    carries is bounded by the price move over one poll interval, well inside
    the thresholds."""
    if position is None or snapshot.side != position.side.value:
        return Decimal(0)
    if snapshot.size_coin is not None and position.size_coin is not None:
        before, after = snapshot.size_coin, position.size_coin
    else:
        before, after = snapshot.size_usd, position.size_usd
    if before <= 0:
        return snapshot.unrealized_pnl
    return snapshot.unrealized_pnl * min(Decimal(1), after / before)
