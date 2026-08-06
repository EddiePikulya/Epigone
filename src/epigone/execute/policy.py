"""The risk policy (issue #137, A5) and every constant ADR-0007 said to record
here.

Two things live in this module and they are deliberately together.

**The constants.** ADR-0007 settles several numbers as "starting proposal,
constant recorded at implementation". Scattering them through the executor
would make the ADR's decisions unreadable from the code and unchangeable
without a hunt; here each sits beside the reasoning that chose it, and each is
a one-line edit. The numbers an OPERATOR tunes live elsewhere on purpose — in
`risk_limits`, one row, re-read every cycle (epigone.execute.limits) — because
a limit you have to redeploy to change is a limit nobody changes during the
incident that needed it. The split is by who owns the number, not by how
important it is.

**The policy.** Orders sign as opaque hashes, so per-order risk can ONLY live
here: no exchange-side feature can express "not this coin, not this much".
A4 shipped a hardcoded conservative stand-in; this is the module that replaces
it, judging every risk-increasing order before it is signed and recording the
verdict verbatim in `execution_audit.risk_decision`.

THE POLICY ONLY EVER DECLINES RISK-INCREASING ACTIONS. That asymmetry runs
through the whole ADR: liveness gates entries and never exits (decision 7),
staleness guards risk-increasing actions and exempts risk-reducing ones
(decision 8), fills accept one-shot on entries and retry on exits (decision
5). A risk policy that could decline a close would be holding risk on a
technicality — the exact failure the asymmetry exists to prevent. Only the
halt outranks an exit, and denial prose says so: a denial never claims
something "did not exit", because nothing here can cause that.

THE THREE GATES, and what each is for:

- **The Liquidity Floor** (`judge_coin`) asks whether the MARKET is healthy
  enough to trade at all. It is an anti-extraction tripwire, not a coin
  preference — thin books are where a copied trade's counterparty can be the
  Leader themselves — so it is judged against the market as it is NOW, and
  exactly ONCE per Copy Episode, at the open that starts it. A live episode is
  never interrupted by it (scale-ins copy even after the coin goes thin) and
  never trapped by it (exits always sign).
- **Mirrored leverage** (`resolve_leverage`) turns the Leader's conviction
  into the copy's exposure, then caps it — by the operator's backstop and by
  the asset's own maximum, whichever binds first. The cap is what stops the
  Leader's leverage dial from being an attack surface: notional, and
  everything that scales with notional, is stake x leverage.
- **The stake caps** (`judge_entry`) bound the MARGIN behind one coin and one
  sub. They CLAMP rather than deny — a copy at reduced size still follows the
  Leader — and the one thing they cannot do is clamp below the exchange's own
  minimum order value, where the only honest answer is a denial.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from epigone.decimals import fixed_point
from epigone.execute.limits import RiskLimits
from epigone.gateway import MarketStats, Position

# --- ADR-0007 constants, recorded at implementation --------------------------

# Decision 8. Age is measured now-minus-observed_at at claim time. Five
# minutes is 15–30× the normal 10–20s signal latency, so it never trips in
# healthy operation and always trips across a real outage — which is the only
# property a guard like this needs. Risk-INCREASING actions only.
ENTRY_STALENESS_GUARD = timedelta(minutes=5)

# Decision 7. An absolute floor, not a relative one: the relative form's
# denominator is exactly the stale stored equity this gate exists to distrust
# (stored values ran 2–6× live, 2026-07-29 research). Entries only; exits are
# never gated. Leader equity never enters SIZING — under Base Stake the money
# at risk is the operator's constant — so this protects SIGNAL QUALITY, i.e.
# "is this still the trader whose stats earned the copy?".
# TEMPORARY (issue #193, revert guarded by issue #192): lowered from the real
# value, Decimal("10000"), to $100 for the shakedown period only, so the
# operator's own ~$300 mainnet wallet — the one leader whose signal timing they
# control — passes the entry gate and can drive the copy path on demand. This
# is a visible code change, not a knob, precisely so it cannot be left behind.
LEADER_EQUITY_FLOOR = Decimal("100")

# Decision 4. Every copy action is an IOC limit at mark with this cap.
# Fidelity-to-leader beats fill price at $100–400 notionals on a 10–20s-stale
# signal, and nothing entry-shaped is allowed to rest, so the cap is what
# bounds the cost of being aggressive.
SLIPPAGE_CAP = Decimal("0.01")  # 1%

# Decision 5, the exit half: a reduce-only remainder is retried a bounded
# number of times before the position is left alone and a reconciliation
# finding is raised. Reduce-only is what makes retrying structurally safe —
# it cannot over-close or reverse — so the only real question is when to stop
# asking. Three tries about ten seconds apart is ~30s of a market that will
# not absorb a ~$200 reduce-only IOC; past that the situation is pathological
# and wants a human, not a fourth order.
EXIT_RETRY_ATTEMPTS = 3
EXIT_RETRY_DELAY_SECONDS = 10.0

# The exchange's own floor (RejectReason.MIN_NOTIONAL), in POSITION dollars —
# the exchange judges order value, which under Base Stake sizing is stake ×
# leverage, never the stake alone. Rounding a relative trim can produce a
# sliver below it; sending that sliver buys a guaranteed reject and an alarming
# audit row, so the executor skips it explicitly and says so instead.
MIN_ORDER_NOTIONAL = Decimal("10")

# A FAT-FINGER GUARD ON THE FUNDING TRANSFER, and deliberately not an
# operator-tunable risk limit. What bounds RISK in A5 is the stake caps: they
# decide how much margin can be at work, and the allocation above that is idle
# collateral. But funding is the one irreversible money move in the whole
# provisioning path, and a mistyped `/copy 0x… 10000 100 mirror default` would
# move ten thousand dollars into a sub before any policy read a single order.
# So the ceiling survives A5 as what it always really was — a typo catcher —
# and stays a one-line constant rather than a knob, because an operator who
# genuinely wants a larger allocation is making a deliberate decision that
# should look like one.
MAX_ALLOCATION_USD = Decimal("2000")

# How often the executor re-verifies that a bracket-mode sub's triggers are
# actually resting on the book. Deliberately slower than the loop: the check
# costs a weight-20 frontendOpenOrders per bracket sub, where the reconcile's
# position read costs 2. It is the self-healing form of decision 9's (r1a)
# bracket re-placement — rather than detecting the /resume transition (which a
# restart would lose), the executor makes "every live position in a bracket
# sub has its triggers" a property it restores every minute, whatever removed
# them: a halt sweep, a partial fill, an operator's own cancel.
BRACKET_VERIFY_INTERVAL = timedelta(minutes=1)

# The per-sub leverage modes (amendment D-4). `mirror` follows the Leader's own
# leverage on the position; `fixed` takes the operator's number. Either answer
# is an ASK — the caps below still apply — which is why there is no third mode
# meaning "uncapped".
MIRROR_LEVERAGE = "mirror"
FIXED_LEVERAGE = "fixed"
LEVERAGE_MODES = (MIRROR_LEVERAGE, FIXED_LEVERAGE)


class UnpriceableStakeError(ValueError):
    """A stake or leverage that cannot express a position. Its own error rather
    than an assertion because both inputs cross a process boundary — the
    operator's `/copy`, and the Leader's own event — and neither is something
    this module gets to assume."""


def open_position_notional(stake_usd: Decimal, leverage: Decimal) -> Decimal:
    """The dollar POSITION a stake buys at a leverage (amendment D-4).

    One line, and it is its own function because it is the whole amendment:
    the configured dollars used to BE the position and are now the margin
    behind it. Every reader going stake → position comes through here — the
    exchange-minimum check below, and `pricing.open_size` — so the two numbers
    cannot be confused in one place and not another. Readers going the other
    way (a scale-in deriving its stake from a size the exchange already fixed,
    a fill report naming the stake behind what filled) divide instead, and are
    deliberately not forced through an inverse helper: their input is a
    measured size, not a configured stake.

    It lives HERE, beside MIN_ORDER_NOTIONAL, rather than in `pricing` where
    the rest of the sizing arithmetic sits, because `pricing` already depends
    on this module for ADR-0007's constants and the dependency runs one way.""" 
    if stake_usd <= 0:
        raise UnpriceableStakeError(f"stake {stake_usd} is not positive")
    if leverage <= 0:
        raise UnpriceableStakeError(f"leverage {leverage} is not positive")
    return stake_usd * leverage


@dataclass(frozen=True)
class RiskVerdict:
    """One policy decision. `decision` is prose because that is what
    `execution_audit.risk_decision` stores and what the operator reads in the
    trail — a code would need a lookup table nobody would maintain.

    `stake_usd` is what the policy GRANTS, which is not always what was asked:
    a cap clamps rather than refuses (`clamped` says so, and the prose carries
    both figures) because a copy at reduced size still follows the Leader.
    None on the judgements that grant no stake."""

    allowed: bool
    decision: str
    stake_usd: Decimal | None = None
    clamped: bool = False


@dataclass(frozen=True)
class LeverageChoice:
    """The leverage a copied open will run at, and which ceiling decided it.
    The binding constraint is carried, not recomputed, because the operator's
    notice and the audit row both want to say WHY 20x became 10x."""

    value: int
    asked: int
    reason: str

    @property
    def capped(self) -> bool:
        return self.value < self.asked


class LeverageUnknownError(ValueError):
    """A `mirror`-mode open whose event carries no Leader leverage.

    Raised rather than defaulted, for the reason `scale_fraction` refuses to
    invent coin units from a notional: every plausible default is a decision
    about position size that nobody made. 1x would silently shrink the copy to
    a tenth of the configured exposure; the backstop would silently maximise
    it. The caller SKIPS and says so, and the next event on that Leader is
    judged fresh."""


def clears_liquidity_floor(stats: MarketStats, limits: RiskLimits) -> bool:
    """Whether this market is healthy enough to start a copy in.

    BOTH halves must clear. A market can be churned by volume with nothing
    standing behind it, and it can carry stale open interest nobody is
    trading; either alone is a market where getting out costs more than the
    thesis was worth. A zero threshold passes everything, which is the
    operator turning that half off (`risk_limits`' own CHECK allows it
    deliberately — the floor is a default stance, not a cage)."""
    return (
        stats.day_notional_volume >= limits.floor_day_notional_usd
        and stats.open_interest_usd >= limits.floor_open_interest_usd
    )


def resolve_leverage(
    *,
    mode: str,
    fixed_leverage: int | None,
    leader_leverage: Decimal | None,
    asset_max_leverage: int,
    limits: RiskLimits,
) -> LeverageChoice:
    """The leverage a copied open runs at: the mode's ask, capped by the
    operator's backstop and by the asset's own ceiling — whichever is lowest.

    The Leader's fractional leverage rounds DOWN, the same conservative
    direction every size rounds: a Leader at 10.9x is mirrored at 10x, which
    can only make the copy's position smaller than theirs in proportion, never
    larger. Below 1x there is no order to place, so a Leader somehow reported
    under 1x mirrors at the venue's own minimum of 1."""
    if mode == FIXED_LEVERAGE:
        if fixed_leverage is None:  # pragma: no cover - the CHECK constraint forbids it
            raise LeverageUnknownError("fixed leverage mode with no configured leverage")
        asked = fixed_leverage
        source = f"fixed {fixed_leverage}x"
    else:
        if leader_leverage is None:
            raise LeverageUnknownError(
                "the event carries no leader leverage — a mirror-mode open cannot be sized"
            )
        asked = max(int(leader_leverage), 1)
        source = f"mirroring the leader's {leader_leverage}x"
    value = min(asked, limits.backstop_leverage, asset_max_leverage)
    if value == asked:
        reason = f"{source}, under every cap"
    elif limits.backstop_leverage <= asset_max_leverage:
        reason = f"{source} capped to {value}x by the operator's backstop"
    else:
        reason = f"{source} capped to {value}x by the asset's own maximum ({asset_max_leverage}x)"
    return LeverageChoice(value=value, asked=asked, reason=reason)


def committed_stake(positions: Iterable[Position]) -> Decimal:
    """How much MARGIN a sub currently has at risk, from the exchange's own
    numbers (`marginUsed`, or notional over leverage when the venue omits it).

    Read from live positions rather than from bookkeeping, which is decision
    10's self-damping principle applied to the caps: a cap measured against
    what we believe we hold drifts with every partial fill, and a cap measured
    against what the exchange says we hold cannot. It includes the operator's
    OWN positions in that sub, deliberately — the sub is one margin pool, and
    a cap that ignored half of what is using the margin would not bound
    anything."""
    return sum((position.margin for position in positions), Decimal(0))


def stake_headroom(
    *, coin_stake_used: Decimal, sub_stake_used: Decimal, limits: RiskLimits
) -> Decimal:
    """How much more margin this coin may take in this sub: the tighter of the
    two caps' remaining room, never negative.

    NO CROSS-SUB COORDINATION, stated rather than overlooked (§5's naive
    choice, carried from the 2026-07-29 wallet research): two Leaders in two
    subs holding the same BTC short look independent here and are not. A
    correlation-aware aggregate is the recorded known gap; what bounds the
    error today is that each sub's allocation is separately funded and
    exchange-enforced."""
    return max(
        min(
            limits.max_coin_stake_usd - coin_stake_used,
            limits.max_sub_stake_usd - sub_stake_used,
        ),
        Decimal(0),
    )


class RiskPolicy:
    """The A5 policy: judged before signing, verdict recorded verbatim.

    Every method returns a verdict even when it allows: an audit row saying
    "$100 of stake at 10x, within the per-coin cap" is the record that
    something judged the action, which a bare absence of a decline is not.

    The GLOBAL limits are passed per call rather than held: the executor
    re-reads `risk_limits` each cycle, and a policy holding its own copy would
    be judging this cycle's order against last restart's numbers.

    The class therefore holds NO configuration at all. A4's version took its
    ceilings as constructor arguments; A5 has exactly two kinds of number —
    the operator's, which arrive per call as `limits`, and ADR-0007's, which
    are module constants documented as one-line edits — and a third,
    per-instance kind would only be somewhere for the two to disagree."""

    def judge_provisioning(
        self, *, allocation_usd: Decimal, base_stake_usd: Decimal, limits: RiskLimits
    ) -> RiskVerdict:
        """Judged before any money moves — the funding transfer is the first
        irreversible act of a new mapping, so the ceilings apply there rather
        than at the first order.

        A Base Stake over a cap is refused here even though `judge_entry`
        would clamp it later, and the difference is intent: a clamp is the
        policy absorbing a Leader's behaviour, while a mapping configured
        above its own cap would be clamped on EVERY open — a copy that never
        does what the operator asked for. Better to say so before funding
        anything."""
        if allocation_usd > MAX_ALLOCATION_USD:
            return RiskVerdict(
                False,
                f"DECLINED: allocation ${fixed_point(allocation_usd)} exceeds the funding "
                f"ceiling ${fixed_point(MAX_ALLOCATION_USD)} — a typo catcher on the one "
                f"irreversible money move, not a risk limit (the stake caps are)",
            )
        if base_stake_usd > allocation_usd:
            return RiskVerdict(
                False,
                f"DECLINED: base stake ${fixed_point(base_stake_usd)} exceeds the allocation "
                f"${fixed_point(allocation_usd)} — the first open could not be margined",
            )
        if base_stake_usd > limits.max_coin_stake_usd:
            return RiskVerdict(
                False,
                f"DECLINED: base stake ${fixed_point(base_stake_usd)} exceeds the per-coin "
                f"stake cap ${fixed_point(limits.max_coin_stake_usd)} — every open would "
                f"be clamped (/limits coin_stake raises it)",
            )
        if base_stake_usd > limits.max_sub_stake_usd:
            return RiskVerdict(
                False,
                f"DECLINED: base stake ${fixed_point(base_stake_usd)} exceeds the per-sub "
                f"aggregate stake cap ${fixed_point(limits.max_sub_stake_usd)} — every "
                f"open would be clamped (/limits sub_stake raises it)",
            )
        return RiskVerdict(
            True,
            f"allocation ${fixed_point(allocation_usd)} / base stake "
            f"${fixed_point(base_stake_usd)} within the caps "
            f"(${fixed_point(limits.max_coin_stake_usd)} per coin, "
            f"${fixed_point(limits.max_sub_stake_usd)} per sub)",
        )

    def judge_coin(
        self, *, coin: str, stats: MarketStats | None, limits: RiskLimits
    ) -> RiskVerdict:
        """Is this coin a Copyable Coin — does its live market clear the
        Liquidity Floor?

        Deliberately NOT a curated list: which coins to trade is the Leader's
        decision, and Epigone's only veto is market health. Asked exactly once
        per Copy Episode, at the open that starts it.

        A coin with no market data is DENIED rather than waved through, and
        that asymmetry is deliberate: this gate only ever stops an ENTRY, and
        not entering costs a missed copy while entering blind costs money in
        exactly the market this gate exists to keep us out of. (The executor
        distinguishes "the read failed" — which defers the whole judgement to
        the next cycle — from "the venue lists no such market", which is
        this.)

        THE MISSING-MARKET DENIAL OUTRANKS THE FLOOR SWITCH, and that ordering
        is load-bearing rather than pedantic: the same read carries the asset's
        own leverage ceiling, so a coin with no entry in it cannot be SIZED
        either. Turning the floor off is the operator saying "trade thin
        markets", never "trade a market I have no data for"."""
        if stats is None:
            return RiskVerdict(
                False,
                f"DECLINED: {coin} has no live market data, so nothing can say whether it "
                f"clears the Liquidity Floor — nor what leverage the asset allows — "
                f"did not enter",
            )
        if limits.floor_disabled:
            return RiskVerdict(True, f"{coin}: the Liquidity Floor is OFF (both halves 0)")
        volume, oi = stats.day_notional_volume, stats.open_interest_usd
        if not clears_liquidity_floor(stats, limits):
            return RiskVerdict(
                False,
                f"DECLINED: {coin} is below the Liquidity Floor — 24h volume "
                f"${_round(volume)} (floor ${fixed_point(limits.floor_day_notional_usd)}), open "
                f"interest ${_round(oi)} (floor "
                f"${fixed_point(limits.floor_open_interest_usd)}). A thin book is "
                f"where a copied trade's counterparty can be the leader — did not enter",
            )
        return RiskVerdict(
            True,
            f"{coin} clears the Liquidity Floor: 24h volume ${_round(volume)}, open "
            f"interest ${_round(oi)}",
        )

    def judge_entry(
        self,
        *,
        coin: str,
        requested_stake_usd: Decimal,
        leverage: LeverageChoice,
        coin_stake_used: Decimal,
        sub_stake_used: Decimal,
        limits: RiskLimits,
    ) -> RiskVerdict:
        """Every risk-increasing order: a fresh open, a scale-in, a flip's open
        leg. Grants a stake — the asked one, or the headroom left under the
        caps — or denies.

        CLAMPING, NOT REFUSING, is the default answer to a cap. A Leader who
        scales into a position we are already near the cap on is still worth
        following at whatever size is left; refusing outright would leave the
        copy half-mirrored on a thesis that is still running. The one thing a
        clamp may not do is produce an order the exchange will refuse, so a
        grant whose POSITION value falls under the venue's minimum is a denial
        with that said plainly — and denials say "did not enter", never "did
        not exit", because nothing in here can stop an exit."""
        headroom = stake_headroom(
            coin_stake_used=coin_stake_used, sub_stake_used=sub_stake_used, limits=limits
        )
        granted = min(requested_stake_usd, headroom)
        clamped = granted < requested_stake_usd
        if granted <= 0:
            return RiskVerdict(
                False,
                f"DECLINED: no stake headroom left for {coin} — ${_round(coin_stake_used)} of "
                f"${fixed_point(limits.max_coin_stake_usd)} on this coin and "
                f"${_round(sub_stake_used)} of "
                f"${fixed_point(limits.max_sub_stake_usd)} in this sub are already "
                f"committed — did not enter, and nothing held was touched",
            )
        notional = open_position_notional(granted, Decimal(leverage.value))
        if notional < MIN_ORDER_NOTIONAL:
            return RiskVerdict(
                False,
                f"DECLINED: ${_round(granted)} of stake at {leverage.value}x is a "
                f"${_round(notional)} position, under the exchange's "
                f"${fixed_point(MIN_ORDER_NOTIONAL)} "
                f"minimum order value"
                + (
                    f" — the ask was ${fixed_point(requested_stake_usd)}, clamped to the "
                    f"stake headroom left "
                    f"({_caps(coin_stake_used, sub_stake_used, limits)}) — did not enter"
                    if clamped
                    else " — did not enter"
                ),
            )
        if clamped:
            return RiskVerdict(
                True,
                f"allowed-clamped: {coin} asked ${fixed_point(requested_stake_usd)} of "
                f"stake, given ${_round(granted)} — the headroom left under the caps "
                f"({_caps(coin_stake_used, sub_stake_used, limits)}). {leverage.reason}; "
                f"position ${_round(notional)}",
                stake_usd=granted,
                clamped=True,
            )
        return RiskVerdict(
            True,
            f"{coin}: ${fixed_point(granted)} of stake at {leverage.value}x — a "
            f"${_round(notional)} position, isolated. {leverage.reason}. Within the caps "
            f"({_caps(coin_stake_used, sub_stake_used, limits)})",
            stake_usd=granted,
        )

    def judge_exit(self) -> RiskVerdict:
        """Exits are NEVER declined (module docstring). The method exists so
        the trail records that the policy saw the action and let it through —
        and so a future policy that wants to log exits has the hook."""
        return RiskVerdict(True, "exits are never declined — closing reduces risk")


def _caps(coin_used: Decimal, sub_used: Decimal, limits: RiskLimits) -> str:
    """The two caps as the operator reads them in a verdict: what is already
    committed against what is allowed."""
    return (
        f"coin ${_round(coin_used)}/${fixed_point(limits.max_coin_stake_usd)}, "
        f"sub ${_round(sub_used)}/${fixed_point(limits.max_sub_stake_usd)}"
    )


def _round(value: Decimal) -> str:
    """A COMPUTED dollar figure as prose. Two places: these strings are read by
    a human in a Telegram message, and `0.010000000000001` is noise, not
    precision. A configured figure — a cap, a floor, the operator's own ask —
    goes through `fixed_point` instead, which drops the exponent form Postgres
    hands back (issue #185) without inventing cents the operator never set."""
    return f"{value:.2f}"


__all__ = [
    "BRACKET_VERIFY_INTERVAL",
    "ENTRY_STALENESS_GUARD",
    "EXIT_RETRY_ATTEMPTS",
    "EXIT_RETRY_DELAY_SECONDS",
    "FIXED_LEVERAGE",
    "LEADER_EQUITY_FLOOR",
    "LEVERAGE_MODES",
    "MAX_ALLOCATION_USD",
    "MIN_ORDER_NOTIONAL",
    "UnpriceableStakeError",
    "open_position_notional",
    "MIRROR_LEVERAGE",
    "SLIPPAGE_CAP",
    "LeverageChoice",
    "LeverageUnknownError",
    "RiskPolicy",
    "RiskVerdict",
    "clears_liquidity_floor",
    "committed_stake",
    "resolve_leverage",
    "stake_headroom",
]
