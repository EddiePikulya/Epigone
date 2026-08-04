"""The v0 risk policy and every constant ADR-0007 said to record here.

Two things live in this module and they are deliberately together.

**The constants.** ADR-0007 settles several numbers as "starting proposal,
constant recorded at implementation". Scattering them through the executor
would make the ADR's decisions unreadable from the code and unchangeable
without a hunt; here each sits beside the reasoning that chose it, and each is
a one-line edit. Per-sub configurability is deliberately NOT offered for any
of them: the operator runs a handful of Leaders, and a knob nobody has asked
for is a second thing to get wrong.

**The policy.** The ticket requires "a conservative hardcoded v0 policy ships
inside A4; A5 replaces it with the real module" (#137). So this is a real
seam — a verdict object, recorded verbatim in `execution_audit.risk_decision`
— with a deliberately dumb implementation behind it. Swapping in A5's module
means replacing the class, not rewiring the executor.

THE POLICY ONLY EVER DECLINES RISK-INCREASING ACTIONS. That asymmetry runs
through the whole ADR: liveness gates entries and never exits (decision 7),
staleness guards risk-increasing actions and exempts risk-reducing ones
(decision 8), fills accept one-shot on entries and retry on exits (decision
5). A risk policy that could decline a close would be holding risk on a
technicality — the exact failure the asymmetry exists to prevent.
"""

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

# --- ADR-0007 constants, recorded at implementation --------------------------

# Decision 8. Age is measured now-minus-observed_at at claim time. Five
# minutes is 15–30× the normal 10–20s signal latency, so it never trips in
# healthy operation and always trips across a real outage — which is the only
# property a guard like this needs. Risk-INCREASING actions only.
ENTRY_STALENESS_GUARD = timedelta(minutes=5)

# Decision 7. An absolute floor, not a relative one: the relative form's
# denominator is exactly the stale stored equity this gate exists to distrust
# (stored values ran 2–6× live, 2026-07-29 research). Entries only; exits are
# never gated. Under fixed Base Notional sizing, Leader equity never enters
# sizing at all — this protects SIGNAL QUALITY, i.e. "is this still the trader
# whose stats earned the copy?".
LEADER_EQUITY_FLOOR = Decimal("10000")

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

# The exchange's own floor (RejectReason.MIN_NOTIONAL). Rounding a relative
# trim can produce a sliver below it; sending that sliver buys a guaranteed
# reject and an alarming audit row, so the executor skips it explicitly and
# says so instead.
MIN_ORDER_NOTIONAL = Decimal("10")

# The v0 policy's own ceilings. Conservative by intent: A4 is the first thing
# in this codebase that can open a position with real money, it runs on
# TESTNET until #137 merges, and the operator can raise these in one line once
# A5's real policy exists. The Base Notional ceiling matches the $100–400
# range ADR-0007 reasons about throughout; the allocation ceiling is what a
# single Leader's sub may hold, which — being the exchange-enforced exposure
# cap (decision 1) — is the largest amount one Leader can lose.
MAX_BASE_NOTIONAL_USD = Decimal("400")
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


@dataclass(frozen=True)
class RiskVerdict:
    """One policy decision. `decision` is prose because that is what
    `execution_audit.risk_decision` stores and what the operator reads in the
    trail — a code would need a lookup table nobody would maintain."""

    allowed: bool
    decision: str


class RiskPolicyV0:
    """The hardcoded conservative policy A4 ships with (#136); A5 (#137)
    replaces the class, not the seam.

    Every method returns a verdict even when it allows: an audit row saying
    "v0 policy: open $200 within the $400 Base Notional ceiling" is the record
    that something judged the action, which a bare absence of a decline is
    not."""

    def __init__(
        self,
        *,
        max_base_notional: Decimal = MAX_BASE_NOTIONAL_USD,
        max_allocation: Decimal = MAX_ALLOCATION_USD,
        min_order_notional: Decimal = MIN_ORDER_NOTIONAL,
    ) -> None:
        self._max_base = max_base_notional
        self._max_allocation = max_allocation
        self._min_notional = min_order_notional

    def judge_provisioning(
        self, *, allocation_usd: Decimal, base_notional_usd: Decimal
    ) -> RiskVerdict:
        """Judged before any money moves — the funding transfer is the first
        irreversible act of a new mapping, so the ceilings apply there rather
        than at the first order."""
        if allocation_usd > self._max_allocation:
            return RiskVerdict(
                False,
                f"v0 policy DECLINED: allocation ${allocation_usd} exceeds the "
                f"hardcoded ceiling ${self._max_allocation} (A4 v0; A5 owns the real limits)",
            )
        if base_notional_usd > self._max_base:
            return RiskVerdict(
                False,
                f"v0 policy DECLINED: base notional ${base_notional_usd} exceeds the "
                f"hardcoded ceiling ${self._max_base} (A4 v0; A5 owns the real limits)",
            )
        if base_notional_usd > allocation_usd:
            return RiskVerdict(
                False,
                f"v0 policy DECLINED: base notional ${base_notional_usd} exceeds the "
                f"allocation ${allocation_usd} — the first open could not be margined",
            )
        return RiskVerdict(
            True,
            f"v0 policy: allocation ${allocation_usd} / base ${base_notional_usd} within "
            f"the hardcoded ceilings (${self._max_allocation} / ${self._max_base})",
        )

    def judge_entry(self, *, notional_usd: Decimal, base_notional_usd: Decimal) -> RiskVerdict:
        """Every risk-increasing order: a fresh open, a scale-in, a flip's open
        leg. The size ceiling is the sub's own Base Notional rather than the
        global one, because a scale-in legitimately grows a position past a
        single Base Notional and the exchange's margin — not this policy — is
        what bounds the total (decision 2: "no separate aggregate cap needed
        in v0")."""
        if notional_usd < self._min_notional:
            return RiskVerdict(
                False,
                f"v0 policy DECLINED: ${notional_usd} is below the exchange's "
                f"${self._min_notional} minimum order value — the order would be rejected",
            )
        if base_notional_usd > self._max_base:
            return RiskVerdict(
                False,
                f"v0 policy DECLINED: this sub's base notional ${base_notional_usd} "
                f"exceeds the hardcoded ceiling ${self._max_base}",
            )
        return RiskVerdict(
            True,
            f"v0 policy: entry ${notional_usd} on a ${base_notional_usd} base notional, "
            f"within the hardcoded ceiling ${self._max_base}",
        )

    def judge_exit(self) -> RiskVerdict:
        """Exits are NEVER declined (module docstring). The method exists so
        the trail records that the policy saw the action and let it through —
        and so a future policy that wants to log exits has the hook."""
        return RiskVerdict(True, "v0 policy: exits are never declined — closing reduces risk")
