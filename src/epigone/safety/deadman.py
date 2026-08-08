"""The scheduleCancel UPGRADE PATH (issue #135) — implemented, probed,
never depended on.

The protocol-native dead-man's switch is gated behind $1M cumulative traded
volume — verified live 2026-07-28 on a funded zero-volume account: "Cannot
set scheduled cancel time until enough volume traded. Required: $1000000.
Traded: $0." (both the set and clear forms). The operator sits at ~$58k, so
the PRIMARY switch is the watchdog process; this module is the belt-and-
braces layer that activates itself if the account ever qualifies.

The probe IS the activation: each attempt is a real scheduleCancel set at
`now + horizon`. A VOLUME_GATED reject means ineligible — re-probe on a slow
cadence (volume moves slowly). Success means the schedule is armed on the
exchange; from then on it is pushed forward at half-horizon cadence, so if
the whole host dies — watchdog included — the exchange cancels every resting
order within `horizon` on its own. Setting/pushing is free; a trigger is
burned (10/day budget) only when the schedule actually FIRES, which by
construction means nobody was alive to push it.

Every attempt rides the audited gateway (attempt/outcome rows); this module
adds EVENT rows only on state transitions, so "which mechanism is active" is
answerable from the trail (the ticket's requirement) without wading through
heartbeat pushes.

scheduleCancel cancels ORDERS only — positions are the unwind policy's
concern (docs/runbooks/halt-and-unwind.md), same as the watchdog's sweep.
"""

import logging
from datetime import datetime, timedelta

from epigone.clock import Clock
from epigone.gateway.execution import ActionRejectedError, RejectReason
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit

log = logging.getLogger(__name__)

UNKNOWN = "unknown"  # not yet probed (or an unexpected reject unsettled it)
INELIGIBLE = "ineligible"  # volume-gated: implemented-but-inactive, re-probing
ACTIVE = "active"  # armed on the exchange, being pushed forward


class DeadMansSwitch:
    """One instance per watchdog process, sharing the watchdog's audited
    gateway (same signer lane — a scheduleCancel is account-scoped, whoever
    signs it). `maintain()` is called once per watchdog cycle and does at
    most one exchange call; between due times it is a no-op."""

    def __init__(
        self,
        gateway: AuditedExecutionGateway,
        audit: ExecutionAudit,
        clock: Clock,
        *,
        horizon: timedelta,
        reprobe: timedelta,
        master_address: str,
    ) -> None:
        self._gateway = gateway
        self._audit = audit
        self._clock = clock
        self._horizon = horizon
        self._reprobe = reprobe
        self._master_address = master_address
        self.state = UNKNOWN
        self._next_attempt_at: datetime | None = None  # None → due now
        # When the schedule the exchange is KNOWN to hold would fire (issue
        # #212). Advanced only by a set that ACCEPTED: an ambiguous one may
        # not have landed and a reject certainly did not, so both leave the
        # older, earlier deadline standing. That is the conservative
        # direction for the one reader this exists for — the watchdog's
        # deadline-aware push, which must keep treating the schedule as
        # nearly lapsed rather than believe in a push that never happened.
        self._armed_until: datetime | None = None

    def armed_until(self) -> datetime | None:
        """When the standing schedule fires, or None if none is known to be
        armed (never probed, volume-gated, or never yet accepted). Deliberately
        NOT cleared once that instant passes: the switch does not observe the
        firing, and "the last schedule this account is known to have held ran
        out at T" is the true statement — a reader that cares whether one is
        still standing compares it to now, as both this module's own `_defer`
        and the watchdog's priority push do.

        THE OPAQUE KEEPALIVE SEAM'S SECOND HALF (issue #212). The watchdog
        holds this switch as a bare `Keepalive` — "something that must not go
        stale while a sweep grinds" — which is deliberately all it needs to
        know to PUSH. To push with PRIORITY it needs one thing more: how long
        the thing it is pushing has left. A method rather than a property so
        the seam is a plain callable the watchdog can hold beside
        `maintain`, without holding the switch itself."""
        return self._armed_until

    async def maintain(self) -> None:
        """Probe or push, whichever the state calls for, when due.

        An AmbiguousExecutionError propagates (the cycle logs it): the
        schedule MAY be armed; `_next_attempt_at` was not advanced, so the
        next cycle re-sets it — a repeated set replaces the schedule, so
        retrying is safe and self-reconciling."""
        now = self._clock.now()
        if self._next_attempt_at is not None and now < self._next_attempt_at:
            return
        self._gateway.decision = (
            "dead-man's switch upgrade path: probe/push scheduleCancel to "
            f"now+{int(self._horizon.total_seconds())}s (issue #135)"
        )
        try:
            await self._gateway.schedule_cancel(now + self._horizon)
        except ActionRejectedError as exc:
            if exc.reason is RejectReason.VOLUME_GATED:
                if self.state != INELIGIBLE:
                    # Event BEFORE state (PR #143 review): if the event write
                    # fails, the state stays unchanged and the next due probe
                    # retries both — a transition can never outrun its trail.
                    await self._audit.record_event(
                        actor=WATCHDOG_ACTOR,
                        action="deadman_ineligible",
                        risk_decision="eligibility probe: scheduleCancel volume-gated",
                        detail={"exchange": exc.message},
                        master_address=self._master_address,
                    )
                    self.state = INELIGIBLE
                self._defer(now)
                return
            # An unexpected reject: nothing new was armed — and if a
            # previously armed schedule still stands, it will FIRE within the
            # horizon (fail-safe: unattended orders die; in A3 there is no
            # executor to strand). Fall back to UNKNOWN, re-probe on the slow
            # cadence, and put the lapse on the trail so its last eligibility
            # event never claims "active" past the truth; the audited gateway
            # already recorded the reject itself.
            log.warning("dead-man's switch: unexpected scheduleCancel reject: %s", exc.message)
            if self.state != UNKNOWN:
                await self._audit.record_event(
                    actor=WATCHDOG_ACTOR,
                    action="deadman_unsettled",
                    risk_decision="unexpected scheduleCancel reject — eligibility unknown; "
                    "an already-armed schedule will fire at its horizon",
                    detail={"exchange": exc.message, "was": self.state},
                    master_address=self._master_address,
                )
                self.state = UNKNOWN
            self._defer(now)
            return
        # The set was ACCEPTED, so this is the schedule the exchange now
        # holds — recorded before the transition event, because it is true
        # of the exchange whether or not the trail write lands.
        self._armed_until = now + self._horizon
        if self.state != ACTIVE:
            # Event before state, same rule as the ineligible branch.
            await self._audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="deadman_activated",
                risk_decision="eligibility probe: scheduleCancel accepted — account "
                "crossed the $1M volume gate",
                detail={
                    "horizon_seconds": int(self._horizon.total_seconds()),
                    "armed_until": (now + self._horizon).isoformat(),
                },
                master_address=self._master_address,
            )
            self.state = ACTIVE
        # Push forward well before the horizon arrives: half-cadence leaves a
        # full half-horizon of slack for a slow cycle before a spurious fire.
        self._next_attempt_at = now + self._horizon / 2

    def _defer(self, now: datetime) -> None:
        """How long a REJECTED attempt waits before the next one — the slow
        eligibility cadence, unless a schedule this switch armed is still
        standing.

        The re-probe cadence answers "has this account crossed $1M yet",
        which moves in weeks; a standing schedule's horizon answers "is the
        last-resort net about to discharge", which moves in minutes. Deferring
        the second by the first is how a single unexpected reject used to
        silence this switch for six hours while the net it had already armed
        ran out — and, since issue #212, how the watchdog's budget-EXEMPT
        priority push could return without ever reaching the wire, logging an
        attempt that never happened. So while something stands, a reject
        leaves this switch DUE: the next call retries, at whatever cadence its
        caller runs (the watchdog's cycle, or a sweep's own pulse). The cost
        is bounded by the horizon — once the schedule lapses there is nothing
        left to lose and the slow cadence takes over again, which is the
        branch a volume-gated account, having never armed anything, takes from
        the start."""
        if self._armed_until is not None and now < self._armed_until:
            self._next_attempt_at = None
            log.warning(
                "dead-man's switch: retrying at once rather than in %dh — the "
                "schedule armed until %s UTC still stands and has %ds left",
                int(self._reprobe.total_seconds() // 3600),
                f"{self._armed_until:%Y-%m-%d %H:%M:%S}",
                int((self._armed_until - now).total_seconds()),
            )
            return
        self._next_attempt_at = now + self._reprobe
