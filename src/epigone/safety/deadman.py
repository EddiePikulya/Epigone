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
                    self.state = INELIGIBLE
                    await self._audit.record_event(
                        actor=WATCHDOG_ACTOR,
                        action="deadman_ineligible",
                        risk_decision="eligibility probe: scheduleCancel volume-gated",
                        detail={"exchange": exc.message},
                        master_address=self._master_address,
                    )
                self._next_attempt_at = now + self._reprobe
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
            self._next_attempt_at = now + self._reprobe
            return
        if self.state != ACTIVE:
            self.state = ACTIVE
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
        # Push forward well before the horizon arrives: half-cadence leaves a
        # full half-horizon of slack for a slow cycle before a spurious fire.
        self._next_attempt_at = now + self._horizon / 2
