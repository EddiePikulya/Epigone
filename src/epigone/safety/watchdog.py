"""The watchdog: the PRIMARY dead-man's switch (issue #135, ADR-0005).

scheduleCancel — the protocol-native switch — is volume-gated out of reach
(deadman.py), so THIS process is what stands between a dead executor and a
book of unattended resting orders. Its whole design is independence from the
thing it watches:

- its own PROCESS (epigone.safety.main; the executor cannot take it down),
- its own AGENT KEY (the keystore's watchdog lane — its own nonce set, so a
  wedged executor signer cannot wedge the kill path),
- its own GATEWAY instance (one signer, one nonce lane — the execution
  seam's contract),
- and only the DB heartbeat row as its view of the executor (ADR-0002:
  processes meet in Postgres) — WITHOUT hard-depending on that database for
  the decision→wire path (below).

One cycle: beat own heartbeat (best-effort — a failed beat never skips the
protective work) → read halt state and executor liveness → trip on a stale
executor heartbeat → sweep whatever halt is active → verify, on a slow
cadence, that this watchdog's agent key is still approved ON-CHAIN
(extraAgents). A halt whose sweep isn't finished is re-swept every cycle
until it is. The sweep is ACCOUNT-WIDE: core plus every builder dex in the
live perpDexs listing (degrading to the covered POSITION_VENUES — a partial
sweep that says so — when the listing endpoint is down; partial coverage
never stamps swept_at).

AN INCIDENT DOES NOT TOUCH POSTGRES BEFORE THE WIRE (PR #143 rounds 2–5).
The failure modes are CORRELATED: the likeliest reason the executor is dead
is infrastructure trouble — precisely when a DB-dependent watchdog would be
blind. Rounds 2–4 bounded one hanging database leg at a time (command
timeout, then release, then rollback) and each time the same bug reappeared
one layer deeper — asyncpg's transaction exit awaits an UNTIMED
cancel_waiter before any per-op timeout arms — so round 5 removed the
dependency instead of bounding another leg:

- once an incident is DECLARED — a DB-blind window or a real stall trip —
  the cycle does no Postgres state work before the cancel pass reaches the
  wire: no heartbeat, no reads, no halt row, and the rate budget drops to
  its in-process bucket (FallbackBudget.incident_mode) without attempting
  the shared row. For a DB-BLIND incident the audit wrapper also defers
  its attempt row to after the call (AuditedExecutionGateway.wire_first) —
  fully Postgres-free to the wire, by construction. A real-stall trip,
  whose liveness reads answered that same cycle, deliberately KEEPS its
  bounded, best-effort write-ahead attempt row (round 6): against a
  healthy database the evidence is worth one plain bounded INSERT, and
  losing it to a crash-after-cancel would be a hole nothing else covers;
- everything durable — halt row, audit events, sweep verification — runs
  AFTER the cancel attempt, best-effort, under a hard real-time ceiling
  (DB_BLOCK_CEILING_SECONDS; safe because Pool.release is shielded, so a
  cancelled block's connection is still terminated within the round-4
  acquire budget). Reconciliation is bookkeeping and never gates
  protection: a failing reconcile keeps the incident open and the next
  cycle cancels again;
- NORMAL operation keeps its bounded DB touches (epigone.safety.db:
  connect, per-query, lock waits, release budget), the same wait_for
  ceiling around every state block of run_cycle, and a per-attempt ceiling
  inside the shared rate bucket itself (SharedWeightBudget's
  attempt_ceiling, wired in main.py — the seam every budget spend passes
  through, the gateway's pre-sign spend included), so even the leg the
  pool timeouts cannot reach (the mid-transaction rollback) costs a
  bounded cycle, never a hung loop that could starve the blind machinery
  of the cycles it needs;
- cannot-read-Postgres is ITSELF a trip: unreadable CONTINUOUSLY past
  `db_blind_after` — a failure streak, reset by any successful read, so
  one dropped connection after a long healthy cycle never trips it
  (default 3× the executor-stall threshold — a deliberate, conservative
  call: cancelling resting orders is cheap and recoverable, the executor
  re-places when it returns, and nothing here closes positions or spends —
  whereas sitting blind through a correlated outage is the exact incident
  this layer exists to prevent). On recovery the incident reconciles: the
  halt row under a DISTINCT headline ("DB-blind sweep" vs a real stall's
  "unrecorded trip"; a trip reconciled within its own cycle keeps the
  plain stall reason), plus a blind-window audit event for any window that
  spanned cycles, even when a standing halt is joined (durable in every
  case except a watchdog death inside the recovery-to-reconcile window —
  the runbook's one carve-out).

THE SWEEP NEVER TRUSTS A CANCEL'S WORD. `swept_at` is stamped only when a
FRESH enumeration of the book answers empty — cancel results, however clean,
don't count, and an AmbiguousExecutionError just means the next cycle
re-enumerates and re-cancels what actually remains (cancel-by-oid re-issued
against a dead order answers MISSING_ORDER, which the verify pass shrugs
at). Treating "the cancel call failed cleanly" as "no orders live" is the
silent-live-order hazard this module exists to not have.

Positions are NOT closed — the sweep applies the documented unwind policy
(v0: hold-and-alert, docs/runbooks/halt-and-unwind.md), records the open-
position snapshot on the halt row, and the monitor's halt alert carries it
to the operator.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway import (
    POSITION_VENUES,
    GatewayError,
    HyperliquidGateway,
    OpenOrder,
    Position,
    fetch_asset_ids,
)
from epigone.gateway.execution import CancelSpec
from epigone.safety import heartbeat
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.budget import FallbackBudget
from epigone.safety.db import SAFETY_DB_TIMEOUT_SECONDS
from epigone.safety.halt import (
    HOLD_POLICY,
    WATCHDOG_SOURCE,
    Halt,
    active_halt,
    mark_swept,
    request_halt,
)

log = logging.getLogger(__name__)

# Read-side weights, billed per venue call like the stream passes bill theirs
# (each consumer owns its billing; the values mirror stream/poller.py and
# stream/orders.py — change those, change these).
ORDERS_WEIGHT = 20  # frontendOpenOrders, per venue
POSITIONS_WEIGHT = 2  # clearinghouseState, per venue
META_WEIGHT = 20  # meta (per venue), perpDexs, extraAgents — the info default

# The hard REAL-TIME ceiling on every durable/state block of a cycle
# (round 5): asyncpg's per-op timeouts cannot reach a transaction exit — a
# ROLLBACK awaits an UNTIMED cancel_waiter before any command timeout arms —
# so instead of bounding one more library leg, every DB block is wrapped in
# asyncio.wait_for at this ceiling. The composition is safe: Pool.release is
# asyncio.shield'ed, so the cancelled block's connection is still terminated
# within the acquire budget (the round-4 pool). Real time, not the injected
# clock: this bounds actual awaits, and the fakes finish instantly.
DB_BLOCK_CEILING_SECONDS = 4 * SAFETY_DB_TIMEOUT_SECONDS

# A failed capability probe (network blip, info outage, unwritable verdict)
# retries on this short fuse instead of waiting out the full check interval —
# but never every cycle, which would burn 20 weight per 10s on a persistent
# outage. The fuse advances on EVERY failure shape (round 2 item 4): a bare
# TimeoutError must not re-fire the probe per-cycle just because it isn't a
# GatewayError.
CAPABILITY_RETRY = timedelta(minutes=5)


@dataclass(frozen=True)
class _BlindIncident:
    """An in-process trip that Postgres couldn't record: either the database
    was unreadable past the blind threshold (`db_blind=True`), or a REAL
    observed stall tripped but the halt row was unwritable (`db_blind=False`
    — the distinction the reconciled halt's headline must preserve, so the
    operator can tell them apart). Held until a cycle can write the halt
    row — the reconcile — so the state machinery and the operator alert
    catch up with what the wire already did."""

    since: datetime
    reason: str
    db_blind: bool


class Watchdog:
    """One cycle's logic, dependency-injected for tests; epigone.safety.main
    wires the real deps and loops it."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        clock: Clock,
        read_gateway: HyperliquidGateway,
        exec_gateway: AuditedExecutionGateway,
        audit: ExecutionAudit,
        budget: Budget,
        *,
        master_address: str,
        signer_address: str,
        executor_stale: timedelta,
        db_blind_after: timedelta,
        capability_interval: timedelta,
    ) -> None:
        self._pool = pool
        self._clock = clock
        self._read = read_gateway
        self._exec = exec_gateway
        self._audit = audit
        self._budget = budget
        self._master = master_address.lower()
        self._signer_address = signer_address.lower()
        self._executor_stale = executor_stale
        self._db_blind_after = db_blind_after
        self._capability_interval = capability_interval
        self._capable: bool | None = None  # last on-chain verdict; None = unchecked
        self._next_capability_at: datetime | None = None  # None → due now
        # Postgres-independence state (module docstring): the onset of the
        # CURRENT unbroken streak of liveness-read failures (None = the last
        # read succeeded — any success resets it, so the blind threshold
        # means "continuously unreadable", never "one failure landing long
        # after the last success"; round 3 item 3), the unrecorded trip
        # awaiting its halt row, and how many cancel passes ran unrecorded.
        self._db_failing_since: datetime | None = None
        self._blind: _BlindIncident | None = None
        self._blind_passes = 0

    async def run_cycle(self) -> None:
        now = self._clock.now()
        # INCIDENT-FIRST (round 5, the structural rule): an open incident
        # reaches the wire before this cycle touches Postgres AT ALL — no
        # beat, no reads, no state. Durable catch-up comes after the cancel.
        if self._blind is not None:
            await self._incident_cycle(now)
            return
        try:
            await asyncio.wait_for(
                heartbeat.beat(self._pool, heartbeat.WATCHDOG_PROCESS, now),
                DB_BLOCK_CEILING_SECONDS,
            )
        except Exception:
            # Best-effort beat (round 2 item 1c): losing the liveness signal
            # is bad; skipping the cycle's protective work over it is worse.
            log.exception("heartbeat write failed — continuing the protective cycle")
        try:
            halt, stall_reason = await asyncio.wait_for(
                self._read_liveness(now), DB_BLOCK_CEILING_SECONDS
            )
        except Exception:
            # Deliberately ANY failure of the liveness reads, not just
            # connection errors: a watchdog that cannot answer "is the
            # executor alive" is blind whatever the cause, and blind fails
            # protective (a spurious blind trip cancels recoverable orders;
            # the inverse mistake strands them).
            log.warning(
                "watchdog: Postgres cannot answer the liveness question", exc_info=True
            )
            self._handle_unreadable(now)
            if self._blind is not None:  # declared just now: cancel THIS cycle
                await self._incident_cycle(now, reconcile=False)  # reads just failed
            return
        self._db_failing_since = None  # any successful read breaks the streak
        if halt is None and stall_reason is not None:
            # A REAL stall is an incident too (round 5): the cancel goes to
            # the wire before the halt row is attempted — the executor is
            # already presumed dead (that IS the trip), and the sweep's
            # verify-by-enumeration catches anything racing the halt write.
            self._declare(_BlindIncident(since=now, reason=stall_reason, db_blind=False))
            log.error(
                "watchdog TRIPPED: %s — cancelling before any state write", stall_reason
            )
            await self._incident_cycle(now, declared_now=True)
            return
        if halt is not None and halt.swept_at is None:
            await self._sweep(halt)
        # After the protective work, never before it: the capability probe is
        # advisory, and its failures must not delay a sweep by one second.
        try:
            await self._verify_capability(now)
        except Exception:
            log.exception("capability probe failed; retrying next cycle")

    async def _read_liveness(self, now: datetime) -> tuple[Halt | None, str | None]:
        halt = await active_halt(self._pool)
        stall = None if halt is not None else await self._executor_stall(now)
        return halt, stall

    # --- incident operation (rounds 2–5) ---

    def _declare(self, incident: _BlindIncident) -> None:
        """Open an incident: from here until reconcile, cancel passes run
        with ZERO Postgres before the wire — the rate budget drops to its
        in-process bucket (FallbackBudget.incident_mode) instead of
        attempting the shared row, and the audit wrapper defers its attempt
        row to after the call (wire_first)."""
        self._blind = incident
        self._blind_passes = 0
        self._set_incident_posture(incident)

    def _clear_incident(self) -> None:
        self._blind = None
        self._blind_passes = 0
        # The failure streak dies with the incident (round 6 item 1): the
        # reconcile's successful writes ARE the interruption, so a later
        # blip must open a FRESH streak — carrying the old onset would
        # re-trip instantly with a false "without interruption" span in the
        # durable record.
        self._db_failing_since = None
        self._set_incident_posture(None)

    def _set_incident_posture(self, incident: _BlindIncident | None) -> None:
        # In-process test budgets (WeightBudget) lack the flag — and are
        # already Postgres-free, so there is nothing to switch off.
        if isinstance(self._budget, FallbackBudget):
            self._budget.incident_mode = incident is not None
        # Wire-first audit only while the database is actually UNREADABLE
        # (round 6 item 3): a real-stall trip's liveness reads answered this
        # very cycle, so its write-ahead attempt row would land and keeps
        # its evidential value — deferring it would open a carve-out (a
        # crash between cancel and deferred write, against a HEALTHY
        # database) that nothing else covers.
        self._exec.wire_first = incident is not None and incident.db_blind

    def _handle_unreadable(self, now: datetime) -> None:
        """A liveness read failed. Open (or age) the CONTINUOUS failure
        streak — any successful read resets it (round 3 item 3: one dropped
        connection after a long healthy cycle must never read as an outage)
        — and declare the blind incident once the streak outlives the
        threshold: past it, an unknowable executor is indistinguishable
        from a dead one, and blind fails protective."""
        if self._db_failing_since is None:
            self._db_failing_since = now  # a new failure streak opens
        blind_for = now - self._db_failing_since
        if blind_for <= self._db_blind_after:
            log.warning(
                "watchdog: Postgres unreadable for %ds unbroken (blind trip at %ds)",
                int(blind_for.total_seconds()),
                int(self._db_blind_after.total_seconds()),
            )
            return
        incident = _BlindIncident(
            since=now,
            reason=(
                f"Postgres unreadable without interruption since "
                f"{self._db_failing_since:%Y-%m-%d %H:%M:%S} UTC — blind for "
                f"{int(blind_for.total_seconds())}s > "
                f"{int(self._db_blind_after.total_seconds())}s; the executor's "
                f"liveness is unknowable, which is indistinguishable from death"
            ),
            db_blind=True,
        )
        self._declare(incident)
        log.error(
            "watchdog BLIND TRIP: %s — cancelling resting orders without halt state",
            incident.reason,
        )

    async def _incident_cycle(
        self, now: datetime, *, reconcile: bool = True, declared_now: bool = False
    ) -> None:
        """One cycle of a declared incident: THE WIRE FIRST — the cancel
        pass performs zero Postgres work (in-process budget, wire-first
        audit) — then the durable catch-up (halt row, events, sweep
        verification), best-effort and hard-ceilinged so no wedged rollback
        can stall the next cycle. `reconcile=False` skips the catch-up on
        cycles where the liveness reads JUST failed — the database has
        already answered the question. `declared_now` carries the explicit
        "this trip opened this very cycle" fact into the reconcile, which
        keys the plain-reason/no-window-event shape on it rather than
        inferring it from timestamp equality."""
        assert self._blind is not None
        kind = "DB-blind sweep" if self._blind.db_blind else "unrecorded trip"
        try:
            await self._unrecorded_pass(f"{kind}: {self._blind.reason}")
        except Exception:
            # An exchange-side failure must not block the reconcile attempt:
            # the halt row is valuable even while cancels are failing.
            log.exception("incident cancel pass failed; reconcile still attempted")
        if reconcile:
            await self._try_reconcile(now, declared_now=declared_now)

    async def _try_reconcile(self, now: datetime, *, declared_now: bool = False) -> None:
        """Close the incident's books, AFTER the wire: halt row + events
        under a hard real-time ceiling (a wedged transaction rollback hangs
        in asyncpg BEFORE any per-op timeout arms — the ceiling cancels the
        block, and the shielded pool release then terminates the wedged
        connection within the acquire budget). Reconciliation is BOOKKEEPING
        and must never gate protection (round 3 item 2): failure keeps the
        incident open and next cycle cancels again."""
        assert self._blind is not None
        try:
            halt = await asyncio.wait_for(
                self._settle_blind_debt(now, same_cycle_trip=declared_now),
                DB_BLOCK_CEILING_SECONDS,
            )
        except Exception:
            log.exception(
                "incident reconcile failed (bounded) — the incident stays open and "
                "the next cycle cancels again"
            )
            return
        if halt is not None and halt.swept_at is None:
            # The incident pass WAS the cancel; this sweep only verifies and
            # stamps — re-cancelling what the pass just killed would be
            # redundant wire work between two enumerations of the same book.
            await self._sweep(halt, skip_cancel=True)

    async def _unrecorded_pass(self, decision: str) -> None:
        """One cancel pass that Postgres cannot record, counted AFTER it
        completes: the counter is audit evidence (blind_window_reconciled
        reports it), so it counts completions, not attempts. The chosen
        error direction: a pass that dies mid-flight counts zero even if
        some cancels landed before the failure — the trail may UNDERcount
        blind activity, never claim passes that didn't finish. Welded to
        the pass here so a call site can neither forget nor miscount it."""
        await self._cancel_resting(decision)
        self._blind_passes += 1

    async def _settle_blind_debt(
        self, now: datetime, *, same_cycle_trip: bool = False
    ) -> Halt | None:
        """Postgres can record the incident: write the halt row (joining any
        halt that appeared meanwhile) under a headline that PRESERVES which
        trip it was — "DB-blind sweep" for the unreadable-database trip,
        "unrecorded trip" for a real observed stall — so the operator can
        tell them apart. A trip reconciled in the SAME cycle it was declared
        (the healthy-database normal case since round 5 put the cancel
        before the halt write) keeps the plain stall reason and skips the
        window event: nothing ran in the dark, and the trail's cancel rows
        landed normally. An incident that SPANNED cycles leaves the durable
        blind-window event whenever this reconcile runs (round 3 item 4):
        joining a standing halt writes no new halt state by design, and the
        window's own best-effort audit rows likely failed with the same
        outage. The one hole — the watchdog dying between DB recovery and
        this cycle, taking the in-process marker with it — is carved out in
        the runbook, not papered over here. The normal sweep machinery then
        verifies the book and stamps swept_at with the position snapshot."""
        assert self._blind is not None
        # A trip can only be "same cycle" via the explicit declared_now flag
        # from the trip branch (not timestamp equality — a frozen or
        # coarse clock must never turn a spanning window into a quiet trip).
        if same_cycle_trip:
            assert not self._blind.db_blind  # blind windows always span cycles
            headline = "trip"
            reason = self._blind.reason
        else:
            # The trip parenthetical says "unconfirmed", not "unwritable"
            # (round 6 item 4): a reconcile ceiling can fire BETWEEN the
            # halt row's commit and clearing the incident, in which case the
            # next cycle joins its own, successfully written row — the label
            # must be true for both that case and a genuinely failing write.
            headline = "DB-blind sweep reconciled" if self._blind.db_blind else (
                "unrecorded trip reconciled (halt row write had not been confirmed)"
            )
            reason = (
                f"{headline}: {self._blind.reason}; resting orders were being "
                f"cancelled unrecorded since "
                f"{self._blind.since:%Y-%m-%d %H:%M:%S} UTC; Postgres answered "
                f"again at {now:%Y-%m-%d %H:%M:%S} UTC"
            )
        halt, created = await request_halt(
            self._pool, self._clock, self._audit, source=WATCHDOG_SOURCE, reason=reason
        )
        if not same_cycle_trip:
            await self._audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="blind_window_reconciled",
                risk_decision=headline,
                detail={
                    "reason": self._blind.reason,
                    "blind_since": self._blind.since.isoformat(),
                    "recovered_at": now.isoformat(),
                    "unrecorded_cancel_passes": self._blind_passes,
                    "halt_id": halt.id,
                    "joined_standing_halt": not created,
                },
                master_address=self._master,
            )
            log.error(
                "watchdog: unrecorded window reconciled into halt #%d (joined=%s)",
                halt.id,
                not created,
            )
        elif created:
            log.error("watchdog: trip recorded as halt #%d", halt.id)
        self._clear_incident()
        return halt

    async def _executor_stall(self, now: datetime) -> str | None:
        """The trip condition: the executor HAS run (its row exists) and its
        heartbeat is older than the threshold. No row means no executor was
        ever deployed — nothing to protect, not an emergency (decommissioning
        deletes the row for the same reason; see the runbook)."""
        beaten = await heartbeat.last_beat(self._pool, heartbeat.EXECUTOR_PROCESS)
        if beaten is None:
            return None
        age = now - beaten
        if age <= self._executor_stale:
            return None
        return (
            f"executor heartbeat stale: {int(age.total_seconds())}s > "
            f"{int(self._executor_stale.total_seconds())}s"
        )

    # --- the sweep ---

    async def _sweep(self, halt: Halt, *, skip_cancel: bool = False) -> None:
        """Cancel-all with verify-by-enumeration (module docstring): cancel
        what rests, then list AGAIN over a fresh venue listing, and only an
        empty, COMPLETE-coverage listing stamps the sweep done — with the
        position snapshot and the unwind policy recorded. Any failure leaves
        the halt unswept for the next cycle; enumeration is idempotent and
        cancels tolerate re-issue.

        ACCOUNT-WIDE by construction (PR #143 review): every builder dex in
        the live perpDexs listing, not just POSITION_VENUES. (Sub-accounts
        are a different axis: agent-reachable per the #142 findings but
        separate ACCOUNTS, outside this master's book — the runbook carries
        that boundary until A5's risk policy forbids or includes them.)

        `skip_cancel` is the incident-reconcile shape (round 5): the
        incident's own pass already cancelled, so this call only enumerates
        and stamps — an order that somehow still rests leaves the halt
        unswept and the NEXT cycle's full sweep cancels it."""
        if skip_cancel:
            dexs, complete = await self._sweep_venues()
            orders = await self._open_orders(dexs)
        else:
            dexs, complete, cancelled = await self._cancel_resting(
                f"halt #{halt.id} ({halt.source}): {halt.reason}"
            )
            if cancelled:
                # Cancels went out: the deciding enumeration must be FRESH —
                # listing included, so a dex appearing mid-sweep can't hide
                # an order from the verify. An already-empty book needs no
                # second look (its first enumeration IS the verify).
                dexs, complete = await self._sweep_venues()
                orders = await self._open_orders(dexs)
            else:
                orders = []
        if orders or not complete:
            log.warning(
                "halt #%d sweep incomplete: %d order(s) resting, venue coverage %s; "
                "retrying next cycle",
                halt.id,
                len(orders),
                "complete" if complete else "PARTIAL (perpDexs unavailable)",
            )
            return
        positions = await self._open_positions(dexs)
        # Durable write, hard-ceilinged like every state block (round 5): a
        # partition striking mid-transaction here must cost a bounded cycle,
        # not a hung loop that can never reach the blind machinery.
        await asyncio.wait_for(
            mark_swept(
                self._pool,
                self._clock,
                self._audit,
                halt=halt,
                positions=[_position_json(p) for p in positions],
                unwind_policy=HOLD_POLICY,
            ),
            DB_BLOCK_CEILING_SECONDS,
        )
        log.error(
            "halt #%d swept: book empty across %d venue(s); %d open position(s) HELD "
            "per %s (docs/runbooks/halt-and-unwind.md)",
            halt.id,
            len(dexs) + 1,
            len(positions),
            HOLD_POLICY,
        )

    async def _cancel_resting(self, decision: str) -> tuple[list[str], bool, list[OpenOrder]]:
        """Enumerate account-wide and cancel whatever rests — the shared
        kernel of the normal sweep and the DB-blind sweep. No verification,
        no stamping: callers own what "done" means. Returns what it saw —
        (venues, coverage-complete, the orders it cancelled) — so the normal
        sweep can treat an already-empty first enumeration as its verify
        instead of re-billing a second one."""
        self._exec.decision = decision
        dexs, complete = await self._sweep_venues()
        orders = await self._open_orders(dexs)
        if orders:
            await self._exec.cancel_orders(await self._cancels_for(orders))
        return dexs, complete, orders

    async def _sweep_venues(self) -> tuple[list[str], bool]:
        """The builder-dex listing for an account-wide sweep — or, when the
        listing endpoint is down, the covered POSITION_VENUES with
        complete=False (round 2 item 3): a partial sweep that says so beats
        a total abort that cancels nothing, but partial coverage can never
        stamp swept_at."""
        await self._budget.spend(META_WEIGHT)
        try:
            return await self._read.get_perp_dexs(), True
        except Exception:
            fallback = [dex for dex in POSITION_VENUES if dex is not None]
            log.error(
                "perpDexs listing unavailable — sweep coverage degraded to the "
                "covered venues %s (PARTIAL; swept_at withheld)",
                fallback,
                exc_info=True,
            )
            return fallback, False

    async def _cancels_for(self, orders: list[OpenOrder]) -> list[CancelSpec]:
        # Map exactly the dexs the enumerated orders sit on (namespaced
        # `dex:COIN` coins), plus the core universe fetch_asset_ids always
        # reads. Billing: core meta + perpDexs (when any dex is needed) +
        # one meta per needed dex.
        needed = sorted({order.coin.split(":", 1)[0] for order in orders if ":" in order.coin})
        spends = 1 + (1 + len(needed) if needed else 0)
        for _ in range(spends):
            await self._budget.spend(META_WEIGHT)
        asset_ids = await fetch_asset_ids(self._read, dexs=needed)
        cancels: list[CancelSpec] = []
        for order in orders:
            asset = asset_ids.get(order.coin)
            if asset is None:
                # Fail the whole sweep loudly rather than skip: a skipped
                # order is a live order the trail would show as swept-over.
                raise GatewayError(
                    f"open order {order.order_id} is on coin {order.coin!r} with no "
                    f"asset id in the universe — cannot cancel it; sweep aborted"
                )
            cancels.append(CancelSpec(asset=asset, oid=order.order_id))
        return cancels

    async def _open_orders(self, dexs: list[str]) -> list[OpenOrder]:
        orders: list[OpenOrder] = []
        for dex in [None, *dexs]:
            await self._budget.spend(ORDERS_WEIGHT)
            orders.extend(await self._read.get_open_orders(self._master, dex=dex))
        return orders

    async def _open_positions(self, dexs: list[str]) -> list[Position]:
        positions: list[Position] = []
        for dex in [None, *dexs]:
            await self._budget.spend(POSITIONS_WEIGHT)
            positions.extend(await self._read.get_open_positions(self._master, dex=dex))
        return positions

    # --- the on-chain capability probe (advisory) ---

    async def _verify_capability(self, now: datetime) -> None:
        """The beating-but-impotent guard (PR #143 review): verify ON-CHAIN —
        via the public extraAgents readback, no signing, no trading action —
        that this watchdog's agent key is still approved and unexpired, so a
        mid-run deregistration or an unrestarted rotation pages the operator
        BEFORE an incident needs the cancel, not during. Verdict state lands
        beside the heartbeat (migration 0025) for the #52 monitor; verdict
        TRANSITIONS go to the audit trail. EVERY failure shape — read errors
        of any type, an unwritable verdict — advances the retry fuse, so no
        outage flavor can re-fire the probe per-cycle (round 2 item 4)."""
        if self._next_capability_at is not None and now < self._next_capability_at:
            return
        await self._budget.spend(META_WEIGHT)
        try:
            agents = await self._read.get_extra_agents(self._master)
        except Exception:
            self._defer_capability_retry(now, "extraAgents read failed")
            return
        approval = next((a for a in agents if a.address == self._signer_address), None)
        if approval is None:
            capable = False
            detail = (
                f"agent {self._signer_address} is NOT among {self._master}'s approved "
                f"agents — deregistered on-chain (or never approved); every cancel "
                f"this watchdog issued would be rejected"
            )
        elif approval.valid_until <= now:
            capable = False
            detail = (
                f"agent {self._signer_address} approval EXPIRED "
                f"{approval.valid_until:%Y-%m-%d %H:%M} UTC — rotate the watchdog "
                f"lane and restart the service (agent-key-rotation runbook)"
            )
        else:
            capable = True
            detail = (
                f"approved on-chain as {approval.name or 'unnamed'} "
                f"until {approval.valid_until:%Y-%m-%d}"
            )
        try:
            await asyncio.wait_for(
                self._store_verdict(capable, detail, now), DB_BLOCK_CEILING_SECONDS
            )
        except Exception:
            self._defer_capability_retry(now, "verdict unrecordable (Postgres?)")
            return
        self._next_capability_at = now + self._capability_interval

    async def _store_verdict(self, capable: bool, detail: str, now: datetime) -> None:
        if capable != self._capable:
            # Event before state (the deadman's rule): a lost event write
            # leaves the verdict unclaimed and re-derived on retry.
            await self._audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="watchdog_capable" if capable else "watchdog_impotent",
                risk_decision="on-chain capability probe (extraAgents)",
                detail={"verdict": detail},
                master_address=self._master,
            )
            self._capable = capable
            if not capable:
                log.error("watchdog IMPOTENT: %s", detail)
        await heartbeat.record_capability(
            self._pool, heartbeat.WATCHDOG_PROCESS, capable=capable, detail=detail, now=now
        )

    def _defer_capability_retry(self, now: datetime, what: str) -> None:
        log.warning(
            "capability probe: %s; retrying in %ds",
            what,
            int(CAPABILITY_RETRY.total_seconds()),
            exc_info=True,
        )
        self._next_capability_at = now + CAPABILITY_RETRY


def _position_json(position: Position) -> dict[str, str]:
    """The halt row's position snapshot entry: what the operator needs to act
    from the alert, Decimals as strings (the JSONB convention)."""
    return {
        "coin": position.coin,
        "side": position.side.value,
        "size_usd": str(position.size_usd),
        "leverage": str(position.leverage),
        "entry_price": str(position.entry_price),
        "unrealized_pnl": str(position.unrealized_pnl),
    }
