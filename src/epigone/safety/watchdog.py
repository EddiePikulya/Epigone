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

THE TRIP→WIRE PATH SURVIVES POSTGRES (PR #143 round 2). The failure modes
are CORRELATED: the likeliest reason the executor is dead is infrastructure
trouble — precisely when a DB-dependent watchdog would be blind. So:

- every DB touch on this path is BOUNDED (epigone.safety.db: connect,
  per-query, lock waits, AND the release-time cancel-wait — the asyncpg
  leg that would otherwise await an untimed CancelRequest connection,
  round 4), so a HANGING database — the shape a partitioned host actually
  fails in — becomes the exception the rest of this design handles instead
  of a stuck await; a fully hung touch costs roughly 2× the configured
  timeout (query bound + release budget), never TCP-retransmission
  timescales;
- the rate budget degrades (safety.budget.FallbackBudget): a dead or
  lock-wedged rate_budget row never queues a cancel;
- a halt row that cannot be written never suppresses cancelling — neither
  at trip time nor for as long as it stays unwritable: reconciliation is
  bookkeeping, and a failing reconcile falls through to cancelling every
  cycle until writes recover (round 3);
- cannot-read-Postgres is ITSELF a trip: unreadable CONTINUOUSLY past
  `db_blind_after` — a failure streak, reset by any successful read, so
  one dropped connection after a long healthy cycle never trips it
  (default 3× the executor-stall threshold — a deliberate, conservative
  call: cancelling resting orders is cheap and recoverable, the executor
  re-places when it returns, and nothing here closes positions or spends —
  whereas sitting blind through a correlated outage is the exact incident
  this layer exists to prevent), the watchdog cancels every resting order
  each cycle until the database answers again, then reconciles: the halt
  row under a DISTINCT headline ("DB-blind sweep" vs a real stall's
  "unrecorded trip"), plus a blind-window audit event even when a standing
  halt is joined (durable in every case except a watchdog death inside the
  recovery-to-reconcile window — the runbook's one carve-out).

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
        try:
            await heartbeat.beat(self._pool, heartbeat.WATCHDOG_PROCESS, now)
        except Exception:
            # Best-effort beat (round 2 item 1c): losing the liveness signal
            # is bad; skipping the cycle's protective work over it is worse.
            log.exception("heartbeat write failed — continuing the protective cycle")
        try:
            halt = await active_halt(self._pool)
            stall_reason = None if halt is not None else await self._executor_stall(now)
        except Exception:
            # Deliberately ANY failure of the liveness reads, not just
            # connection errors: a watchdog that cannot answer "is the
            # executor alive" is blind whatever the cause, and blind fails
            # protective (a spurious blind trip cancels recoverable orders;
            # the inverse mistake strands them).
            log.warning(
                "watchdog: Postgres cannot answer the liveness question", exc_info=True
            )
            if self._db_failing_since is None:
                self._db_failing_since = now  # a new failure streak opens
            await self._run_blind(now)
            return
        self._db_failing_since = None  # any successful read breaks the streak
        if self._blind is not None:
            try:
                halt = await self._settle_blind_debt(now)
            except Exception:
                # Reconciliation is BOOKKEEPING and must never gate
                # protection (round 3 item 2): reads recovered but writes
                # didn't (read-only recovery, full WAL, a locked halt table)
                # — keep the incident open and keep cancelling; the halt row
                # lands when writes do.
                log.exception(
                    "blind-incident reconcile failed (writes still broken?) — "
                    "continuing to cancel unrecorded"
                )
                await self._unrecorded_pass(
                    f"unreconciled blind incident: {self._blind.reason}"
                )
                halt = None
        elif halt is None and stall_reason is not None:
            halt = await self._trip(stall_reason, now)
        if halt is not None and halt.swept_at is None:
            await self._sweep(halt)
        # After the protective work, never before it: the capability probe is
        # advisory, and its failures must not delay a sweep by one second.
        try:
            await self._verify_capability(now)
        except Exception:
            log.exception("capability probe failed; retrying next cycle")

    # --- Postgres-blind operation (round 2 item 1) ---

    async def _run_blind(self, now: datetime) -> None:
        """The database cannot say whether the executor is alive or a halt
        stands. While the CONTINUOUS failure streak (any successful read
        resets it — round 3 item 3: one dropped connection after a long
        healthy cycle must never read as an outage) is inside the blind
        threshold: wait, a blip is not an incident. Past it — or with an
        unrecorded trip already pending — cancel every resting order, every
        cycle, until Postgres answers again; the halt row and a durable
        blind-window audit event are reconciled on recovery."""
        assert self._db_failing_since is not None  # run_cycle stamped the streak
        blind_for = now - self._db_failing_since
        if self._blind is None:
            if blind_for <= self._db_blind_after:
                log.warning(
                    "watchdog: Postgres unreadable for %ds unbroken (blind trip at %ds)",
                    int(blind_for.total_seconds()),
                    int(self._db_blind_after.total_seconds()),
                )
                return
            self._blind = _BlindIncident(
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
            self._blind_passes = 0
            log.error(
                "watchdog BLIND TRIP: %s — cancelling resting orders without halt state",
                self._blind.reason,
            )
        await self._unrecorded_pass(f"DB-blind sweep: {self._blind.reason}")

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

    async def _settle_blind_debt(self, now: datetime) -> Halt | None:
        """Postgres answers again after an unrecorded trip: write the halt
        row (joining any halt that appeared meanwhile) under a headline that
        PRESERVES which trip it was — "DB-blind sweep" for the unreadable-
        database trip, "unrecorded trip" for a real observed stall whose
        halt row couldn't be written — so the operator can tell them apart.
        The blind window leaves a durable audit event whenever this
        reconcile RUNS (round 3 item 4): joining a standing halt writes no
        new halt state by design, and the blind sweeps' own best-effort
        audit rows likely failed with the same outage, so without this
        event a whole blind window could exist nowhere but process logs.
        The one hole — the watchdog dying between DB recovery and this
        cycle, taking the in-process marker with it — is carved out in the
        runbook, not papered over here. The normal sweep machinery then
        verifies the book and stamps swept_at with the position snapshot."""
        assert self._blind is not None
        headline = "DB-blind sweep reconciled" if self._blind.db_blind else (
            "unrecorded trip reconciled (halt row was unwritable)"
        )
        halt, created = await request_halt(
            self._pool,
            self._clock,
            self._audit,
            source=WATCHDOG_SOURCE,
            reason=(
                f"{headline}: {self._blind.reason}; resting orders were being "
                f"cancelled unrecorded since "
                f"{self._blind.since:%Y-%m-%d %H:%M:%S} UTC; Postgres answered "
                f"again at {now:%Y-%m-%d %H:%M:%S} UTC"
            ),
        )
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
        if created:
            log.error("watchdog: unrecorded sweep reconciled into halt #%d", halt.id)
        else:
            log.error(
                "watchdog: blind window recorded against standing halt #%d", halt.id
            )
        self._blind = None
        self._blind_passes = 0
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

    async def _trip(self, reason: str, now: datetime) -> Halt | None:
        """A real stall trip. The halt row is attempted first (state before
        wire, when the state store works) — but an unwritable halt row must
        never suppress the cancel (round 2 item 1a): the sweep runs
        unrecorded and reconciles like a blind trip."""
        try:
            halt, created = await request_halt(
                self._pool, self._clock, self._audit, source=WATCHDOG_SOURCE, reason=reason
            )
        except Exception:
            log.exception(
                "watchdog TRIPPED (%s) but the halt row is unwritable — cancelling "
                "without it; the halt will be reconciled when Postgres returns",
                reason,
            )
            self._blind = _BlindIncident(since=now, reason=reason, db_blind=False)
            self._blind_passes = 0
            await self._unrecorded_pass(f"unrecorded trip: {reason} (halt row unwritable)")
            return None
        if created:
            log.error("watchdog TRIPPED: %s — sweeping resting orders", reason)
        return halt

    # --- the sweep ---

    async def _sweep(self, halt: Halt) -> None:
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
        that boundary until A5's risk policy forbids or includes them.)"""
        dexs, complete, cancelled = await self._cancel_resting(
            f"halt #{halt.id} ({halt.source}): {halt.reason}"
        )
        if cancelled:
            # Cancels went out: the deciding enumeration must be FRESH —
            # listing included, so a dex appearing mid-sweep can't hide an
            # order from the verify. An already-empty book needs no second
            # look (its first enumeration IS the verify).
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
        await mark_swept(
            self._pool,
            self._clock,
            self._audit,
            halt=halt,
            positions=[_position_json(p) for p in positions],
            unwind_policy=HOLD_POLICY,
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
        except Exception:
            self._defer_capability_retry(now, "verdict unrecordable (Postgres?)")
            return
        self._next_capability_at = now + self._capability_interval

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
