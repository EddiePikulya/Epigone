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
  processes meet in Postgres).

One cycle: beat own heartbeat (the #52 monitor watches the watcher) → trip
if the executor's heartbeat exists but went stale → sweep whatever halt is
active (watchdog-tripped or operator /kill alike) → verify, on a slow
cadence, that this watchdog's agent key is still approved ON-CHAIN
(extraAgents — a beating-but-impotent watchdog must page BEFORE an incident,
PR #143 review). A halt whose sweep isn't finished is re-swept every cycle
until it is. The sweep is ACCOUNT-WIDE: core plus every builder dex in the
live perpDexs listing, because the agent key is account-wide and an order
surviving a kill on an uncovered venue would be the silent version of the
gap this module exists to close.

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
from datetime import datetime, timedelta

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway import (
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

# A failed capability read (network blip, info outage) retries on this short
# fuse instead of waiting out the full check interval — but never every
# cycle, which would burn 20 weight per 10s on a persistent outage.
CAPABILITY_RETRY = timedelta(minutes=5)


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
        self._capability_interval = capability_interval
        self._capable: bool | None = None  # last on-chain verdict; None = unchecked
        self._next_capability_at: datetime | None = None  # None → due now

    async def run_cycle(self) -> None:
        now = self._clock.now()
        # Beat FIRST: a cycle that then fails still proves the process alive,
        # while a DB outage stops the beat and trips the monitor's check —
        # exactly the two signals the #52 monitor wants.
        await heartbeat.beat(self._pool, heartbeat.WATCHDOG_PROCESS, now)
        halt = await active_halt(self._pool)
        if halt is None:
            halt = await self._trip_if_executor_stale(now)
        if halt is not None and halt.swept_at is None:
            await self._sweep(halt)
        # After the protective work, never before it: the capability probe is
        # advisory, and its failures must not delay a sweep by one second.
        try:
            await self._verify_capability(now)
        except Exception:
            log.exception("capability probe failed; retrying next cycle")

    async def _verify_capability(self, now: datetime) -> None:
        """The beating-but-impotent guard (PR #143 review): verify ON-CHAIN —
        via the public extraAgents readback, no signing, no trading action —
        that this watchdog's agent key is still approved and unexpired, so a
        mid-run deregistration or an unrestarted rotation pages the operator
        BEFORE an incident needs the cancel, not during. Verdict state lands
        beside the heartbeat (migration 0025) for the #52 monitor; verdict
        TRANSITIONS go to the audit trail."""
        if self._next_capability_at is not None and now < self._next_capability_at:
            return
        await self._budget.spend(META_WEIGHT)
        try:
            agents = await self._read.get_extra_agents(self._master)
        except GatewayError:
            log.warning(
                "capability probe: extraAgents read failed; retrying in %ds",
                int(CAPABILITY_RETRY.total_seconds()),
                exc_info=True,
            )
            self._next_capability_at = now + CAPABILITY_RETRY
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
        if capable != self._capable:
            # Event before state (the deadman's rule): a lost event write
            # leaves the verdict unclaimed and the next cycle re-derives it.
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
        self._next_capability_at = now + self._capability_interval

    async def _trip_if_executor_stale(self, now: datetime) -> Halt | None:
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
        reason = (
            f"executor heartbeat stale: {int(age.total_seconds())}s > "
            f"{int(self._executor_stale.total_seconds())}s"
        )
        halt, created = await request_halt(
            self._pool, self._clock, self._audit, source=WATCHDOG_SOURCE, reason=reason
        )
        if created:
            log.error("watchdog TRIPPED: %s — sweeping resting orders", reason)
        return halt

    async def _sweep(self, halt: Halt) -> None:
        """Cancel-all with verify-by-enumeration (module docstring): list the
        book, cancel what rests, list AGAIN, and only an empty second listing
        stamps the sweep done — with the position snapshot and the unwind
        policy recorded. Any failure leaves the halt unswept for the next
        cycle; enumeration is idempotent and cancels tolerate re-issue.

        ACCOUNT-WIDE by construction (PR #143 review): the agent key is
        account-wide, so the sweep enumerates the core venue plus EVERY
        builder dex in the live perpDexs listing — not just the venues the
        product covers for tracking (POSITION_VENUES). A dex added to
        trading, or an order that somehow landed on an uncovered dex, is
        swept with no code change here. (Sub-accounts are a different axis:
        agent-reachable per the #142 findings but separate ACCOUNTS, outside
        this master's book — the runbook carries that boundary until A5's
        risk policy forbids or includes them.)"""
        self._exec.decision = f"halt #{halt.id} ({halt.source}): {halt.reason}"
        dexs = await self._perp_dexs()
        orders = await self._open_orders(dexs)
        if orders:
            await self._exec.cancel_orders(await self._cancels_for(orders))
            # The fresh, deciding enumeration — over a fresh listing too, so
            # a dex appearing mid-sweep cannot hide an order from the verify.
            dexs = await self._perp_dexs()
            orders = await self._open_orders(dexs)
        if orders:
            log.warning(
                "halt #%d sweep: %d order(s) still resting after cancel; retrying next cycle",
                halt.id,
                len(orders),
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

    async def _perp_dexs(self) -> list[str]:
        await self._budget.spend(META_WEIGHT)
        return await self._read.get_perp_dexs()

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
