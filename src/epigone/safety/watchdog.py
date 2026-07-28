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
active (watchdog-tripped or operator /kill alike) → nothing else. A halt
whose sweep isn't finished is re-swept every cycle until it is.

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
    POSITION_VENUES,
    GatewayError,
    HyperliquidGateway,
    OpenOrder,
    Position,
    fetch_asset_ids,
    fetch_open_orders,
    fetch_open_positions,
)
from epigone.gateway.execution import CancelSpec
from epigone.safety import heartbeat
from epigone.safety.audit import AuditedExecutionGateway, ExecutionAudit
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
META_WEIGHT = 20  # meta (per venue) and perpDexs alike — the info default


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
        executor_stale: timedelta,
    ) -> None:
        self._pool = pool
        self._clock = clock
        self._read = read_gateway
        self._exec = exec_gateway
        self._audit = audit
        self._budget = budget
        self._master = master_address.lower()
        self._executor_stale = executor_stale

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
        cycle; enumeration is idempotent and cancels tolerate re-issue."""
        self._exec.decision = f"halt #{halt.id} ({halt.source}): {halt.reason}"
        orders = await self._open_orders()
        if orders:
            await self._exec.cancel_orders(await self._cancels_for(orders))
            orders = await self._open_orders()  # the fresh, deciding enumeration
        if orders:
            log.warning(
                "halt #%d sweep: %d order(s) still resting after cancel; retrying next cycle",
                halt.id,
                len(orders),
            )
            return
        positions = await self._open_positions()
        await mark_swept(
            self._pool,
            self._clock,
            self._audit,
            halt=halt,
            positions=[_position_json(p) for p in positions],
            unwind_policy=HOLD_POLICY,
        )
        log.error(
            "halt #%d swept: book empty; %d open position(s) HELD per %s "
            "(docs/runbooks/halt-and-unwind.md)",
            halt.id,
            len(positions),
            HOLD_POLICY,
        )

    async def _cancels_for(self, orders: list[OpenOrder]) -> list[CancelSpec]:
        asset_ids = await self._asset_ids()
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

    async def _open_orders(self) -> list[OpenOrder]:
        for _ in POSITION_VENUES:
            await self._budget.spend(ORDERS_WEIGHT)
        return await fetch_open_orders(self._read, self._master)

    async def _open_positions(self) -> list[Position]:
        for _ in POSITION_VENUES:
            await self._budget.spend(POSITIONS_WEIGHT)
        return await fetch_open_positions(self._read, self._master)

    async def _asset_ids(self) -> dict[str, int]:
        # One meta per venue plus the perpDexs listing (fetch_asset_ids).
        for _ in range(len(POSITION_VENUES) + 1):
            await self._budget.spend(META_WEIGHT)
        return await fetch_asset_ids(self._read)


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
