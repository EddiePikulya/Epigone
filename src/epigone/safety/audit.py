"""The append-only execution audit trail (issue #135, ADR-0005).

Two row shapes in one table (`execution_audit`, migration 0024):

- ATTEMPT/OUTCOME pairs for signed exchange actions. The attempt row is
  written BEFORE anything touches the wire — write-ahead discipline: if the
  audit insert fails, the action is never sent, and a crash between signing
  and the response still leaves the attempt on record (an attempt row with
  no outcome row is exactly the "reconcile me" signal an operator needs).
  The outcome row links back via `attempt_of` and classifies what happened
  in the ExecutionError hierarchy's own vocabulary: `ok`, `rejected`
  (ActionRejectedError — nothing executed), `ambiguous`
  (AmbiguousExecutionError — the action MAY have executed; the caller
  reconciles, and this row records that obligation), `error` (nothing
  reached the exchange).
- EVENT rows for safety-state changes: halt, resume, sweep completion,
  dead-man's-switch eligibility/activation. Same table so one query tells
  the whole story of an incident in order.

`risk_decision` states what authorized the action — in A3 that is the
safety layer's own authorizations ("operator command /kill", "executor
heartbeat stale 65s > 60s"); A5's risk policy will write its verdicts here.

Append-only is enforced by the DB trigger, not convention; this module only
ever INSERTs. AuditedExecutionGateway is the structural guarantee that no
execution path can forget the trail: wrap any ExecutionGateway and every
call is paired with its rows.
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar

import asyncpg

from epigone.clock import Clock
from epigone.gateway.execution import (
    ActionRejectedError,
    AmbiguousExecutionError,
    BuilderFee,
    CancelOk,
    CancelResult,
    CancelSpec,
    CloidCancelSpec,
    ExecutionGateway,
    Grouping,
    ModifySpec,
    OrderFilled,
    OrderResting,
    OrderResult,
    OrderSpec,
    SubAccountProvisioning,
)
from epigone.safety.db import SAFETY_DB_TIMEOUT_SECONDS

log = logging.getLogger(__name__)

# Who acted. The operator acts through bot commands (/kill, /resume); the
# watchdog and (future, A4+) executor are processes with their own signers.
OPERATOR_ACTOR = "operator"
WATCHDOG_ACTOR = "watchdog"
EXECUTOR_ACTOR = "executor"

# Outcome vocabulary — the ExecutionError hierarchy's split, plus the two
# non-wire shapes.
SUBMITTED = "submitted"  # attempt rows: written before the wire
OK = "ok"
REJECTED = "rejected"  # whole-action pre-validation reject; nothing executed
AMBIGUOUS = "ambiguous"  # MAY have executed; reconcile before re-issuing
ERROR = "error"  # nothing reached the exchange
EVENT = "event"  # a safety-state change, not a wire action

T = TypeVar("T")

# The wire-first posture's deferred attempt/outcome pair runs under this
# hard real-time ceiling (round 6 item 2): the same bound the incident
# cycle's other durable blocks obey (4× the safety pool's touch timeout),
# so no post-cancel bookkeeping can stretch a cycle past its stated budget.
DEFERRED_AUDIT_CEILING_SECONDS = 4 * SAFETY_DB_TIMEOUT_SECONDS


@dataclass(frozen=True)
class AuditedAttempt:
    """The handle record_attempt returns, carrying what the outcome row must
    repeat so each row reads standalone in the trail."""

    id: int
    actor: str
    action: str
    master_address: str | None
    signer_address: str | None
    risk_decision: str


class ExecutionAudit:
    """INSERT-only writer over execution_audit. Timestamps come from the
    injected clock; JSON payloads serialize Decimals and datetimes as strings
    (default=str) so the trail records exactly what was meant, never a float
    approximation."""

    def __init__(self, pool: asyncpg.Pool, clock: Clock) -> None:
        self._pool = pool
        self._clock = clock

    async def record_attempt(
        self,
        *,
        actor: str,
        action: str,
        request: Any,
        risk_decision: str,
        master_address: str | None = None,
        signer_address: str | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> AuditedAttempt:
        """`conn` writes the attempt inside the CALLER's open transaction, so
        the row can commit atomically with whatever else makes the decision
        durable. The copy executor is why it exists (ADR-0006): its
        `position_event_claims` row and this attempt row must land together
        or the write-ahead property is gone — a claim without an attempt is
        an event silently dropped, an attempt without a claim is an event the
        next loop copies again."""
        db: asyncpg.Pool | asyncpg.Connection = conn if conn is not None else self._pool
        row = await db.fetchrow(
            """
            INSERT INTO execution_audit
                (occurred_at, actor, action, master_address, signer_address,
                 request, outcome, risk_decision)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            RETURNING id
            """,
            self._clock.now(),
            actor,
            action,
            _lower(master_address),
            _lower(signer_address),
            _json(request),
            SUBMITTED,
            risk_decision,
        )
        assert row is not None
        return AuditedAttempt(
            id=row["id"],
            actor=actor,
            action=action,
            master_address=_lower(master_address),
            signer_address=_lower(signer_address),
            risk_decision=risk_decision,
        )

    async def record_outcome(
        self,
        attempt: AuditedAttempt,
        *,
        outcome: str,
        detail: Any = None,
        conn: asyncpg.Connection | None = None,
    ) -> None:
        db: asyncpg.Pool | asyncpg.Connection = conn if conn is not None else self._pool
        await db.execute(
            """
            INSERT INTO execution_audit
                (occurred_at, actor, action, master_address, signer_address,
                 request, outcome, detail, risk_decision, attempt_of)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9, $10)
            """,
            self._clock.now(),
            attempt.actor,
            attempt.action,
            attempt.master_address,
            attempt.signer_address,
            _json({"attempt_of": attempt.id}),
            outcome,
            _json(detail),
            attempt.risk_decision,
            attempt.id,
        )

    async def record_event(
        self,
        *,
        actor: str,
        action: str,
        risk_decision: str,
        detail: Any = None,
        master_address: str | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> None:
        """A safety-state change. `conn` lets a state module write the event
        inside the same transaction as the state row it describes (halt.py),
        so state and trail can never disagree about whether it happened."""
        db: asyncpg.Pool | asyncpg.Connection = conn if conn is not None else self._pool
        await db.execute(
            """
            INSERT INTO execution_audit
                (occurred_at, actor, action, master_address, request, outcome,
                 detail, risk_decision)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8)
            """,
            self._clock.now(),
            actor,
            action,
            _lower(master_address),
            _json({}),
            EVENT,
            _json(detail),
            risk_decision,
        )


class AuditedExecutionGateway:
    """An ExecutionGateway whose every call leaves attempt/outcome rows —
    wrap the real gateway once and no code path can sign unaudited.

    `decision` is the risk decision recorded with the NEXT call(s); the
    acting process sets it before each action (the watchdog re-states it per
    cycle). A deliberate plain attribute, not a constructor freeze: the
    authorization genuinely changes call to call while the gateway instance —
    one per signer per process, the nonce contract — must not.

    TWO AUDIT DISCIPLINES, chosen at construction (PR #143 review):

    - write-ahead (default): the attempt row must land BEFORE the wire — no
      evidence, no action. Right for the executor's ORDER path, where the
      dangerous failure is an order without a record.
    - best_effort_audit=True: the wire call proceeds even when an audit
      write fails (logged loudly, never silently). Right — and mandatory —
      for the SAFETY path (watchdog cancel-all, deadman): its dangerous
      failure is the opposite one, a protective cancel suppressed because
      Postgres was down at the exact moment the account most needed
      protecting. Best-effort is about DB failures only; action failures
      still raise exactly as always.

    Plus one INCIDENT posture on top of best-effort (PR #143 rounds 5–6):
    `wire_first=True` (a plain attribute the watchdog flips while a
    DB-BLIND incident is declared) moves the attempt row AFTER the wire
    call, so the blind cancel performs ZERO Postgres work before the wire —
    the attempt-then-outcome pair still lands (best-effort, under its own
    hard ceiling) once the call returns or fails. The trade is explicit:
    while wire_first is on, a process crash between signing and returning
    leaves NO attempt row — accepted for exactly the DB-blind window,
    where the database is UNREADABLE and the write-ahead row would have
    failed anyway. It is NOT set for a real-stall trip (round 6): there
    the liveness reads answered that same cycle, the write-ahead row would
    land, and its evidence is kept. Meaningful only with
    best_effort_audit; the executor's order path never sets it."""

    def __init__(
        self,
        inner: ExecutionGateway,
        audit: ExecutionAudit,
        *,
        actor: str,
        master_address: str,
        signer_address: str,
        best_effort_audit: bool = False,
    ) -> None:
        self._inner = inner
        self._audit = audit
        self._actor = actor
        self._master_address = master_address
        self._signer_address = signer_address
        self._best_effort_audit = best_effort_audit
        self.decision: str = "unspecified"
        self.wire_first: bool = False

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
        vault_address: str | None = None,
    ) -> list[OrderResult]:
        # `vault_address` rides the REQUEST payload rather than a column of
        # its own: it says which BOOK the action landed on, and the trail's
        # master_address stays the account whose agent signed — the two facts
        # a per-sub incident needs told apart (issue #136).
        request = {
            "orders": [_order_json(order) for order in orders],
            "grouping": grouping.value,
            "builder": None
            if builder is None
            else {"address": builder.address, "fee_tenth_bp": builder.fee_tenth_bp},
            "vault_address": _lower(vault_address),
        }
        return await self._audited(
            "order",
            request,
            lambda: self._inner.place_orders(
                orders, grouping=grouping, builder=builder, vault_address=vault_address
            ),
            _order_results_json,
        )

    async def cancel_orders(
        self, cancels: list[CancelSpec], *, vault_address: str | None = None
    ) -> list[CancelResult]:
        request = {
            "cancels": [{"asset": c.asset, "oid": c.oid} for c in cancels],
            "vault_address": _lower(vault_address),
        }
        return await self._audited(
            "cancel",
            request,
            lambda: self._inner.cancel_orders(cancels, vault_address=vault_address),
            _cancel_results_json,
        )

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        request = {"cancels": [{"asset": c.asset, "cloid": c.cloid} for c in cancels]}
        return await self._audited(
            "cancelByCloid",
            request,
            lambda: self._inner.cancel_orders_by_cloid(cancels),
            _cancel_results_json,
        )

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        request = {
            "modifies": [{"oid": m.oid, "order": _order_json(m.order)} for m in modifies]
        }
        return await self._audited(
            "batchModify",
            request,
            lambda: self._inner.modify_orders(modifies),
            _order_results_json,
        )

    async def update_leverage(self, asset: int, leverage: int, *, is_cross: bool = True) -> None:
        request = {"asset": asset, "leverage": leverage, "is_cross": is_cross}
        await self._audited(
            "updateLeverage",
            request,
            lambda: self._inner.update_leverage(asset, leverage, is_cross=is_cross),
            lambda _: None,
        )

    async def schedule_cancel(self, at: datetime | None) -> None:
        request = {"time": None if at is None else at.isoformat()}
        await self._audited(
            "scheduleCancel",
            request,
            lambda: self._inner.schedule_cancel(at),
            lambda _: None,
        )

    async def _audited(
        self,
        action: str,
        request: Any,
        call: Callable[[], Awaitable[T]],
        result_json: Callable[[T], Any],
    ) -> T:
        # wire_first (DB-blind posture, class docstring): the attempt row is
        # deferred to after the call, so nothing touches Postgres pre-wire.
        wire_first = self.wire_first
        attempt = None if wire_first else await self._record_attempt(action, request)
        try:
            result = await call()
        except Exception as exc:
            outcome, detail = classify_failure(exc)
            await self._finish_audit(
                attempt,
                wire_first,
                action,
                request,
                outcome=outcome,
                detail=detail,
            )
            raise
        await self._finish_audit(
            attempt, wire_first, action, request, outcome=OK, detail=result_json(result)
        )
        return result

    async def _finish_audit(
        self,
        attempt: AuditedAttempt | None,
        wire_first: bool,
        action: str,
        request: Any,
        *,
        outcome: str,
        detail: Any,
    ) -> None:
        """Close the trail for one call. Write-ahead mode records the
        outcome against the pre-wire attempt; the wire-first posture writes
        the DEFERRED attempt/outcome pair here, under its own hard real-time
        ceiling (round 6 item 2): the pair is pool-bounded plain INSERTs,
        but nothing on an incident cycle may stretch past the same ceiling
        the reconcile obeys — and in this posture audit loss is always
        best-effort-tolerated, the ceiling included."""
        if not wire_first:
            await self._record_outcome(attempt, outcome=outcome, detail=detail)
            return

        async def deferred_pair() -> None:
            deferred = await self._record_attempt(action, request)
            await self._record_outcome(deferred, outcome=outcome, detail=detail)

        try:
            await asyncio.wait_for(deferred_pair(), DEFERRED_AUDIT_CEILING_SECONDS)
        except Exception:
            if not self._best_effort_audit:
                raise
            log.exception(
                "deferred wire-first audit pair for %s failed or hit its ceiling — "
                "the action already ran; reconcile the trail from the exchange if "
                "it matters",
                action,
            )

    async def _record_attempt(self, action: str, request: Any) -> AuditedAttempt | None:
        """The write-ahead attempt row — or, in best-effort mode, None when
        the write fails: the SAFETY action must proceed unaudited rather than
        be suppressed by a DB outage (class docstring)."""
        try:
            return await self._audit.record_attempt(
                actor=self._actor,
                action=action,
                request=request,
                risk_decision=self.decision,
                master_address=self._master_address,
                signer_address=self._signer_address,
            )
        except Exception:
            if not self._best_effort_audit:
                raise
            log.exception(
                "AUDIT WRITE FAILED for %s attempt (%s) — proceeding UNAUDITED: "
                "best-effort audit, the protective action must not be suppressed",
                action,
                self.decision,
            )
            return None

    async def _record_outcome(
        self, attempt: AuditedAttempt | None, *, outcome: str, detail: Any
    ) -> None:
        if attempt is None:
            # Best-effort mode lost the attempt row to a DB outage; there is
            # nothing to link an outcome to. The action itself was the point.
            return
        try:
            await self._audit.record_outcome(attempt, outcome=outcome, detail=detail)
        except Exception:
            if not self._best_effort_audit:
                raise
            log.exception(
                "AUDIT WRITE FAILED for %s outcome %r (attempt %d) — the action "
                "already ran; reconcile the trail from the exchange if it matters",
                attempt.action,
                outcome,
                attempt.id,
            )


class AuditedProvisioning:
    """The same attempt/outcome discipline for Copy Sub-account provisioning
    (issue #136, ADR-0007 decision 12).

    A separate wrapper for a separate protocol, mirroring the split in
    `epigone.gateway.execution`: anything typed as `ExecutionGateway` still
    provably cannot move funds, while the one path that creates and funds subs
    is audited exactly as heavily as an order — write-ahead, because the
    dangerous failure here is money moving with no record of who asked.

    `decision` is the risk verdict recorded with the next call(s), the same
    plain attribute AuditedExecutionGateway carries and for the same reason:
    the authorization changes call to call while the instance must not."""

    def __init__(
        self,
        inner: SubAccountProvisioning,
        audit: ExecutionAudit,
        *,
        actor: str,
        master_address: str,
        signer_address: str,
    ) -> None:
        self._inner = inner
        self._audit = audit
        self._actor = actor
        self._master_address = master_address
        self._signer_address = signer_address
        self.decision: str = "unspecified"

    async def create_sub_account(self, name: str) -> str:
        return await self._audited(
            "createSubAccount", {"name": name}, lambda: self._inner.create_sub_account(name)
        )

    async def rename_sub_account(self, sub_address: str, name: str) -> None:
        await self._audited(
            "subAccountModify",
            {"sub_account_user": _lower(sub_address), "name": name},
            lambda: self._inner.rename_sub_account(sub_address, name),
        )

    async def sub_account_transfer(
        self, sub_address: str, *, is_deposit: bool, usd_micro: int
    ) -> None:
        await self._audited(
            "subAccountTransfer",
            {
                "sub_account_user": _lower(sub_address),
                "is_deposit": is_deposit,
                "usd_micro": usd_micro,
            },
            lambda: self._inner.sub_account_transfer(
                sub_address, is_deposit=is_deposit, usd_micro=usd_micro
            ),
        )

    async def _audited(
        self, action: str, request: Any, call: Callable[[], Awaitable[T]]
    ) -> T:
        attempt = await self._audit.record_attempt(
            actor=self._actor,
            action=action,
            request=request,
            risk_decision=self.decision,
            master_address=self._master_address,
            signer_address=self._signer_address,
        )
        try:
            result = await call()
        except Exception as exc:
            outcome, detail = classify_failure(exc)
            await self._audit.record_outcome(attempt, outcome=outcome, detail=detail)
            raise
        await self._audit.record_outcome(attempt, outcome=OK, detail={"result": result})
        return result


def classify_failure(exc: BaseException) -> tuple[str, dict[str, Any]]:
    """A failed action as the trail records it: the outcome word plus what to
    say about it.

    ONE definition, shared by both audited wrappers, because the mapping IS
    the ExecutionError hierarchy's load-bearing split — could anything have
    reached the exchange? — and two copies of it could drift into disagreeing
    about whether an action needs reconciling. Anything unforeseen classifies
    ERROR alongside a plain ExecutionError: the trail must never lose a
    failure, whatever its shape."""
    if isinstance(exc, ActionRejectedError):
        return REJECTED, {"reason": exc.reason.value, "message": exc.message}
    if isinstance(exc, AmbiguousExecutionError):
        return AMBIGUOUS, {"type": type(exc).__name__, "message": str(exc)}
    return ERROR, {"type": type(exc).__name__, "message": str(exc)}


def _json(value: Any) -> str:
    return json.dumps(value, default=str)


def _lower(address: str | None) -> str | None:
    return None if address is None else address.lower()


def _order_json(order: OrderSpec) -> dict[str, Any]:
    return {
        "asset": order.asset,
        "is_buy": order.is_buy,
        "size": str(order.size),
        "limit_price": str(order.limit_price),
        "tif": order.tif.value,
        "reduce_only": order.reduce_only,
        "trigger": None
        if order.trigger is None
        else {
            "trigger_price": str(order.trigger.trigger_price),
            "is_market": order.trigger.is_market,
            "tpsl": order.trigger.tpsl.value,
        },
        "cloid": order.cloid,
    }


def _order_results_json(results: list[OrderResult]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for result in results:
        if isinstance(result, OrderResting):
            serialized.append({"status": "resting", "oid": result.oid, "cloid": result.cloid})
        elif isinstance(result, OrderFilled):
            serialized.append(
                {
                    "status": "filled",
                    "oid": result.oid,
                    "total_size": str(result.total_size),
                    "avg_price": str(result.avg_price),
                    "cloid": result.cloid,
                }
            )
        else:
            serialized.append(
                {
                    "status": "rejected",
                    "reason": result.reason.value,
                    "message": result.message,
                }
            )
    return serialized


def _cancel_results_json(results: list[CancelResult]) -> list[dict[str, Any]]:
    return [
        {"status": "ok"}
        if isinstance(result, CancelOk)
        else {"status": "rejected", "reason": result.reason.value, "message": result.message}
        for result in results
    ]
