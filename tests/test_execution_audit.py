"""The append-only execution audit trail (issue #135).

Two properties under test: the TABLE is append-only in the database itself
(the trigger, not convention), and the WRAPPER makes an unaudited signed
action structurally impossible — every ExecutionGateway call leaves an
attempt row before the wire and an outcome row after, in the ExecutionError
hierarchy's own vocabulary. The ambiguous case matters most: an
AmbiguousExecutionError outcome row is the durable record that a reconcile
is owed (the silent-live-order hazard)."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
import pytest

from epigone.gateway.execution import (
    ActionRejectedError,
    AmbiguousExecutionError,
    CancelSpec,
    CloidCancelSpec,
    ExecutionError,
    ModifySpec,
    OrderSpec,
    RejectReason,
)
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.safety.audit import (
    AMBIGUOUS,
    ERROR,
    EVENT,
    OK,
    REJECTED,
    SUBMITTED,
    WATCHDOG_ACTOR,
    AuditedExecutionGateway,
    ExecutionAudit,
)
from tests.support.clock import FakeClock

MASTER = "0x" + "ab" * 20
SIGNER = "0x" + "cd" * 20


@pytest.fixture
def audit(pool: asyncpg.Pool, clock: FakeClock) -> ExecutionAudit:
    return ExecutionAudit(pool, clock)


@pytest.fixture
def fake() -> FakeExecutionGateway:
    return FakeExecutionGateway()


@pytest.fixture
def audited(fake: FakeExecutionGateway, audit: ExecutionAudit) -> AuditedExecutionGateway:
    return AuditedExecutionGateway(
        fake, audit, actor=WATCHDOG_ACTOR, master_address=MASTER, signer_address=SIGNER
    )


async def _rows(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return list(await pool.fetch("SELECT * FROM execution_audit ORDER BY id"))


def _order() -> OrderSpec:
    return OrderSpec(asset=3, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("101.5"))


# --- the table is append-only in the database itself ---


async def test_audit_rows_cannot_be_updated_or_deleted(
    pool: asyncpg.Pool, audit: ExecutionAudit
) -> None:
    await audit.record_event(
        actor=WATCHDOG_ACTOR, action="halt", risk_decision="test", detail=None
    )
    with pytest.raises(asyncpg.PostgresError, match="append-only"):
        await pool.execute("UPDATE execution_audit SET actor = 'forged'")
    with pytest.raises(asyncpg.PostgresError, match="append-only"):
        await pool.execute("DELETE FROM execution_audit")


# --- attempt/outcome pairing through the wrapper ---


async def test_success_leaves_attempt_then_outcome(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway, clock: FakeClock
) -> None:
    audited.decision = "halt #1: cancel-all on stale executor heartbeat"
    results = await audited.cancel_orders([CancelSpec(asset=3, oid=77)])
    assert len(results) == 1

    attempt, outcome = await _rows(pool)
    assert attempt["outcome"] == SUBMITTED
    assert attempt["actor"] == WATCHDOG_ACTOR
    assert attempt["action"] == "cancel"
    assert attempt["master_address"] == MASTER
    assert attempt["signer_address"] == SIGNER
    assert attempt["risk_decision"] == "halt #1: cancel-all on stale executor heartbeat"
    assert '"oid": 77' in attempt["request"]
    assert attempt["occurred_at"] == clock.now()

    assert outcome["outcome"] == OK
    assert outcome["attempt_of"] == attempt["id"]
    assert outcome["action"] == "cancel"
    assert outcome["risk_decision"] == attempt["risk_decision"]
    assert '"status": "ok"' in outcome["detail"]


async def test_rejection_is_recorded_and_reraised(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway, fake: FakeExecutionGateway
) -> None:
    fake.errors.append(ActionRejectedError("Order must have minimum value of $10."))
    with pytest.raises(ActionRejectedError):
        await audited.place_orders([_order()])
    attempt, outcome = await _rows(pool)
    assert attempt["outcome"] == SUBMITTED
    assert outcome["outcome"] == REJECTED
    assert f'"{RejectReason.MIN_NOTIONAL.value}"' in outcome["detail"]
    assert "minimum value" in outcome["detail"]


async def test_ambiguity_is_recorded_as_its_own_outcome(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway, fake: FakeExecutionGateway
) -> None:
    fake.errors.append(AmbiguousExecutionError("timed out — the action may have executed"))
    with pytest.raises(AmbiguousExecutionError):
        await audited.cancel_orders([CancelSpec(asset=1, oid=5)])
    _, outcome = await _rows(pool)
    # Ambiguous is NOT error and NOT rejected: the trail records that the
    # action may be live and a reconcile is owed.
    assert outcome["outcome"] == AMBIGUOUS
    assert "AmbiguousExecutionError" in outcome["detail"]


async def test_clean_failure_is_recorded_as_error(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway, fake: FakeExecutionGateway
) -> None:
    fake.errors.append(ExecutionError("exchange connection failed"))
    with pytest.raises(ExecutionError):
        await audited.schedule_cancel(None)
    _, outcome = await _rows(pool)
    assert outcome["outcome"] == ERROR


async def test_every_gateway_method_is_audited(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway
) -> None:
    await audited.place_orders([_order()])
    await audited.cancel_orders([CancelSpec(asset=3, oid=1)])
    await audited.cancel_orders_by_cloid([CloidCancelSpec(asset=3, cloid="0x" + "0" * 32)])
    await audited.modify_orders([ModifySpec(oid=1, order=_order())])
    await audited.update_leverage(3, 5)
    await audited.schedule_cancel(datetime(2026, 7, 10, 12, 5, tzinfo=UTC))

    rows = await _rows(pool)
    attempts = [r["action"] for r in rows if r["outcome"] == SUBMITTED]
    outcomes = [r["action"] for r in rows if r["outcome"] == OK]
    expected = ["order", "cancel", "cancelByCloid", "batchModify",
                "updateLeverage", "scheduleCancel"]
    assert attempts == expected
    assert outcomes == expected


async def test_decision_is_read_per_call(
    pool: asyncpg.Pool, audited: AuditedExecutionGateway
) -> None:
    audited.decision = "first"
    await audited.update_leverage(1, 2)
    audited.decision = "second"
    await audited.update_leverage(1, 3)
    decisions = [r["risk_decision"] for r in await _rows(pool) if r["outcome"] == SUBMITTED]
    assert decisions == ["first", "second"]


# --- standalone safety events ---


async def test_events_are_single_rows(pool: asyncpg.Pool, audit: ExecutionAudit) -> None:
    await audit.record_event(
        actor=WATCHDOG_ACTOR,
        action="deadman_ineligible",
        risk_decision="probe: volume gate",
        detail={"required": "$1000000"},
    )
    (row,) = await _rows(pool)
    assert row["outcome"] == EVENT
    assert row["attempt_of"] is None
    assert "1000000" in row["detail"]
