"""The watchdog: the primary dead-man's switch (issue #135).

The acceptance criteria under test, in this file's order: a simulated
executor stall trips a halt and cancel-alls, independently of any executor
process (the stall is nothing but a stale DB row — no executor exists);
/kill-sourced halts are swept the same way within one cycle; the sweep NEVER
stamps done on a cancel's word alone (verify-by-enumeration, the
AmbiguousExecutionError discipline); positions are held and snapshotted per
the documented policy; and the watchdog beats its own heartbeat for the
monitor."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import GatewayError, OpenOrder, Position, Side
from epigone.gateway.execution import AmbiguousExecutionError, CancelSpec
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.safety import heartbeat
from epigone.safety.audit import (
    AMBIGUOUS,
    EVENT,
    WATCHDOG_ACTOR,
    AuditedExecutionGateway,
    ExecutionAudit,
)
from epigone.safety.halt import (
    HOLD_POLICY,
    KILL_SOURCE,
    WATCHDOG_SOURCE,
    active_halt,
    request_halt,
    resume,
)
from epigone.safety.watchdog import Watchdog
from tests.support.clock import FakeClock

MASTER = "0x" + "ab" * 20
SIGNER = "0x" + "cd" * 20
ADMIN = 370818090
STALE = timedelta(seconds=60)


def _order(coin: str, oid: int) -> OpenOrder:
    return OpenOrder(
        coin=coin,
        is_buy=True,
        limit_price=Decimal("100"),
        size=Decimal("1"),
        order_id=oid,
        placed_at=datetime(2026, 7, 10, 11, 0, tzinfo=UTC),
        order_type="Limit",
        is_trigger=False,
        trigger_price=None,
        is_position_tpsl=False,
        reduce_only=False,
    )


def _position(coin: str) -> Position:
    return Position(
        coin=coin,
        side=Side.LONG,
        size_usd=Decimal("1500.5"),
        leverage=Decimal("3"),
        entry_price=Decimal("100"),
        unrealized_pnl=Decimal("-12.25"),
    )


@pytest.fixture
def read_gateway() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.perp_universes[None] = ["BTC", "ETH", "SOL"]
    fake.perp_dex_listing = ["xyz", "mkts"]
    fake.perp_universes["xyz"] = ["xyz:META", "xyz:BB"]
    fake.perp_universes["mkts"] = ["mkts:US500"]
    return fake


@pytest.fixture
def exec_gateway() -> FakeExecutionGateway:
    return FakeExecutionGateway()


@pytest.fixture
def audit(pool: asyncpg.Pool, clock: FakeClock) -> ExecutionAudit:
    return ExecutionAudit(pool, clock)


@pytest.fixture
def audited(
    exec_gateway: FakeExecutionGateway, audit: ExecutionAudit
) -> AuditedExecutionGateway:
    return AuditedExecutionGateway(
        exec_gateway, audit, actor=WATCHDOG_ACTOR, master_address=MASTER, signer_address=SIGNER
    )


@pytest.fixture
def watchdog(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audited: AuditedExecutionGateway,
    audit: ExecutionAudit,
) -> Watchdog:
    return Watchdog(
        pool,
        clock,
        read_gateway,
        audited,
        audit,
        WeightBudget(1_000_000, clock),
        master_address=MASTER,
        executor_stale=STALE,
    )


def _cancels(exec_gateway: FakeExecutionGateway) -> list[CancelSpec]:
    return [
        spec
        for name, payload in exec_gateway.actions
        if name == "cancel_orders"
        for spec in payload  # type: ignore[union-attr]
    ]


async def _events(pool: asyncpg.Pool) -> list[str]:
    rows = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    return [r["action"] for r in rows]


async def test_simulated_stall_trips_and_cancel_alls(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
) -> None:
    # The stall is nothing but a stale row — no executor process exists here,
    # which is itself the independence criterion.
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(120)
    read_gateway.set_open_orders(MASTER, [_order("ETH", 11), _order("SOL", 12)])
    read_gateway.set_open_orders(MASTER, [_order("xyz:BB", 13)], dex="xyz")

    await watchdog.run_cycle()

    halt = await active_halt(pool)
    assert halt is not None
    assert halt.source == WATCHDOG_SOURCE
    assert "stale" in halt.reason
    # Cancel-all named every order across venues by its asset id + oid:
    # core coins by universe index, builder coins offset (110000 + …).
    assert _cancels(exec_gateway) == [
        CancelSpec(asset=1, oid=11),
        CancelSpec(asset=2, oid=12),
        CancelSpec(asset=110_001, oid=13),
    ]
    # The book still ENUMERATES non-empty (the read fake keeps serving the
    # orders), so the sweep is NOT stamped done — a cancel's word is not
    # enough.
    assert halt.swept_at is None

    # The exchange now shows an empty book; the next cycle's enumeration —
    # not the cancel — completes the sweep.
    read_gateway.set_open_orders(MASTER, [])
    read_gateway.set_open_orders(MASTER, [], dex="xyz")
    await watchdog.run_cycle()
    swept = await active_halt(pool)
    assert swept is not None
    assert swept.swept_at is not None
    assert await _events(pool) == ["halt", "halt_swept"]


async def test_fresh_heartbeat_and_no_heartbeat_do_not_trip(
    watchdog: Watchdog, pool: asyncpg.Pool, clock: FakeClock,
    exec_gateway: FakeExecutionGateway,
) -> None:
    # Never deployed: no row, no emergency (the pre-A4 production state).
    await watchdog.run_cycle()
    assert await active_halt(pool) is None
    # Deployed and beating: no trip either.
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(30)  # under the 60s threshold
    await watchdog.run_cycle()
    assert await active_halt(pool) is None
    assert exec_gateway.actions == []


async def test_kill_halt_is_swept_with_positions_held(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audit: ExecutionAudit,
) -> None:
    await request_halt(
        pool, clock, audit, source=KILL_SOURCE, reason="operator /kill", requested_by=ADMIN
    )
    read_gateway.set_positions(MASTER, [_position("ETH")])
    read_gateway.set_positions(MASTER, [_position("xyz:META")], dex="xyz")

    await watchdog.run_cycle()  # empty book: one cycle sweeps

    halt = await active_halt(pool)
    assert halt is not None
    assert halt.swept_at is not None
    assert halt.unwind_policy == HOLD_POLICY
    assert halt.positions is not None
    assert [p["coin"] for p in halt.positions] == ["ETH", "xyz:META"]
    assert halt.positions[0]["size_usd"] == "1500.5"  # Decimals ride as strings


async def test_ambiguous_cancel_is_never_read_as_swept(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """THE hazard test: a halt path that misreads an ambiguous cancel as
    "nothing happened" leaves live orders behind a swept stamp."""
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [_order("ETH", 21)])
    exec_gateway.errors.append(AmbiguousExecutionError("timed out — may have executed"))

    with pytest.raises(AmbiguousExecutionError):
        await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.swept_at is None  # ambiguous ≠ done
    ambiguous = await pool.fetch(
        "SELECT 1 FROM execution_audit WHERE outcome = $1", AMBIGUOUS
    )
    assert len(ambiguous) == 1  # the reconcile obligation is on the trail

    # Next cycle reconciles BY ENUMERATION: the order still rests, so it is
    # re-cancelled; once the book reads empty the sweep completes.
    await watchdog.run_cycle()
    assert len(_cancels(exec_gateway)) == 2  # first (ambiguous) + the retry
    read_gateway.set_open_orders(MASTER, [])
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.swept_at is not None


async def test_unmappable_coin_aborts_the_sweep_loudly(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [_order("DELISTED", 31)])
    with pytest.raises(GatewayError, match="DELISTED"):
        await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.swept_at is None  # a skipped order must never hide as swept
    assert _cancels(exec_gateway) == []


async def test_watchdog_beats_its_own_heartbeat(
    watchdog: Watchdog, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await watchdog.run_cycle()
    assert await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS) == clock.now()


async def test_watchdog_loop_survives_failing_cycles_and_maintains_the_deadman(
    watchdog: Watchdog,
    audited: AuditedExecutionGateway,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """The supervision property: a cycle that raises (here: an unmappable
    order coin aborting every sweep) is logged and retried, never allowed to
    stop the loop — and the deadman is maintained regardless."""
    from epigone.safety.deadman import DeadMansSwitch
    from epigone.safety.main import watchdog_loop

    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [_order("DELISTED", 41)])
    deadman = DeadMansSwitch(
        audited,  # the shared audited gateway, exactly as main.py wires it
        audit,
        clock,
        horizon=timedelta(seconds=300),
        reprobe=timedelta(hours=6),
        master_address=MASTER,
    )

    await watchdog_loop(watchdog, deadman, clock, 10.0, max_cycles=2)

    # Both cycles ran (the loop outlived the GatewayError) and beat the
    # heartbeat; the deadman probed once (the second tick was inside its
    # half-horizon cadence) and activated.
    assert await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS) is not None
    schedules = [name for name, _ in exec_gateway.actions if name == "schedule_cancel"]
    assert len(schedules) == 1
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.swept_at is None  # still unswept: every sweep aborted loudly


async def test_resume_with_a_still_stale_executor_retrips(
    watchdog: Watchdog, pool: asyncpg.Pool, clock: FakeClock, audit: ExecutionAudit
) -> None:
    """Resume is consent to trade, not an override: with the executor still
    silent, the switch trips again within one cycle."""
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(120)
    await watchdog.run_cycle()
    first = await active_halt(pool)
    assert first is not None
    await resume(pool, clock, audit, resumed_by=ADMIN)

    await watchdog.run_cycle()
    second = await active_halt(pool)
    assert second is not None
    assert second.id != first.id
    assert second.source == WATCHDOG_SOURCE
