"""Execution-halt state and process heartbeats (issue #135).

The halt store is the kill switch's memory: at most one active halt
(DB-enforced), joined rather than stacked, swept exactly once, resumed only
explicitly — and every transition writes its audit event in the same
transaction, so the trail can never disagree with the state."""

import asyncpg
import pytest

from epigone.safety import heartbeat
from epigone.safety.audit import EVENT, ExecutionAudit
from epigone.safety.halt import (
    HOLD_POLICY,
    KILL_SOURCE,
    WATCHDOG_SOURCE,
    active_halt,
    is_halted,
    mark_swept,
    request_halt,
    resume,
)
from tests.support.clock import FakeClock

ADMIN = 370818090


@pytest.fixture
def audit(pool: asyncpg.Pool, clock: FakeClock) -> ExecutionAudit:
    return ExecutionAudit(pool, clock)


async def _events(pool: asyncpg.Pool) -> list[tuple[str, str]]:
    rows = await pool.fetch(
        "SELECT actor, action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    return [(r["actor"], r["action"]) for r in rows]


async def test_kill_halt_opens_and_audits(
    pool: asyncpg.Pool, clock: FakeClock, audit: ExecutionAudit
) -> None:
    assert not await is_halted(pool)
    halt, created = await request_halt(
        pool, clock, audit, source=KILL_SOURCE, reason="operator /kill", requested_by=ADMIN
    )
    assert created
    assert halt.source == KILL_SOURCE
    assert halt.requested_by == ADMIN
    assert halt.halted_at == clock.now()
    assert halt.swept_at is None
    assert await is_halted(pool)
    assert await _events(pool) == [("operator", "halt")]


async def test_second_halt_joins_the_standing_one(
    pool: asyncpg.Pool, clock: FakeClock, audit: ExecutionAudit
) -> None:
    first, _ = await request_halt(
        pool, clock, audit, source=WATCHDOG_SOURCE, reason="heartbeat stale"
    )
    second, created = await request_halt(
        pool, clock, audit, source=KILL_SOURCE, reason="operator /kill", requested_by=ADMIN
    )
    assert not created
    assert second.id == first.id
    assert second.source == WATCHDOG_SOURCE  # the standing halt, not a new one
    # One incident, one audit event — the joined request writes nothing.
    assert await _events(pool) == [("watchdog", "halt")]


async def test_sweep_stamps_once_with_positions(
    pool: asyncpg.Pool, clock: FakeClock, audit: ExecutionAudit
) -> None:
    halt, _ = await request_halt(
        pool, clock, audit, source=WATCHDOG_SOURCE, reason="heartbeat stale"
    )
    snapshot = [{"coin": "ETH", "side": "long", "size_usd": "1500.5"}]
    clock.advance(5)
    await mark_swept(
        pool, clock, audit, halt=halt, positions=snapshot, unwind_policy=HOLD_POLICY
    )
    swept = await active_halt(pool)
    assert swept is not None
    assert swept.swept_at == clock.now()
    assert swept.unwind_policy == HOLD_POLICY
    assert swept.positions == snapshot

    # A second sweep of the same halt is a no-op — no double audit event.
    await mark_swept(pool, clock, audit, halt=halt, positions=[], unwind_policy=HOLD_POLICY)
    still = await active_halt(pool)
    assert still is not None
    assert still.positions == snapshot
    assert await _events(pool) == [("watchdog", "halt"), ("watchdog", "halt_swept")]


async def test_resume_requires_an_active_halt_and_keeps_history(
    pool: asyncpg.Pool, clock: FakeClock, audit: ExecutionAudit
) -> None:
    assert await resume(pool, clock, audit, resumed_by=ADMIN) is None

    first, _ = await request_halt(
        pool, clock, audit, source=KILL_SOURCE, reason="operator /kill", requested_by=ADMIN
    )
    clock.advance(60)
    closed = await resume(pool, clock, audit, resumed_by=ADMIN)
    assert closed is not None
    assert closed.id == first.id
    assert closed.resumed_at == clock.now()
    assert closed.resumed_by == ADMIN
    assert not await is_halted(pool)

    # A new halt after resume is a NEW row — halt rows are history.
    second, created = await request_halt(
        pool, clock, audit, source=WATCHDOG_SOURCE, reason="stale again"
    )
    assert created
    assert second.id != first.id
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 2
    assert await _events(pool) == [
        ("operator", "halt"),
        ("operator", "resume"),
        ("watchdog", "halt"),
    ]


async def test_heartbeats_upsert_and_read(pool: asyncpg.Pool, clock: FakeClock) -> None:
    assert await heartbeat.last_beat(pool, heartbeat.EXECUTOR_PROCESS) is None
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    first = await heartbeat.last_beat(pool, heartbeat.EXECUTOR_PROCESS)
    assert first == clock.now()
    clock.advance(30)
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    assert await heartbeat.last_beat(pool, heartbeat.EXECUTOR_PROCESS) == clock.now()
    # One row per process — state, not history.
    assert await pool.fetchval("SELECT count(*) FROM process_heartbeats") == 1
