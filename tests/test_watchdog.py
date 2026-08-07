"""The watchdog: the primary dead-man's switch (issue #135).

The acceptance criteria under test, in this file's order: a simulated
executor stall trips a halt and cancel-alls, independently of any executor
process (the stall is nothing but a stale DB row — no executor exists);
/kill-sourced halts are swept the same way within one cycle; the sweep NEVER
stamps done on a cancel's word alone (verify-by-enumeration, the
AmbiguousExecutionError discipline); positions are held and snapshotted per
the documented policy; and the watchdog beats its own heartbeat for the
monitor."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone.budget import SharedWeightBudget, WeightBudget
from epigone.gateway import ExtraAgent, GatewayError, OpenOrder, Position, Side
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
from epigone.safety.budget import FallbackBudget
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
from tests.support.orders import open_order

MASTER = "0x" + "ab" * 20
SIGNER = "0x" + "cd" * 20
ADMIN = 370818090
STALE = timedelta(seconds=60)


def _position(coin: str) -> Position:
    return Position(
        coin=coin,
        side=Side.LONG,
        size_usd=Decimal("1500.5"),
        leverage=Decimal("3"),
        entry_price=Decimal("100"),
        unrealized_pnl=Decimal("-12.25"),
    )


APPROVED_UNTIL = datetime(2026, 12, 1, tzinfo=UTC)
CAPABILITY_INTERVAL = timedelta(hours=6)
DB_BLIND = timedelta(seconds=180)


@pytest.fixture
def read_gateway() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.perp_universes[None] = ["BTC", "ETH", "SOL"]
    # `flip` is deliberately NOT in POSITION_VENUES: the sweep is account-wide
    # over the live listing, so an uncovered dex must be swept too.
    fake.perp_dex_listing = ["xyz", "flip", "mkts"]
    fake.perp_universes["xyz"] = ["xyz:META", "xyz:BB"]
    fake.perp_universes["flip"] = ["flip:GME"]
    fake.perp_universes["mkts"] = ["mkts:US500"]
    # The watchdog agent is approved and unexpired by default; impotence is
    # arranged explicitly by the capability tests.
    fake.extra_agents[MASTER] = [
        ExtraAgent(address=SIGNER, name="epigone-watchdog-a", valid_until=APPROVED_UNTIL)
    ]
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
        signer_address=SIGNER,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
    )


def _cancels(exec_gateway: FakeExecutionGateway) -> list[CancelSpec]:
    """Every cancelled (asset, oid), flattened across accounts. The sweep
    issues one cancel action PER ACCOUNT now (issue #136), so the fake's
    payload is (cancels, vault_address); `_cancels_by_account` is the view
    that keeps the account axis."""
    return [spec for _account, specs in _cancels_by_account(exec_gateway) for spec in specs]


def _cancels_by_account(
    exec_gateway: FakeExecutionGateway,
) -> list[tuple[str | None, list[CancelSpec]]]:
    return [
        (payload[1], payload[0])  # type: ignore[index]
        for name, payload in exec_gateway.actions
        if name == "cancel_orders"
    ]


async def _events(pool: asyncpg.Pool) -> list[str]:
    """Halt-lifecycle events only: the capability probe's verdict events have
    their own tests and would otherwise ride along in every cycle-driven
    sequence assertion (the first verdict is always evented by design)."""
    rows = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    return [r["action"] for r in rows if not r["action"].startswith("watchdog_")]


async def _capability_events(pool: asyncpg.Pool) -> list[str]:
    rows = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    return [r["action"] for r in rows if r["action"].startswith("watchdog_")]


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
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 11), open_order("SOL", 12)])
    read_gateway.set_open_orders(MASTER, [open_order("xyz:BB", 13)], dex="xyz")

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
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 21)])
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
    read_gateway.set_open_orders(MASTER, [open_order("DELISTED", 31)])
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
    read_gateway.set_open_orders(MASTER, [open_order("DELISTED", 41)])
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
    await resume(pool, clock, audit, halt_id=first.id, resumed_by=ADMIN)

    await watchdog.run_cycle()
    second = await active_halt(pool)
    assert second is not None
    assert second.id != first.id
    assert second.source == WATCHDOG_SOURCE


# --- the on-chain capability probe (PR #143 review) ---


async def test_capable_watchdog_records_its_verdict_once(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
) -> None:
    await watchdog.run_cycle()
    row = await pool.fetchrow(
        "SELECT capable, capability_detail FROM process_heartbeats WHERE process = 'watchdog'"
    )
    assert row is not None and row["capable"] is True
    assert "epigone-watchdog-a" in row["capability_detail"]
    assert await _capability_events(pool) == ["watchdog_capable"]
    assert read_gateway.extra_agents_calls == [MASTER]

    # Within the interval: no re-read. Past it, unchanged verdict: a fresh
    # read but NO second event — events mark transitions only.
    await watchdog.run_cycle()
    assert read_gateway.extra_agents_calls == [MASTER]
    clock.advance(CAPABILITY_INTERVAL.total_seconds())
    await watchdog.run_cycle()
    assert read_gateway.extra_agents_calls == [MASTER, MASTER]
    assert await _capability_events(pool) == ["watchdog_capable"]


async def test_deregistered_agent_pages_as_impotent(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    read_gateway: FakeHyperliquidGateway,
) -> None:
    """The beating-but-impotent hazard: heartbeats keep landing, but the
    on-chain verdict must flip to impotent so the monitor pages BEFORE an
    incident needs the cancel."""
    read_gateway.extra_agents[MASTER] = []  # deregistered mid-run
    await watchdog.run_cycle()
    row = await pool.fetchrow(
        "SELECT beaten_at, capable, capability_detail FROM process_heartbeats "
        "WHERE process = 'watchdog'"
    )
    assert row is not None
    assert row["beaten_at"] is not None  # still beating…
    assert row["capable"] is False  # …but powerless, and the row says so
    assert "deregistered" in row["capability_detail"]
    assert await _capability_events(pool) == ["watchdog_impotent"]


async def test_expired_approval_is_impotent(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
) -> None:
    read_gateway.extra_agents[MASTER] = [
        ExtraAgent(
            address=SIGNER,
            name="epigone-watchdog-a",
            valid_until=clock.now() - timedelta(hours=1),
        )
    ]
    await watchdog.run_cycle()
    row = await pool.fetchrow(
        "SELECT capable, capability_detail FROM process_heartbeats WHERE process = 'watchdog'"
    )
    assert row is not None and row["capable"] is False
    assert "EXPIRED" in row["capability_detail"]


async def test_capability_read_failure_neither_flaps_nor_blocks(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audit: ExecutionAudit,
) -> None:
    """An info outage is not a verdict — and, above all, must not delay the
    sweep (the probe runs after the protective work and swallows failures)."""
    read_gateway.extra_agents_errors[MASTER] = GatewayError("info down")
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    await watchdog.run_cycle()  # sweeps (empty book) despite the probe failing
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None
    row = await pool.fetchrow(
        "SELECT capable FROM process_heartbeats WHERE process = 'watchdog'"
    )
    assert row is not None and row["capable"] is None  # no verdict recorded
    assert await _capability_events(pool) == []


# --- account-wide sweep coverage (PR #143 review) ---


async def test_sweep_covers_dexs_outside_position_venues(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """The agent key is account-wide, so the kill must be too: an order on
    `flip` — listed on-chain but NOT in POSITION_VENUES — is enumerated and
    cancelled with the offset its listing position fixes (110000 + 1×10000)."""
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [open_order("flip:GME", 51)], dex="flip")

    await watchdog.run_cycle()

    assert _cancels(exec_gateway) == [CancelSpec(asset=120_000, oid=51)]
    read_gateway.set_open_orders(MASTER, [], dex="flip")
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None


async def test_sweep_reaches_every_copy_sub_account(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """ADR-0007 decision 1: A4 is the first thing that can place an order on a
    Copy Sub-account, so the sweep must reach one. Each account's cancel
    carries that account's vault flag — the master's is None — because a
    cancel names a book, and cancelling a sub's order against the master's
    book would silently do nothing."""
    sub_a, sub_b = "0x" + "11" * 20, "0x" + "22" * 20
    read_gateway.sub_accounts[MASTER] = [sub_a, sub_b]
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 11)])
    read_gateway.set_open_orders(sub_a, [open_order("SOL", 21)])
    read_gateway.set_open_orders(sub_b, [open_order("xyz:BB", 31)], dex="xyz")

    await watchdog.run_cycle()

    assert _cancels_by_account(exec_gateway) == [
        (None, [CancelSpec(asset=1, oid=11)]),
        (sub_a, [CancelSpec(asset=2, oid=21)]),
        (sub_b, [CancelSpec(asset=110_001, oid=31)]),
    ]


async def test_a_sub_accounts_outage_degrades_to_a_partial_sweep(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """The same degrade rule the perpDexs outage already obeys, on the account
    axis: sweep what is certainly covered, say so, and never stamp swept_at —
    a halt marked swept while a sub-account's book was never enumerated is
    exactly the silent-live-order hazard the verify exists to prevent."""
    read_gateway.sub_account_errors[MASTER] = GatewayError("subAccounts down")
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 11)])

    await watchdog.run_cycle()

    assert _cancels(exec_gateway) == [CancelSpec(asset=1, oid=11)]  # master swept
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is None  # …but never stamped


async def test_the_halt_snapshot_covers_positions_held_in_a_sub(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audit: ExecutionAudit,
) -> None:
    # Positions are HELD, not closed — so the operator's alert has to list the
    # ones sitting in a Copy Sub-account too, or hold-and-alert is only half a
    # policy for exactly the accounts A4 put money in.
    sub = "0x" + "11" * 20
    read_gateway.sub_accounts[MASTER] = [sub]
    read_gateway.set_positions(sub, [_position("SOL")])
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")

    await watchdog.run_cycle()

    halt = await active_halt(pool)
    assert halt is not None and halt.positions is not None
    assert [p["coin"] for p in halt.positions] == ["SOL"]


# --- Postgres independence: the trip→wire path (PR #143 round 2) ---


async def test_blind_trip_cancels_at_the_real_budget_seam(
    database_url: str,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
) -> None:
    """THE round-2 structural guarantee, tested with the production wiring
    shape: pool dead, SharedWeightBudget over that same dead pool (the
    FOR UPDATE seam the round-1 test never touched), best-effort audit —
    and the cancel still reaches the exchange. Within the blind threshold
    the watchdog waits (a blip is not an incident); past it, it trips."""
    dead_pool = await asyncpg.create_pool(database_url)
    assert dead_pool is not None
    await dead_pool.close()
    audit = ExecutionAudit(dead_pool, clock)
    audited = AuditedExecutionGateway(
        exec_gateway, audit,
        actor=WATCHDOG_ACTOR, master_address=MASTER, signer_address=SIGNER,
        best_effort_audit=True,
    )
    watchdog = Watchdog(
        dead_pool,
        clock,
        read_gateway,
        audited,
        audit,
        FallbackBudget(SharedWeightBudget(dead_pool, clock, reserve=0), clock),
        master_address=MASTER,
        signer_address=SIGNER,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
    )
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 61)])

    await watchdog.run_cycle()  # blind for 0s: wait, don't trip, don't raise
    assert _cancels(exec_gateway) == []

    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()  # blind past threshold: the wire still works
    assert _cancels(exec_gateway) == [CancelSpec(asset=1, oid=61)]

    # And it KEEPS sweeping while blind — the book stays empty all outage.
    clock.advance(10)
    await watchdog.run_cycle()
    assert len(_cancels(exec_gateway)) == 2


async def test_blind_sweep_reconciles_into_a_distinct_halt_on_recovery(
    watchdog: Watchdog,
    audited: AuditedExecutionGateway,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from epigone.safety import watchdog as watchdog_module

    async def db_down(_pool: asyncpg.Pool) -> None:
        raise ConnectionError("postgres unreachable")

    read_gateway.set_open_orders(MASTER, [open_order("ETH", 62)])
    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    await watchdog.run_cycle()  # the failure STREAK opens here (round 3 item 3)
    assert _cancels(exec_gateway) == []
    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()  # unbroken past the threshold: blind trip
    assert len(_cancels(exec_gateway)) == 1
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 0

    # Postgres answers again: the halt row lands with the DISTINCT blind
    # reason (operator can tell it from a real stall), then the normal sweep
    # verifies the (now empty) book and stamps it.
    monkeypatch.undo()
    read_gateway.set_open_orders(MASTER, [])
    await watchdog.run_cycle()
    assert audited.wire_first is False  # posture cleared with the incident
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.source == WATCHDOG_SOURCE
    assert "DB-blind sweep reconciled" in halt.reason
    assert "Postgres unreadable without interruption" in halt.reason
    assert halt.swept_at is not None


async def test_beat_failure_never_skips_the_protective_work(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    audit: ExecutionAudit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def beat_down(*args: object) -> None:
        raise ConnectionError("heartbeat write refused")

    monkeypatch.setattr(heartbeat, "beat", beat_down)
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    await watchdog.run_cycle()  # the sweep must run despite the failed beat
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None


async def test_unwritable_halt_row_never_suppresses_the_trip_cancel(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from epigone.safety import watchdog as watchdog_module

    async def halt_write_down(*args: object, **kwargs: object) -> object:
        raise ConnectionError("halt row unwritable")

    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(120)  # a REAL stall, with only the halt WRITE failing
    read_gateway.set_open_orders(MASTER, [open_order("SOL", 63)])
    monkeypatch.setattr(watchdog_module, "request_halt", halt_write_down)
    await watchdog.run_cycle()
    assert _cancels(exec_gateway) == [CancelSpec(asset=2, oid=63)]
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 0

    # Recovery: the halt is reconciled and carries the ORIGINAL stall reason.
    monkeypatch.undo()
    read_gateway.set_open_orders(MASTER, [])
    clock.advance(10)  # a later cycle: this is a reconciled window, not a same-cycle trip
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None
    assert "unrecorded trip reconciled" in halt.reason  # NOT labelled DB-blind
    assert "executor heartbeat stale" in halt.reason
    assert halt.swept_at is not None


async def test_perp_dexs_outage_degrades_to_a_partial_sweep(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
) -> None:
    """Round 2 item 3: a dead listing endpoint must not abort the whole
    sweep — core-venue orders still die (partial coverage, reported) — but
    partial coverage can never stamp swept_at."""
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 64)])
    read_gateway.perp_dex_error = GatewayError("perpDexs down")

    await watchdog.run_cycle()
    assert _cancels(exec_gateway) == [CancelSpec(asset=1, oid=64)]
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is None  # PARTIAL ≠ swept

    # Even with an EMPTY book, partial coverage withholds the stamp…
    read_gateway.set_open_orders(MASTER, [])
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is None
    # …and full coverage restored completes the sweep.
    read_gateway.perp_dex_error = None
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None


async def test_capability_fuse_advances_on_any_failure_shape(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
) -> None:
    """Round 2 item 4: a bare TimeoutError (not a GatewayError) must hit the
    5-minute retry fuse, not re-fire the probe every cycle."""
    read_gateway.extra_agents_errors[MASTER] = TimeoutError("info hang")
    await watchdog.run_cycle()
    assert read_gateway.extra_agents_calls == [MASTER]
    clock.advance(10)
    await watchdog.run_cycle()  # inside the fuse: no re-read
    assert read_gateway.extra_agents_calls == [MASTER]
    clock.advance(5 * 60)
    await watchdog.run_cycle()  # past the fuse: retried
    assert read_gateway.extra_agents_calls == [MASTER, MASTER]


async def test_unrecordable_verdict_also_hits_the_fuse(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def verdict_write_down(*args: object, **kwargs: object) -> None:
        raise ConnectionError("postgres refused")

    monkeypatch.setattr(heartbeat, "record_capability", verdict_write_down)
    await watchdog.run_cycle()
    assert read_gateway.extra_agents_calls == [MASTER]
    clock.advance(10)
    await watchdog.run_cycle()  # write failure advanced the fuse too
    assert read_gateway.extra_agents_calls == [MASTER]


# --- round 3: hangs handled elsewhere (test_safety_db); here, the half-
# --- recovered database and the blind clock's semantics ---


async def test_writes_broken_window_keeps_cancelling_every_cycle(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 3 item 2: reads recovered, writes broken (read-only recovery,
    full WAL). Reconciliation is bookkeeping — it must never gate
    protection: the cancel pass runs EVERY cycle of the writes-broken
    window, not just once at trip time."""
    from epigone.safety import watchdog as watchdog_module

    async def writes_broken(*args: object, **kwargs: object) -> object:
        raise ConnectionError("read-only transaction")

    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(120)  # a REAL stall
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 71)])
    monkeypatch.setattr(watchdog_module, "request_halt", writes_broken)

    for cycle in range(1, 5):  # trip + three writes-still-broken cycles
        await watchdog.run_cycle()
        cancel_calls = [n for n, _ in exec_gateway.actions if n == "cancel_orders"]
        assert len(cancel_calls) == cycle  # a cancel pass EVERY cycle
        clock.advance(10)
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 0

    # Writes recover: the halt lands under the real-stall headline, the
    # blind window's audit event counts every unrecorded pass, and the
    # (now empty) book sweeps.
    monkeypatch.undo()
    read_gateway.set_open_orders(MASTER, [])
    await watchdog.run_cycle()
    halt = await active_halt(pool)
    assert halt is not None
    assert "unrecorded trip reconciled" in halt.reason
    assert halt.swept_at is not None
    event = await pool.fetchrow(
        "SELECT detail FROM execution_audit WHERE action = 'blind_window_reconciled'"
    )
    assert event is not None
    # 4 passes across the writes-broken cycles + 1 on the recovery cycle
    # (the incident cancels BEFORE reconciling — round 5's wire-first rule).
    assert '"unrecorded_cancel_passes": 5' in event["detail"]


async def test_alternating_read_failures_never_blind_trip(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 3 item 3: the threshold means 'unreadable CONTINUOUSLY', not
    'one failure landing long after the last success'. Long gaps between
    cycles (a degraded exchange stretching each cycle) plus isolated
    dropped connections must never sum into a false account-wide cancel."""
    from epigone.safety import watchdog as watchdog_module

    real_active_halt = watchdog_module.active_halt
    calls = {"n": 0}

    async def flaky_active_halt(p: asyncpg.Pool) -> object | None:
        calls["n"] += 1
        if calls["n"] % 2 == 1:  # every other cycle drops its connection
            raise ConnectionError("connection reset by peer")
        return await real_active_halt(p)

    monkeypatch.setattr(watchdog_module, "active_halt", flaky_active_halt)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 72)])
    for _ in range(6):
        clock.advance(200)  # every gap alone exceeds the 180s threshold
        await watchdog.run_cycle()
    assert _cancels(exec_gateway) == []  # never tripped: each success reset the streak
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 0


async def test_unbroken_failure_streak_still_trips(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from epigone.safety import watchdog as watchdog_module

    async def db_down(p: asyncpg.Pool) -> object | None:
        raise ConnectionError("still down")

    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 73)])
    await watchdog.run_cycle()  # streak opens here; 0s blind → wait
    assert _cancels(exec_gateway) == []
    clock.advance(100)
    await watchdog.run_cycle()  # 100s unbroken → still waiting
    assert _cancels(exec_gateway) == []
    clock.advance(100)
    await watchdog.run_cycle()  # 200s unbroken → trip
    assert _cancels(exec_gateway) == [CancelSpec(asset=1, oid=73)]


async def test_blind_window_joining_a_standing_halt_still_leaves_a_trace(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audit: ExecutionAudit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 3 item 4: /kill stands, THEN Postgres dies, blind sweeps run
    (their best-effort audit rows failing with the same outage), recovery
    joins the standing halt — which by design writes no new halt state. The
    blind window must still land durably: the reconcile event is
    unconditional."""
    from epigone.safety import watchdog as watchdog_module

    await request_halt(
        pool, clock, audit, source=KILL_SOURCE, reason="operator /kill", requested_by=ADMIN
    )

    async def db_down(p: asyncpg.Pool) -> object | None:
        raise ConnectionError("down")

    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()  # streak opens
    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()  # blind trip + first unrecorded pass

    monkeypatch.undo()
    await watchdog.run_cycle()  # reconcile: joins the /kill halt
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 1
    halt = await active_halt(pool)
    assert halt is not None and halt.source == KILL_SOURCE  # joined, not replaced
    event = await pool.fetchrow(
        "SELECT detail, risk_decision FROM execution_audit "
        "WHERE action = 'blind_window_reconciled'"
    )
    assert event is not None
    assert '"joined_standing_halt": true' in event["detail"]
    assert "DB-blind sweep" in event["risk_decision"]


async def test_a_trip_reaches_the_wire_before_any_halt_state_is_written(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
    audited: AuditedExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 5's ordering rule (as amended by round 6), pinned: a real stall
    trip cancels BEFORE the halt row is attempted, the cancel pass runs with
    the rate budget in incident mode — the shared Postgres bucket untouched —
    but the trip KEEPS its write-ahead audit attempt (wire_first is a
    DB-blind-only posture): the attempt row precedes the cancel."""
    from epigone.safety import watchdog as watchdog_module
    from tests.test_safety_budget import _RecordingDeadPrimary

    shared = _RecordingDeadPrimary()
    watchdog = Watchdog(
        pool,
        clock,
        read_gateway,
        audited,
        audit,
        FallbackBudget(shared, clock),
        master_address=MASTER,
        signer_address=SIGNER,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
    )
    real_request_halt = watchdog_module.request_halt
    observed = {"cancels_at_halt_write": -1, "shared_calls_at_halt_write": -1}

    async def order_asserting_request_halt(*args: object, **kwargs: object) -> object:
        observed["cancels_at_halt_write"] = len(
            [n for n, _ in exec_gateway.actions if n == "cancel_orders"]
        )
        observed["shared_calls_at_halt_write"] = shared.calls
        return await real_request_halt(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(watchdog_module, "request_halt", order_asserting_request_halt)
    # The trip's audit stays WRITE-AHEAD (round 6 item 3): record where each
    # attempt row lands relative to the cancel on the fake gateway's tape.
    real_record_attempt = audit.record_attempt
    attempt_orderings: list[int] = []

    async def order_recording_attempt(*args: object, **kwargs: object) -> object:
        attempt_orderings.append(
            len([n for n, _ in exec_gateway.actions if n == "cancel_orders"])
        )
        return await real_record_attempt(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(audit, "record_attempt", order_recording_attempt)
    await heartbeat.beat(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
    clock.advance(120)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 81)])

    await watchdog.run_cycle()

    assert observed["cancels_at_halt_write"] == 1  # the wire came FIRST
    assert observed["shared_calls_at_halt_write"] == 0  # and touched no shared bucket
    # Write-ahead evidence kept on a live DB: the cancel's attempt row was
    # recorded BEFORE the cancel reached the fake gateway.
    assert attempt_orderings and attempt_orderings[0] == 0
    halt = await active_halt(pool)
    assert halt is not None and "stale" in halt.reason


async def test_a_post_reconcile_blip_does_not_retrip(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    audit: ExecutionAudit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 6 item 1 — the streak dies with the incident: after a DB-blind
    window reconciles, a SINGLE later read failure must open a FRESH streak
    (the reconcile's successful writes were the interruption), never re-trip
    instantly off the old onset with a false 'without interruption' span."""
    from epigone.safety import watchdog as watchdog_module

    async def db_down(_pool: asyncpg.Pool) -> None:
        raise ConnectionError("postgres unreachable")

    # A full blind incident, reconciled: outage, trip, recovery.
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 95)])
    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    await watchdog.run_cycle()  # streak opens
    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()  # blind trip (cancel #1)
    monkeypatch.undo()
    read_gateway.set_open_orders(MASTER, [])
    clock.advance(10)
    await watchdog.run_cycle()  # reconciles + sweeps
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None
    await resume(pool, clock, audit, halt_id=halt.id, resumed_by=ADMIN)
    cancels_after_incident = len(_cancels(exec_gateway))

    # Much later, ONE dropped connection. Before the fix, blind_for was
    # computed from the ORIGINAL outage onset and re-tripped immediately.
    clock.advance(3600)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 96)])
    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    await watchdog.run_cycle()
    assert len(_cancels(exec_gateway)) == cancels_after_incident  # no false sweep
    assert await pool.fetchval(
        "SELECT count(*) FROM execution_halts WHERE resumed_at IS NULL"
    ) == 0

    # The blip heals: still nothing. Only a fresh unbroken streak may trip.
    monkeypatch.undo()
    clock.advance(10)
    await watchdog.run_cycle()
    assert len(_cancels(exec_gateway)) == cancels_after_incident


# --- sweep liveness (issue #201) ---------------------------------------
#
# The 2026-08-07 shakedown's first real /kill: the sweep ground through
# account-wide REST enumeration under budget pacing for eight minutes, and
# for all eight the watchdog's heartbeat was frozen and its dead-man's
# schedule went un-refreshed (it fired mid-halt). The process was alive and
# doing its most important work while every liveness signal it owns said
# otherwise. These tests pin the fix: a sweep pulses.


async def test_the_heartbeat_beats_while_a_long_sweep_grinds(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audit: ExecutionAudit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #201, deliverable 1: the beat happens INSIDE the enumeration
    loop, not only between cycles. Before the fix `beaten_at` froze at the
    cycle's first instant and the #52 monitor read the busiest watchdog in
    the system as dead."""
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    cycle_start = clock.now()
    seen: list[datetime | None] = []
    inner = read_gateway.get_open_orders

    async def paced(address: str, dex: str | None = None) -> list[OpenOrder]:
        # Every enumeration costs ORDERS_WEIGHT against a 900/min bucket
        # shared with the stream and ingest — seconds of pacing per call in
        # production, compressed here into the fake clock.
        clock.advance(30)
        seen.append(await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS))
        return await inner(address, dex)

    monkeypatch.setattr(read_gateway, "get_open_orders", paced)

    await watchdog.run_cycle()

    assert len(seen) >= 4  # core + three builder dexs
    assert seen[-1] is not None and seen[-1] > cycle_start
    beaten = await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS)
    assert beaten is not None and beaten > cycle_start


async def test_a_grinding_sweep_keeps_the_dead_mans_switch_pushed_forward(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audited: AuditedExecutionGateway,
    audit: ExecutionAudit,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #201, deliverable 2: the dead-man refresh no longer waits for
    the cycle to end. In production the 18:18 push armed the exchange for
    ~18:23 and the sweep entered at 18:20:15 — so the schedule FIRED
    un-refreshed mid-halt, discharging the last-resort net while the
    watchdog was still working. The push now rides the sweep's own pulse."""
    from epigone.safety.deadman import DeadMansSwitch

    horizon = timedelta(seconds=300)
    deadman = DeadMansSwitch(
        audited,
        audit,
        clock,
        horizon=horizon,
        reprobe=timedelta(hours=6),
        master_address=MASTER,
    )
    watchdog = Watchdog(
        pool,
        clock,
        read_gateway,
        audited,
        audit,
        WeightBudget(1_000_000, clock),
        master_address=MASTER,
        signer_address=SIGNER,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
        keepalive=deadman.maintain,
    )
    await deadman.maintain()  # armed: now+300, next push due at now+150
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    inner = read_gateway.get_open_orders

    async def paced(address: str, dex: str | None = None) -> list[OpenOrder]:
        clock.advance(100)
        return await inner(address, dex)

    monkeypatch.setattr(read_gateway, "get_open_orders", paced)

    await watchdog.run_cycle()

    armed = [at for name, at in exec_gateway.actions if name == "schedule_cancel"]
    assert len(armed) >= 3  # the initial arm plus pushes from INSIDE the sweep
    latest = armed[-1]
    assert isinstance(latest, datetime) and latest > clock.now()  # never lapsed


async def test_a_blind_sweep_still_touches_no_postgres_before_the_wire(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #201 pulse is GATED ON THE INCIDENT (round 5's structural rule,
    module docstring): once an incident is declared the cycle does no
    Postgres before the cancel reaches the wire — the liveness beat
    included. A watchdog that is blind because the database is unreachable
    cannot say "alive" to that database, and trying is precisely the hang
    round 5 removed."""
    from epigone.safety import watchdog as watchdog_module

    async def db_down(_pool: asyncpg.Pool) -> None:
        raise ConnectionError("postgres unreachable")

    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 201)])
    await watchdog.run_cycle()  # the failure streak opens
    clock.advance(DB_BLIND.total_seconds() + 1)

    beats: list[object] = []
    real_beat = heartbeat.beat

    async def counting_beat(*args: object) -> None:
        beats.append(args)
        await real_beat(*args)  # type: ignore[arg-type]

    monkeypatch.setattr(heartbeat, "beat", counting_beat)
    inner = read_gateway.get_open_orders

    async def paced(address: str, dex: str | None = None) -> list[OpenOrder]:
        clock.advance(30)
        return await inner(address, dex)

    monkeypatch.setattr(read_gateway, "get_open_orders", paced)

    await watchdog.run_cycle()  # blind trip: the cancel pass runs dark

    assert len(_cancels(exec_gateway)) == 1  # the wire still works
    # Exactly one beat: the cycle-top beat that ran BEFORE the reads failed
    # and the incident was declared. The enumeration itself stayed dark.
    assert len(beats) == 1


async def test_a_long_sweep_reports_its_progress(
    watchdog: Watchdog,
    pool: asyncpg.Pool,
    clock: FakeClock,
    audit: ExecutionAudit,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #201, deliverable 4: the operator could not tell "sweeping, 60%
    done" from "wedged" — `swept_at` is the only signal and it lands only at
    the very end. Per-(account, dex) progress makes the grind observable."""
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")

    with caplog.at_level(logging.INFO, logger="epigone.safety.watchdog"):
        await watchdog.run_cycle()

    progress = [r.getMessage() for r in caplog.records if "sweep progress" in r.getMessage()]
    assert len(progress) >= 4  # core + three builder dexs, per account
    assert any("flip" in line for line in progress)
    # And the scope is announced up front, so the wall clock is predictable.
    assert any("sweep scope" in r.getMessage() for r in caplog.records)


async def test_a_wedged_keepalive_never_stalls_the_sweep(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    audited: AuditedExecutionGateway,
    audit: ExecutionAudit,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pulse's keepalive is ADVISORY work riding the protective path:
    a dead-man push that hangs on the exchange must cost its own ceiling and
    nothing else. The sweep — the thing that actually cancels orders —
    finishes and stamps, and the next pulse retries the push."""
    from epigone.safety import watchdog as watchdog_module

    monkeypatch.setattr(watchdog_module, "KEEPALIVE_CEILING_SECONDS", 0.05)
    attempts = 0

    async def wedged() -> None:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(3600)  # real seconds: this call never returns

    watchdog = Watchdog(
        pool,
        clock,
        read_gateway,
        audited,
        audit,
        WeightBudget(1_000_000, clock),
        master_address=MASTER,
        signer_address=SIGNER,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
        keepalive=wedged,
    )
    await request_halt(pool, clock, audit, source=KILL_SOURCE, reason="/kill")
    inner = read_gateway.get_open_orders

    async def paced(address: str, dex: str | None = None) -> list[OpenOrder]:
        clock.advance(30)
        return await inner(address, dex)

    monkeypatch.setattr(read_gateway, "get_open_orders", paced)

    await watchdog.run_cycle()

    assert attempts >= 3  # cut, and retried at each later pulse
    halt = await active_halt(pool)
    assert halt is not None and halt.swept_at is not None
