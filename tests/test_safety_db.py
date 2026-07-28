"""The safety pool's bounded DB touches (issue #135, PR #143 round 3).

Round 2's degradation design engages on EXCEPTIONS — but the classic
correlated failure hangs instead of raising. These tests build the
HANG-shaped outages (a socket that accepts and never answers; a query that
sleeps; a FOR UPDATE behind a holder that never commits) and assert the
safety pool turns each into a bounded exception — real wall-clock bounds,
because the timeouts under test are real. Sub-second test values; the
production defaults are epigone.safety.db's."""

import asyncio
import time

import asyncpg
import pytest

from epigone.budget import SharedWeightBudget
from epigone.safety.budget import FallbackBudget
from epigone.safety.db import create_safety_pool
from tests.support.clock import FakeClock

# Generous real-time ceilings: an order of magnitude above the configured
# sub-second timeouts, far below the hang timescales (60s connect default,
# ~15min TCP retransmission, infinite lock wait) they replace.
ELAPSED_CEILING = 10.0


async def test_a_black_holed_connect_raises_within_the_bound() -> None:
    stop = asyncio.Event()

    async def never_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Accept, then say nothing — the partitioned-host shape. Released via
        # the event: Server.wait_closed (3.12+) waits for handlers, so a
        # sleep-forever handler would hang the TEST'S cleanup instead.
        try:
            await stop.wait()
        finally:
            writer.close()

    server = await asyncio.start_server(never_answer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await create_safety_pool(
                f"postgresql://epigone@127.0.0.1:{port}/nowhere", timeout_seconds=0.5
            )
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        stop.set()
        server.close()
        await server.wait_closed()


async def test_a_hanging_query_raises_within_the_bound(database_url: str) -> None:
    pool = await create_safety_pool(database_url, timeout_seconds=0.5)
    try:
        started = time.monotonic()
        # command_timeout is enforced CLIENT-side by asyncpg — the property
        # that makes it hold even against a server that never answers.
        with pytest.raises(TimeoutError):
            await pool.execute("SELECT pg_sleep(30)")
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        await pool.close()


async def test_a_wedged_lock_row_degrades_to_the_fallback_within_the_bound(
    pool: asyncpg.Pool, database_url: str, clock: FakeClock
) -> None:
    """The round-3 headline case: SELECT … FOR UPDATE behind a holder that
    died mid-transaction blocks FOREVER with no exception — FallbackBudget
    never fires, the cancel queues behind a corpse. With the safety pool's
    lock_timeout the wait becomes an error, the fallback grants, and
    time-to-spend (the gate before every cancel) is bounded."""
    await SharedWeightBudget(pool, clock, reserve=0).spend(1)  # the row exists
    safety_pool = await create_safety_pool(
        database_url, timeout_seconds=2.0, lock_timeout="500ms"
    )
    wedge = await asyncpg.connect(database_url)
    try:
        wedge_tx = wedge.transaction()
        await wedge_tx.start()
        await wedge.execute("SELECT * FROM rate_budget FOR UPDATE")  # …and never commits

        budget = FallbackBudget(SharedWeightBudget(safety_pool, clock, reserve=0), clock)
        started = time.monotonic()
        await budget.spend(20)  # must be granted (fallback), not queued forever
        assert time.monotonic() - started < ELAPSED_CEILING
        await wedge_tx.rollback()
    finally:
        await wedge.close()
        await safety_pool.close()


async def test_time_to_first_cancel_is_bounded_under_a_fully_hanging_database(
    pool: asyncpg.Pool,
    database_url: str,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The COMPOSED round-3 guarantee, end to end: every DB touch of a full
    watchdog cycle hangs (each bounded by the safety pool — the heartbeat
    write, the liveness read, and every rate-budget spend behind a wedged
    lock row), and a cancel still reaches the gateway within real seconds,
    not TCP-retransmission minutes. The blind threshold itself rides the
    injected clock; what this test bounds in wall time is the hangs."""
    from datetime import timedelta

    from epigone.gateway.execution_fake import FakeExecutionGateway
    from epigone.gateway.fake import FakeHyperliquidGateway
    from epigone.safety import heartbeat
    from epigone.safety import watchdog as watchdog_module
    from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
    from epigone.safety.watchdog import Watchdog
    from tests.test_watchdog import MASTER, SIGNER, _order

    await SharedWeightBudget(pool, clock, reserve=0).spend(1)  # the row exists
    safety_pool = await create_safety_pool(
        database_url, timeout_seconds=0.5, lock_timeout="500ms"
    )
    wedge = await asyncpg.connect(database_url)
    try:
        wedge_tx = wedge.transaction()
        await wedge_tx.start()
        await wedge.execute("SELECT * FROM rate_budget FOR UPDATE")

        async def hang_bounded(*args: object, **kwargs: object) -> None:
            # The hang shape: a query that would sleep far past the bound —
            # the safety pool's client-side command_timeout cuts it off.
            await safety_pool.execute("SELECT pg_sleep(30)")

        monkeypatch.setattr(heartbeat, "beat", hang_bounded)
        monkeypatch.setattr(watchdog_module, "active_halt", hang_bounded)

        read_gateway = FakeHyperliquidGateway()
        read_gateway.perp_universes[None] = ["BTC", "ETH"]
        read_gateway.perp_dex_listing = []
        read_gateway.set_open_orders(MASTER, [_order("ETH", 91)])
        exec_gateway = FakeExecutionGateway()
        audit = ExecutionAudit(safety_pool, clock)
        watchdog = Watchdog(
            safety_pool,
            clock,
            read_gateway,
            AuditedExecutionGateway(
                exec_gateway, audit,
                actor=WATCHDOG_ACTOR, master_address=MASTER, signer_address=SIGNER,
                best_effort_audit=True,
            ),
            audit,
            FallbackBudget(SharedWeightBudget(safety_pool, clock, reserve=0), clock),
            master_address=MASTER,
            signer_address=SIGNER,
            executor_stale=timedelta(seconds=60),
            db_blind_after=timedelta(seconds=180),
            capability_interval=timedelta(hours=6),
        )

        started = time.monotonic()
        await watchdog.run_cycle()  # the failure streak opens (hangs, bounded)
        clock.advance(181)
        await watchdog.run_cycle()  # blind trip: cancel through the wedged budget
        elapsed = time.monotonic() - started

        cancels = [n for n, _ in exec_gateway.actions if n == "cancel_orders"]
        assert cancels == ["cancel_orders"]
        (_, payload) = exec_gateway.actions[-1]
        assert [(spec.asset, spec.oid) for spec in payload] == [(1, 91)]  # type: ignore[union-attr]
        # ~2 beats + 2 liveness reads at ≤0.5s each, plus a handful of budget
        # spends each paying the ≤0.5s lock timeout before falling back: the
        # whole trip-to-wire path in single-digit real seconds.
        assert elapsed < 30
        await wedge_tx.rollback()
    finally:
        await wedge.close()
        await safety_pool.close()
