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
    from tests.support.orders import open_order
    from tests.test_watchdog import MASTER, SIGNER

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
        read_gateway.set_open_orders(MASTER, [open_order("ETH", 91)])
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
        # (cancels, vault_address) — the master's book carries no vault flag.
        assert [(s.asset, s.oid) for s in payload[0]] == [(1, 91)]  # type: ignore[index]
        assert payload[1] is None  # type: ignore[index]
        # ~2 beats + 2 liveness reads at ≤0.5s each, plus a handful of budget
        # spends each paying the ≤0.5s lock timeout before falling back: the
        # whole trip-to-wire path in single-digit real seconds.
        assert elapsed < 30
        await wedge_tx.rollback()
    finally:
        await wedge.close()
        await safety_pool.close()


class _BlackholeProxy:
    """A TCP proxy to the real Postgres that can turn into a black hole
    MID-LIFE: connections established before `blackholed` keep their sockets
    open but all bytes are silently dropped — the partitioned-host shape for
    an ALREADY-POOLED connection, which the round-3 tests never covered."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream = (upstream_host, upstream_port)
        self.blackholed = False
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    async def _handle(
        self, client_r: asyncio.StreamReader, client_w: asyncio.StreamWriter
    ) -> None:
        up_r, up_w = await asyncio.open_connection(*self._upstream)
        for src, dst in ((client_r, up_w), (up_r, client_w)):
            task = asyncio.get_running_loop().create_task(self._pump(src, dst))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _pump(self, src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await src.read(65536)
                if not data:
                    break
                if not self.blackholed:  # the black hole: drop, keep sockets open
                    dst.write(data)
                    await dst.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                dst.close()
            except Exception:
                pass

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


async def test_a_black_holed_established_connection_is_bounded_on_the_release_leg(
    database_url: str,
) -> None:
    """THE round-4 trap, at the real library seam (verified against asyncpg
    0.31.0): when command_timeout fires, asyncpg sends the CancelRequest
    over a fresh, UNTIMED TCP connection, and the pool's release awaits that
    cancellation with the holder's budget — None for every plain touch — so
    a black-holed ESTABLISHED connection costs 5s + a kernel SYN/hang
    timeout per touch, not 5s. The safety pool's default acquire timeout is
    the fix (it becomes the release budget); this test would hang for
    minutes without it."""
    from urllib.parse import urlsplit

    parts = urlsplit(database_url)
    assert parts.hostname is not None and parts.port is not None
    proxy = _BlackholeProxy(parts.hostname, parts.port)
    proxy_port = await proxy.start()
    query = f"?{parts.query}" if parts.query else ""
    proxied_url = (
        f"postgresql://{parts.username}:{parts.password}"
        f"@127.0.0.1:{proxy_port}{parts.path}{query}"
    )
    pool = await create_safety_pool(proxied_url, timeout_seconds=0.5)
    try:
        await pool.execute("SELECT 1")  # healthy handshake; the connection POOLS
        proxy.blackholed = True
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await pool.execute("SELECT 1")  # same pooled connection, now a black hole
        # ~0.5s query bound + ~0.5s release budget for the cancel-wait, then
        # asyncpg terminates the connection. Without the acquire-timeout
        # default this awaits the untimed CancelRequest connection instead.
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        pool.terminate()  # abortive on purpose: graceful close would hang here
        await proxy.stop()


async def test_a_mid_transaction_black_hole_is_bounded_by_the_ceiling(
    database_url: str,
) -> None:
    """THE round-5 shape all four earlier rounds of tests missed: the black
    hole flips after BEGIN, before COMMIT. The failing statement times out,
    then Transaction.__aexit__ issues ROLLBACK — and every asyncpg protocol
    op awaits an UNTIMED cancel_waiter before its own command_timeout arms
    (protocol.pyx), so no pool bound applies. The production answer is the
    hard wait_for ceiling around every DB block (watchdog.py): cancellation
    lands in the hanging wait, and the SHIELDED pool release then terminates
    the wedged connection within the round-4 acquire budget. This test pins
    that composition with real wall-clock bounds."""
    from urllib.parse import urlsplit

    parts = urlsplit(database_url)
    assert parts.hostname is not None and parts.port is not None
    proxy = _BlackholeProxy(parts.hostname, parts.port)
    proxy_port = await proxy.start()
    query = f"?{parts.query}" if parts.query else ""
    proxied_url = (
        f"postgresql://{parts.username}:{parts.password}"
        f"@127.0.0.1:{proxy_port}{parts.path}{query}"
    )
    pool = await create_safety_pool(proxied_url, timeout_seconds=0.5)
    try:
        await pool.execute("SELECT 1")  # healthy, pooled

        async def txn_touch() -> None:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute("SELECT 1")
                proxy.blackholed = True  # the partition strikes MID-transaction
                await conn.execute("SELECT 1")  # times out; ROLLBACK then hangs untimed

        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(txn_touch(), 2.0)  # the production ceiling shape
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        pool.terminate()
        await proxy.stop()


async def test_the_shared_buckets_attempt_ceiling_bounds_an_untimed_lock_wait(
    pool: asyncpg.Pool, database_url: str, clock: FakeClock
) -> None:
    """Round 5's normal-mode belt: the shared bucket's own attempt ceiling
    (SharedWeightBudget attempt_ceiling) bounds a database attempt even on a
    pool WITHOUT lock_timeout — the FOR UPDATE would otherwise wait forever
    and no pool timeout applies. This is the seam every safety-lane spend
    passes through, the gateway's pre-sign spend included."""
    await SharedWeightBudget(pool, clock, reserve=0).spend(1)  # the row exists
    plain_pool = await asyncpg.create_pool(database_url)  # no lock_timeout at all
    assert plain_pool is not None
    wedge = await asyncpg.connect(database_url)
    try:
        wedge_tx = wedge.transaction()
        await wedge_tx.start()
        await wedge.execute("SELECT * FROM rate_budget FOR UPDATE")

        ceilinged = SharedWeightBudget(plain_pool, clock, reserve=0, attempt_ceiling=0.5)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await ceilinged.spend(20)
        assert time.monotonic() - started < ELAPSED_CEILING
        await wedge_tx.rollback()
    finally:
        await wedge.close()
        plain_pool.terminate()
