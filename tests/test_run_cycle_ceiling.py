"""The execution loops' end-to-end real-time ceiling (issue #136, the
carry-forward from PR #143 round 6).

A3 already pins the mid-transaction black hole — the shape no pool timeout can
reach, because asyncpg's ROLLBACK awaits an UNTIMED cancel_waiter before any
command_timeout arms — but it pins it with the test's OWN
`asyncio.wait_for`. That proves the LIBRARY composition works; it does not
prove production uses it. Deleting the `wait_for` wrapper around a DB block in
`watchdog.py` would leave the whole suite green and the switch hung.

So this drives the same black hole through the REAL `Watchdog.run_cycle` and
bounds it in wall-clock time. The clock the trip logic reads is still the
injected one; what is measured here is actual awaits.

WHY THERE IS NO SUCH TEST FOR THE COPY EXECUTOR, stated so the omission is a
decision and not a gap: `CopyExecutor.run_cycle` deliberately has NO internal
ceiling. Its dead-man's switch is the watchdog, which reads the heartbeat the
cycle beats at its start — so a wedged executor must LOOK DEAD, and a hang is
exactly that: the loop never comes round, no further beat lands, and the
watchdog halts and sweeps within its stall threshold. Adding a ceiling would
INVERT that: each wedged cycle would be cancelled, the next would beat again,
and a permanently stuck executor would report itself healthy forever while
copying nothing. The watchdog needs the ceiling because nothing watches the
watchdog; the executor needs the opposite.
"""

import time
from datetime import timedelta

import pytest

from epigone.budget import SharedWeightBudget
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.safety import watchdog as watchdog_module
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.budget import FallbackBudget
from epigone.safety.db import create_safety_pool
from epigone.safety.watchdog import DB_BLOCK_CEILING_SECONDS, Watchdog
from tests.support.clock import FakeClock
from tests.support.orders import open_order
from tests.test_safety_db import ELAPSED_CEILING, _BlackholeProxy
from tests.test_watchdog import MASTER, SIGNER


async def test_a_mid_transaction_black_hole_cannot_stall_run_cycle(
    database_url: str, clock: FakeClock
) -> None:
    """The end-to-end shape: a partition strikes INSIDE the liveness read's
    transaction, so the statement times out and the ROLLBACK hangs untimed.
    `run_cycle` must still return — the production ceiling, not the test's —
    and the cycle it returns from must be the one that declares the process
    blind."""
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
    pool = await create_safety_pool(proxied_url, timeout_seconds=0.5, lock_timeout="500ms")
    try:
        await pool.execute("SELECT 1")  # healthy and pooled before the partition

        async def halt_read_that_black_holes(*args: object, **kwargs: object) -> None:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute("SELECT 1")
                proxy.blackholed = True  # the partition strikes MID-transaction
                await conn.execute("SELECT 1")  # times out; ROLLBACK then hangs

        read_gateway = FakeHyperliquidGateway()
        read_gateway.perp_universes[None] = ["BTC", "ETH"]
        read_gateway.perp_dex_listing = []
        read_gateway.set_open_orders(MASTER, [open_order("ETH", 91)])
        exec_gateway = FakeExecutionGateway()
        audit = ExecutionAudit(pool, clock)
        watchdog = Watchdog(
            pool,
            clock,
            read_gateway,
            AuditedExecutionGateway(
                exec_gateway,
                audit,
                actor=WATCHDOG_ACTOR,
                master_address=MASTER,
                signer_address=SIGNER,
                best_effort_audit=True,
            ),
            audit,
            FallbackBudget(SharedWeightBudget(pool, clock, reserve=0), clock),
            master_address=MASTER,
            signer_address=SIGNER,
            executor_stale=timedelta(seconds=60),
            db_blind_after=timedelta(seconds=180),
            capability_interval=timedelta(hours=6),
        )

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(watchdog_module, "active_halt", halt_read_that_black_holes)
            started = time.monotonic()
            # No local wait_for: if the production ceiling is gone, this hangs
            # until pytest-timeout kills the whole run — which is the point.
            await watchdog.run_cycle()  # the failure streak opens
            first = time.monotonic() - started
            clock.advance(181)  # past the blind threshold on the injected clock
            await watchdog.run_cycle()  # blind trip → cancel, still Postgres-free
            elapsed = time.monotonic() - started

        # Bounded by the PRODUCTION ceiling, expressed in its own units: one
        # hanging block per cycle, so two cycles cost at most a small multiple
        # of it. What matters is the comparison this replaces — an untimed
        # ROLLBACK waits forever, and the TCP retransmission it rides on is
        # ~15 minutes.
        assert first < DB_BLOCK_CEILING_SECONDS + ELAPSED_CEILING
        assert elapsed < 3 * DB_BLOCK_CEILING_SECONDS
        cancels = [name for name, _ in exec_gateway.actions if name == "cancel_orders"]
        assert cancels == ["cancel_orders"]  # the protective action still ran
    finally:
        pool.terminate()
        await proxy.stop()


async def test_the_ceiling_is_wired_into_every_db_block_of_a_cycle() -> None:
    """The companion structural check, because the test above can only prove
    the ONE block it black-holes. Every `await` on a Postgres-touching block
    inside `run_cycle` and its incident path goes through `asyncio.wait_for`
    at the module's ceiling — so a future block added without one is a visible
    diff here rather than a hang at 3am.

    ONE ceiling in the module is deliberately not a DB one: the sweep pulse's
    keepalive (issue #201) bounds an EXCHANGE call, so it gets its own,
    separately reasoned KEEPALIVE_CEILING_SECONDS. It is named here rather
    than exempted by pattern — a second non-DB ceiling must be argued for in
    this test's diff, not slipped in."""
    import ast
    import inspect

    source = inspect.getsource(watchdog_module)
    tree = ast.parse(source)
    ceilinged = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "wait_for"
    ]
    # heartbeat.beat, the liveness read, the reconcile, mark_swept and the
    # capability verdict — the five durable/state blocks the module docstring
    # enumerates — plus the keepalive's own.
    assert len(ceilinged) >= 6
    ceilings = [
        call.args[1].id for call in ceilinged if isinstance(call.args[1], ast.Name)
    ]
    assert len(ceilings) == len(ceilinged)  # every one is a named constant
    assert ceilings.count("KEEPALIVE_CEILING_SECONDS") == 1
    assert all(
        name in ("DB_BLOCK_CEILING_SECONDS", "KEEPALIVE_CEILING_SECONDS")
        for name in ceilings
    )
