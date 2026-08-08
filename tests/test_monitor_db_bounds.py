"""The monitor's bounded DB access (issue #213).

The #52 monitor's failure was not a slow page, it was NO page. A black-holed
database host does not refuse a query, it swallows it, so the gather HUNG
instead of raising, the DB-down check never evaluated, the fast tick never
came round again, and the checker went perfectly silent — for the ~15 minutes
it takes kernel TCP retransmission to give up, or forever. A dead database
host being among the likeliest reasons the watchdog is dead too, that silence
was correlated with the thing the monitor exists to check.

So these tests build HANG-shaped outages the way tests/test_safety_db.py does
— a query the server accepts and never answers — and assert the monitor turns
each into the signal it already knew how to send. Real wall-clock bounds,
because the timeouts under test are real; sub-second test values, the
production ones being epigone.monitor.main's.

Two of them cover the hard real-time ceiling the gathers ALSO sit under: that
it catches a hang no pool bound could, and — the mistake found reviewing this
change — that it is sized from the number of touches the gather actually
makes, since a ceiling under-priced against a legal slow run is a FALSE
DB-down page rather than a missing one.
"""

import asyncio
import time
from datetime import timedelta

import asyncpg
import pytest
from aiogram import Bot

import epigone.monitor.main as monitor_main
from epigone.db import create_pool
from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import (
    CheckThresholds,
    HealthSnapshot,
    gather_snapshot,
    gather_watchdog_snapshot,
)
from epigone.monitor.config import MonitorConfig
from epigone.monitor.main import run_monitor_cycle, run_watchdog_liveness_cycle
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession

ADMIN_ID = 999
# An order of magnitude above the sub-second bounds under test, far below the
# hang timescales (asyncpg's 60s connect default, ~15min TCP retransmission)
# they replace: anything slower than this IS the hang, not a slow machine.
ELAPSED_CEILING = 10.0
BOUND = 0.5
# Far past every bound here, so the only way a test finishes is a bound firing.
FOREVER = 30


class FakeDiskProbe:
    def percent_used(self) -> float | None:
        return 47.0


def _config() -> MonitorConfig:
    return MonitorConfig(
        interval=timedelta(minutes=15),
        watchdog_check=timedelta(seconds=60),
        reminder=timedelta(hours=6),
        heartbeat_hour=23,  # FakeClock sits at noon: no daily digest in the way
        thresholds=CheckThresholds(
            ingest_stall=timedelta(minutes=30),
            coarse_stale=timedelta(minutes=120),
            alert_backlog=timedelta(minutes=5),
            rate_window=timedelta(minutes=15),
            rate_max_events=5,
            disk_percent=85,
            starvation_window=timedelta(minutes=45),
            starvation_min_due=50,
            agent_key_warn=timedelta(days=14),
            watchdog_stale=timedelta(seconds=300),
        ),
        disk_path="/",
    )


def _monitor() -> Monitor:
    return Monitor(reminder=timedelta(hours=6), heartbeat_hour=23)


# --- the pool's own bounds ---


async def test_the_monitor_pool_turns_a_hanging_query_into_an_error(
    database_url: str,
) -> None:
    """The bound that was missing. `command_timeout` is enforced CLIENT-side
    by asyncpg, which is the property that makes it fire against a server that
    never answers rather than only against one that says no."""
    pool = await create_pool(database_url, timeout_seconds=BOUND)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await pool.execute(f"SELECT pg_sleep({FOREVER})")
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        await pool.close()


async def test_the_default_pool_stays_unbounded_for_the_scanners(
    database_url: str,
) -> None:
    """The carve-out, asserted rather than trusted: the ingest and backfill
    processes run legitimately long queries and MUST NOT inherit a timeout
    tuned for a process with a deadline. A query the bounded pool above cuts,
    this one completes."""
    pool = await create_pool(database_url)
    try:
        await pool.execute(f"SELECT pg_sleep({BOUND * 2})")
    finally:
        await pool.close()


async def test_the_default_pool_keeps_asyncpgs_own_connect_bound(
    database_url: str,
) -> None:
    """"Unbounded" must mean asyncpg's defaults, NOT looser than them — the
    trap in making the bounds optional (review of this change).

    `create_pool` forwards **connect_kwargs straight into `connect`, whose
    default is `timeout=60`, so passing `timeout=None` to say "no override"
    actually says `asyncio.timeout(None)`: no connect bound at all. Every
    scanner would then hang FOREVER on a black-holed host where it used to
    fail in a minute — a regression in the exact direction this issue exists
    to close, and invisible to any test that only exercises queries.

    Asserted on the kwargs rather than by waiting out 60s against a black hole,
    which is the only other way to see it."""
    pool = await create_pool(database_url)
    try:
        assert "timeout" not in pool._connect_kwargs
        assert "command_timeout" not in pool._connect_kwargs
    finally:
        await pool.close()

    bounded = await create_pool(database_url, timeout_seconds=BOUND)
    try:
        assert bounded._connect_kwargs["timeout"] == BOUND
        assert bounded._connect_kwargs["command_timeout"] == BOUND
    finally:
        await bounded.close()


# --- what the loop does with the error ---


def _gather_hangs_on(pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point both gathers at a query the server accepts and never answers,
    issued through the pool under test — so what the cycle meets is the real
    pool bound firing on a real hung query, not a raise a fake decided on."""

    async def hang(*args: object, **kwargs: object) -> HealthSnapshot:
        await pool.fetchrow(f"SELECT pg_sleep({FOREVER})")
        raise AssertionError("the pool bound should have fired long before this")

    monkeypatch.setattr(monitor_main, "gather_snapshot", hang)
    monkeypatch.setattr(monitor_main, "gather_watchdog_snapshot", hang)


async def test_a_black_holed_gather_pages_db_down_instead_of_hanging(
    database_url: str,
    bot: Bot,
    session: RecordingSession,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The headline. The full cycle already knew how to report an unreachable
    database; what it could not do was NOTICE one that swallowed the query
    rather than refusing it. With the pool bounded, a hang is an exception,
    and an exception is a 🚨 the operator actually receives."""
    pool = await create_pool(database_url, timeout_seconds=BOUND)
    _gather_hangs_on(pool, monkeypatch)
    try:
        started = time.monotonic()
        messages = await run_monitor_cycle(
            pool, bot, ADMIN_ID, _monitor(), _config(), clock, FakeDiskProbe()
        )
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        await pool.close()

    assert any("Database" in text for text in messages)
    (sent,) = session.sent_messages()
    assert sent.chat_id == ADMIN_ID


async def test_a_black_holed_liveness_read_drops_the_tick_not_the_loop(
    database_url: str,
    bot: Bot,
    session: RecordingSession,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fast tick deliberately does NOT page on a read failure — the
    DB-down signal belongs to the full cycle, which gathers enough to say so
    with its numbers. What matters here is that it RETURNS: dropping a tick
    costs a minute of latency, hanging on it costs the whole loop, including
    the full cycle that owns the page."""
    pool = await create_pool(database_url, timeout_seconds=BOUND)
    _gather_hangs_on(pool, monkeypatch)
    try:
        started = time.monotonic()
        messages = await run_watchdog_liveness_cycle(
            pool, bot, ADMIN_ID, _monitor(), _config(), clock
        )
        assert time.monotonic() - started < ELAPSED_CEILING
    finally:
        await pool.close()

    assert messages == []
    assert session.sent_messages() == []


async def test_the_ceiling_catches_the_leg_no_pool_bound_can(
    pool: asyncpg.Pool,
    bot: Bot,
    session: RecordingSession,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """asyncpg's per-op timeouts cannot reach every leg — a transaction exit
    awaits an UNTIMED cancel_waiter before any of them arms (the round-5
    finding that ended the bound-one-more-library-leg approach in
    epigone.safety.watchdog). So each gather sits under a hard real-time
    ceiling as well, and a gather that hangs where NO pool bound could reach
    still ends as the DB-down page rather than as silence.

    Simulated by a gather that simply never returns, because the shape being
    guarded against is exactly "an await nothing underneath us will end"."""
    monkeypatch.setattr(monitor_main, "MONITOR_FULL_CEILING_SECONDS", BOUND)

    async def never_returns(*args: object, **kwargs: object) -> HealthSnapshot:
        await asyncio.sleep(FOREVER)
        raise AssertionError("the ceiling should have fired long before this")

    monkeypatch.setattr(monitor_main, "gather_snapshot", never_returns)

    started = time.monotonic()
    messages = await run_monitor_cycle(
        pool, bot, ADMIN_ID, _monitor(), _config(), clock, FakeDiskProbe()
    )
    assert time.monotonic() - started < ELAPSED_CEILING

    assert any("Database" in text for text in messages)
    assert len(session.sent_messages()) == 1


# --- the ceilings are derived from the gathers, and must stay that way ---


class _CountingPool:
    """A pool that records every touch and delegates it, so a gather's touch
    COUNT is observable without guessing from the source."""

    def __init__(self, inner: asyncpg.Pool, touches: list[str]) -> None:
        self._inner = inner
        self._touches = touches

    async def fetchrow(self, *args: object, **kwargs: object) -> object:
        self._touches.append("fetchrow")
        return await self._inner.fetchrow(*args, **kwargs)

    async def fetchval(self, *args: object, **kwargs: object) -> object:
        self._touches.append("fetchval")
        return await self._inner.fetchval(*args, **kwargs)

    async def fetch(self, *args: object, **kwargs: object) -> object:
        self._touches.append("fetch")
        return await self._inner.fetch(*args, **kwargs)

    async def execute(self, *args: object, **kwargs: object) -> object:
        self._touches.append("execute")
        return await self._inner.execute(*args, **kwargs)


async def test_the_ceilings_match_what_the_gathers_actually_touch(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The bug this pins (review of PR #216): the full gather's ceiling was
    set to ONE touch's worth of budget while the gather makes TWO sequential
    touches — the big fetchrow, then `count_due_traders`. Their worst legal
    composition is twice the ceiling, so a loaded-but-answering database could
    outrun it and fire a 🚨 DB-down page for a database that was merely busy —
    exactly the false page the per-touch bound is priced to avoid.

    The arithmetic is only right while the counts it is derived from are, and
    a comment cannot enforce that. Adding a touch to either gather fails HERE,
    where the fix is one number, rather than in production at 3am."""
    full_touches: list[str] = []
    await gather_snapshot(
        _CountingPool(pool, full_touches),  # type: ignore[arg-type]
        clock,
        FakeDiskProbe(),
        _config().thresholds,
    )
    assert len(full_touches) == monitor_main.FULL_GATHER_TOUCHES

    liveness_touches: list[str] = []
    await gather_watchdog_snapshot(
        _CountingPool(pool, liveness_touches),  # type: ignore[arg-type]
        clock,
    )
    assert len(liveness_touches) == monitor_main.LIVENESS_GATHER_TOUCHES

    # And the ceilings really are those counts times one hung touch, rather
    # than a number that happens to be near them.
    assert monitor_main.MONITOR_FULL_CEILING_SECONDS == (
        monitor_main.FULL_GATHER_TOUCHES * monitor_main.WORST_TOUCH_SECONDS
    )
    assert monitor_main.MONITOR_LIVENESS_CEILING_SECONDS == (
        monitor_main.LIVENESS_GATHER_TOUCHES * monitor_main.WORST_TOUCH_SECONDS
    )
    # The liveness ceiling must stay under the fast tick's cadence, or a hung
    # tick would swallow the one after it as well.
    assert monitor_main.MONITOR_LIVENESS_CEILING_SECONDS < 60


async def test_one_hung_touch_really_costs_no_more_than_the_ceiling_prices_it(
    database_url: str,
) -> None:
    """`WORST_TOUCH_SECONDS = 3 × the bound` is an ASSUMPTION about asyncpg —
    that a fully hung touch costs an acquire, a query and a release, each
    bounded — and the ceilings are built on it. Measured here rather than
    reasoned about, because if a hung touch actually costs more than that, the
    ceilings are under-priced again and every argument above them is wrong.

    Sub-second values; production's are the module's. The claim under test is
    the RATIO, which is what the ceilings are derived from."""
    pool = await create_pool(database_url, timeout_seconds=BOUND)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await pool.fetchrow(f"SELECT pg_sleep({FOREVER})")
        cost = time.monotonic() - started
    finally:
        await pool.close()

    multiple = cost / BOUND
    assert multiple <= 3, (
        f"a fully hung touch cost {multiple:.1f}× the per-touch bound, but the "
        f"ceilings are priced at 3× — they are under-priced"
    )


# --- startup, where nothing is watching yet ---


async def test_a_startup_that_cannot_migrate_crashes_instead_of_hanging(
    database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bounding the loop while leaving STARTUP unbounded left this issue's own
    failure shape alive at the worst moment (review of PR #216): a host that
    black-holes while the monitor is migrating hangs it before the first
    cycle, so there is no DB-down page, no digest, and silence that looks
    exactly like health. Crashing crash-loops the container instead, which is
    at least visible.

    The bound must ALSO not hang while enforcing itself — `asyncio.wait_for`
    would cancel the migration and then await a rollback through asyncpg's
    untimed cancel_waiter, which is the hang it was added to prevent. So the
    elapsed-time assertion is the real content of this test."""
    monkeypatch.setattr(monitor_main, "MONITOR_MIGRATE_CEILING_SECONDS", BOUND)

    async def never_finishes(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(FOREVER)
        raise AssertionError("the startup deadline should have fired long before this")

    monkeypatch.setattr(monitor_main, "migrate", never_finishes)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await monitor_main.open_monitor_pool(database_url)
    assert time.monotonic() - started < ELAPSED_CEILING


async def test_a_healthy_startup_still_migrates_and_returns_a_bounded_pool(
    database_url: str,
) -> None:
    """The deadline must not have changed the normal path: migrations run,
    and what comes back is the BOUNDED pool the loop is supposed to use, not
    the unbounded one they ran on."""
    pool = await monitor_main.open_monitor_pool(database_url)
    try:
        assert pool._connect_kwargs["command_timeout"] == monitor_main.MONITOR_DB_TIMEOUT_SECONDS
        assert await pool.fetchval("SELECT 1") == 1
    finally:
        await pool.close()
