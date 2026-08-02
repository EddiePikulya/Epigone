"""Starting the watchdog when Postgres is unreachable (issue #145).

PR #143 made an ALREADY-RUNNING watchdog survive a Postgres outage: once an
incident is declared the cancel path reaches the wire with zero database
work. Its startup, though, still needed Postgres three times over — the
pool, the migration run, and the keystore row holding the agent key — so a
crash, an OOM, a host reboot, or a deploy *during* an outage left the
account with no cancel path at all until the database came back. Those
events are CORRELATED with the outage, and in a full outage the #52 monitor
is equally blind, so nothing would page either. This module removes that
boundary.

THE START, in one sentence: probe Postgres on a bounded connect; keep
probing for a grace window equal to the blind threshold; if it answers,
start exactly as before (migrate, load the key from the keystore, refresh
the local key cache, stamp the launch); if it does not, COLD-START BLIND —
load the watchdog lane's key from the encrypted local cache
(epigone.safety.keycache), skip nothing but DEFER the migration run, and
hand the watchdog a pool that keeps trying to become real.

Three decisions worth stating, because each one could reasonably have gone
the other way:

- THE GRACE IS THE BLIND THRESHOLD, not zero. A cold start that went blind
  immediately would cancel the whole book because a deploy landed during a
  five-second database blip. Waiting the same span an already-running
  watchdog waits (`WATCHDOG_COLDSTART_GRACE_SECONDS`, default = the blind
  threshold) makes one rule cover both cases: Postgres unreachable
  *continuously* for that long is an incident, however the process got
  there. The probe returns the instant the database answers, so a restart
  loop against a healthy database costs nothing.
- THE MIGRATION CHECK IS DEFERRED, NOT SKIPPED. It runs on the reconnect,
  before any other write, on the same plain unbounded pool a normal start
  uses — schema changes legitimately take long and must not inherit
  incident-tuned timeouts. Until then the process simply has no database,
  which is a state the watchdog already knows how to be in.
- THE LAUNCH STAMP IS RETROACTIVE. `process_heartbeats.started_at` is what
  the #52 monitor ages the never-verified capability grace period from
  (migration 0026). A cold start cannot stamp it, so the reconnect writes it
  with the ACTUAL launch time rather than the reconnect time — otherwise a
  cold start during an outage would silently restart that grace clock and
  defer the page that says "this watchdog has never verified it can act".

WHAT A BLIND COLD START CAN DO: cancel. Its gateway is wrapped cancel-only
(epigone.safety.cancel_only), its key is the watchdog lane's alone (the
cache refuses any other), and its blind window reconciles into
`execution_audit` under a reason that says COLD-START — distinct from a
running process's blind window, because the operator's follow-up differs
(a cold start means the process was restarted mid-outage; a running-process
window does not).
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

import asyncpg
from eth_account.signers.local import LocalAccount

from epigone.clock import Clock
from epigone.db import create_pool, migrate
from epigone.keystore import WATCHDOG_LANE, AgentKeystore, Kek, KeystoreError
from epigone.safety import heartbeat
from epigone.safety.audit import WATCHDOG_ACTOR, ExecutionAudit
from epigone.safety.db import SAFETY_DB_TIMEOUT_SECONDS, create_safety_pool
from epigone.safety.keycache import WatchdogKeyCache

log = logging.getLogger(__name__)

# One bounded question — "is Postgres there?" — asked with the safety lane's
# own touch bound, so a black-holed host answers in seconds instead of
# waiting out asyncpg's 60s connect default (or, worse, a TCP retransmission
# window) on the one path that must decide quickly.
PROBE_TIMEOUT_SECONDS = SAFETY_DB_TIMEOUT_SECONDS

PoolOpener = Callable[[], Awaitable[asyncpg.Pool]]


class DatabaseUnavailableError(ConnectionError):
    """Every touch a ColdStartPool cannot serve. Deliberately a
    ConnectionError: the watchdog's blind machinery already treats "the
    database raised" as the whole signal (any failure of the liveness reads
    is blindness, whatever its cause), so a cold-started process needs no
    special case anywhere — it simply looks like an outage, which is what it
    is."""


class ColdStartPool:
    """The pool a blind cold start hands to the watchdog: no connection yet,
    every touch raising immediately, and a bounded attempt to become a real
    pool no more often than `retry_after`.

    RAISING FAST IS THE FEATURE. The watchdog's contract is that an
    unanswerable database is an incident; a proxy that blocked waiting for
    Postgres would starve the very cycles the blind machinery needs. The
    rate limit matters for the same reason: without it every audit row, halt
    read, and heartbeat of a blind cycle would each pay a fresh connect
    timeout.

    The first attempt that succeeds runs `opener`, which owns the deferred
    startup work (migrations, the retroactive launch stamp, refreshing the
    key cache) and returns a ready safety pool; from then on every call
    delegates straight through and this object is invisible. An opener that
    fails leaves the pool unopened and the incident open — the next cycle
    cancels again and tries again."""

    def __init__(self, opener: PoolOpener, clock: Clock, *, retry_after: timedelta) -> None:
        self._opener = opener
        self._clock = clock
        self._retry_after = retry_after
        self._live: asyncpg.Pool | None = None
        self._next_attempt_at: datetime | None = None  # None → due now

    @property
    def is_live(self) -> bool:
        return self._live is not None

    async def _require(self) -> asyncpg.Pool:
        if self._live is not None:
            return self._live
        now = self._clock.now()
        if self._next_attempt_at is not None and now < self._next_attempt_at:
            raise DatabaseUnavailableError(
                "watchdog cold start: Postgres was unreachable at launch and the next "
                "reconnect attempt is not due yet"
            )
        self._next_attempt_at = now + self._retry_after
        try:
            self._live = await self._opener()
        except Exception as error:
            log.warning("watchdog cold start: Postgres still unreachable (%s)", error)
            raise DatabaseUnavailableError(
                f"watchdog cold start: Postgres still unreachable ({error})"
            ) from error
        log.error(
            "watchdog cold start: Postgres answered — deferred startup done, the blind "
            "window reconciles on this cycle"
        )
        return self._live

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        status: str = await (await self._require()).execute(query, *args, **kwargs)
        return status

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[asyncpg.Record]:
        rows: list[asyncpg.Record] = await (await self._require()).fetch(query, *args, **kwargs)
        return rows

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> asyncpg.Record | None:
        return await (await self._require()).fetchrow(query, *args, **kwargs)

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        return await (await self._require()).fetchval(query, *args, **kwargs)

    def acquire(self, *, timeout: float | None = None) -> "_DeferredAcquire":
        # asyncpg's acquire() is sync-returning-an-async-context-manager, and
        # every state module uses it as `async with pool.acquire()`. The
        # connection attempt therefore has to happen in __aenter__.
        return _DeferredAcquire(self, timeout)

    async def close(self) -> None:
        if self._live is not None:
            await self._live.close()


class _DeferredAcquire:
    """`async with cold_start_pool.acquire()` — the reconnect happens on
    entry, and a pool that still cannot open raises there, before any
    connection object exists to release."""

    def __init__(self, owner: ColdStartPool, timeout: float | None) -> None:
        self._owner = owner
        self._timeout = timeout
        self._inner: Any = None

    async def __aenter__(self) -> Any:
        pool = await self._owner._require()
        self._inner = (
            pool.acquire() if self._timeout is None else pool.acquire(timeout=self._timeout)
        )
        return await self._inner.__aenter__()

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._inner.__aexit__(*exc)


@dataclass(frozen=True)
class Startup:
    """What the process starts with, and whether it started blind."""

    pool: asyncpg.Pool
    signer: LocalAccount
    master_address: str
    cold_start: bool
    reason: str | None  # cold starts only: what the blind incident records


async def probe_database(
    database_url: str, *, timeout_seconds: float = PROBE_TIMEOUT_SECONDS
) -> bool:
    """Is Postgres there? One bounded connect, closed again immediately —
    the whole thing wrapped in a real-time ceiling, because a black-holed
    host can hang a close as readily as a connect."""
    try:
        return await asyncio.wait_for(
            _probe(database_url, timeout_seconds), timeout_seconds * 2
        )
    except Exception as error:
        log.warning("watchdog startup: Postgres did not answer (%s)", error)
        return False


async def _probe(database_url: str, timeout_seconds: float) -> bool:
    connection = await asyncpg.connect(database_url, timeout=timeout_seconds)
    try:
        await connection.fetchval("SELECT 1")
    finally:
        await connection.close(timeout=timeout_seconds)
    return True


async def open_db_backed_pool(database_url: str) -> asyncpg.Pool:
    """The normal startup shape, unchanged: migrations on a plain unbounded
    pool (they legitimately run long and must not inherit incident-tuned
    timeouts), then the bounded safety pool everything else uses."""
    startup_pool = await create_pool(database_url)
    try:
        await migrate(startup_pool)
    finally:
        await startup_pool.close()
    return await create_safety_pool(database_url)


async def open_startup(
    *,
    database_url: str,
    clock: Clock,
    kek: Kek,
    cache: WatchdogKeyCache,
    operator_id: int,
    launched_at: datetime,
    grace: timedelta,
    retry_every: timedelta,
) -> Startup:
    """Start DB-backed if Postgres answers within the grace window; cold-start
    blind if it does not.

    Fails fast either way when there is no usable key: a keystore with no
    watchdog-lane row refuses the same as it always has, and a cold start
    with no readable cache refuses too. Both are the same rule — a watchdog
    that beats its heartbeat while unable to cancel is false safety."""
    while True:
        if await probe_database(database_url):
            try:
                return await _db_backed_startup(
                    database_url=database_url,
                    clock=clock,
                    kek=kek,
                    cache=cache,
                    operator_id=operator_id,
                    launched_at=launched_at,
                )
            except KeystoreError:
                raise  # no usable key: fail fast, loudly, as this always has
            except Exception:
                # The probe said yes and the real work then failed — most
                # often the outage racing us between the two, but a broken
                # migration lands here too. Either way this is the safety
                # process: keep going round the loop, and if the grace runs
                # out, cold-start blind rather than exit. Cancelling is cheap
                # and recoverable; a watchdog that isn't running is not.
                log.exception("watchdog startup: Postgres answered but startup failed")
        waited = clock.now() - launched_at
        if waited >= grace:
            break
        log.error(
            "watchdog startup: Postgres unreachable for %ds — retrying (cold start at %ds)",
            int(waited.total_seconds()),
            int(grace.total_seconds()),
        )
        await clock.sleep(retry_every.total_seconds())

    reason = (
        f"COLD START with Postgres unreachable: the process launched at "
        f"{launched_at:%Y-%m-%d %H:%M:%S} UTC and the database did not answer within "
        f"{int(grace.total_seconds())}s, so this watchdog has never seen halt state or "
        f"executor liveness — an unknowable executor is indistinguishable from a dead one"
    )
    cached = cache.load(clock.now())
    log.error(
        "watchdog COLD START BLIND: cancelling with the cached %s-lane key %s for %s "
        "(cached %s); migrations, the launch stamp and the audit trail are DEFERRED to "
        "the reconnect",
        WATCHDOG_LANE,
        cached.agent_address,
        cached.master_address,
        cached.refreshed_at.isoformat(),
    )
    pool = ColdStartPool(
        reconnect_opener(
            database_url=database_url,
            clock=clock,
            kek=kek,
            cache=cache,
            operator_id=operator_id,
            launched_at=launched_at,
            cached_agent_address=cached.agent_address,
        ),
        clock,
        retry_after=retry_every,
    )
    return Startup(
        pool=cast(asyncpg.Pool, pool),
        signer=cached.signer,
        master_address=cached.master_address,
        cold_start=True,
        reason=reason,
    )


async def _db_backed_startup(
    *,
    database_url: str,
    clock: Clock,
    kek: Kek,
    cache: WatchdogKeyCache,
    operator_id: int,
    launched_at: datetime,
) -> Startup:
    pool = await open_db_backed_pool(database_url)
    keystore = AgentKeystore(pool, kek, clock)
    # Fail fast on a missing signer (epigone.safety.main's first wiring note):
    # no watchdog-lane key means this process cannot cancel anything.
    signer = await keystore.signer(operator_id, WATCHDOG_LANE)
    record = await keystore.active_record(operator_id, WATCHDOG_LANE)
    assert record is not None  # signer() above proved it
    # EVERY successful DB-backed start refreshes the cache, so the copy a
    # future cold start reads is never older than the last healthy restart.
    cache.write(record, bytes(signer.key), now=clock.now())
    await heartbeat.record_start(pool, heartbeat.WATCHDOG_PROCESS, launched_at)
    return Startup(
        pool=pool,
        signer=signer,
        master_address=record.master_address,
        cold_start=False,
        reason=None,
    )


def reconnect_opener(
    *,
    database_url: str,
    clock: Clock,
    kek: Kek,
    cache: WatchdogKeyCache,
    operator_id: int,
    launched_at: datetime,
    cached_agent_address: str,
) -> PoolOpener:
    """The deferred startup, run once, on the first cycle whose database touch
    finds Postgres answering again."""

    async def open_pool() -> asyncpg.Pool:
        if not await probe_database(database_url):
            raise DatabaseUnavailableError("Postgres still unreachable")
        pool = await open_db_backed_pool(database_url)  # the DEFERRED migration check
        try:
            await deferred_startup(
                pool,
                clock=clock,
                kek=kek,
                cache=cache,
                operator_id=operator_id,
                launched_at=launched_at,
                cached_agent_address=cached_agent_address,
            )
        except BaseException:
            await _close_quietly(pool)
            raise
        return pool

    return open_pool


async def deferred_startup(
    pool: asyncpg.Pool,
    *,
    clock: Clock,
    kek: Kek,
    cache: WatchdogKeyCache,
    operator_id: int,
    launched_at: datetime,
    cached_agent_address: str,
) -> None:
    """What a DB-backed start would have done at launch, done late.

    The launch stamp goes FIRST and carries `launched_at` — the true process
    start, not this moment (module docstring): the #52 monitor's
    never-verified capability grace period must not restart because a cold
    start happened."""
    await heartbeat.record_start(pool, heartbeat.WATCHDOG_PROCESS, launched_at)
    now = clock.now()
    refreshed, active_agent = await _refresh_cache(pool, clock, kek, cache, operator_id)
    stale_signer = active_agent is not None and active_agent != cached_agent_address
    if stale_signer:
        # Not fatal, and deliberately not a restart: this process still holds
        # an agent key that the chain may well still honour, and dropping the
        # cancel path mid-outage to fix bookkeeping would be the wrong trade.
        # The on-chain capability probe is what decides whether the key can
        # still act; this line is what tells the operator to restart.
        log.error(
            "watchdog cold start: the %s lane was rotated to %s while this process was "
            "blind — it is still signing with the cached %s; RESTART it once the "
            "incident is closed (docs/runbooks/agent-key-rotation.md)",
            WATCHDOG_LANE,
            active_agent,
            cached_agent_address,
        )
    await ExecutionAudit(pool, clock).record_event(
        actor=WATCHDOG_ACTOR,
        action="watchdog_cold_start_reconnected",
        risk_decision="cold start reconciled with Postgres",
        detail={
            "launched_at": launched_at.isoformat(),
            "reconnected_at": now.isoformat(),
            "blind_seconds": int((now - launched_at).total_seconds()),
            "process_start_stamped_retroactively": True,
            "signer_address": cached_agent_address,
            "key_cache_refreshed": refreshed,
            "signer_matches_keystore": not stale_signer,
        },
    )


async def _refresh_cache(
    pool: asyncpg.Pool, clock: Clock, kek: Kek, cache: WatchdogKeyCache, operator_id: int
) -> tuple[bool, str | None]:
    """Bring the cache back in step with the keystore. Best-effort on
    purpose: a rotation problem must not keep the reconnect — and with it the
    retroactive stamp and the blind-window reconcile — from landing."""
    keystore = AgentKeystore(pool, kek, clock)
    try:
        record = await keystore.active_record(operator_id, WATCHDOG_LANE)
        if record is None:
            log.error(
                "watchdog cold start: no active %s-lane key in the keystore — the cache "
                "keeps its copy",
                WATCHDOG_LANE,
            )
            return False, None
        signer = await keystore.signer(operator_id, WATCHDOG_LANE)
        cache.write(record, bytes(signer.key), now=clock.now())
        return True, record.agent_address
    except (KeystoreError, OSError):
        log.exception("watchdog cold start: could not refresh the local key cache")
        return False, None


async def _close_quietly(pool: asyncpg.Pool) -> None:
    try:
        await asyncio.wait_for(pool.close(), SAFETY_DB_TIMEOUT_SECONDS)
    except Exception:
        log.warning("watchdog cold start: closing a half-opened pool failed", exc_info=True)
