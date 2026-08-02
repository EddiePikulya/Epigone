"""Cold-starting the watchdog while Postgres is unreachable (issue #145).

PR #143's Postgres-outage guarantee covered an ALREADY-RUNNING watchdog:
startup needed the pool, the migration run, and the keystore row holding the
agent key, so a crash, a reboot, or a deploy during an outage left the
account with no cancel path at all. The acceptance criteria under test, in
this file's order:

- CUSTODY: the local key cache is encrypted at rest under the keystore's own
  KEK, holds the WATCHDOG lane only, tolerates no tampering, and is
  refreshed by every successful DB-backed start.
- THE COLD START: with Postgres unreachable a fresh process reaches its
  first cycle and cancels resting orders there — using only the cached key,
  with the production budget/audit wiring, and within the documented bound
  (the startup grace, then one cycle).
- BLIND ≠ ARMED: the order-placement path of a cold-started watchdog is
  structurally unreachable, asserted rather than merely unexercised.
- RECONCILIATION: when Postgres returns, the blind window lands in
  execution_audit under a COLD-START reason, distinct from a running
  process's blind window, and the process-start stamp lands retroactively
  with the true launch time so the #52 never-verified grace period (migration
  0026) does not restart.
"""

import asyncio
import json
import os
from datetime import timedelta
from pathlib import Path

import asyncpg
import pytest

from epigone.budget import SharedWeightBudget
from epigone.gateway.execution import CancelSpec
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.keystore import (
    EXECUTOR_LANE,
    WATCHDOG_LANE,
    AgentKeyRecord,
    AgentKeystore,
    KeystoreError,
    generate_kek,
    load_kek,
)
from epigone.safety import heartbeat
from epigone.safety.audit import EVENT, ExecutionAudit
from epigone.safety.budget import FallbackBudget
from epigone.safety.cancel_only import OrderPlacementForbiddenError
from epigone.safety.coldstart import (
    ColdStartPool,
    DatabaseUnavailableError,
    Startup,
    deferred_startup,
    open_startup,
    reconnect_opener,
)
from epigone.safety.halt import active_halt
from epigone.safety.keycache import WatchdogKeyCache
from epigone.safety.main import safety_gateway
from epigone.safety.watchdog import Watchdog
from tests.support.clock import FakeClock
from tests.support.orders import open_order

OPERATOR = 424242
MASTER = "0x" + "ab" * 20
STALE = timedelta(seconds=60)
DB_BLIND = timedelta(seconds=180)
GRACE = timedelta(seconds=30)
INTERVAL = timedelta(seconds=10)
CAPABILITY_INTERVAL = timedelta(hours=6)

# A port nothing listens on: `connect` refuses immediately, which is the
# cheap shape of "Postgres is not there". The hanging shape is bounded by
# probe_database's own ceiling and is covered in test_safety_db.py.
UNREACHABLE_URL = "postgresql://epigone:epigone@127.0.0.1:1/epigone"


@pytest.fixture
def kek_path(tmp_path: Path) -> Path:
    return generate_kek(tmp_path / "keystore.kek").path


@pytest.fixture
def cache_path(tmp_path: Path) -> Path:
    # Deliberately inside a directory that does not exist yet: the first
    # DB-backed start must create its own custody directory.
    return tmp_path / "state" / "watchdog-key-cache.enc"


@pytest.fixture
def cache(cache_path: Path, kek_path: Path) -> WatchdogKeyCache:
    return WatchdogKeyCache(cache_path, load_kek(kek_path))


@pytest.fixture
async def keystore(pool: asyncpg.Pool, kek_path: Path, clock: FakeClock) -> AgentKeystore:
    await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", OPERATOR)
    return AgentKeystore(pool, load_kek(kek_path), clock)


@pytest.fixture
async def watchdog_key(keystore: AgentKeystore, clock: FakeClock) -> AgentKeyRecord:
    return await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        agent_name="epigone-watchdog-a",
        expires_at=clock.now() + timedelta(days=170),
        lane=WATCHDOG_LANE,
    )


@pytest.fixture
def read_gateway() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.perp_universes[None] = ["BTC", "ETH", "SOL"]
    fake.perp_dex_listing = []
    return fake


def _cache_file(path: Path) -> tuple[str, dict[str, object]]:
    """The cache file's raw text and parsed payload. A plain sync helper: the
    async tests read it, and pathlib in an async body is a lint the repo takes
    seriously for real I/O paths."""
    text = path.read_text()
    return text, json.loads(text)


def _rewrite(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload))


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def _make_unwritable_dir(path: Path) -> None:
    path.mkdir(parents=True)
    path.chmod(0o500)


def _restore_dir(path: Path) -> None:
    path.chmod(0o700)


def _cancels(exec_gateway: FakeExecutionGateway) -> list[CancelSpec]:
    return [
        spec
        for name, payload in exec_gateway.actions
        if name == "cancel_orders"
        for spec in payload  # type: ignore[union-attr]
    ]


def _blind_watchdog(
    startup: Startup,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    exec_gateway: FakeExecutionGateway,
) -> Watchdog:
    """The production wiring (epigone.safety.main.safety_gateway) over
    whatever pool the startup produced, plus the FallbackBudget whose shared
    bucket sits on that same pool — so a blind cycle is exercised through
    every seam that could touch Postgres."""
    audit = ExecutionAudit(startup.pool, clock)
    # main's OWN wiring function, not a hand-rolled copy of it: the
    # cancel-only guarantee below is then a property of what production
    # builds, and deleting that wrapper fails these tests.
    audited = safety_gateway(
        exec_gateway,
        audit,
        master_address=startup.master_address,
        signer_address=startup.signer.address,
    )
    watchdog = Watchdog(
        startup.pool,
        clock,
        read_gateway,
        audited,
        audit,
        FallbackBudget(SharedWeightBudget(startup.pool, clock, reserve=0), clock),
        master_address=startup.master_address,
        signer_address=startup.signer.address,
        executor_stale=STALE,
        db_blind_after=DB_BLIND,
        capability_interval=CAPABILITY_INTERVAL,
    )
    if startup.cold_start_reason is not None:
        watchdog.declare_cold_start(clock.now(), startup.cold_start_reason)
    return watchdog


async def _cold_start(
    cache: WatchdogKeyCache, clock: FakeClock, kek_path: Path, *, url: str = UNREACHABLE_URL
) -> Startup:
    return await open_startup(
        database_url=url,
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=clock.now(),
        grace=GRACE,
        retry_every=INTERVAL,
    )


# --- custody: the cached key material ---


async def test_a_db_backed_start_caches_the_watchdog_key_encrypted_at_rest(
    database_url: str,
    cache: WatchdogKeyCache,
    cache_path: Path,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    startup = await open_startup(
        database_url=database_url,
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=clock.now(),
        grace=GRACE,
        retry_every=INTERVAL,
    )
    try:
        assert startup.cold_start_reason is None
        assert startup.signer.address == signer.address
    finally:
        await startup.pool.close()

    # Owner-only, and no key material in the clear: the file holds the same
    # two sealed blobs an agent_keys row does, and nothing else.
    assert _mode(cache_path) == 0o600
    text, payload = _cache_file(cache_path)
    assert bytes(signer.key).hex() not in text.lower()
    assert payload["lane"] == WATCHDOG_LANE
    assert payload["agent_address"] == watchdog_key.agent_address
    assert set(payload) >= {"dek_wrapped", "key_ciphertext", "kek_id"}

    # And it decrypts back to exactly the keystore's key.
    cached = cache.load(clock.now())
    assert cached.signer.address == signer.address
    assert cached.master_address == MASTER


async def test_the_cache_is_useless_without_the_kek_and_rejects_tampering(
    cache: WatchdogKeyCache,
    cache_path: Path,
    tmp_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())

    other_kek = generate_kek(tmp_path / "other.kek")
    with pytest.raises(KeystoreError, match="sealed by KEK"):
        WatchdogKeyCache(cache_path, other_kek).load(clock.now())

    # Every header field is AAD-bound: repointing the cache at another
    # account fails authentication instead of silently retargeting the key.
    _, payload = _cache_file(cache_path)
    payload["master_address"] = "0x" + "99" * 20
    _rewrite(cache_path, payload)
    with pytest.raises(KeystoreError, match="failed authentication"):
        cache.load(clock.now())


async def test_the_cache_refuses_any_lane_but_the_watchdog_s(
    cache: WatchdogKeyCache, clock: FakeClock, keystore: AgentKeystore
) -> None:
    executor_key = await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        agent_name="epigone-executor-a",
        expires_at=clock.now() + timedelta(days=170),
        lane=EXECUTOR_LANE,
    )
    signer = await keystore.signer(OPERATOR, EXECUTOR_LANE)
    with pytest.raises(KeystoreError, match="watchdog lane ONLY"):
        cache.write(executor_key, bytes(signer.key), now=clock.now())
    assert not cache.path.exists()


async def test_every_db_backed_start_refreshes_the_cache(
    database_url: str,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
) -> None:
    async def start() -> None:
        startup = await open_startup(
            database_url=database_url,
            clock=clock,
            kek=load_kek(kek_path),
            cache=cache,
            operator_id=OPERATOR,
            launched_at=clock.now(),
            grace=GRACE,
            retry_every=INTERVAL,
        )
        await startup.pool.close()

    await start()
    first = cache.load(clock.now()).refreshed_at
    clock.advance(3600)
    await start()
    assert cache.load(clock.now()).refreshed_at == first + timedelta(seconds=3600)


async def test_an_expired_cached_key_is_refused_rather_than_used(
    cache: WatchdogKeyCache, clock: FakeClock, watchdog_key: AgentKeyRecord, keystore: AgentKeystore
) -> None:
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    clock.advance(timedelta(days=171).total_seconds())
    with pytest.raises(KeystoreError, match="expired"):
        cache.load(clock.now())


# --- the cold start itself ---


async def test_cold_start_with_postgres_unreachable_cancels_on_its_first_cycle(
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    """THE acceptance criterion: a fresh process, no database, no keystore,
    no halt state — and resting orders still die, within the documented
    bound (the startup grace, then one cycle)."""
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    launched_at = clock.now()

    startup = await _cold_start(cache, clock, kek_path)

    assert startup.signer.address == signer.address  # the CACHED key, only
    assert startup.master_address == MASTER
    assert startup.cold_start_reason is not None
    assert "COLD START" in startup.cold_start_reason
    # Bounded and documented: the grace window, and nothing beyond it.
    assert clock.now() - launched_at <= GRACE

    exec_gateway = FakeExecutionGateway()
    watchdog = _blind_watchdog(startup, clock, read_gateway, exec_gateway)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 71)])

    await watchdog.run_cycle()  # the FIRST cycle — no threshold left to wait

    assert _cancels(exec_gateway) == [CancelSpec(asset=1, oid=71)]
    # ... and it keeps sweeping for as long as the outage lasts.
    clock.advance(INTERVAL.total_seconds())
    await watchdog.run_cycle()
    assert len(_cancels(exec_gateway)) == 2


async def test_a_cold_started_watchdog_cannot_place_orders(
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    """Blind ≠ armed, structurally: the gateway a cold start signs with has
    no order-placing half at all. Asserted, not merely untested."""
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    startup = await _cold_start(cache, clock, kek_path)
    exec_gateway = FakeExecutionGateway()
    gateway = safety_gateway(
        exec_gateway,
        ExecutionAudit(startup.pool, clock),
        master_address=startup.master_address,
        signer_address=startup.signer.address,
    )

    with pytest.raises(OrderPlacementForbiddenError):
        await gateway.place_orders([])
    with pytest.raises(OrderPlacementForbiddenError):
        await gateway.modify_orders([])
    with pytest.raises(OrderPlacementForbiddenError):
        await gateway.update_leverage(1, 5)
    # Nothing reached the inner gateway: the refusal is before the signer.
    assert exec_gateway.actions == []

    # The subtractive half still works — that is the whole point of the lane.
    await gateway.cancel_orders([CancelSpec(asset=1, oid=7)])
    await gateway.schedule_cancel(None)
    assert [name for name, _ in exec_gateway.actions] == ["cancel_orders", "schedule_cancel"]


async def test_a_cold_start_without_a_usable_cache_refuses_to_start(
    cache: WatchdogKeyCache, kek_path: Path, clock: FakeClock
) -> None:
    """The false-safety rule: no key means no cancel path, so the process
    refuses rather than beating a heartbeat it cannot act behind."""
    with pytest.raises(KeystoreError, match="no usable watchdog key cache"):
        await _cold_start(cache, clock, kek_path)


# --- reconnection: the retroactive stamp and the reconciled window ---


async def test_the_reconnect_stamps_the_true_launch_time_and_refreshes_the_cache(
    database_url: str,
    pool: asyncpg.Pool,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    """The #52 grace clock must not restart because of a cold start: the
    stamp lands late, carrying the ACTUAL launch time (migration 0026)."""
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    launched_at = clock.now()
    clock.advance(600)  # ten minutes blind before the database answers

    opener = reconnect_opener(
        database_url=database_url,
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=launched_at,
        cached_agent_address=watchdog_key.agent_address,
    )
    reconnected = await opener()  # probe + the DEFERRED migration check + stamp
    await reconnected.close()

    started_at = await pool.fetchval(
        "SELECT started_at FROM process_heartbeats WHERE process = $1",
        heartbeat.WATCHDOG_PROCESS,
    )
    assert started_at == launched_at  # NOT clock.now(): the grace clock is untouched
    assert cache.load(clock.now()).refreshed_at == clock.now()
    detail = await pool.fetchval(
        "SELECT detail FROM execution_audit WHERE action = $1",
        "watchdog_cold_start_reconnected",
    )
    assert json.loads(detail)["blind_seconds"] == 600
    assert json.loads(detail)["signer_matches_keystore"] is True


async def test_a_rotation_during_the_blind_window_is_reported_not_silently_adopted(
    pool: asyncpg.Pool,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
    database_url: str,
) -> None:
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    launched_at = clock.now()
    await keystore.revoke(OPERATOR, WATCHDOG_LANE)
    rotated = await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        agent_name="epigone-watchdog-b",
        expires_at=clock.now() + timedelta(days=170),
        lane=WATCHDOG_LANE,
    )

    reconnected = await reconnect_opener(
        database_url=database_url,
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=launched_at,
        cached_agent_address=watchdog_key.agent_address,
    )()
    await reconnected.close()

    detail = json.loads(
        await pool.fetchval(
            "SELECT detail FROM execution_audit WHERE action = $1",
            "watchdog_cold_start_reconnected",
        )
    )
    assert detail["signer_matches_keystore"] is False
    assert detail["signer_address"] == watchdog_key.agent_address
    # The cache follows the keystore, so the NEXT cold start uses the new key.
    assert cache.load(clock.now()).agent_address == rotated.agent_address


async def test_the_cold_start_blind_window_reconciles_under_its_own_reason(
    pool: asyncpg.Pool,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    """When Postgres returns, the window lands in the trail as a COLD-START
    blind sweep — distinguishable from a running process's blind window,
    because the operator's follow-up differs."""
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    launched_at = clock.now()

    answering = False

    async def opener() -> asyncpg.Pool:
        if not answering:
            raise DatabaseUnavailableError("postgres unreachable")
        await deferred_startup(
            pool,
            clock=clock,
            kek=load_kek(kek_path),
            cache=cache,
            operator_id=OPERATOR,
            launched_at=launched_at,
            cached_agent_address=watchdog_key.agent_address,
        )
        return pool

    cold_pool = ColdStartPool(opener, clock, retry_after=INTERVAL)
    startup = Startup(
        pool=cold_pool,  # type: ignore[arg-type]
        signer=signer,
        master_address=MASTER,
        cold_start_reason="COLD START with Postgres unreachable: the test's outage",
    )
    exec_gateway = FakeExecutionGateway()
    watchdog = _blind_watchdog(startup, clock, read_gateway, exec_gateway)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 72)])

    await watchdog.run_cycle()  # blind: cancels, cannot record anything
    assert len(_cancels(exec_gateway)) == 1
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 0

    answering = True
    read_gateway.set_open_orders(MASTER, [])
    clock.advance(INTERVAL.total_seconds())
    await watchdog.run_cycle()  # Postgres answers: reconnect, then reconcile

    halt = await active_halt(pool)
    assert halt is not None
    assert "cold-start DB-blind sweep reconciled" in halt.reason
    assert halt.swept_at is not None  # the verify pass found the book empty
    detail = json.loads(
        await pool.fetchval(
            "SELECT detail FROM execution_audit WHERE action = $1 AND outcome = $2",
            "blind_window_reconciled",
            EVENT,
        )
    )
    assert detail["cold_start"] is True
    # Both cycles' passes: the incident cancels BEFORE it reconciles, so the
    # recovery cycle's (empty-book) pass counts too.
    assert detail["unrecorded_cancel_passes"] == 2
    # The deferred startup ran as part of the reconnect: the launch stamp is
    # retroactive even here, on the path a real outage actually takes.
    assert (
        await pool.fetchval(
            "SELECT started_at FROM process_heartbeats WHERE process = $1",
            heartbeat.WATCHDOG_PROCESS,
        )
        == launched_at
    )


async def test_a_running_process_blind_window_keeps_its_own_reason(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read_gateway: FakeHyperliquidGateway,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other side of the distinction — a DB-blind window opened by a
    RUNNING process must not acquire the cold-start label."""
    from epigone.safety import watchdog as watchdog_module

    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    startup = Startup(
        pool=pool, signer=signer, master_address=MASTER, cold_start_reason=None
    )
    exec_gateway = FakeExecutionGateway()
    watchdog = _blind_watchdog(startup, clock, read_gateway, exec_gateway)
    read_gateway.set_open_orders(MASTER, [open_order("ETH", 73)])

    async def db_down(_pool: asyncpg.Pool) -> None:
        raise ConnectionError("postgres unreachable")

    monkeypatch.setattr(watchdog_module, "active_halt", db_down)
    await watchdog.run_cycle()
    clock.advance(DB_BLIND.total_seconds() + 1)
    await watchdog.run_cycle()
    monkeypatch.undo()
    read_gateway.set_open_orders(MASTER, [])
    await watchdog.run_cycle()

    halt = await active_halt(pool)
    assert halt is not None
    assert "DB-blind sweep reconciled" in halt.reason
    assert "cold-start" not in halt.reason
    detail = json.loads(
        await pool.fetchval(
            "SELECT detail FROM execution_audit WHERE action = $1 AND outcome = $2",
            "blind_window_reconciled",
            EVENT,
        )
    )
    assert detail["cold_start"] is False


async def test_the_cold_start_pool_paces_its_reconnect_attempts(
    clock: FakeClock,
) -> None:
    """Without the fuse, every audit row, halt read, and heartbeat of a blind
    cycle would pay a fresh connect timeout."""
    attempts = 0

    async def opener() -> asyncpg.Pool:
        nonlocal attempts
        attempts += 1
        raise DatabaseUnavailableError("still down")

    cold_pool = ColdStartPool(opener, clock, retry_after=INTERVAL)
    for _ in range(5):
        with pytest.raises(DatabaseUnavailableError):
            await cold_pool.execute("SELECT 1")
    assert attempts == 1
    assert cold_pool.is_live is False

    clock.advance(INTERVAL.total_seconds())
    with pytest.raises(DatabaseUnavailableError):
        await cold_pool.fetchval("SELECT 1")
    assert attempts == 2


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores the directory mode this test sets")
async def test_an_unwritable_cache_never_turns_a_healthy_start_blind(
    database_url: str,
    pool: asyncpg.Pool,
    cache_path: Path,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
) -> None:
    """Review of this PR: refreshing the cache is bookkeeping for a FUTURE
    cold start. A read-only mount or a full disk must not make a process
    with a perfectly healthy database cancel the whole book blind."""
    _make_unwritable_dir(cache_path.parent)  # writes refused; the DB is fine
    try:
        startup = await open_startup(
            database_url=database_url,
            clock=clock,
            kek=load_kek(kek_path),
            cache=WatchdogKeyCache(cache_path, load_kek(kek_path)),
            operator_id=OPERATOR,
            launched_at=clock.now(),
            grace=GRACE,
            retry_every=INTERVAL,
        )
    finally:
        _restore_dir(cache_path.parent)
    try:
        assert startup.cold_start_reason is None  # DB-backed, as it should be
        assert startup.signer.address is not None
    finally:
        await startup.pool.close()
    with pytest.raises(KeystoreError, match="no usable watchdog key cache"):
        WatchdogKeyCache(cache_path, load_kek(kek_path)).load(clock.now())


async def test_the_reconnect_leaves_a_fresh_beat_beside_the_retroactive_stamp(
    database_url: str,
    pool: asyncpg.Pool,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
) -> None:
    """Review of this PR: record_start upserts beaten_at too, so stamping the
    launch retroactively would leave the LIVENESS column reading as of the
    launch — and the #52 staleness check would page for a process that is
    plainly alive. started_at looks back; beaten_at says "now"."""
    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())
    launched_at = clock.now()
    clock.advance(3600)  # an hour blind: far past any staleness threshold

    reconnected = await reconnect_opener(
        database_url=database_url,
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=launched_at,
        cached_agent_address=watchdog_key.agent_address,
    )()
    await reconnected.close()

    row = await pool.fetchrow(
        "SELECT started_at, beaten_at FROM process_heartbeats WHERE process = $1",
        heartbeat.WATCHDOG_PROCESS,
    )
    assert row is not None
    assert row["started_at"] == launched_at
    assert row["beaten_at"] == clock.now()


async def test_a_startup_that_stalls_after_a_good_probe_still_goes_blind(
    database_url: str,
    cache: WatchdogKeyCache,
    kek_path: Path,
    clock: FakeClock,
    watchdog_key: AgentKeyRecord,
    keystore: AgentKeystore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of this PR: the probe answering is not a promise that the next
    step RETURNS — `migrate` waits on an advisory lock with no timeout, and
    asyncpg's transaction exit is the one leg no per-op bound reaches. A
    started-but-stalled attempt must be abandoned at the grace, not awaited
    forever, or the process hangs unstarted and unprotecting."""
    from epigone.safety import coldstart

    signer = await keystore.signer(OPERATOR, WATCHDOG_LANE)
    cache.write(watchdog_key, bytes(signer.key), now=clock.now())

    async def stall(**kwargs: object) -> Startup:
        await asyncio.sleep(30)  # far past the ceiling; never returns in time
        raise AssertionError("unreachable")

    monkeypatch.setattr(coldstart, "_db_backed_startup", stall)
    tick = timedelta(seconds=1)

    startup = await open_startup(
        database_url=database_url,  # REACHABLE: the probe succeeds every time
        clock=clock,
        kek=load_kek(kek_path),
        cache=cache,
        operator_id=OPERATOR,
        launched_at=clock.now(),
        grace=tick,
        retry_every=tick,
    )

    assert startup.cold_start_reason is not None  # blind, not hung
    assert startup.signer.address == signer.address
