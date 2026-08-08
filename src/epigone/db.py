"""Pool creation and the migration runner (issue #16).

Schema changes ship as numbered SQL files in src/epigone/migrations/
(NNNN_name.sql). Every process (ADR-0002) calls migrate() at startup, which
applies the not-yet-applied ones in version order and records each in
schema_migrations. A shipped migration file is frozen history — never edit
one; add the next number. Migration 0001 is the pre-runner schema.sql and is
idempotent so it doubles as the baseline for databases that predate the
runner; later migrations run exactly once and need no such care.

`create_pool` is UNBOUNDED BY DEFAULT and stays that way (issue #213): fine
backfills and migrations run legitimately long queries and must not inherit
a timeout tuned for a process that has a deadline. The optional bounds exist
for the processes that DO have one — the watchdog (epigone.safety.db) and,
since #213, the health monitor — and the mechanism lives here rather than in
either caller because the release-budget seam below is asyncpg-version-
specific knowledge that must not be maintained in two places.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from importlib.resources import files

import asyncpg

# Serializes concurrent migrate() calls: all three processes run it at startup.
_MIGRATE_LOCK_KEY = 0x45504947  # "EPIG"

_FILENAME_RE = re.compile(r"(\d{4})_([a-z0-9_]+)\.sql")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


async def create_pool(
    database_url: str,
    *,
    timeout_seconds: float | None = None,
    lock_timeout: str | None = None,
) -> asyncpg.Pool:
    """A pool over `database_url`. With no arguments it is asyncpg's plain,
    unbounded one — the scanners' and the migration runner's pool.

    `timeout_seconds` bounds EVERY plain touch of the pool, which is what a
    process with a deadline needs and what the module docstring explains the
    default cannot be:

    - CONNECT: asyncpg's per-connection `timeout`, so a dead host refuses
      fast instead of waiting out the 60s default;
    - PER QUERY: `command_timeout`, enforced CLIENT-side by asyncpg — which
      is what makes it fire even when the server is a black hole rather than
      a server that says no;
    - THE CANCEL-WAIT ON RELEASE: when `command_timeout` fires, asyncpg sends
      its CancelRequest over a FRESH connection opened with NO timeout, and
      release then AWAITS that cancellation with the holder's budget — which
      is the ACQUIRE timeout, `None` for a plain `pool.execute`. Against a
      black-holed host that turns "≤N seconds per query" into N seconds plus
      a kernel SYN timeout (~2 minutes on Linux) PER TOUCH. So the pool's
      `acquire()` carries a DEFAULT timeout, which asyncpg stamps onto the
      holder and applies as the release budget: a cancellation that cannot
      complete inside it gets the connection TERMINATED instead of awaited,
      and a fully hung touch costs roughly 2× `timeout_seconds`.

    `lock_timeout` (a Postgres interval string) additionally bounds server-
    side lock waits, so a `SELECT … FOR UPDATE` behind a holder that died
    mid-transaction answers with an error instead of queueing forever.

    HONEST SCOPE, unchanged from where this mechanism came from (PR #143
    round 5): these bounds reach plain execute/fetch/acquire touches. They
    can NOT reach a transaction exit — every asyncpg protocol op awaits an
    UNTIMED cancel_waiter before its own command timeout arms, so a ROLLBACK
    issued after a black-holed statement outlives all of them. Callers that
    need a bound on a whole block wrap it in `asyncio.wait_for` as well
    (epigone.safety.watchdog's DB_BLOCK_CEILING_SECONDS, the monitor's
    MONITOR_DB_CEILING_SECONDS); that composes safely because Pool.release is
    shielded, so a cancelled block's connection is still terminated within
    the acquire budget.
    """
    server_settings = {"lock_timeout": lock_timeout} if lock_timeout is not None else None
    pool = await asyncpg.create_pool(
        database_url,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
        server_settings=server_settings,
    )
    assert pool is not None
    if timeout_seconds is None:
        return pool

    # The acquire-timeout default (docstring above). asyncpg 0.31 offers no
    # pool_class hook and Pool declares __slots__, so the least invasive seam
    # is a layout-compatible subclass swapped onto the live pool: acquire()
    # gains a default timeout, which asyncpg itself then carries into the
    # holder as the RELEASE budget ("Record the timeout, as we will apply it
    # by default in release()" — asyncpg/pool.py). Every caller — pool.execute,
    # pool.fetchrow, a bare pool.acquire() in a shared state module —
    # inherits the bound with no call-site changes.
    bound = timeout_seconds

    class _BoundedAcquirePool(type(pool)):  # type: ignore[misc]
        # Empty slots keep the layout identical to Pool (which declares
        # __slots__), which is what makes the __class__ assignment legal —
        # and is also why the per-pool timeout lives in this closure rather
        # than on the instance. HONESTLY: this construction can NEVER fail
        # loudly on an asyncpg upgrade — deriving from type(pool) with empty
        # slots is layout-compatible with whatever Pool becomes, and every
        # realistic drift (helpers no longer routing through self.acquire(),
        # the acquire timeout no longer becoming the release budget) degrades
        # SILENTLY. The real guard is the version pin (pyproject:
        # asyncpg>=0.31,<0.32 — an upgrade is a deliberate visit to this
        # seam) plus the black-holed-connection tests in CI, which exercise
        # the actual behaviour.
        __slots__ = ()

        def acquire(self, *, timeout: float | None = None) -> "asyncpg.pool.PoolAcquireContext":
            return super().acquire(timeout=bound if timeout is None else timeout)

    pool.__class__ = _BoundedAcquirePool
    return pool


def load_migrations() -> list[Migration]:
    """The packaged migrations, sorted by version."""
    migrations = []
    for entry in (files("epigone") / "migrations").iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _FILENAME_RE.fullmatch(entry.name)
        if match is None:
            raise ValueError(f"migration filename must be NNNN_name.sql: {entry.name}")
        migrations.append(Migration(int(match.group(1)), match.group(2), entry.read_text()))
    return sorted(migrations, key=lambda m: m.version)


async def migrate(pool: asyncpg.Pool, migrations: Sequence[Migration] | None = None) -> None:
    """Bring the database to the current schema.

    The whole run is a single transaction under an advisory lock: concurrent
    process startups serialize, and a failed migration rolls everything back —
    bookkeeping never claims more than what committed.
    """
    if migrations is None:
        migrations = load_migrations()
    versions = [m.version for m in migrations]
    if len(set(versions)) != len(versions):
        raise ValueError(f"duplicate migration versions: {sorted(versions)}")

    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", _MIGRATE_LOCK_KEY)
        # applied_at is operational bookkeeping stamped at startup, before any
        # clock is wired up — the injected-clock convention is for domain data.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        applied: set[int] = {
            r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")
        }
        for migration in sorted(migrations, key=lambda m: m.version):
            if migration.version in applied:
                continue
            await conn.execute(migration.sql)
            await conn.execute(
                "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
                migration.version,
                migration.name,
            )
