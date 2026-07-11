"""The migration runner (issue #16): numbered SQL migrations applied in order
at process startup, tracked in schema_migrations.

These tests run against their own scratch database (not the shared epigone_test
one) because they exercise database states the shared fixtures deliberately
erase: a fresh empty database and a pre-migration-era database with data.
"""

import asyncio
import os
from collections.abc import AsyncGenerator

import asyncpg
import pytest

from epigone.db import Migration, load_migrations, migrate
from tests.conftest import DEFAULT_TEST_DATABASE_URL
from tests.support.db import reset_database

SCRATCH_DBNAME = "epigone_test_migrations"
# The live-vs-fresh convergence check (issue #37) needs two independent DBs side
# by side, so it gets its own pair on top of the shared scratch one.
FRESH_DBNAME = "epigone_test_migrations_fresh"
LIVE_DBNAME = "epigone_test_migrations_live"


async def _make_pool(dbname: str) -> asyncpg.Pool:
    base = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    server_url, _, _ = base.rpartition("/")
    await reset_database(server_url, dbname)
    pool = await asyncpg.create_pool(f"{server_url}/{dbname}")
    assert pool is not None
    return pool


@pytest.fixture
async def scratch_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await _make_pool(SCRATCH_DBNAME)
    yield pool
    await pool.close()


@pytest.fixture
async def fresh_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await _make_pool(FRESH_DBNAME)
    yield pool
    await pool.close()


@pytest.fixture
async def live_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await _make_pool(LIVE_DBNAME)
    yield pool
    await pool.close()


# The pre-runner drift the live DB carries that a fresh 0001 DB never had (issue
# #37): the #26-vestigial traders columns + index that the old add-only
# apply_schema could not drop, and the #10 hand-named scale-check constraint.
_LIVE_DRIFT_SQL = """
    ALTER TABLE traders ADD COLUMN coarse_attempted_at TIMESTAMPTZ;
    ALTER TABLE traders ADD COLUMN coarse_refreshed_at TIMESTAMPTZ;
    CREATE INDEX traders_coarse_attempt_order
        ON traders (coarse_attempted_at ASC NULLS FIRST, address);
    ALTER TABLE position_alerts
        RENAME CONSTRAINT position_alerts_check3 TO position_alerts_scale_check;
"""


async def _schema_snapshot(conn: asyncpg.Connection) -> dict[str, list[tuple[object, ...]]]:
    """A structural fingerprint of the public schema: columns, constraints (by
    name + definition), and indexes. Compares shape, not data, so two DBs that
    reached the same schema by different routes fingerprint identically."""
    columns = await conn.fetch(
        "SELECT table_name, column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema = 'public' "
        "ORDER BY table_name, column_name"
    )
    constraints = await conn.fetch(
        "SELECT conrelid::regclass::text AS tbl, conname, pg_get_constraintdef(oid) AS def "
        "FROM pg_constraint WHERE connamespace = 'public'::regnamespace "
        "ORDER BY tbl, conname"
    )
    indexes = await conn.fetch(
        "SELECT tablename, indexname, indexdef FROM pg_indexes "
        "WHERE schemaname = 'public' ORDER BY tablename, indexname"
    )
    return {
        "columns": [tuple(r) for r in columns],
        "constraints": [tuple(r) for r in constraints],
        "indexes": [tuple(r) for r in indexes],
    }


def counting_migration(version: int = 1) -> Migration:
    """A migration with an observable side effect per execution."""
    return Migration(
        version=version,
        name="counter",
        sql=(
            "CREATE TABLE IF NOT EXISTS runs (id BIGSERIAL PRIMARY KEY);"
            "INSERT INTO runs DEFAULT VALUES;"
        ),
    )


async def test_fresh_database_gets_full_schema_and_bookkeeping(
    scratch_pool: asyncpg.Pool,
) -> None:
    await migrate(scratch_pool)

    async with scratch_pool.acquire() as conn:
        recorded = await conn.fetch("SELECT version, name FROM schema_migrations ORDER BY version")
        tables = {
            r["tablename"]
            for r in await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        }

    packaged = load_migrations()
    assert packaged, "no packaged migrations found"
    assert (packaged[0].version, packaged[0].name) == (1, "baseline")
    assert [r["version"] for r in recorded] == [m.version for m in packaged]
    assert {"users", "traders", "tracks", "position_alerts", "criteria"} <= tables


async def test_rerun_executes_nothing(scratch_pool: asyncpg.Pool) -> None:
    await migrate(scratch_pool, [counting_migration()])
    await migrate(scratch_pool, [counting_migration()])

    async with scratch_pool.acquire() as conn:
        runs = await conn.fetchval("SELECT count(*) FROM runs")
    assert runs == 1


async def test_pending_migrations_apply_in_version_order(scratch_pool: asyncpg.Pool) -> None:
    v1 = Migration(1, "log", "CREATE TABLE log (version INT); INSERT INTO log VALUES (1)")
    v2 = Migration(2, "two", "INSERT INTO log VALUES (2)")
    v3 = Migration(3, "three", "INSERT INTO log VALUES (3)")

    await migrate(scratch_pool, [v1])
    # Later runs see one already-applied and two pending, passed out of order.
    await migrate(scratch_pool, [v3, v1, v2])

    async with scratch_pool.acquire() as conn:
        logged = [r["version"] for r in await conn.fetch("SELECT version FROM log ORDER BY ctid")]
        recorded = [
            r["version"]
            for r in await conn.fetch("SELECT version FROM schema_migrations ORDER BY version")
        ]
    assert logged == [1, 2, 3]
    assert recorded == [1, 2, 3]


async def test_preexisting_database_without_bookkeeping_is_baselined(
    scratch_pool: asyncpg.Pool,
) -> None:
    """The live-DB scenario from issue #16: every table already at the target
    shape (created by the old apply_schema + hand-applied ALTERs), no
    schema_migrations. The idempotent baseline must no-op, keep the data, and
    record itself as applied."""
    baseline = load_migrations()[0]
    async with scratch_pool.acquire() as conn:
        await conn.execute(baseline.sql)
        await conn.execute("INSERT INTO users (telegram_id, username) VALUES (7, 'live')")

    await migrate(scratch_pool)

    async with scratch_pool.acquire() as conn:
        username = await conn.fetchval("SELECT username FROM users WHERE telegram_id = 7")
        recorded = [
            r["version"]
            for r in await conn.fetch("SELECT version FROM schema_migrations ORDER BY version")
        ]
    assert username == "live"
    assert recorded == [m.version for m in load_migrations()]


async def test_duplicate_versions_are_rejected(scratch_pool: asyncpg.Pool) -> None:
    with pytest.raises(ValueError, match="duplicate"):
        await migrate(scratch_pool, [Migration(1, "a", "SELECT 1"), Migration(1, "b", "SELECT 1")])


async def test_failed_run_leaves_no_trace(scratch_pool: asyncpg.Pool) -> None:
    good = Migration(1, "good", "CREATE TABLE fine (id INT)")
    bad = Migration(2, "bad", "CREATE TABLE broken (id NO_SUCH_TYPE)")

    with pytest.raises(asyncpg.PostgresError):
        await migrate(scratch_pool, [good, bad])

    async with scratch_pool.acquire() as conn:
        tables = {
            r["tablename"]
            for r in await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        }
    # The whole run is one transaction: the good migration rolled back too,
    # and no bookkeeping claims otherwise.
    assert "fine" not in tables
    assert "schema_migrations" not in tables


async def test_live_and_fresh_converge_after_migration(
    fresh_pool: asyncpg.Pool, live_pool: asyncpg.Pool
) -> None:
    """Issue #37: running the full chain on a fresh DB and on a
    baseline-then-#10-hand-applied ("live") DB must yield an identical schema.
    0002's whole job is to erase the drift the live DB carries."""
    baseline = load_migrations()[0]

    # The live DB as it existed before the runner: baseline shape plus the
    # hand-applied drift, no schema_migrations. migrate() then runs 0001 (a
    # no-op on the already-present schema) and 0002 (which cleans the drift).
    async with live_pool.acquire() as conn:
        await conn.execute(baseline.sql)
        await conn.execute(_LIVE_DRIFT_SQL)

    await migrate(fresh_pool)
    await migrate(live_pool)

    async with fresh_pool.acquire() as fresh_conn, live_pool.acquire() as live_conn:
        fresh = await _schema_snapshot(fresh_conn)
        live = await _schema_snapshot(live_conn)
    assert live == fresh


async def test_0002_is_schema_noop_on_a_fresh_db(scratch_pool: asyncpg.Pool) -> None:
    """Issue #37: a DB that never had the drift must come out of 0002 with its
    schema unchanged — 0002 only records itself as applied."""
    baseline = [m for m in load_migrations() if m.version == 1]
    await migrate(scratch_pool, baseline)
    async with scratch_pool.acquire() as conn:
        before = await _schema_snapshot(conn)

    await migrate(scratch_pool)  # applies 0002 (and re-no-ops 0001)

    async with scratch_pool.acquire() as conn:
        after = await _schema_snapshot(conn)
        applied = [
            r["version"]
            for r in await conn.fetch("SELECT version FROM schema_migrations ORDER BY version")
        ]
    assert before == after
    assert applied == [m.version for m in load_migrations()]


async def test_concurrent_startups_apply_once(scratch_pool: asyncpg.Pool) -> None:
    """All three processes (ADR-0002) migrate at startup; the advisory lock
    must serialize them so a migration executes exactly once."""
    await asyncio.gather(*(migrate(scratch_pool, [counting_migration()]) for _ in range(5)))

    async with scratch_pool.acquire() as conn:
        runs = await conn.fetchval("SELECT count(*) FROM runs")
    assert runs == 1
