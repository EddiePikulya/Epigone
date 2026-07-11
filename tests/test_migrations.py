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


@pytest.fixture
async def scratch_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    base = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    server_url, _, _ = base.rpartition("/")
    await reset_database(server_url, SCRATCH_DBNAME)

    pool = await asyncpg.create_pool(f"{server_url}/{SCRATCH_DBNAME}")
    assert pool is not None
    yield pool
    await pool.close()


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


async def test_concurrent_startups_apply_once(scratch_pool: asyncpg.Pool) -> None:
    """All three processes (ADR-0002) migrate at startup; the advisory lock
    must serialize them so a migration executes exactly once."""
    await asyncio.gather(*(migrate(scratch_pool, [counting_migration()]) for _ in range(5)))

    async with scratch_pool.acquire() as conn:
        runs = await conn.fetchval("SELECT count(*) FROM runs")
    assert runs == 1
