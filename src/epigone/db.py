"""Pool creation and the migration runner (issue #16).

Schema changes ship as numbered SQL files in src/epigone/migrations/
(NNNN_name.sql). Every process (ADR-0002) calls migrate() at startup, which
applies the not-yet-applied ones in version order and records each in
schema_migrations. A shipped migration file is frozen history — never edit
one; add the next number. Migration 0001 is the pre-runner schema.sql and is
idempotent so it doubles as the baseline for databases that predate the
runner; later migrations run exactly once and need no such care.
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


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url)


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
