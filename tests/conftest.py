import asyncio
import os
from collections.abc import AsyncGenerator

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import build_router
from epigone.db import apply_schema
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, make_bot

DEFAULT_TEST_DATABASE_URL = "postgresql://epigone:epigone@localhost:5432/epigone_test"


async def _rebuild_schema(url: str) -> None:
    server_url, _, dbname = url.rpartition("/")
    admin = await asyncpg.connect(f"{server_url}/postgres")
    exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
    if not exists:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()

    conn = await asyncpg.connect(url)
    # CREATE TABLE IF NOT EXISTS never ALTERs an existing table, so a schema.sql
    # change on main would otherwise leave this database stale (UndefinedColumnError).
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    await conn.close()

    pool = await asyncpg.create_pool(url)
    assert pool is not None
    await apply_schema(pool)
    await pool.close()


@pytest.fixture(scope="session")
def database_url() -> str:
    """Once per test run: the throwaway DB with its schema rebuilt from scratch,
    so it is actually throwaway. Sync + asyncio.run keeps it out of pytest-asyncio's
    per-test event loops."""
    url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    asyncio.run(_rebuild_schema(url))
    return url


@pytest.fixture
async def pool(database_url: str) -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await asyncpg.create_pool(database_url)
    assert pool is not None
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE users, traders, coarse_metrics, fine_metrics, tracks, "
            "position_poll_state, position_snapshots, position_alerts, criteria, rate_budget"
        )
    yield pool
    await pool.close()


@pytest.fixture
def session() -> RecordingSession:
    return RecordingSession()


@pytest.fixture
async def bot(session: RecordingSession) -> AsyncGenerator[Bot, None]:
    bot = make_bot(session)
    yield bot
    await bot.session.close()


@pytest.fixture
def gateway() -> FakeHyperliquidGateway:
    return FakeHyperliquidGateway()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def dp(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher["gateway"] = gateway
    dispatcher["clock"] = clock
    dispatcher["drafts"] = {}  # per-User criteria-builder drafts (bot/criteria.py)
    dispatcher.include_router(build_router())
    return dispatcher
