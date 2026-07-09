import os
from collections.abc import AsyncGenerator

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import build_router
from epigone.db import apply_schema
from tests.support.telegram import RecordingSession, make_bot

DEFAULT_TEST_DATABASE_URL = "postgresql://epigone:epigone@localhost:5432/epigone_test"


@pytest.fixture
async def pool() -> AsyncGenerator[asyncpg.Pool, None]:
    url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    server_url, _, dbname = url.rpartition("/")
    admin = await asyncpg.connect(f"{server_url}/postgres")
    exists = await admin.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
    if not exists:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
    await admin.close()

    pool = await asyncpg.create_pool(url)
    assert pool is not None
    await apply_schema(pool)
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE users, traders, coarse_metrics")
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
def dp(pool: asyncpg.Pool) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher.include_router(build_router())
    return dispatcher
