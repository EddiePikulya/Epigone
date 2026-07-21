import asyncio
import os
from collections.abc import AsyncGenerator

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import build_router
from epigone.db import migrate
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.db import reset_database
from tests.support.telegram import RecordingSession, make_bot

DEFAULT_TEST_DATABASE_URL = "postgresql://epigone:epigone@localhost:5432/epigone_test"


async def _rebuild_schema(url: str) -> None:
    server_url, _, dbname = url.rpartition("/")
    # Migrations assume a shipped file never changes, but dev branches edit
    # them freely — so rebuild from scratch rather than trust the bookkeeping
    # of whatever branch touched this database last.
    await reset_database(server_url, dbname)

    pool = await asyncpg.create_pool(url)
    assert pool is not None
    await migrate(pool)
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
            "TRUNCATE users, traders, coarse_metrics, fine_metrics, fine_trades, "
            "fine_open_episodes, tracks, position_poll_state, position_snapshots, "
            "position_alerts, first_data_notices, criteria, criteria_preset_dismissals, "
            "rate_budget, rate_limit_events, allowlist"
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
    # No admin here and no gate installed: the shared dispatcher is ungated so
    # the bulk of the suite exercises handlers directly. The invite-only gate is
    # tested against its own gated dispatcher in test_invite_only.py. Admin
    # commands still resolve this key (None → owner-only refusal).
    dispatcher["admin_telegram_id"] = None
    dispatcher["drafts"] = {}  # per-User criteria-builder drafts (bot/criteria.py)
    dispatcher["min_size_pending"] = {}  # per-User min-size prompts (bot/controls.py)
    dispatcher["rename_pending"] = {}  # per-User wallet-rename prompts (bot/names.py)
    dispatcher.include_router(build_router())
    return dispatcher
