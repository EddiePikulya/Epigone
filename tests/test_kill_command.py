"""The operator kill switch: /kill and /resume (issue #135).

/kill is immediate (an emergency stop takes no dialog); /resume is the
two-step one — resuming is consent to trade again. Both are owner-only,
and the resume-confirm callback re-checks the owner because callback
payloads are client-forgeable."""

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.access import ADMIN_ONLY_TEXT
from epigone.bot.handlers import build_router
from epigone.bot.operator import (
    NOT_HALTED_TEXT,
    RESUME_CANCEL_CALLBACK,
    RESUME_CANCELLED_TEXT,
    RESUME_CONFIRM_CALLBACK,
    RESUME_GONE_TEXT,
    RESUME_PROMPT_TEXT,
    RESUMED_TEXT,
    WATCHDOG_SILENT_WARNING,
)
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.safety import heartbeat
from epigone.safety.audit import EVENT
from epigone.safety.halt import KILL_SOURCE, active_halt, is_halted
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text

ADMIN = 370818090
GUEST = 111


@pytest.fixture
def admin_dp(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> Dispatcher:
    """A dispatcher with a configured owner (the conftest `dp` deliberately has
    none), so the owner-only path is drivable; no gate — gating has its own
    suite (test_invite_only)."""
    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher["gateway"] = gateway
    dispatcher["clock"] = clock
    dispatcher["admin_telegram_id"] = ADMIN
    dispatcher["drafts"] = {}
    dispatcher["min_size_pending"] = {}
    dispatcher["rename_pending"] = {}
    dispatcher.include_router(build_router())
    return dispatcher


async def test_kill_warns_when_no_watchdog_is_beating(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # No watchdog heartbeat exists at all: /kill must not promise a sweep
    # nobody will run.
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    assert WATCHDOG_SILENT_WARNING in (session.sent_messages()[-1].text or "")


async def test_kill_halts_immediately_and_audits(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await heartbeat.beat(pool, heartbeat.WATCHDOG_PROCESS, clock.now())
    await feed_text(admin_dp, bot, "/kill fat-finger ladder", user_id=ADMIN)
    assert await is_halted(pool)
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.source == KILL_SOURCE
    assert halt.requested_by == ADMIN
    assert "fat-finger ladder" in halt.reason
    reply = session.sent_messages()[-1].text or ""
    assert "halted" in reply.lower()
    assert "positions" in reply.lower()  # the unwind policy is stated, not implied
    assert WATCHDOG_SILENT_WARNING not in reply  # the watchdog IS beating here
    events = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1", EVENT
    )
    assert [r["action"] for r in events] == ["halt"]


async def test_kill_is_owner_only(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=GUEST)
    assert not await is_halted(pool)
    assert session.sent_messages()[-1].text == ADMIN_ONLY_TEXT


async def test_kill_while_halted_joins_not_stacks(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/kill again", user_id=ADMIN)
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 1
    assert "Already halted" in (session.sent_messages()[-1].text or "")


async def test_resume_confirm_flow(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    prompt = session.sent_messages()[-1]
    assert prompt.text == RESUME_PROMPT_TEXT
    assert prompt.reply_markup is not None  # the confirm/cancel keyboard

    await feed_callback(admin_dp, bot, RESUME_CONFIRM_CALLBACK, user_id=ADMIN)
    assert not await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUMED_TEXT
    events = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    assert [r["action"] for r in events] == ["halt", "resume"]


async def test_resume_cancel_keeps_the_halt(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    await feed_callback(admin_dp, bot, RESUME_CANCEL_CALLBACK, user_id=ADMIN)
    assert await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUME_CANCELLED_TEXT


async def test_resume_confirm_is_owner_only_even_as_callback(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    # A forged/stray tap from a non-owner must not lift the halt.
    await feed_callback(admin_dp, bot, RESUME_CONFIRM_CALLBACK, user_id=GUEST)
    assert await is_halted(pool)
    assert (session.callback_answers()[-1].text or "") == ADMIN_ONLY_TEXT


async def test_resume_without_a_halt(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    assert session.sent_messages()[-1].text == NOT_HALTED_TEXT
    # A stale confirm tap (halt already gone) is a no-op with a clear edit.
    await feed_callback(admin_dp, bot, RESUME_CONFIRM_CALLBACK, user_id=ADMIN)
    assert session.edited_messages()[-1].text == RESUME_GONE_TEXT
