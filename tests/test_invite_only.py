"""Invite-only gate (issue #33) acceptance: a single middleware seam refuses
every non-allowlisted User before any handler runs; the admin always has access
and grants/revokes at runtime; grants persist across restarts.

These build their own gated Dispatcher (the shared `dp` fixture is ungated) so
the gate is exercised end-to-end, exactly as bot/main.py wires it.
"""

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone import allowlist
from epigone.bot.access import (
    ADMIN_ONLY_TEXT,
    CANT_REVOKE_ADMIN_TEXT,
    REFUSAL_TEXT,
    install_allowlist_gate,
)
from epigone.bot.handlers import build_router
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text

ADMIN_ID = 370818090
STRANGER_ID = 999
GUEST_ID = 555

# A valid-looking wallet address, so the paste path would run if not gated.
WALLET = "0x" + "a" * 40


def _make_gated_dp(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> Dispatcher:
    dp = Dispatcher()
    dp["pool"] = pool
    dp["gateway"] = gateway
    dp["clock"] = clock
    dp["admin_telegram_id"] = ADMIN_ID
    dp["drafts"] = {}
    dp["min_size_pending"] = {}
    install_allowlist_gate(dp)
    dp.include_router(build_router())
    return dp


@pytest.fixture
def gated_dp(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> Dispatcher:
    return _make_gated_dp(pool, gateway, clock)


async def test_stranger_is_refused_on_start_and_no_handler_runs(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(gated_dp, bot, "/start", user_id=STRANGER_ID)

    sent = session.sent_messages()
    assert len(sent) == 1
    assert sent[0].text == REFUSAL_TEXT
    # cmd_start would have upserted a users row — proof the handler never ran.
    rows = await pool.fetchval("SELECT count(*) FROM users WHERE telegram_id = $1", STRANGER_ID)
    assert rows == 0


@pytest.mark.parametrize("text", ["/start", "/help", "/screener", "/tracked", "/criteria", WALLET])
async def test_stranger_is_refused_on_every_command(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession, text: str
) -> None:
    await feed_text(gated_dp, bot, text, user_id=STRANGER_ID)

    sent = session.sent_messages()
    assert [m.text for m in sent] == [REFUSAL_TEXT]


async def test_stranger_is_refused_on_callbacks(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_callback(gated_dp, bot, "screen:1", user_id=STRANGER_ID)

    # Refusal rides the callback answer (a toast), and no handler-sent message.
    answers = session.callback_answers()
    assert len(answers) == 1
    assert answers[0].text == REFUSAL_TEXT
    assert session.sent_messages() == []


async def test_admin_always_has_access_without_a_row(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # No allowlist row for the admin — they're allowed purely from config.
    await feed_text(gated_dp, bot, "/start", user_id=ADMIN_ID, username="owner")

    text = session.sent_messages()[0].text or ""
    assert REFUSAL_TEXT not in text
    assert "Epigone" in text  # the real /start greeting
    assert await pool.fetchval("SELECT count(*) FROM users WHERE telegram_id = $1", ADMIN_ID) == 1


async def test_admin_can_allow_and_the_guest_then_uses_the_bot(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(gated_dp, bot, f"/allow {GUEST_ID}", user_id=ADMIN_ID)
    assert str(GUEST_ID) in (session.sent_messages()[-1].text or "")
    assert await allowlist.is_allowed(pool, GUEST_ID) is True

    await feed_text(gated_dp, bot, "/start", user_id=GUEST_ID, username="guest")
    greeting = session.sent_messages()[-1].text or ""
    assert REFUSAL_TEXT not in greeting
    assert "Epigone" in greeting


async def test_revoked_guest_is_blocked_on_next_message(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(gated_dp, bot, f"/allow {GUEST_ID}", user_id=ADMIN_ID)
    await feed_text(gated_dp, bot, "/help", user_id=GUEST_ID)
    assert REFUSAL_TEXT not in (session.sent_messages()[-1].text or "")  # allowed → normal reply

    await feed_text(gated_dp, bot, f"/revoke {GUEST_ID}", user_id=ADMIN_ID)
    await feed_text(gated_dp, bot, "/help", user_id=GUEST_ID)
    assert session.sent_messages()[-1].text == REFUSAL_TEXT


async def test_grant_persists_across_a_restart(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    bot: Bot,
    session: RecordingSession,
) -> None:
    first = _make_gated_dp(pool, gateway, clock)
    await feed_text(first, bot, f"/allow {GUEST_ID}", user_id=ADMIN_ID)

    # A "restart": a brand-new Dispatcher on the same database, no shared state.
    second = _make_gated_dp(pool, gateway, clock)
    await feed_text(second, bot, "/help", user_id=GUEST_ID)
    assert REFUSAL_TEXT not in (session.sent_messages()[-1].text or "")


async def test_admin_cannot_revoke_themselves(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(gated_dp, bot, f"/revoke {ADMIN_ID}", user_id=ADMIN_ID)
    assert session.sent_messages()[-1].text == CANT_REVOKE_ADMIN_TEXT

    # And the admin still has access afterwards.
    await feed_text(gated_dp, bot, "/help", user_id=ADMIN_ID)
    assert REFUSAL_TEXT not in (session.sent_messages()[-1].text or "")


async def test_allowed_lists_owner_and_members(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(gated_dp, bot, "/allow 111", user_id=ADMIN_ID)
    await feed_text(gated_dp, bot, "/allow 222", user_id=ADMIN_ID)

    await feed_text(gated_dp, bot, "/allowed", user_id=ADMIN_ID)
    listing = session.sent_messages()[-1].text or ""
    assert str(ADMIN_ID) in listing
    assert "111" in listing
    assert "222" in listing


async def test_allowlisted_guest_cannot_use_admin_commands(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(gated_dp, bot, f"/allow {GUEST_ID}", user_id=ADMIN_ID)

    # The guest is allowed (passes the gate) but is not the owner.
    await feed_text(gated_dp, bot, "/allow 777", user_id=GUEST_ID)
    assert session.sent_messages()[-1].text == ADMIN_ONLY_TEXT
    assert await allowlist.is_allowed(pool, 777) is False


async def test_allow_without_an_id_shows_usage(
    gated_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(gated_dp, bot, "/allow", user_id=ADMIN_ID)
    assert "Usage" in (session.sent_messages()[-1].text or "")
