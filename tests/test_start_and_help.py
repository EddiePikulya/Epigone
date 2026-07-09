"""Ticket #2 acceptance: /start and /help respond; Users are persisted."""

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_text


async def test_start_greets_with_what_epigone_does(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=111, username="edik")

    sent = session.sent_messages()
    assert len(sent) == 1
    text = sent[0].text or ""
    assert "Hyperliquid" in text
    assert "criteria" in text.lower()
    assert "/help" in text


async def test_start_persists_the_user(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=222, username="whale_fan")

    row = await pool.fetchrow("SELECT * FROM users WHERE telegram_id = 222")
    assert row is not None
    assert row["username"] == "whale_fan"


async def test_start_twice_is_idempotent(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=333)
    await feed_text(dp, bot, "/start", user_id=333)

    count = await pool.fetchval("SELECT count(*) FROM users WHERE telegram_id = 333")
    assert count == 1
    assert len(session.sent_messages()) == 2  # greeted both times, stored once


async def test_help_lists_commands(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=444)

    text = session.sent_messages()[0].text or ""
    assert "/start" in text
    assert "/help" in text
