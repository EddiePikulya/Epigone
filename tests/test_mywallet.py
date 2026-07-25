"""My-wallet copy alerts (#121), command seam: /mywallet set / replace / clear.

Seam test per the house convention: real dispatcher + real Postgres, fake
Telegram transport. The poller and delivery halves live in
tests/test_position_poller.py and tests/test_alert_delivery.py.
"""

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_text

WALLET = "0x" + "ab" * 20
WALLET_MIXED = "0x" + "Ab" * 20  # same wallet, mixed case — stored lowercased
OTHER = "0x" + "cd" * 20


async def linked(pool: asyncpg.Pool, user_id: int) -> str | None:
    return await pool.fetchval("SELECT linked_wallet FROM users WHERE telegram_id = $1", user_id)


async def test_mywallet_links_a_valid_address_lowercased(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, f"/mywallet {WALLET_MIXED}", user_id=101, username="edik")

    assert await linked(pool, 101) == WALLET  # lowercased
    reply = session.sent_messages()[0].text or ""
    assert WALLET[:6] in reply and WALLET[-4:] in reply  # short address confirmed


async def test_mywallet_replaces_a_previously_linked_wallet(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, f"/mywallet {WALLET}", user_id=102)
    await feed_text(dp, bot, f"/mywallet {OTHER}", user_id=102)

    assert await linked(pool, 102) == OTHER  # one wallet per user, re-set replaces


async def test_mywallet_clear_unlinks(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, f"/mywallet {WALLET}", user_id=103)
    await feed_text(dp, bot, "/mywallet clear", user_id=103)

    assert await linked(pool, 103) is None


async def test_mywallet_rejects_a_non_address(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/mywallet notawallet", user_id=104)

    assert await linked(pool, 104) is None
    reply = session.sent_messages()[0].text or ""
    assert "0x" in reply  # usage hint mentions the address shape


async def test_mywallet_no_arg_reports_status(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/mywallet", user_id=105)  # nothing linked yet
    none_reply = session.sent_messages()[-1].text or ""

    await feed_text(dp, bot, f"/mywallet {WALLET}", user_id=105)
    await feed_text(dp, bot, "/mywallet", user_id=105)  # now linked
    status_reply = session.sent_messages()[-1].text or ""

    assert WALLET[:6] not in none_reply  # nothing to show
    assert WALLET[:6] in status_reply  # shows the linked short address


async def test_mywallet_clear_when_nothing_linked_is_graceful(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/mywallet clear", user_id=106)

    assert await linked(pool, 106) is None
    assert len(session.sent_messages()) == 1  # answered, didn't crash


async def test_help_documents_mywallet(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=107)

    assert "/mywallet" in (session.sent_messages()[0].text or "")
