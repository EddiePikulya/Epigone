"""Issue #73: a one-tap 🗑 delete button on the bot's informational/terminal
messages, whose callback deletes the message it rides on.

Seam tests in the house style — aiogram fake transport + real Postgres. Covers:
the button is appended (never replaces) to alerts, screener results, positions
views, the tracked list, help/command replies, criteria results, and the
monitor's DMs; tapping it deletes the message; a >48h message answers gracefully;
and the criteria-builder flow prompts stay button-free.
"""

from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import DeleteMessage, TelegramMethod
from aiogram.methods.base import TelegramType
from aiogram.types import InlineKeyboardMarkup

from epigone.bot.alerts import deliver_pending
from epigone.bot.delete import DELETE_CALLBACK, DELETE_TOO_OLD_TOAST
from epigone.monitor.main import run_monitor_cycle
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text, make_bot
from tests.test_alert_delivery import queue_alert
from tests.test_monitor_loop import ADMIN_ID, _add_trader, _config, _monitor
from tests.test_screener_ux import add_trader

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


def _rows(markup: InlineKeyboardMarkup | None) -> list[list[str]]:
    assert markup is not None
    return [[b.callback_data or "" for b in row] for row in markup.inline_keyboard]


def _has_delete_row(markup: InlineKeyboardMarkup | None) -> bool:
    return _rows(markup)[-1] == [DELETE_CALLBACK]


async def track(pool: asyncpg.Pool, user_id: int, address: str) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", user_id)
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        address,
        NOW,
    )
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)", user_id, address
    )


# --- the button is appended to informational replies ---


async def test_help_reply_carries_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=111)

    (sent,) = session.sent_messages()
    assert _has_delete_row(sent.reply_markup)


async def test_start_reply_carries_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=111)

    (sent,) = session.sent_messages()
    assert _has_delete_row(sent.reply_markup)


async def test_unrecognized_input_reply_carries_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "just chatting", user_id=111)

    (sent,) = session.sent_messages()
    assert _has_delete_row(sent.reply_markup)


async def test_tracked_list_appends_delete_below_existing_controls(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await track(pool, 111, "0xaaa")

    await feed_text(dp, bot, "/tracked", user_id=111)

    (sent,) = session.sent_messages()
    rows = _rows(sent.reply_markup)
    # Existing controls survive untouched: the positions/mute and min/unfollow
    # rows plus the global-min row are all still there, delete tacked on last.
    assert ["positions:0xaaa", "mute:0xaaa"] in rows
    assert ["tmin:0xaaa", "unfollow:0xaaa"] in rows
    assert rows[-1] == [DELETE_CALLBACK]


async def test_empty_tracked_list_still_carries_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/tracked", user_id=111)

    (sent,) = session.sent_messages()
    assert _has_delete_row(sent.reply_markup)


async def test_screener_results_carry_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xaaa", month_roi="0.5", month_pnl="9000")

    await feed_text(dp, bot, "/screener", user_id=111)

    (sent,) = session.sent_messages()
    assert _has_delete_row(sent.reply_markup)


async def test_screener_delete_button_survives_paging(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    for i in range(7):  # more than one page
        await add_trader(pool, f"0x{i:03d}", month_roi=f"0.{9 - i}", month_pnl="9000")

    await feed_text(dp, bot, "/screener", user_id=111)
    (page,) = session.sent_messages()
    next_offset = next(
        b.callback_data for row in page.reply_markup.inline_keyboard for b in row
        if (b.callback_data or "").startswith("screen:")
    )
    await feed_callback(dp, bot, next_offset, user_id=111)

    (edited,) = session.edited_messages()
    assert _has_delete_row(edited.reply_markup)


async def test_alert_appends_delete_below_the_positions_button(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address)

    await deliver_pending(pool, bot, clock)

    (sent,) = session.sent_messages()
    rows = _rows(sent.reply_markup)
    assert rows[0] == [f"positions:{address}"]  # tap-through survives
    assert rows[-1] == [DELETE_CALLBACK]


async def test_monitor_dm_carries_a_delete_button(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # A wedged ingest → one admin DM (see test_monitor_loop).
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(days=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )

    await run_monitor_cycle(
        pool, bot, ADMIN_ID, _monitor(), _config(), clock, _DiskProbe()
    )

    (sent,) = session.sent_messages()
    assert sent.chat_id == ADMIN_ID
    assert _has_delete_row(sent.reply_markup)


class _DiskProbe:
    def percent_used(self) -> float | None:
        return 47.0


# --- tapping the button deletes the message ---


async def test_tapping_delete_removes_the_message(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_callback(dp, bot, DELETE_CALLBACK, user_id=111, message_id=77)

    (deleted,) = session.deleted_messages()
    assert deleted.message_id == 77
    assert deleted.chat_id == 111
    # Answered with no toast — a clean, silent removal.
    (answer,) = session.callback_answers()
    assert answer.text is None


class _DeleteRefusingSession(RecordingSession):
    """Telegram refusing to delete a >48h-old message, as a 400 Bad Request."""

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[TelegramType],
        timeout: int | None = None,
    ) -> TelegramType:
        if isinstance(method, DeleteMessage):
            raise TelegramBadRequest(method=method, message="message can't be deleted")
        return cast(TelegramType, await super().make_request(bot, method, timeout))


async def test_a_tap_on_a_too_old_message_answers_gracefully(pool: asyncpg.Pool) -> None:
    session = _DeleteRefusingSession()
    bot = make_bot(session)
    dp = _dispatcher(pool, bot)

    await feed_callback(dp, bot, DELETE_CALLBACK, user_id=111, message_id=1)

    assert session.deleted_messages() == []  # the refusal is swallowed
    (answer,) = session.callback_answers()
    assert answer.text == DELETE_TOO_OLD_TOAST
    assert answer.show_alert is True
    await bot.session.close()


def _dispatcher(pool: asyncpg.Pool, bot: Bot) -> Dispatcher:
    from epigone.bot.handlers import build_router
    from epigone.gateway.fake import FakeHyperliquidGateway

    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher["gateway"] = FakeHyperliquidGateway()
    dispatcher["clock"] = FakeClock()
    dispatcher["admin_telegram_id"] = None
    dispatcher["drafts"] = {}
    dispatcher["min_size_pending"] = {}
    dispatcher.include_router(build_router())
    return dispatcher


# --- the criteria builder flow stays button-free ---


async def test_criteria_builder_home_has_no_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)

    (sent,) = session.sent_messages()
    assert not _has_delete_row(sent.reply_markup)


async def test_criteria_add_filter_prompt_has_no_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "cfadd", user_id=111)

    for edited in session.edited_messages():
        assert not _has_delete_row(edited.reply_markup)


async def test_criteria_results_carry_a_delete_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xaaa", month_roi="0.5", month_pnl="9000")
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "crun:d:0", user_id=111)  # run the empty draft

    results = session.edited_messages()[-1]  # cnew drew the builder, crun the results
    assert _has_delete_row(results.reply_markup)
