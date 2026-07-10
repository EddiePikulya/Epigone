"""Ticket #10 UX: mute/unmute and set a minimum position size from /tracked.

Full-stack bot flow (fake Telegram transport + real Postgres): tap the buttons,
type the amounts, assert on what the User sees and what lands in `tracks` /
`users`. The suppression these controls drive is covered at the poll seam in
tests/test_alert_controls.py.
"""

from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_callback, feed_text

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
WHALE_SHORT = "0xaf0f…e92e"


async def _track(pool: asyncpg.Pool, dp: Dispatcher, bot: Bot, user_id: int = 111) -> None:
    await feed_text(dp, bot, WHALE, user_id=user_id)


async def _track_row(pool: asyncpg.Pool, user_id: int = 111) -> asyncpg.Record:
    row = await pool.fetchrow(
        """
        SELECT muted, min_size_usd FROM tracks
        WHERE user_telegram_id = $1 AND trader_address = $2
        """,
        user_id,
        WHALE,
    )
    assert row is not None
    return row


async def test_tracked_list_shows_mute_and_min_size_controls(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)

    await feed_text(dp, bot, "/tracked", user_id=111)

    listing = session.sent_messages()[-1]
    text = listing.text or ""
    assert "alerts on" in text  # default state, visible
    assert "no min size" in text
    assert listing.reply_markup is not None
    data = [b.callback_data for row in listing.reply_markup.inline_keyboard for b in row]
    assert f"mute:{WHALE}" in data
    assert f"tmin:{WHALE}" in data
    assert "gmin" in data  # the global-floor entry point


async def test_mute_button_mutes_without_unfollowing(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)

    await feed_callback(dp, bot, f"mute:{WHALE}", user_id=111)

    assert (await _track_row(pool))["muted"] is True
    # Still tracking — mute never drops the Track.
    assert await pool.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = 111 AND trader_address = $1", WHALE
    )
    edited = session.edited_messages()[-1].text or ""
    assert "muted" in edited  # the list redraws with the new state
    answer = session.callback_answers()[-1].text or ""
    assert "muted" in answer.lower()


async def test_unmute_button_restores_alerts(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, f"mute:{WHALE}", user_id=111)

    await feed_callback(dp, bot, f"unmute:{WHALE}", user_id=111)

    assert (await _track_row(pool))["muted"] is False
    assert "alerts on" in (session.edited_messages()[-1].text or "")


async def test_setting_a_per_track_min_size_persists_and_confirms(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)

    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)
    prompt = session.sent_messages()[-1].text or ""
    assert "minimum position size" in prompt.lower()
    assert WHALE_SHORT in prompt

    await feed_text(dp, bot, "$5,000", user_id=111)  # accepts $ and commas

    assert (await _track_row(pool))["min_size_usd"] == Decimal("5000")
    confirmation = session.sent_messages()[-1].text or ""
    assert "5,000" in confirmation
    assert "min $5,000" in confirmation  # the refreshed list reflects the floor


async def test_clearing_a_per_track_min_size_falls_back_to_global(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)
    await feed_text(dp, bot, "5000", user_id=111)

    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)
    await feed_text(dp, bot, "0", user_id=111)  # 0 clears it

    assert (await _track_row(pool))["min_size_usd"] is None
    assert "global" in (session.sent_messages()[-1].text or "").lower()


async def test_setting_a_global_min_size_persists(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)

    await feed_callback(dp, bot, "gmin", user_id=111)
    prompt = session.sent_messages()[-1].text or ""
    assert "global" in prompt.lower()

    await feed_text(dp, bot, "10000", user_id=111)

    stored = await pool.fetchval("SELECT min_size_usd FROM users WHERE telegram_id = 111")
    assert stored == Decimal("10000")
    listing = session.sent_messages()[-1]
    assert "set to $10,000" in (listing.text or "")  # the confirmation
    assert listing.reply_markup is not None
    labels = [b.text for row in listing.reply_markup.inline_keyboard for b in row]
    assert "⚙️ Global min: $10,000" in labels  # the entry-point button reflects it


async def test_global_min_shows_as_the_effective_floor_when_no_track_override(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, "gmin", user_id=111)
    await feed_text(dp, bot, "10000", user_id=111)

    await feed_text(dp, bot, "/tracked", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "min $10,000 (global)" in text


async def test_a_bad_min_size_is_rejected_and_the_prompt_stays_live(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)

    await feed_text(dp, bot, "biggish", user_id=111)

    assert (await _track_row(pool))["min_size_usd"] is None  # nothing stored
    assert "couldn't read" in (session.sent_messages()[-1].text or "").lower()

    await feed_text(dp, bot, "2500", user_id=111)  # prompt still armed, retry works
    assert (await _track_row(pool))["min_size_usd"] == Decimal("2500")


async def test_cancel_drops_a_pending_min_size_prompt(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)

    await feed_callback(dp, bot, "mincancel", user_id=111)

    # After cancel, a typed number is no longer consumed as a floor — it falls
    # through to the normal input handlers instead of setting anything.
    await feed_text(dp, bot, "5000", user_id=111)
    assert (await _track_row(pool))["min_size_usd"] is None


async def test_min_size_prompt_for_an_untracked_trader_is_refused(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=111)  # a User tracking nobody

    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)

    answers = session.callback_answers()
    assert answers and "not tracking" in (answers[-1].text or "").lower()


async def test_commands_cut_through_a_pending_min_size_prompt(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(pool, dp, bot)
    await feed_callback(dp, bot, f"tmin:{WHALE}", user_id=111)

    await feed_text(dp, bot, "/help", user_id=111)  # not a floor amount

    assert "/tracked" in (session.sent_messages()[-1].text or "")  # /help answered
    assert (await _track_row(pool))["min_size_usd"] is None
