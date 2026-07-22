"""Per-user wallet names (issue #86).

Three seams, real Postgres throughout (the house convention):
- the pure sanitizer and the DB store (sanitize_name, set_track_name) exercised
  directly,
- the rename flow end to end over the fake Telegram transport — arm from the
  positions view, type a name, clear it, the per-User isolation and the
  unfollow-forgets guarantee,
- the label on the surfaces that render it for the naming User: tracked list,
  positions header, a position alert, the #83 first-data notice.
"""

from datetime import UTC, datetime

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.bot.alerts import deliver_pending
from epigone.bot.first_data_notice import deliver_first_data_notices
from epigone.bot.handlers import track_address
from epigone.bot.names import MAX_NAME_LENGTH, sanitize_name, set_track_name
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text
from tests.test_alert_delivery import queue_alert

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
WHALE_SHORT = "0xaf0f…e92e"


async def _track(dp: Dispatcher, bot: Bot, *, user_id: int = 111, address: str = WHALE) -> None:
    await feed_text(dp, bot, address, user_id=user_id)


async def _name(pool: asyncpg.Pool, user_id: int, address: str = WHALE) -> str | None:
    return await pool.fetchval(
        "SELECT name FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )


# --- the sanitizer, directly -------------------------------------------------


def test_sanitize_keeps_a_plain_name() -> None:
    assert sanitize_name("silver guy") == "silver guy"


def test_sanitize_trims_and_collapses_whitespace() -> None:
    assert sanitize_name("  scalper   whale  ") == "scalper whale"


def test_sanitize_strips_newlines_and_control_chars() -> None:
    # Newlines/tabs collapse to a single space; a control char is dropped.
    assert sanitize_name("silver\n\tguy\x07!") == "silver guy!"


def test_sanitize_keeps_emoji() -> None:
    assert sanitize_name("🐳 whale") == "🐳 whale"


def test_sanitize_empty_or_whitespace_only_is_none() -> None:
    assert sanitize_name("   ") is None
    assert sanitize_name("\n\t") is None
    assert sanitize_name("") is None


def test_sanitize_does_not_truncate() -> None:
    # The cap is the caller's to enforce (reject, not silently cut).
    long = "x" * (MAX_NAME_LENGTH + 5)
    assert sanitize_name(long) == long


# --- the store seam, directly ------------------------------------------------


async def _seed_track(pool: asyncpg.Pool, user_id: int, address: str = WHALE) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", user_id)
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) "
        "VALUES ($1, now(), now()) ON CONFLICT DO NOTHING",
        address,
    )
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)",
        user_id,
        address,
    )


async def test_set_track_name_persists_and_clears(pool: asyncpg.Pool) -> None:
    await _seed_track(pool, 7)

    assert await set_track_name(pool, 7, WHALE, "silver guy") is True
    assert await _name(pool, 7) == "silver guy"

    assert await set_track_name(pool, 7, WHALE, None) is True
    assert await _name(pool, 7) is None


async def test_set_track_name_on_untracked_wallet_changes_nothing(pool: asyncpg.Pool) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES (7)")
    assert await set_track_name(pool, 7, WHALE, "silver guy") is False


async def test_set_track_name_is_scoped_to_the_user(pool: asyncpg.Pool) -> None:
    await _seed_track(pool, 1)
    await _seed_track(pool, 2)

    await set_track_name(pool, 1, WHALE, "mine")

    assert await _name(pool, 1) == "mine"
    assert await _name(pool, 2) is None  # user 2's own Track untouched


# --- the rename flow, end to end ---------------------------------------------


async def test_positions_view_offers_a_rename_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    markup = session.sent_messages()[-1].reply_markup
    assert markup is not None
    data = [b.callback_data for row in markup.inline_keyboard for b in row]
    assert f"rename:{WHALE}" in data


async def test_rename_persists_and_shows_on_the_positions_header(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)

    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)
    prompt = session.sent_messages()[-1].text or ""
    assert WHALE_SHORT in prompt

    await feed_text(dp, bot, "silver guy", user_id=111)

    assert await _name(pool, 111) == "silver guy"
    confirmation = session.sent_messages()[-1].text or ""
    assert f"silver guy ({WHALE_SHORT})" in confirmation

    # The positions header reads name (0xfull…) with the full address (#93).
    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)
    header = session.sent_messages()[-1].text or ""
    assert header.startswith(f"silver guy ({WHALE})")


async def test_tracked_list_shows_the_name(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await set_track_name(pool, 111, WHALE, "silver guy")

    await feed_text(dp, bot, "/tracked", user_id=111)

    listing = session.sent_messages()[-1].text or ""
    assert f"silver guy ({WHALE_SHORT})" in listing


async def test_rename_is_clearable_back_to_the_address(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await set_track_name(pool, 111, WHALE, "silver guy")

    # A named wallet's prompt offers Clear name.
    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)
    kb = session.sent_messages()[-1].reply_markup
    assert kb is not None
    data = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert f"nameclear:{WHALE}" in data

    await feed_callback(dp, bot, f"nameclear:{WHALE}", user_id=111)

    assert await _name(pool, 111) is None
    edited = session.edited_messages()[-1].text or ""
    assert WHALE_SHORT in edited and "cleared" in edited.lower()


async def test_an_overlong_name_is_rejected_and_the_prompt_stays_live(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)

    await feed_text(dp, bot, "x" * (MAX_NAME_LENGTH + 1), user_id=111)
    assert await _name(pool, 111) is None  # nothing stored
    assert "too long" in (session.sent_messages()[-1].text or "").lower()

    await feed_text(dp, bot, "ok name", user_id=111)  # prompt still armed, retry works
    assert await _name(pool, 111) == "ok name"


async def test_a_whitespace_only_name_is_rejected(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)

    await feed_text(dp, bot, "   ", user_id=111)

    assert await _name(pool, 111) is None
    assert "empty" in (session.sent_messages()[-1].text or "").lower()


async def test_cancel_drops_a_pending_rename_prompt(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)

    await feed_callback(dp, bot, "namecancel", user_id=111)

    # After cancel a typed name is no longer consumed — it falls through to the
    # normal handlers and sets nothing.
    await feed_text(dp, bot, "silver guy", user_id=111)
    assert await _name(pool, 111) is None


async def test_rename_for_an_untracked_trader_is_refused(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=111)  # tracking nobody

    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)

    answers = session.callback_answers()
    assert answers and "not tracking" in (answers[-1].text or "").lower()


async def test_commands_cut_through_a_pending_rename_prompt(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot)
    await feed_callback(dp, bot, f"rename:{WHALE}", user_id=111)

    await feed_text(dp, bot, "/help", user_id=111)  # a command, not a name

    assert "/tracked" in (session.sent_messages()[-1].text or "")  # /help answered
    assert await _name(pool, 111) is None


# --- per-user isolation + unfollow forgets -----------------------------------


async def test_names_never_leak_between_users(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _track(dp, bot, user_id=1)
    await _track(dp, bot, user_id=2)
    await set_track_name(pool, 1, WHALE, "silver guy")

    await feed_text(dp, bot, "/tracked", user_id=2)

    listing = session.sent_messages()[-1].text or ""
    assert "silver guy" not in listing  # user 2 sees the bare address
    assert WHALE_SHORT in listing


async def test_unfollow_forgets_the_name(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await _track(dp, bot)
    await set_track_name(pool, 111, WHALE, "silver guy")

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)  # drop the Track
    assert await _name(pool, 111) is None  # row gone → name gone

    # A refollow starts unnamed.
    async with pool.acquire() as conn, conn.transaction():
        await track_address(conn, 111, None, WHALE, clock.now())
    assert await _name(pool, 111) is None


# --- the label on alerts + the first-data notice -----------------------------


async def test_a_position_alert_uses_the_recipients_name(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(pool, user_id=42, address=WHALE, display_name="Ansem", kind="open")
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address, name) VALUES (42, $1, 'silver guy')",
        WHALE,
    )

    assert await deliver_pending(pool, bot, clock) == 1

    (message,) = session.sent_messages()
    # The recipient's own name wins over the leaderboard label, address alongside.
    assert f"silver guy ({WHALE_SHORT})" in message.text
    assert "Ansem" not in message.text


async def test_an_alert_without_a_name_falls_back_to_the_leaderboard_label(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(pool, user_id=42, address=WHALE, display_name="Ansem", kind="open")
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address) VALUES (42, $1)", WHALE
    )

    assert await deliver_pending(pool, bot, clock) == 1

    (message,) = session.sent_messages()
    assert f"Ansem ({WHALE_SHORT})" in message.text


async def test_first_data_notice_uses_the_recipients_name(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    await pool.execute("INSERT INTO users (telegram_id) VALUES (42)")
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        WHALE,
        now,
    )
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address, name) VALUES (42, $1, 'silver guy')",
        WHALE,
    )
    await pool.execute(
        "INSERT INTO first_data_notices (user_telegram_id, trader_address, status, created_at) "
        "VALUES (42, $1, 'ready', $2)",
        WHALE,
        now,
    )

    assert await deliver_first_data_notices(pool, bot, clock) == 1

    (message,) = session.sent_messages()
    assert f"silver guy ({WHALE_SHORT})" in message.text
    assert "full track-record data" in message.text
