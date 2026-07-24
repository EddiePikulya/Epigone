"""Unfollow memory (issue #99): every unfollow is logged (user, wallet, when, and
the #86 nickname it had then), latest-unfollow-wins per (user, wallet). The log
surfaces as a profile line and a `↩` marker on screener/criteria result rows for
wallets this User followed before and dropped — but not ones currently tracked.

Three seams, real Postgres throughout (the house convention):
- the shared store seam `untrack_address`, exercised directly,
- all three unfollow paths (tracked-list button, profile toggle, positions view)
  over the fake Telegram transport, each asserted to log,
- the surfaces: the profile's previously-followed line and the row marker on the
  screener and criteria results.
"""

from datetime import UTC, datetime

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import track_address, untrack_address
from epigone.bot.names import set_track_name
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text, follow_wallet

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
WHALE_SHORT = "0xaf0f…e92e"
NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


# --- helpers -----------------------------------------------------------------


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


async def _unfollow_row(
    pool: asyncpg.Pool, user_id: int, address: str = WHALE
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT unfollowed_at, name FROM unfollows "
        "WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )


async def add_trader(
    pool: asyncpg.Pool, address: str, *, display_name: str | None = None
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3)
        """,
        address,
        display_name,
        NOW,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', 5000, 0.5, 50000, 10000, $2)
        """,
        address,
        NOW,
    )


def _row_lines_with_marker(text: str) -> list[str]:
    return [line for line in text.splitlines() if "↩" in line]


# --- the store seam, directly ------------------------------------------------


async def test_untrack_records_the_unfollow_with_timestamp_and_name(
    pool: asyncpg.Pool,
) -> None:
    await _seed_track(pool, 7)
    await set_track_name(pool, 7, WHALE, "avax")

    async with pool.acquire() as conn, conn.transaction():
        removed = await untrack_address(conn, 7, WHALE, NOW)

    assert removed is True
    # The Track (and its name, #86) is gone…
    assert await pool.fetchval(
        "SELECT count(*) FROM tracks WHERE user_telegram_id = 7"
    ) == 0
    # …but the log preserves what it was called at that moment.
    row = await _unfollow_row(pool, 7)
    assert row is not None
    assert row["unfollowed_at"] == NOW
    assert row["name"] == "avax"


async def test_untrack_records_null_name_for_an_unnamed_wallet(pool: asyncpg.Pool) -> None:
    await _seed_track(pool, 7)

    async with pool.acquire() as conn, conn.transaction():
        await untrack_address(conn, 7, WHALE, NOW)

    row = await _unfollow_row(pool, 7)
    assert row is not None and row["name"] is None


async def test_untrack_of_an_untracked_wallet_records_nothing(pool: asyncpg.Pool) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES (7)")
    await add_trader(pool, WHALE)  # exists, but this User never tracked it

    async with pool.acquire() as conn, conn.transaction():
        removed = await untrack_address(conn, 7, WHALE, NOW)

    assert removed is False
    assert await _unfollow_row(pool, 7) is None


async def test_refollow_then_unfollow_keeps_only_the_latest_unfollow(
    pool: asyncpg.Pool,
) -> None:
    clock = FakeClock(start=NOW)
    await _seed_track(pool, 7)
    await set_track_name(pool, 7, WHALE, "old name")

    async with pool.acquire() as conn, conn.transaction():
        await untrack_address(conn, 7, WHALE, clock.now())

    # Re-follow (unnamed again, #86), rename, then unfollow later.
    clock.advance(3 * 86400)
    async with pool.acquire() as conn, conn.transaction():
        await track_address(conn, 7, None, WHALE, clock.now())
    await set_track_name(pool, 7, WHALE, "new name")
    clock.advance(86400)
    later = clock.now()
    async with pool.acquire() as conn, conn.transaction():
        await untrack_address(conn, 7, WHALE, later)

    # Exactly one row, reflecting the latest unfollow.
    assert await pool.fetchval(
        "SELECT count(*) FROM unfollows WHERE user_telegram_id = 7"
    ) == 1
    row = await _unfollow_row(pool, 7)
    assert row["unfollowed_at"] == later
    assert row["name"] == "new name"


async def test_the_log_is_per_user(pool: asyncpg.Pool) -> None:
    await _seed_track(pool, 1)
    await _seed_track(pool, 2)

    async with pool.acquire() as conn, conn.transaction():
        await untrack_address(conn, 1, WHALE, NOW)

    assert await _unfollow_row(pool, 1) is not None
    assert await _unfollow_row(pool, 2) is None  # user 2 still tracks, no log


# --- all three unfollow paths log --------------------------------------------


async def test_tracked_list_unfollow_button_logs(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)  # follow via the Follow tap

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _unfollow_row(pool, 111) is not None


async def test_profile_unfollow_toggle_logs(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"punfollow:{WHALE}", user_id=111)

    assert await _unfollow_row(pool, 111) is not None


async def test_positions_view_unfollow_logs(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"posunfollow:{WHALE}", user_id=111)

    assert await _unfollow_row(pool, 111) is not None


# --- the profile line --------------------------------------------------------


async def test_profile_shows_previously_followed_line_after_unfollow(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await add_trader(pool, WHALE)
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await set_track_name(pool, 111, WHALE, "avax")
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)
    clock.advance(3 * 86400)  # three days pass

    await feed_callback(dp, bot, f"profile:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "↩ Previously followed" in text
    assert "unfollowed 3d ago" in text
    assert '(as "avax")' in text


async def test_profile_line_omits_name_when_none_was_set(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await add_trader(pool, WHALE)
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)
    clock.advance(3600)

    await feed_callback(dp, bot, f"profile:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "↩ Previously followed — unfollowed 1h ago" in text
    assert "(as" not in text  # no name clause


async def test_profile_line_absent_while_currently_tracked(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE)
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)
    await follow_wallet(dp, bot, WHALE, user_id=111)  # re-follow

    await feed_callback(dp, bot, f"profile:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Previously followed" not in text


async def test_profile_line_absent_for_a_never_followed_wallet(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE)

    await feed_callback(dp, bot, f"profile:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Previously followed" not in text


# --- the row marker on screener + criteria -----------------------------------


async def test_screener_marks_previously_followed_rows(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE, display_name="Dropped")
    await add_trader(pool, "0xkept", display_name="Kept")
    await follow_wallet(dp, bot, WHALE, user_id=111)  # follow both
    await follow_wallet(dp, bot, "0xkept", user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)  # drop one

    await feed_text(dp, bot, "/screener", user_id=111)

    text = session.sent_messages()[-1].text or ""
    marked = _row_lines_with_marker(text)
    assert any("Dropped" in line for line in marked)  # the dropped row gets ↩
    assert not any("Kept" in line for line in marked)  # the tracked one does not


async def test_screener_marker_gone_once_refollowed(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE, display_name="Dropped")
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)
    await follow_wallet(dp, bot, WHALE, user_id=111)  # re-follow → keeps Following

    await feed_text(dp, bot, "/screener", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "↩" not in text
    # The row still reads as Following on its button.
    markup = session.sent_messages()[-1].reply_markup
    assert markup is not None
    assert any(
        b.text == "✓ Following"
        for row in markup.inline_keyboard
        for b in row
    )


async def test_screener_marker_is_per_user(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE, display_name="Dropped")
    await follow_wallet(dp, bot, WHALE, user_id=1)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=1)

    await feed_text(dp, bot, "/screener", user_id=2)  # a different User

    text = session.sent_messages()[-1].text or ""
    assert "↩" not in text  # user 2 never followed it


async def test_criteria_results_mark_previously_followed_rows(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, WHALE, display_name="Dropped")
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    # Run a filterless draft — every scanned trader qualifies.
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "crun:d:0", user_id=111)

    edits = session.edited_messages()
    text = edits[-1].text or ""
    assert "Dropped" in text
    assert any("Dropped" in line for line in _row_lines_with_marker(text))
