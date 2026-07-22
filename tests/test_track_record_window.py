"""Issue #102: the track record is windowable in place. The wallet views open
on the all-time record (unchanged, #101 span header), and a keyboard toggle row
(7d · 30d · All) re-renders the record over the round-trips closed inside the
chosen window — trip-derived stats reduced from fine_trades via the shared
engine reducer, the whole-history accumulator lines (maker share) omitted."""

from datetime import UTC, datetime, timedelta

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_callback, feed_text

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def follow(dp: Dispatcher, bot: Bot, user_id: int = 111) -> None:
    await feed_text(dp, bot, WHALE, user_id=user_id)


async def add_coarse_month(pool: asyncpg.Pool, address: str = WHALE) -> None:
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', 3000000, 0.21, 90000000, 1000, $2)
        """,
        address,
        NOW,
    )


async def add_fine(pool: asyncpg.Pool, address: str = WHALE) -> None:
    """The all-time fine_metrics row the default (All) view reads — carries the
    accumulator readings (maker share) a window cannot reconstruct."""
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, avg_hold_seconds, realized_pnl,
             window_start, window_end, computed_at)
        VALUES ($1, 3, 0.667, 70, 50, 1.5, 50, 2.5, 0.94, 3600, 90, $2, $2, $2)
        """,
        address,
        NOW,
    )


async def _trade(
    pool: asyncpg.Pool,
    coin: str,
    pnl: str,
    *,
    closed_days_ago: float,
    peak: str = "0",
    hold_hours: float = 1,
    address: str = WHALE,
) -> None:
    closed_at = NOW - timedelta(days=closed_days_ago)
    opened_at = closed_at - timedelta(hours=hold_hours)
    await pool.execute(
        """
        INSERT INTO fine_trades (address, coin, pnl, peak_notional, opened_at, closed_at, seq)
        VALUES ($1, $2, $3, $4, $5, $6, 0)
        """,
        address,
        coin,
        pnl,
        peak,
        opened_at,
        closed_at,
    )


async def _spanning_trades(pool: asyncpg.Pool, address: str = WHALE) -> None:
    """Three round-trips: one inside 7d, one inside 30d only, one beyond both."""
    await _trade(pool, "HYPE", "100", closed_days_ago=3, peak="2500", address=address)  # in 7d
    await _trade(pool, "SOL", "-50", closed_days_ago=20, peak="1000", address=address)  # in 30d
    await _trade(pool, "BTC", "40", closed_days_ago=50, peak="500", address=address)  # beyond both


def _button_data(session: RecordingSession, *, edited: bool = False) -> list[str]:
    message = (session.edited_messages() if edited else session.sent_messages())[-1]
    markup = message.reply_markup
    assert markup is not None
    return [b.callback_data or "" for row in markup.inline_keyboard for b in row]


# --- The default view is unchanged --------------------------------------------


async def test_the_default_positions_view_is_all_time_with_the_span_header(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    # #101 span header (oldest trade 50 days back) and the accumulator line stay.
    assert "Track record (trades from the last 50 days):" in text
    assert "94% maker" in text


async def test_opening_a_wallet_shows_the_window_toggle_row(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    data = _button_data(session)
    assert f"poswin:7d:{WHALE}" in data
    assert f"poswin:30d:{WHALE}" in data
    assert f"poswin:all:{WHALE}" in data
    # Every pre-existing button survives alongside the new row.
    assert f"rename:{WHALE}" in data
    assert f"posunfollow:{WHALE}" in data


# --- Button availability rules ------------------------------------------------


async def test_a_short_history_gets_no_window_buttons(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # Five days of history: a 7d or 30d window would equal All, so neither shows
    # (and with no window button, All is not shown alone either).
    await follow(dp, bot)
    await add_fine(pool)
    await _trade(pool, "HYPE", "100", closed_days_ago=2)
    await _trade(pool, "SOL", "40", closed_days_ago=5)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    data = _button_data(session)
    assert not any(d.startswith("poswin:") for d in data)


async def test_no_trades_this_week_hides_only_the_7d_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await _trade(pool, "SOL", "-50", closed_days_ago=20)  # in 30d, not in 7d
    await _trade(pool, "BTC", "40", closed_days_ago=50)  # beyond both

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    data = _button_data(session)
    assert f"poswin:7d:{WHALE}" not in data
    assert f"poswin:30d:{WHALE}" in data
    assert f"poswin:all:{WHALE}" in data


# --- Tapping a window edits in place ------------------------------------------


async def test_tapping_7d_reduces_the_record_over_that_window(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse_month(pool)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"poswin:7d:{WHALE}", user_id=111)

    edited = session.edited_messages()[-1]
    text = edited.text or ""
    # Header names the window; only the single in-7d trip (a win) is reduced.
    assert "Track record (trades from the last 7 days):" in text
    assert "100% win rate over 1 closed trades" in text
    # The accumulator line is omitted, but the trip-derived avg size survives.
    assert "maker" not in text
    assert "avg size ~2.5x of account" in text
    # Every button, including the toggle row, survives the in-place edit.
    data = _button_data(session, edited=True)
    assert f"rename:{WHALE}" in data
    assert f"posunfollow:{WHALE}" in data
    assert f"poswin:all:{WHALE}" in data


async def test_tapping_all_returns_to_the_all_time_record(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"poswin:all:{WHALE}", user_id=111)

    text = session.edited_messages()[-1].text or ""
    assert "Track record (trades from the last 50 days):" in text
    assert "94% maker" in text  # accumulator line is back


# --- The profile path carries the same toggle ---------------------------------


async def test_the_profile_view_carries_the_window_toggle(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"profile:{WHALE}", user_id=111)

    data = _button_data(session)
    assert f"profwin:7d:{WHALE}" in data
    assert f"profwin:30d:{WHALE}" in data
    assert f"profwin:all:{WHALE}" in data


async def test_tapping_a_window_on_the_profile_edits_in_place_keeping_its_buttons(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse_month(pool)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"profwin:30d:{WHALE}", user_id=111)

    edited = session.edited_messages()[-1]
    text = edited.text or ""
    assert "Track record (trades from the last 30 days):" in text
    # Two trips inside 30d (one win, one loss) → 50% over 2.
    assert "50% win rate over 2 closed trades" in text
    assert "maker" not in text
    data = _button_data(session, edited=True)
    # A followed wallet's profile keeps rename + unfollow through the edit.
    assert f"rename:{WHALE}" in data
    assert f"punfollow:{WHALE}" in data
    assert f"profwin:all:{WHALE}" in data


async def test_the_profile_header_copy_entity_survives_a_window_edit(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # The #93 tap-to-copy header entity must ride the in-place edit.
    await follow(dp, bot)
    await add_fine(pool)
    await _spanning_trades(pool)

    await feed_callback(dp, bot, f"profwin:7d:{WHALE}", user_id=111)

    edited = session.edited_messages()[-1]
    assert edited.entities is not None
    assert any(e.type == "code" for e in edited.entities)
