"""Issue #8 acceptance: the profile view (📊 button) exposes fine metrics
where available and visibly distinguishes coarse-only Traders."""

from datetime import UTC, datetime

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_callback, follow_wallet

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def follow(dp: Dispatcher, bot: Bot, user_id: int = 111) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=user_id)


async def add_coarse(pool: asyncpg.Pool, address: str = WHALE) -> None:
    """Seed the coarse rows the views read: the all-time row the default view's
    activity line shows (#104) and the month row the track record's coarse-only
    path reads. One leaderboard entry ships every window, so they carry the same
    account value (#85)."""
    for window in ("month", "allTime"):
        await pool.execute(
            """
            INSERT INTO coarse_metrics
                (address, time_window, pnl, roi, volume, account_value, computed_at)
            VALUES ($1, $2, 3000000, 0.21, 90000000, 13400000, $3)
            """,
            address,
            window,
            NOW,
        )


async def add_fine(pool: asyncpg.Pool, address: str = WHALE) -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, avg_hold_seconds, median_trade, profit_factor,
             top_trade_share, realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, 161, 0.708, 3951, 841, 12.97, 13893, 2.5, 0.94, 187200, 210, 2.4, 0.18,
                531967, $2, $2, $2)
        """,
        address,
        NOW,
    )


async def test_profile_shows_fine_metrics_when_available(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse(pool)
    await add_fine(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "71% win rate over 161 closed trades" in text
    assert "avg win $3,951 · avg loss $841" in text
    assert "Sharpe 13.0 · max drawdown $13,893" in text
    # Sizing language, not exchange leverage — positions already show "at 25x" (#85).
    assert "94% maker · avg size ~2.5x of account" in text
    # The anti-deception trio line (#113), trip-derived: median trade, profit
    # factor, top-trade share (a stored fraction rendered as a percent).
    assert "median trade $210 · PF 2.4 · top trade 18%" in text
    assert "leverage" not in text.lower()
    assert "⏱ Avg hold: 2d 4h" in text
    # Account value appears on the #72 activity line as the denominator (#85).
    assert "account $13.4M" in text
    assert "coarse" not in text.lower()


async def test_profile_omits_holding_time_when_absent(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # A trader with too little history for a completed episode shows no hold line
    # at all — never a misleading 0 (docs/metrics.md NULL convention).
    await follow(dp, bot)
    await add_fine(pool)
    await pool.execute("UPDATE fine_metrics SET avg_hold_seconds = NULL WHERE address = $1", WHALE)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Avg hold" not in text


async def test_a_coarse_only_trader_is_visibly_coarse_only(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Coarse metrics only" in text
    assert "30d PnL +$3,000,000 · ROI 21%" in text
    assert "win rate" not in text.lower()


async def test_an_unscanned_trader_says_so(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "No metrics yet" in text


async def test_the_positions_view_offers_an_unfollow_button(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    markup = session.sent_messages()[-1].reply_markup
    assert markup is not None
    buttons = [b for row in markup.inline_keyboard for b in row]
    assert any(b.callback_data == f"posunfollow:{WHALE}" for b in buttons)


async def test_unfollow_from_the_positions_view_drops_the_track_in_place(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    assert await pool.fetchval("SELECT count(*) FROM tracks WHERE trader_address = $1", WHALE) == 1

    await feed_callback(dp, bot, f"posunfollow:{WHALE}", user_id=111)

    # the Track is gone, confirmed in place (button removed) and via the toast
    assert await pool.fetchval("SELECT count(*) FROM tracks WHERE trader_address = $1", WHALE) == 0
    edited = session.edited_messages()[-1]
    assert "Unfollowed" in (edited.text or "")
    assert edited.reply_markup is None
    assert "Unfollowed" in (session.callback_answers()[-1].text or "")


async def test_a_tracked_bot_carries_its_flag(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_fine(pool)
    await pool.execute(
        "UPDATE traders SET bot_flagged_at = $2, bot_reason = $1 WHERE address = $3",
        "100% win rate over 637 exits",
        NOW,
        WHALE,
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "market-maker bot" in text.lower()
    assert "100% win rate over 637 exits" in text
