"""Issue #8 acceptance: the profile view (📊 button) exposes fine metrics
where available and visibly distinguishes coarse-only Traders."""

from datetime import UTC, datetime

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
        VALUES ($1, 'month', 3000000, 0.21, 90000000, 13400000, $2)
        """,
        address,
        NOW,
    )


async def add_fine(pool: asyncpg.Pool, address: str = WHALE) -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, 161, 0.708, 3951, 841, 12.97, 13893, 2.5, 0.94, 531967, $2, $2, $2)
        """,
        address,
        NOW,
    )


async def test_profile_shows_fine_metrics_when_available(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse_month(pool)
    await add_fine(pool)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "71% win rate over 161 closed trades" in text
    assert "avg win $3,951 · avg loss $841" in text
    assert "Sharpe 13.0 · max drawdown $13,893" in text
    assert "94% maker · ~2.5x leverage" in text
    assert "coarse" not in text.lower()


async def test_a_coarse_only_trader_is_visibly_coarse_only(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow(dp, bot)
    await add_coarse_month(pool)

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
