"""Starter Criteria presets in the Telegram flow (issue #71): every User sees
the three presets in their /criteria list, can run each with no setup, and can
delete one for themselves — a hide-for-me that survives restarts and leaves
other Users and the User's own saved Criteria untouched."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup

from epigone.bot.handlers import build_router
from epigone.criteria_presets import PRESETS_BY_KEY
from epigone.criteria_store import hidden_preset_keys, save_criteria
from epigone.screener import Criteria, run_criteria
from tests.support.telegram import RecordingSession, feed_callback, feed_text

NOW = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)


async def add_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    roi: str = "0.1",
    pnl: str = "1000",
    account_value: str = "10000",
    window: str = "month",
    display_name: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3) ON CONFLICT (address) DO NOTHING
        """,
        address,
        display_name,
        NOW,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, $2, $3, $4, 50000, $5, $6)
        """,
        address,
        window,
        Decimal(pnl),
        Decimal(roi),
        Decimal(account_value),
        NOW,
    )


async def add_fine(
    pool: asyncpg.Pool,
    address: str,
    *,
    sharpe: str = "3.2",
    trade_count: int = 104,
    avg_leverage: str = "2.5",
) -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, $2, 0.7, 500, 100, $3, 900, $4, 0.7, 22000, $5, $5, $5)
        """,
        address,
        trade_count,
        Decimal(sharpe),
        Decimal(avg_leverage),
        NOW,
    )


async def add_user(pool: asyncpg.Pool, telegram_id: int) -> None:
    await pool.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", telegram_id
    )


def _callback_data(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.callback_data or "" for row in markup.inline_keyboard for b in row]


async def test_a_brand_new_user_sees_the_three_presets_ready_to_run(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # No prior /start, nothing saved: the presets need no backfill.
    await feed_text(dp, bot, "/criteria", user_id=111)

    home = session.sent_messages()[-1]
    text = home.text or ""
    assert "Steady earners" in text
    assert "Careful whales" in text
    assert "Hot hands" in text
    assert "⭐" in text  # visibly marked apart from own criteria
    data = _callback_data(home.reply_markup)
    assert "crun:psteady_earners:0" in data  # runnable straight away
    assert "crun:pcareful_whales:0" in data
    assert "crun:phot_hands:0" in data


async def test_running_a_preset_matches_running_the_same_filters_by_hand(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # A trader that clears Hot hands (week: ROI ≥ 30%, PnL ≥ $25k, ≥ 5 trades)…
    await add_trader(
        pool, "0xhot", roi="0.5", pnl="40000", window="week", display_name="Hotshot"
    )
    await add_fine(pool, "0xhot", trade_count=8)
    # …and one that misses on ROI.
    await add_trader(pool, "0xmeh", roi="0.1", pnl="40000", window="week", display_name="Meh")
    await add_fine(pool, "0xmeh", trade_count=8)

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "crun:phot_hands:0", user_id=111)

    text = session.edited_messages()[-1].text or ""
    assert "Hotshot" in text
    assert "Meh" not in text
    # Identical to running the preset's Criteria straight through the screener.
    by_hand = await run_criteria(pool, PRESETS_BY_KEY["hot_hands"].criteria)
    assert [r.display_name for r in by_hand] == ["Hotshot"]


async def test_deleting_a_preset_hides_it_for_that_user_only_and_permanently(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: object,
    clock: object,
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cdelp:careful_whales", user_id=111)

    home = session.edited_messages()[-1]
    assert "Careful whales" not in (home.text or "")
    assert "Steady earners" in (home.text or "")  # the others stay
    assert "removed" in (session.callback_answers()[-1].text or "").lower()
    assert await hidden_preset_keys(pool, 111) == {"careful_whales"}

    # Another User is unaffected — they still see all three.
    await feed_text(dp, bot, "/criteria", user_id=222)
    assert "Careful whales" in (session.sent_messages()[-1].text or "")

    # And the deletion is durable across a restart (fresh Dispatcher, same DB).
    dp2 = Dispatcher()
    dp2["pool"] = pool
    dp2["gateway"] = gateway
    dp2["clock"] = clock
    dp2["drafts"] = {}
    dp2.include_router(build_router())
    await feed_text(dp2, bot, "/criteria", user_id=111)
    assert "Careful whales" not in (session.sent_messages()[-1].text or "")


async def test_deleting_a_preset_leaves_the_users_own_criteria_untouched(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)
    await save_criteria(pool, 111, "My keeper", Criteria(), NOW)

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cdelp:steady_earners", user_id=111)

    home = session.edited_messages()[-1]
    assert "My keeper" in (home.text or "")  # own criteria survives a preset delete
    assert "Steady earners" not in (home.text or "")


async def test_a_second_delete_of_a_preset_is_harmless(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cdelp:hot_hands", user_id=111)
    await feed_callback(dp, bot, "cdelp:hot_hands", user_id=111)  # e.g. a stale keyboard

    assert await hidden_preset_keys(pool, 111) == {"hot_hands"}
    assert "Hot hands" not in (session.edited_messages()[-1].text or "")


async def test_deleting_every_preset_falls_back_to_the_empty_state(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    for key in ("steady_earners", "careful_whales", "hot_hands"):
        await feed_callback(dp, bot, f"cdelp:{key}", user_id=111)

    home = session.edited_messages()[-1]
    assert "haven't saved any criteria yet" in (home.text or "").lower()


async def test_a_new_criteria_cannot_reuse_a_visible_preset_name(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "csave", user_id=111)

    await feed_text(dp, bot, "Steady earners", user_id=111)  # collides with the preset
    assert "already have" in (session.sent_messages()[-1].text or "").lower()

    # Deleting the preset frees its name — the prompt is still live.
    await feed_callback(dp, bot, "cdelp:steady_earners", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "csave", user_id=111)
    await feed_text(dp, bot, "Steady earners", user_id=111)
    assert "Saved ‘Steady earners’" in (session.sent_messages()[-1].text or "")


async def test_following_straight_from_a_preset_result(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(
        pool, "0xhot", roi="0.5", pnl="40000", window="week", display_name="Hotshot"
    )
    await add_fine(pool, "0xhot", trade_count=8)

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "crun:phot_hands:0", user_id=111)
    results = session.edited_messages()[-1]
    follow = next(d for d in _callback_data(results.reply_markup) if d.startswith("cfw:"))

    await feed_callback(dp, bot, follow, user_id=111)

    tracked = await pool.fetch("SELECT trader_address FROM tracks WHERE user_telegram_id = 111")
    assert [r["trader_address"] for r in tracked] == ["0xhot"]
