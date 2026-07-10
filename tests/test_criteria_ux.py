"""Ticket #7 acceptance: a guided menu flow builds a Criteria — filters
(metric → operator → threshold), a timeframe, a sort — then runs it or saves
it under a name. Saved Criteria can be listed, re-run, edited, and deleted,
and survive restarts. Every metric shows its one-line plain-language
explanation during building; a zero-result run names the strictest filter."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup

from epigone.bot.handlers import build_router
from epigone.criteria import get_criteria, list_criteria, save_criteria
from epigone.gateway import Window
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.metrics.library import METRICS
from epigone.screener import Criteria, Filter, Op
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def add_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    roi: str = "0.1",
    pnl: str = "1000",
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
        VALUES ($1, $2, $3, $4, 50000, 10000, $5)
        """,
        address,
        window,
        Decimal(pnl),
        Decimal(roi),
        NOW,
    )


async def add_fine(pool: asyncpg.Pool, address: str, *, win_rate: str = "0.76") -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, 104, $2, 500, 100, 3.2, 900, 2.5, 0.7, 22000, $3, $3, $3)
        """,
        address,
        Decimal(win_rate),
        NOW,
    )


async def add_user(pool: asyncpg.Pool, telegram_id: int) -> None:
    await pool.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", telegram_id
    )


def _callback_data(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.callback_data or "" for row in markup.inline_keyboard for b in row]


def _button_texts(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.text for row in markup.inline_keyboard for b in row]


async def _build_filter(
    dp: Dispatcher, bot: Bot, *, user_id: int, metric: str, op: str, threshold: str
) -> None:
    """Walk the guided add-filter flow: menu → metric → operator → threshold."""
    await feed_callback(dp, bot, "cfadd", user_id=user_id)
    await feed_callback(dp, bot, f"cfm:{metric}", user_id=user_id)
    await feed_callback(dp, bot, f"cfo:{metric}:{op}", user_id=user_id)
    await feed_text(dp, bot, threshold, user_id=user_id)


async def test_guided_flow_builds_and_runs_a_criteria(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xsharp", roi="0.4", display_name="Sharp")
    await add_fine(pool, "0xsharp", win_rate="0.71")
    await add_trader(pool, "0xfresh", roi="3.0", display_name="Fresh")  # coarse-only

    await feed_text(dp, bot, "/criteria", user_id=111)
    home = session.sent_messages()[-1]
    assert "cnew" in _callback_data(home.reply_markup)

    await feed_callback(dp, bot, "cnew", user_id=111)
    builder = session.edited_messages()[-1]
    assert "no filters yet" in (builder.text or "").lower()

    await feed_callback(dp, bot, "cfadd", user_id=111)
    picker = session.edited_messages()[-1]
    assert "cfm:win_rate" in _callback_data(picker.reply_markup)

    await feed_callback(dp, bot, "cfm:win_rate", user_id=111)
    operators = session.edited_messages()[-1]
    # The metric's one-line explanation appears the moment it is picked.
    assert "share that ended in profit" in (operators.text or "")
    assert "cfo:win_rate:gte" in _callback_data(operators.reply_markup)

    await feed_callback(dp, bot, "cfo:win_rate:gte", user_id=111)
    prompt = session.edited_messages()[-1]
    assert "60 for 60%" in (prompt.text or "")  # the threshold prompt suggests the unit

    await feed_text(dp, bot, "60", user_id=111)
    built = session.sent_messages()[-1]
    assert "Win rate ≥ 60%" in (built.text or "")

    await feed_callback(dp, bot, "crun:d:0", user_id=111)
    results = session.edited_messages()[-1]
    text = results.text or ""
    assert "Sharp" in text
    assert "Fresh" not in text  # coarse-only can't clear a fine filter


async def test_every_metric_explains_itself_during_building(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)

    for key, spec in METRICS.items():
        await feed_callback(dp, bot, "cfadd", user_id=111)
        await feed_callback(dp, bot, f"cfm:{key}", user_id=111)
        assert spec.explanation in (session.edited_messages()[-1].text or "")


async def test_timeframe_and_sort_shape_the_run(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xsmall", pnl="100", roi="0.9", window="week", display_name="Small")
    await add_trader(pool, "0xbig", pnl="5000", roi="0.1", window="week", display_name="Big")

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "cwin", user_id=111)
    await feed_callback(dp, bot, "cw:week", user_id=111)
    await feed_callback(dp, bot, "csort", user_id=111)
    await feed_callback(dp, bot, "csm:pnl", user_id=111)
    direction = session.edited_messages()[-1]
    assert "made or lost over the timeframe" in (direction.text or "")  # explanation again
    await feed_callback(dp, bot, "csd:pnl:a", user_id=111)

    await feed_callback(dp, bot, "crun:d:0", user_id=111)
    text = session.edited_messages()[-1].text or ""
    assert "7d" in text
    assert "PnL, lowest first" in text
    assert text.index("Small") < text.index("Big")  # ascending PnL


async def test_zero_results_name_the_strictest_filter(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    for i in range(3):
        await add_trader(pool, f"0x{i:03d}", pnl="5000")
        await add_fine(pool, f"0x{i:03d}", win_rate="0.6")
    await add_user(pool, 111)
    saved_id = await save_criteria(
        pool,
        111,
        "Impossible",
        Criteria(
            filters=(
                Filter(metric="pnl", op=Op.GTE, threshold=Decimal("1000")),
                Filter(metric="win_rate", op=Op.GTE, threshold=Decimal("0.99")),
            )
        ),
        NOW,
    )

    await feed_callback(dp, bot, f"crun:{saved_id}:0", user_id=111)

    text = session.edited_messages()[-1].text or ""
    assert "no traders match" in text.lower()
    assert "Win rate ≥ 99%" in text  # the strictest filter, by name
    assert "PnL" not in text.split("strictest")[-1]  # …and only that one
    assert "loosen" in text.lower()


async def test_save_then_rerun_after_a_restart(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    await add_trader(pool, "0xwinner", roi="0.5", display_name="Winner")
    await add_trader(pool, "0xflat", roi="0.05", display_name="Flat")

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await _build_filter(dp, bot, user_id=111, metric="roi", op="gte", threshold="20")
    await feed_callback(dp, bot, "csave", user_id=111)
    assert "name" in (session.edited_messages()[-1].text or "").lower()

    await feed_text(dp, bot, "Winners only", user_id=111)
    assert "Winners only" in (session.sent_messages()[-1].text or "")

    # A restart: fresh Dispatcher, fresh in-memory drafts, same database.
    dp2 = Dispatcher()
    dp2["pool"] = pool
    dp2["gateway"] = gateway
    dp2["clock"] = clock
    dp2["drafts"] = {}
    dp2.include_router(build_router())

    await feed_text(dp2, bot, "/criteria", user_id=111)
    home = session.sent_messages()[-1]
    assert "Winners only" in (home.text or "")
    run_data = next(d for d in _callback_data(home.reply_markup) if d.startswith("crun:"))

    await feed_callback(dp2, bot, run_data, user_id=111)
    text = session.edited_messages()[-1].text or ""
    assert "Winner" in text
    assert "Flat" not in text  # ROI 5% misses the 20% floor


async def test_a_user_keeps_multiple_saved_criteria(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)
    await save_criteria(pool, 111, "Scalpers", Criteria(), NOW)
    await save_criteria(pool, 111, "Whales", Criteria(sort_key="account_value"), NOW)

    await feed_text(dp, bot, "/criteria", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Scalpers" in text and "Whales" in text


async def test_edit_changes_contents_and_keeps_the_name(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)
    saved_id = await save_criteria(
        pool,
        111,
        "Mine",
        Criteria(filters=(Filter(metric="roi", op=Op.GTE, threshold=Decimal("0.2")),)),
        NOW,
    )

    await feed_callback(dp, bot, f"cedit:{saved_id}", user_id=111)
    builder = session.edited_messages()[-1]
    assert "ROI ≥ 20%" in (builder.text or "")  # the saved filter is loaded

    await feed_callback(dp, bot, "cwin", user_id=111)
    await feed_callback(dp, bot, "cw:week", user_id=111)
    await feed_callback(dp, bot, "csave", user_id=111)  # editing: saves in place, no name prompt

    answer = session.callback_answers()[-1].text or ""
    assert "saved" in answer.lower()
    loaded = await get_criteria(pool, 111, saved_id)
    assert loaded is not None
    assert loaded.name == "Mine"
    assert loaded.criteria.time_window == Window.WEEK
    assert loaded.criteria.filters == (Filter(metric="roi", op=Op.GTE, threshold=Decimal("0.2")),)


async def test_a_reused_name_never_silently_replaces(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)
    await save_criteria(
        pool,
        111,
        "Mine",
        Criteria(filters=(Filter(metric="roi", op=Op.GTE, threshold=Decimal("0.2")),)),
        NOW,
    )

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "csave", user_id=111)
    await feed_text(dp, bot, "Mine", user_id=111)

    assert "already have" in (session.sent_messages()[-1].text or "").lower()
    saved = await list_criteria(pool, 111)  # the original survives untouched
    assert len(saved) == 1
    assert saved[0].criteria.filters != ()

    await feed_text(dp, bot, "Mine 2", user_id=111)  # the prompt is still live
    assert {s.name for s in await list_criteria(pool, 111)} == {"Mine", "Mine 2"}


async def test_remove_filter_buttons_guard_against_stale_keyboards(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await _build_filter(dp, bot, user_id=111, metric="roi", op="gte", threshold="20")
    await _build_filter(dp, bot, user_id=111, metric="win_rate", op="gte", threshold="60")

    # A keyboard rendered before the list shifted: index 0 described the ROI
    # filter. After it is removed, the same button must not delete win rate.
    await feed_callback(dp, bot, "cfdel:0:roi", user_id=111)
    assert "ROI" not in "".join(
        t for t in _button_texts(session.edited_messages()[-1].reply_markup) if "Remove" in t
    )

    await feed_callback(dp, bot, "cfdel:0:roi", user_id=111)  # stale: index 0 is now win rate

    assert "out of date" in (session.callback_answers()[-1].text or "").lower()
    remove_buttons = [
        t for t in _button_texts(session.edited_messages()[-1].reply_markup) if "Remove" in t
    ]
    assert any("Win rate" in t for t in remove_buttons)  # still there


async def test_delete_removes_a_saved_criteria(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)
    saved_id = await save_criteria(pool, 111, "Mine", Criteria(), NOW)

    await feed_callback(dp, bot, f"cdel:{saved_id}", user_id=111)

    assert "Mine" not in (session.edited_messages()[-1].text or "")
    assert await list_criteria(pool, 111) == []
    assert "deleted" in (session.callback_answers()[-1].text or "").lower()


async def test_follow_straight_from_criteria_results(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xstar", roi="1.5")

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "crun:d:0", user_id=111)
    results = session.edited_messages()[-1]
    follow_data = next(d for d in _callback_data(results.reply_markup) if d.startswith("cfw:"))

    await feed_callback(dp, bot, follow_data, user_id=111)

    tracked = await pool.fetch("SELECT trader_address FROM tracks WHERE user_telegram_id = 111")
    assert [r["trader_address"] for r in tracked] == ["0xstar"]
    assert "following" in (session.callback_answers()[-1].text or "").lower()
    # The page re-renders in place so the row reflects the new state.
    assert any("Following" in t for t in _button_texts(session.edited_messages()[-1].reply_markup))


async def test_unreadable_thresholds_reprompt_and_commands_still_work(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xrich", pnl="50000")

    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "cfadd", user_id=111)
    await feed_callback(dp, bot, "cfm:pnl", user_id=111)
    await feed_callback(dp, bot, "cfo:pnl:gte", user_id=111)

    await feed_text(dp, bot, "lots", user_id=111)
    assert "couldn't read" in (session.sent_messages()[-1].text or "").lower()

    await feed_text(dp, bot, "/help", user_id=111)  # commands cut through a pending prompt
    assert "/criteria" in (session.sent_messages()[-1].text or "")

    await feed_text(dp, bot, "10k", user_id=111)  # the prompt is still live
    assert "PnL ≥ $10,000" in (session.sent_messages()[-1].text or "")


async def test_a_run_of_a_lost_draft_says_so(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_user(pool, 111)

    await feed_callback(dp, bot, "crun:d:0", user_id=111)  # e.g. after a bot restart

    assert "expired" in (session.callback_answers()[-1].text or "").lower()


async def test_help_mentions_the_criteria_builder(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "/criteria" in text
    assert "coming soon" not in text.lower()
