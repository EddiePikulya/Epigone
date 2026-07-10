"""Ticket #6 acceptance: /screener runs the default Criteria over the Metric
Library and returns a ranked, paginated page of Traders with key stats and a
Follow button per row; tapping a Trader opens a profile (coarse metrics,
freshness, current positions, follow/unfollow). A screener run is a database
query only — zero Hyperliquid calls (the profile view is the one place that
reaches the gateway, and only on an explicit tap)."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup

from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)

ETH_SHORT_POS = Position(
    coin="ETH",
    side=Side.SHORT,
    size_usd=Decimal("2625150"),
    leverage=Decimal("20"),
    entry_price=Decimal("1677.9"),
    unrealized_pnl=Decimal("-108299.96"),
)


async def add_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    month_roi: str = "0.1",
    month_pnl: str = "1000",
    display_name: str | None = None,
    bot_reason: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at,
                             bot_flagged_at, bot_reason)
        VALUES ($1, $2, $3, $3, $4, $5)
        """,
        address,
        display_name,
        NOW,
        NOW if bot_reason is not None else None,
        bot_reason,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', $2, $3, 50000, 10000, $4)
        """,
        address,
        Decimal(month_pnl),
        Decimal(month_roi),
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


def _callback_data(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.callback_data or "" for row in markup.inline_keyboard for b in row]


def _button_texts(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.text for row in markup.inline_keyboard for b in row]


def _follow_data(markup: InlineKeyboardMarkup | None) -> str:
    return next(d for d in _callback_data(markup) if d.startswith("sfollow:"))


async def _tracked(pool: asyncpg.Pool, user_id: int) -> list[str]:
    rows = await pool.fetch(
        "SELECT trader_address FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at",
        user_id,
    )
    return [r["trader_address"] for r in rows]


async def test_screener_lists_ranked_traders_with_key_stats_and_follow_buttons(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xmid", month_roi="0.5", month_pnl="5000", display_name="Mid")
    await add_trader(pool, "0xbest", month_roi="2.0", month_pnl="30000", display_name="Best")
    await add_fine(pool, "0xbest", win_rate="0.71")
    await add_trader(pool, "0xworst", month_roi="-0.3", month_pnl="-2000", display_name="Worst")

    await feed_text(dp, bot, "/screener", user_id=111)

    msg = session.sent_messages()[-1]
    text = msg.text or ""
    # Ranked best-first with rank numbers.
    assert text.index("Best") < text.index("Mid") < text.index("Worst")
    assert "1." in text and "2." in text and "3." in text
    # Key stats: ROI, PnL, and the fine win rate when available.
    assert "+200%" in text
    assert "+$30,000" in text
    assert "71% win" in text
    # A Follow button and a Profile button for the top row.
    data = _callback_data(msg.reply_markup)
    assert any(d.startswith("sfollow:") and d.endswith("0xbest") for d in data)
    assert "profile:0xbest" in data


async def test_coarse_only_rows_read_as_pending_not_a_verdict(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # A high-ROI trader the fine pass hasn't reached yet: likely strong, not weak.
    await add_trader(pool, "0xnew", month_roi="0.9", month_pnl="9000", display_name="FreshWhale")

    await feed_text(dp, bot, "/screener", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "FreshWhale" in text
    assert "analyzing" in text.lower()  # framed as in-progress…
    assert "coarse" not in text.lower()  # …never as a quality verdict


async def test_screener_excludes_bots(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xhuman", month_roi="0.1", display_name="RealTrader")
    await add_trader(
        pool,
        "0xbot",
        month_roi="99.0",
        display_name="BotMaker",
        bot_reason="100% win rate over 637 exits",
    )

    await feed_text(dp, bot, "/screener", user_id=111)

    msg = session.sent_messages()[-1]
    text = msg.text or ""
    data = _callback_data(msg.reply_markup)
    assert "RealTrader" in text
    assert "BotMaker" not in text  # flagged Bot never reaches a result (issue #8)
    assert "profile:0xbot" not in data


async def test_a_screener_run_makes_zero_gateway_calls(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    for i in range(3):
        await add_trader(pool, f"0x{i:03d}", month_roi=str(i))

    await feed_text(dp, bot, "/screener", user_id=111)

    # The acceptance line: a screener run is a database query only.
    assert gateway.positions_calls == []
    assert gateway.portfolio_calls == []
    assert gateway.fills_calls == []


async def test_screener_paginates(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    for i in range(7):
        await add_trader(pool, f"0x{i:03d}", month_roi=str(i))  # 006 best … 000 worst

    await feed_text(dp, bot, "/screener", user_id=111)

    page1 = session.sent_messages()[-1]
    text1 = page1.text or ""
    assert "0x006" in text1 and "0x002" in text1  # first five
    assert "0x001" not in text1
    data1 = _callback_data(page1.reply_markup)
    assert any(d.startswith("screen:") for d in data1)  # a Next button
    next_data = next(d for d in data1 if d.startswith("screen:"))

    await feed_callback(dp, bot, next_data, user_id=111)

    page2 = session.edited_messages()[-1]
    text2 = page2.text or ""
    assert "0x001" in text2 and "0x000" in text2  # the remaining two
    assert "0x006" not in text2
    assert any(d.startswith("screen:") for d in _callback_data(page2.reply_markup))  # a Prev button


async def test_follow_from_results_feeds_the_track_pipeline(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xstar", month_roi="1.5")

    await feed_text(dp, bot, "/screener", user_id=111)
    await feed_callback(
        dp, bot, _follow_data(session.sent_messages()[-1].reply_markup), user_id=111
    )

    # A follow from results is a Track — exactly what the alert poller reads (#3/#4).
    assert await _tracked(pool, 111) == ["0xstar"]
    answer = session.callback_answers()[-1].text or ""
    assert "following" in answer.lower()
    # The page re-renders in place so the row reflects the new state.
    assert any("Following" in t for t in _button_texts(session.edited_messages()[-1].reply_markup))


async def test_follow_from_results_is_idempotent(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xstar", month_roi="1.5")
    await feed_text(dp, bot, "/screener", user_id=111)
    follow_data = _follow_data(session.sent_messages()[-1].reply_markup)

    await feed_callback(dp, bot, follow_data, user_id=111)
    await feed_callback(dp, bot, follow_data, user_id=111)

    assert await _tracked(pool, 111) == ["0xstar"]
    assert "already" in (session.callback_answers()[-1].text or "").lower()


async def test_screener_empty_state(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/screener", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "no traders" in text.lower() or "no matching" in text.lower()


async def test_profile_from_screener_shows_metrics_freshness_positions_and_follow(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    await add_trader(pool, "0xstar", month_roi="1.5", month_pnl="42000")
    await add_fine(pool, "0xstar", win_rate="0.71")
    gateway.set_positions("0xstar", [ETH_SHORT_POS])
    clock.advance(3 * 3600)  # metrics were computed three hours ago

    await feed_callback(dp, bot, "profile:0xstar", user_id=111)

    msg = session.sent_messages()[-1]
    text = msg.text or ""
    assert "71% win rate over 104 closed trades" in text  # fine metrics
    assert "ETH" in text and "SHORT" in text  # current positions
    assert "3h ago" in text  # metric freshness
    # Not tracked yet: the profile offers a Follow.
    assert "pfollow:0xstar" in _callback_data(msg.reply_markup)


async def test_profile_follow_then_unfollow_toggles_in_place(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    await add_trader(pool, "0xstar", month_roi="1.5")

    await feed_callback(dp, bot, "profile:0xstar", user_id=111)
    await feed_callback(dp, bot, "pfollow:0xstar", user_id=111)

    assert await _tracked(pool, 111) == ["0xstar"]
    edited = session.edited_messages()[-1]
    assert "punfollow:0xstar" in _callback_data(edited.reply_markup)  # now offers Unfollow

    await feed_callback(dp, bot, "punfollow:0xstar", user_id=111)

    assert await _tracked(pool, 111) == []
    assert "pfollow:0xstar" in _callback_data(session.edited_messages()[-1].reply_markup)


async def test_profile_for_a_coarse_only_trader_is_visibly_coarse(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    await add_trader(pool, "0xcoarse", month_roi="0.21", month_pnl="3000000")

    await feed_callback(dp, bot, "profile:0xcoarse", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Coarse metrics only" in text
    assert "updated" in text.lower()  # freshness still shown


async def test_profile_degrades_when_hyperliquid_is_delayed(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    from epigone.gateway import GatewayError

    await add_trader(pool, "0xstar", month_roi="1.5")
    gateway.positions_errors["0xstar"] = GatewayError("info API timed out")
    sent_before = len(session.sent_messages())

    await feed_callback(dp, bot, "profile:0xstar", user_id=111)

    assert len(session.sent_messages()) == sent_before  # no half-rendered profile leaked
    assert "delayed" in (session.callback_answers()[-1].text or "").lower()


async def test_help_mentions_the_screener(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=111)

    assert "/screener" in (session.sent_messages()[-1].text or "")
