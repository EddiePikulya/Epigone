"""Ticket #3 acceptance: paste a wallet to Follow a Trader, view positions, unfollow."""

from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.telegram import RecordingSession, feed_callback, feed_text

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
WHALE_SHORT = "0xaf0f…e92e"
OTHER = "0x" + "1" * 40
OTHER_SHORT = "0x1111…1111"

ETH_SHORT_POS = Position(
    coin="ETH",
    side=Side.SHORT,
    size_usd=Decimal("2625150"),
    leverage=Decimal("20"),
    entry_price=Decimal("1677.9"),
    unrealized_pnl=Decimal("-108299.96"),
)
SOL_LONG_POS = Position(
    coin="SOL",
    side=Side.LONG,
    size_usd=Decimal("4834732.53"),
    leverage=Decimal("20"),
    entry_price=Decimal("73.2257"),
    unrealized_pnl=Decimal("307999.31"),
)


async def _tracked_addresses(pool: asyncpg.Pool, user_id: int) -> list[str]:
    rows = await pool.fetch(
        "SELECT trader_address FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at",
        user_id,
    )
    return [r["trader_address"] for r in rows]


async def test_pasting_a_valid_address_follows_the_trader(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111, username="edik")

    assert await _tracked_addresses(pool, 111) == [WHALE]
    text = session.sent_messages()[-1].text or ""
    assert "tracking" in text.lower()
    assert WHALE_SHORT in text


async def test_mixed_case_address_is_accepted_and_stored_lowercase(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "0xAF0FDD39E5D92499B0ED9F68693DA99C0EC1E92E", user_id=111)

    assert await _tracked_addresses(pool, 111) == [WHALE]


async def test_refollowing_is_idempotent(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_text(dp, bot, WHALE, user_id=111)

    assert await _tracked_addresses(pool, 111) == [WHALE]
    text = session.sent_messages()[-1].text or ""
    assert "already" in text.lower()
    assert WHALE_SHORT in text


async def test_two_users_can_follow_the_same_trader(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_text(dp, bot, WHALE, user_id=222)

    assert await _tracked_addresses(pool, 111) == [WHALE]
    assert await _tracked_addresses(pool, 222) == [WHALE]


async def test_invalid_input_gets_a_clear_error(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "0x123notanaddress", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    text = session.sent_messages()[-1].text or ""
    assert "address" in text.lower()
    assert "0x" in text  # tells the User what a valid address looks like


async def test_tracked_list_shows_every_trader_with_positions_summary(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS, SOL_LONG_POS])
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_text(dp, bot, OTHER, user_id=111)

    await feed_text(dp, bot, "/tracked", user_id=111)

    listing = session.sent_messages()[-1]
    text = listing.text or ""
    assert WHALE_SHORT in text
    assert OTHER_SHORT in text
    assert "2 positions" in text
    assert "no open positions" in text

    assert listing.reply_markup is not None
    callback_data = [
        button.callback_data
        for row in listing.reply_markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    ]
    assert f"positions:{WHALE}" in callback_data
    assert f"unfollow:{WHALE}" in callback_data
    assert f"positions:{OTHER}" in callback_data
    assert f"unfollow:{OTHER}" in callback_data


async def test_unknown_command_gets_a_clear_error(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/track 0xabc", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    text = session.sent_messages()[-1].text or ""
    assert "/help" in text


async def test_tracked_list_when_tracking_nobody(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/tracked", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "not tracking" in text.lower()
    assert "paste" in text.lower()  # points the User at the way in


async def test_positions_button_shows_current_positions_on_demand(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS, SOL_LONG_POS])
    await feed_text(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert WHALE_SHORT in text
    # coin, side, size, leverage, entry, unrealized PnL — all six alert fields
    assert "ETH" in text and "SOL" in text
    assert "SHORT" in text and "LONG" in text
    assert "$2,625,150" in text
    assert "20x" in text
    assert "1677.9" in text
    assert "-$108,300" in text
    assert "+$307,999" in text


async def test_positions_view_for_a_trader_with_no_open_positions(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert WHALE_SHORT in text
    assert "no open positions" in text.lower()


async def test_positions_button_for_an_untracked_trader_is_refused(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/start", user_id=111)
    sent_before = len(session.sent_messages())

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    assert len(session.sent_messages()) == sent_before  # no positions view leaked
    answers = session.callback_answers()
    assert answers and "not tracking" in (answers[-1].text or "").lower()


async def test_unfollow_button_removes_the_track_and_refreshes_the_list(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_text(dp, bot, OTHER, user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == [OTHER]
    edited = session.edited_messages()[-1]
    text = edited.text or ""
    assert WHALE_SHORT not in text
    assert OTHER_SHORT in text


async def test_unfollowing_the_last_trader_shows_the_empty_state(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    text = session.edited_messages()[-1].text or ""
    assert "not tracking" in text.lower()


async def test_stale_unfollow_tap_does_not_claim_success(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)  # stale button

    answer = session.callback_answers()[-1].text or ""
    assert "unfollowed" not in answer.lower()
    assert "not tracking" in answer.lower() or "weren't tracking" in answer.lower()


async def test_unfollow_only_affects_the_tapping_user(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    await feed_text(dp, bot, WHALE, user_id=222)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    assert await _tracked_addresses(pool, 222) == [WHALE]


async def test_help_mentions_tracking(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "/tracked" in text
    assert "paste" in text.lower()
