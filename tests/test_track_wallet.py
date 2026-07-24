"""Ticket #3 + #111: paste a wallet to open its profile, tap Follow to track a
Trader, view positions, unfollow. Pasting an address now opens the profile view
(with a Follow/Unfollow toggle) rather than following outright — following is the
deliberate ➕ tap; arrange steps here follow via that same tap (follow_wallet)."""

from datetime import timedelta
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup

from epigone.budget import WeightBudget
from epigone.gateway import GatewayError, Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import run_poll_pass
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text, follow_wallet

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
    margin_used=Decimal("131257.5"),
    return_on_equity=Decimal("-0.8253"),
)
SOL_LONG_POS = Position(
    coin="SOL",
    side=Side.LONG,
    size_usd=Decimal("4834732.53"),
    leverage=Decimal("20"),
    entry_price=Decimal("73.2257"),
    unrealized_pnl=Decimal("307999.31"),
    margin_used=Decimal("241736.63"),
    return_on_equity=Decimal("1.2741"),
)
# A HIP-3 builder-DEX position (issue #21): namespaced coin, from the xyz venue.
XYZ_SP500_POS = Position(
    coin="xyz:SP500",
    side=Side.LONG,
    size_usd=Decimal("120000"),
    leverage=Decimal("5"),
    entry_price=Decimal("5321.4"),
    unrealized_pnl=Decimal("8400"),
    margin_used=Decimal("24000"),
    return_on_equity=Decimal("0.35"),
)


async def _tracked_addresses(pool: asyncpg.Pool, user_id: int) -> list[str]:
    rows = await pool.fetch(
        "SELECT trader_address FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at",
        user_id,
    )
    return [r["trader_address"] for r in rows]


def _callback_data(markup: InlineKeyboardMarkup | None) -> list[str]:
    if markup is None:
        return []
    return [button.callback_data for row in markup.inline_keyboard for button in row]


async def test_pasting_an_untracked_address_opens_its_profile_with_follow(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111, username="edik")

    # #111: pasting opens the profile, it does not write a Track — following is now
    # the deliberate ➕ Follow tap.
    assert await _tracked_addresses(pool, 111) == []
    opened = session.sent_messages()[-1]
    assert WHALE in (opened.text or "")  # full address in the header (#93)
    data = _callback_data(opened.reply_markup)
    assert f"pfollow:{WHALE}" in data  # the Follow tap that actually tracks
    assert f"punfollow:{WHALE}" not in data


async def test_pasting_a_tracked_address_opens_its_profile_with_unfollow(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_text(dp, bot, WHALE, user_id=111)

    # A wallet the User already tracks: the same profile, showing Unfollow — a
    # shortcut to its full view, not an "already tracking" dead-end (#111).
    assert await _tracked_addresses(pool, 111) == [WHALE]
    data = _callback_data(session.sent_messages()[-1].reply_markup)
    assert f"punfollow:{WHALE}" in data
    assert f"pfollow:{WHALE}" not in data


async def test_mixed_case_paste_opens_the_lowercased_profile(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "0xAF0FDD39E5D92499B0ED9F68693DA99C0EC1E92E", user_id=111)

    # The follow callback carries the lowercased address, so a later tap tracks it
    # in canonical form — same normalization the old paste-follow did.
    assert f"pfollow:{WHALE}" in _callback_data(session.sent_messages()[-1].reply_markup)


async def test_tapping_follow_from_a_pasted_profile_tracks_the_trader(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, WHALE, user_id=111)
    assert await _tracked_addresses(pool, 111) == []  # paste alone tracks nothing

    await feed_callback(dp, bot, f"pfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == [WHALE]


async def test_two_users_can_follow_the_same_trader(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await follow_wallet(dp, bot, WHALE, user_id=222)

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
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await follow_wallet(dp, bot, OTHER, user_id=111)

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
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert f"{WHALE} — current positions:" in text  # full address in the header (#93)
    # coin, side, size, leverage, entry, unrealized PnL — all six alert fields
    assert "ETH" in text and "SOL" in text
    assert "SHORT" in text and "LONG" in text
    assert "$2,625,150 notional" in text
    assert "20x" in text
    assert "1677.9" in text
    assert "-$108,300" in text
    assert "+$307,999" in text
    # The real money at risk and its return, not just leveraged size (issue #35).
    assert "$131,258 margin" in text  # marginUsed, exact
    assert "(-83%)" in text  # return on margin makes the leverage legible


async def test_positions_view_for_a_trader_with_no_open_positions(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert f"{WHALE} has no open positions" in text  # full address in the header (#93)
    assert "no open positions" in text.lower()


# --- last-trade recency + recent performance (issue #72) --------------------
#
# The positions view also says when the wallet last traded and how it's been
# doing lately — most useful in the zero-position case, which otherwise gives no
# sense of dormant-vs-resting. Last trade is the fine store's newest folded perp
# fill (window_end, perp-only by construction); PnL/ROI are the coarse month.


async def _seed_last_perp_fill(
    pool: asyncpg.Pool, address: str, *, at: object, computed_at: object
) -> None:
    """The fine store's newest folded perp fill (window_end) and when we last
    scanned this wallet's fills (computed_at) — the only two fields the last-trade
    line reads."""
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, max_drawdown, realized_pnl,
             window_start, window_end, computed_at)
        VALUES ($1, 0, 0, 0, $2, $2, $3)
        """,
        address,
        at,
        computed_at,
    )


async def _seed_coarse_all_time(
    pool: asyncpg.Pool, address: str, *, pnl: Decimal, roi: Decimal, computed_at: object
) -> None:
    # The default wallet view reads the all-time coarse row for its activity line
    # (#104), so that is the window this seeds.
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'allTime', $2, $3, 1000000, 500000, $4)
        """,
        address,
        pnl,
        roi,
        computed_at,
    )


async def test_positions_view_shows_last_trade_and_recent_pnl_when_no_positions(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    # The motivating case: no open positions, but the wallet traded 2h ago and is
    # up all-time — "resting", not "dormant". The default view is all-time (#104).
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    await _seed_last_perp_fill(pool, WHALE, at=now - timedelta(hours=2), computed_at=now)
    await _seed_coarse_all_time(
        pool, WHALE, pnl=Decimal("48000"), roi=Decimal("0.12"), computed_at=now
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "no open positions" in text.lower()
    assert "Last trade: 2h ago" in text
    assert "all-time PnL +$48,000 (ROI +12%)" in text


async def test_positions_view_shows_last_trade_alongside_open_positions(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS])
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    await _seed_last_perp_fill(pool, WHALE, at=now - timedelta(minutes=30), computed_at=now)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "ETH" in text and "SHORT" in text  # positions still render
    assert "Last trade: 30m ago" in text


async def test_positions_view_hedges_last_trade_when_fills_knowledge_is_stale(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    # The last fine refresh was 3 days ago, so we can't imply a live last-trade
    # time — the wallet may have traded since our last scan.
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    stale = now - timedelta(days=3)
    await _seed_last_perp_fill(pool, WHALE, at=stale, computed_at=stale)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Last trade: 3d ago (as of last scan)" in text


async def test_positions_view_says_no_trading_activity_when_no_fills_captured(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    # No fine row at all — say so plainly rather than showing nothing.
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "No recent trading activity seen" in text
    # No fine data at all → no most-played line either (never an empty one).
    assert "Most played" not in text


# --- most-played tickers (issue #80) ----------------------------------------
#
# The positions view names the wallet's most-played coins — round-trips per coin
# over the fill window plus current open exposure. This is the on_positions
# view-assembly path; the profile path is covered in test_screener_ux.py (PR #77's
# lesson: a line added to only one path).


async def _seed_round_trip(
    pool: asyncpg.Pool, address: str, coin: str, *, closed_at: object, seq: int = 0
) -> None:
    """One completed round-trip in the fine store (#58) — the unit the most-played
    ranking counts per coin."""
    await pool.execute(
        """
        INSERT INTO fine_trades
            (address, coin, pnl, peak_notional, opened_at, closed_at, seq)
        VALUES ($1, $2, 100, 10000, $3, $3, $4)
        """,
        address,
        coin,
        closed_at,
        seq,
    )


async def _seed_open_episode(
    pool: asyncpg.Pool, address: str, coin: str, *, opened_at: object, net_position: str
) -> None:
    await pool.execute(
        """
        INSERT INTO fine_open_episodes (address, coin, opened_at, net_position)
        VALUES ($1, $2, $3, $4)
        """,
        address,
        coin,
        opened_at,
        Decimal(net_position),
    )


async def test_positions_view_shows_most_played_tickers(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    for seq in range(3):
        await _seed_round_trip(pool, WHALE, "SOL", closed_at=now - timedelta(hours=seq), seq=seq)
    for seq in range(2):
        await _seed_round_trip(pool, WHALE, "BTC", closed_at=now - timedelta(hours=seq), seq=seq)
    await _seed_round_trip(pool, WHALE, "ETH", closed_at=now - timedelta(hours=1))

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Most played: SOL · BTC · ETH" in text


async def test_positions_view_most_played_counts_a_currently_open_position(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    # A wallet parked in one big BTC short has no completed BTC trips, but the open
    # position makes BTC its coin — it must rank even with zero round-trips.
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    await _seed_round_trip(pool, WHALE, "ETH", closed_at=now - timedelta(hours=1))
    await _seed_open_episode(
        pool, WHALE, "BTC", opened_at=now - timedelta(days=20), net_position="-5"
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Most played: BTC · ETH" in text


async def test_positions_view_most_played_renders_dex_coins_cleanly(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    now = clock.now()
    for seq in range(2):
        await _seed_round_trip(
            pool, WHALE, "xyz:SP500", closed_at=now - timedelta(hours=seq), seq=seq
        )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Most played: SP500" in text
    assert "xyz:SP500" not in text.split("Most played:")[1].splitlines()[0]


async def test_positions_view_shows_holding_time_from_the_poller_snapshots(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    """Age comes from position_snapshots.opened_at (issue #35). A position already
    open at baseline (#4) only knows time-since-tracking, so it reads as an
    at-least age; one the poller saw open reads precisely."""
    gateway.set_positions(WHALE, [ETH_SHORT_POS])
    await follow_wallet(dp, bot, WHALE, user_id=111)

    # First pass baselines ETH: its opened_at is the baseline moment, not a true
    # open — so its age must be presented honestly.
    await run_poll_pass(pool, gateway, WeightBudget(1_000_000, clock), clock)

    # A day and a bit later, SOL opens and the poller sees it open.
    clock.advance(93_600)  # 1d 2h
    gateway.set_positions(WHALE, [ETH_SHORT_POS, SOL_LONG_POS])
    await run_poll_pass(pool, gateway, WeightBudget(1_000_000, clock), clock)

    clock.advance(7_200)  # 2h more before the User looks
    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "open ≥1d 4h" in text  # ETH: baselined → at-least age (tracked 1d 4h)
    assert "open 2h" in text  # SOL: seen opening → precise age


# --- xyz builder DEX coverage (issue #31) -----------------------------------
#
# Every position display must match what the stream poller tracks: core perps
# plus the xyz HIP-3 builder DEX. A wallet's xyz:* positions were invisible in
# the profile even though the poller alerts on them — closing that gap here.


async def test_positions_view_merges_core_and_xyz_venues(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS])
    gateway.set_positions(WHALE, [XYZ_SP500_POS], dex="xyz")
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "ETH" in text and "SHORT" in text  # core, unchanged
    assert "xyz:SP500" in text and "LONG" in text  # the builder-DEX position
    # Every covered venue fetched, core then the builder DEXes — matching the
    # poller's coverage.
    assert gateway.positions_calls[-3:] == [(WHALE, None), (WHALE, "xyz"), (WHALE, "mkts")]


async def test_tracked_list_summary_counts_core_and_xyz(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS, SOL_LONG_POS])
    gateway.set_positions(WHALE, [XYZ_SP500_POS], dex="xyz")
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_text(dp, bot, "/tracked", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "3 positions" in text  # 2 core + 1 xyz, merged


async def test_positions_view_hides_nothing_when_one_venue_fails(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    # Core succeeds, xyz is delayed: showing only the core half would read as a
    # wallet that closed all its xyz positions. Degrade instead of half-render.
    gateway.set_positions(WHALE, [ETH_SHORT_POS])
    gateway.positions_errors_by_dex[(WHALE, "xyz")] = GatewayError("xyz venue delayed")
    await follow_wallet(dp, bot, WHALE, user_id=111)
    sent_before = len(session.sent_messages())

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    assert len(session.sent_messages()) == sent_before  # no half-empty list leaked
    assert "delayed" in (session.callback_answers()[-1].text or "").lower()


async def test_tracked_list_degrades_when_only_the_xyz_venue_fails(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.set_positions(WHALE, [ETH_SHORT_POS])
    gateway.positions_errors_by_dex[(WHALE, "xyz")] = GatewayError("xyz venue delayed")
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_text(dp, bot, "/tracked", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "delayed" in text.lower()
    assert await _tracked_addresses(pool, 111) == [WHALE]  # a data hiccup never loses Tracks


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
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await follow_wallet(dp, bot, OTHER, user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == [OTHER]
    edited = session.edited_messages()[-1]
    text = edited.text or ""
    assert WHALE_SHORT not in text
    assert OTHER_SHORT in text


async def test_unfollowing_the_last_trader_shows_the_empty_state(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    text = session.edited_messages()[-1].text or ""
    assert "not tracking" in text.lower()


async def test_stale_unfollow_tap_does_not_claim_success(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)  # stale button

    answer = session.callback_answers()[-1].text or ""
    assert "unfollowed" not in answer.lower()
    assert "not tracking" in answer.lower() or "weren't tracking" in answer.lower()


async def test_unfollow_only_affects_the_tapping_user(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await follow_wallet(dp, bot, WHALE, user_id=222)

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    assert await _tracked_addresses(pool, 111) == []
    assert await _tracked_addresses(pool, 222) == [WHALE]


async def test_tracked_list_degrades_gracefully_when_hyperliquid_is_delayed(
    dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    gateway.positions_errors[WHALE] = GatewayError("info API timed out")

    await feed_text(dp, bot, "/tracked", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "delayed" in text.lower()
    assert await _tracked_addresses(pool, 111) == [WHALE]  # a data hiccup never loses Tracks


async def test_help_mentions_tracking(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/help", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "/tracked" in text
    assert "paste" in text.lower()
