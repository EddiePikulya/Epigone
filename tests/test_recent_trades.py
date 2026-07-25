"""Issue #116: the wallet views' Recent trades section — the last 5 completed
round-trips, newest first, with dates/PnL/peak size and, where recorded,
entry/exit VWAPs. Rendered on both view-assembly paths (the follower's
positions view and the screener/paste profile), after the track record;
wallets with no stored trips omit the section entirely. Deliberately constant
across the #102 window toggle (see _recent_trades_section)."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher

from tests.support.telegram import RecordingSession, feed_callback, feed_text, follow_wallet

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)


async def add_trader(pool: asyncpg.Pool, address: str = WHALE) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT DO NOTHING
        """,
        address,
        NOW,
    )


async def add_trip(
    pool: asyncpg.Pool,
    *,
    coin: str = "SOL",
    pnl: str = "1240",
    peak: str = "39000",
    opened: datetime,
    closed: datetime,
    seq: int = 0,
    entry_vwap: str | None = None,
    exit_vwap: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO fine_trades
            (address, coin, pnl, peak_notional, opened_at, closed_at, seq, entry_vwap, exit_vwap)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        WHALE,
        coin,
        Decimal(pnl),
        Decimal(peak),
        opened,
        closed,
        seq,
        Decimal(entry_vwap) if entry_vwap is not None else None,
        Decimal(exit_vwap) if exit_vwap is not None else None,
    )


async def test_the_positions_view_renders_a_priced_trade_line(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await add_trip(
        pool,
        opened=datetime(2026, 7, 22, 14, 10, tzinfo=UTC),
        closed=datetime(2026, 7, 23, 9, 55, tzinfo=UTC),
        entry_vwap="77.51",
        exit_vwap="79.02",
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Recent trades:" in text
    assert (
        "SOL +$1,240 · $39k peak · 07-22 14:10 → 07-23 09:55 (19h 45m)"
        " · in 77.51 → out 79.02" in text
    )
    # The section reads as recency under the record, so it sits after the
    # track-record block ("No metrics yet …" is that block's unscanned fallback).
    assert text.index("Recent trades:") > text.index("No metrics yet")


async def test_the_section_lists_the_last_five_trips_newest_first(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    coins = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]  # oldest to newest close
    for day, coin in enumerate(coins, start=1):
        await add_trip(
            pool,
            coin=coin,
            opened=datetime(2026, 7, day, 8, 0, tzinfo=UTC),
            closed=datetime(2026, 7, day, 20, 0, tzinfo=UTC),
        )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    # Assert inside the section only — "Most played" also names coins above it.
    section = text.split("Recent trades:")[1]
    assert "AAA" not in section  # the sixth-newest fell off the list
    positions = [section.index(coin) for coin in ["FFF", "EEE", "DDD", "CCC", "BBB"]]
    assert positions == sorted(positions)  # newest first, top to bottom


async def test_the_profile_path_shows_the_section_too(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # The screener/paste profile is the second view-assembly path; an
    # untracked wallet's stored trips render the same section (the data is in
    # Postgres — no API cost, tracked and untracked alike).
    await add_trader(pool)
    await add_trip(
        pool,
        opened=datetime(2026, 7, 22, 14, 10, tzinfo=UTC),
        closed=datetime(2026, 7, 23, 9, 55, tzinfo=UTC),
    )

    await feed_text(dp, bot, WHALE, user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Recent trades:" in text
    assert "SOL +$1,240 · $39k peak" in text


async def test_a_wallet_with_no_trips_omits_the_section(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "Recent trades" not in text


async def test_a_pre_vwap_trip_renders_without_the_price_clause(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # Trips folded before #116 shipped have NULL prices — honest omission of
    # the `in → out` part, everything else intact (no backfill possible).
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await add_trip(
        pool,
        pnl="-380",
        peak="12000",
        opened=datetime(2026, 7, 20, 6, 0, tzinfo=UTC),
        closed=datetime(2026, 7, 20, 18, 30, tzinfo=UTC),
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "SOL -$380 · $12k peak · 07-20 06:00 → 07-20 18:30 (12h 30m)" in text
    assert "in " not in text.split("Recent trades:")[1]
    assert "out" not in text.split("Recent trades:")[1]


async def test_builder_dex_trips_render_as_the_bare_ticker(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    await add_trip(
        pool,
        coin="xyz:SP500",
        opened=datetime(2026, 7, 22, 14, 10, tzinfo=UTC),
        closed=datetime(2026, 7, 23, 9, 55, tzinfo=UTC),
    )

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    text = session.sent_messages()[-1].text or ""
    assert "SP500 +$1,240" in text
    assert "xyz:" not in text.split("Recent trades:")[1]
