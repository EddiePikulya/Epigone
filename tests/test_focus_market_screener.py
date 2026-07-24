"""Focus-market filters in the screener (issue #108): category mode keeps
wallets whose round-trips are majority-share in the category; ticker mode
keeps wallets with the ticker in their top-2 most-played coins per the shared
#80 ranking. Both run as ordinary filters through run_criteria, so saved
criteria re-run them like any other."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg

from epigone.criteria_store import get_criteria, save_criteria
from epigone.focus_market import (
    FOCUS_MARKET_KEY,
    Category,
    category_threshold,
    ticker_threshold,
)
from epigone.screener import Criteria, Filter, Op, run_criteria, strictest_filter

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


def focus(threshold: str) -> Criteria:
    return Criteria(
        filters=(Filter(metric=FOCUS_MARKET_KEY, op=Op.GTE, threshold=threshold),)
    )


async def add_trader(pool: asyncpg.Pool, address: str) -> None:
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        address,
        NOW,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', 1000, 0.1, 50000, 10000, $2)
        """,
        address,
        NOW,
    )


async def add_round_trips(pool: asyncpg.Pool, address: str, *coins: str) -> None:
    for seq, coin in enumerate(coins):
        await pool.execute(
            """
            INSERT INTO fine_trades
                (address, coin, pnl, peak_notional, opened_at, closed_at, seq)
            VALUES ($1, $2, 100, 10000, $3, $3, $4)
            """,
            address,
            coin,
            NOW - timedelta(hours=seq),
            seq,
        )


async def add_open_episode(pool: asyncpg.Pool, address: str, coin: str) -> None:
    await pool.execute(
        """
        INSERT INTO fine_open_episodes (address, coin, opened_at, pnl, peak_notional)
        VALUES ($1, $2, $3, 0, 10000)
        """,
        address,
        coin,
        NOW,
    )


# --- category mode ---------------------------------------------------------------


async def test_majority_share_in_the_category_qualifies(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xmetals")
    await add_round_trips(pool, "0xmetals", "xyz:SILVER", "xyz:GOLD", "xyz:GOLD", "BTC")

    rows = await run_criteria(pool, focus(category_threshold(Category.METALS)))

    assert [r.address for r in rows] == ["0xmetals"]


async def test_a_fifty_fifty_split_does_not_qualify(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xsplit")
    await add_round_trips(pool, "0xsplit", "xyz:SILVER", "xyz:GOLD", "BTC", "ETH")

    assert await run_criteria(pool, focus(category_threshold(Category.METALS))) == []
    # The same wallet is exactly half crypto too — neither side clears >50%.
    assert await run_criteria(pool, focus(category_threshold(Category.CRYPTO))) == []


async def test_core_coins_count_as_crypto_without_a_map_entry(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xdegen")
    await add_round_trips(pool, "0xdegen", "BTC", "kPEPE", "hyna:SOL", "xyz:AAPL")

    rows = await run_criteria(pool, focus(category_threshold(Category.CRYPTO)))

    assert [r.address for r in rows] == ["0xdegen"]


async def test_uncategorized_tickers_count_toward_no_category(pool: asyncpg.Pool) -> None:
    # Three unknown dex tickers drown one silver trip: 25% metals, not >50% —
    # and the unknowns push no other category over the line either.
    await add_trader(pool, "0xmystery")
    await add_round_trips(
        pool, "0xmystery", "xyz:SILVER", "xyz:WAT", "xyz:WAT", "newdex:THING"
    )

    for category in Category:
        assert await run_criteria(pool, focus(category_threshold(category))) == []


async def test_wallets_without_fine_data_never_qualify(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xcoarse")  # scanned coarse, no fine round-trips

    for category in Category:
        assert await run_criteria(pool, focus(category_threshold(category))) == []


async def test_an_open_episode_alone_is_not_a_round_trip_share(pool: asyncpg.Pool) -> None:
    # Category mode reads completed round-trips only — a wallet holding one
    # open silver position with no closed trades has no majority of anything.
    await add_trader(pool, "0xholding")
    await add_open_episode(pool, "0xholding", "xyz:SILVER")

    assert await run_criteria(pool, focus(category_threshold(Category.METALS))) == []


# --- ticker mode -------------------------------------------------------------------


async def test_rank_two_passes_and_rank_three_fails(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xplayer")
    await add_round_trips(
        pool, "0xplayer", "SOL", "SOL", "SOL", "xyz:SILVER", "xyz:SILVER", "BTC"
    )

    assert [
        r.address for r in await run_criteria(pool, focus(ticker_threshold("SILVER")))
    ] == ["0xplayer"]
    assert await run_criteria(pool, focus(ticker_threshold("BTC"))) == []


async def test_an_open_episode_lifts_its_coin_into_the_top_two(pool: asyncpg.Pool) -> None:
    # BTC: 1 trip + open bonus = 2, tying ETH's 2 — the coin-name tiebreak
    # (#80) puts BTC ahead, so BTC is rank 2 and ETH rank 3.
    await add_trader(pool, "0xparked")
    await add_round_trips(pool, "0xparked", "SOL", "SOL", "SOL", "ETH", "ETH", "BTC")
    await add_open_episode(pool, "0xparked", "BTC")

    assert [
        r.address for r in await run_criteria(pool, focus(ticker_threshold("BTC")))
    ] == ["0xparked"]
    assert await run_criteria(pool, focus(ticker_threshold("ETH"))) == []


async def test_ticker_matching_ignores_venue_prefix_and_case(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xsilver")
    await add_round_trips(pool, "0xsilver", "xyz:SILVER", "flx:SILVER")

    rows = await run_criteria(pool, focus(ticker_threshold("silver")))

    assert [r.address for r in rows] == ["0xsilver"]


async def test_ticker_mode_needs_fine_data_too(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xcoarse")

    assert await run_criteria(pool, focus(ticker_threshold("BTC"))) == []


# --- through the shared pipeline ----------------------------------------------------


async def test_focus_combines_with_numeric_filters(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xmetals")
    await add_round_trips(pool, "0xmetals", "xyz:GOLD", "xyz:GOLD", "BTC")

    criteria = Criteria(
        filters=(
            Filter(metric="pnl", op=Op.GTE, threshold=Decimal("500")),
            Filter(
                metric=FOCUS_MARKET_KEY,
                op=Op.GTE,
                threshold=category_threshold(Category.METALS),
            ),
        )
    )

    assert [r.address for r in await run_criteria(pool, criteria)] == ["0xmetals"]


async def test_strictest_filter_diagnoses_a_zero_result_focus_run(
    pool: asyncpg.Pool,
) -> None:
    await add_trader(pool, "0xcrypto")
    await add_round_trips(pool, "0xcrypto", "BTC", "ETH")

    criteria = focus(category_threshold(Category.ENERGY))
    assert await run_criteria(pool, criteria) == []

    strictness = await strictest_filter(pool, criteria)
    assert strictness is not None
    assert strictness.filter.threshold == "cat:ENERGY"
    assert strictness.solo_matches == 0


async def test_saved_focus_criteria_round_trip_through_jsonb(pool: asyncpg.Pool) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES (111)")
    await add_trader(pool, "0xsilver")
    await add_round_trips(pool, "0xsilver", "xyz:SILVER", "BTC")

    saved_id = await save_criteria(
        pool, 111, "Silver bugs", focus(ticker_threshold("SILVER")), NOW
    )
    saved = await get_criteria(pool, 111, saved_id)

    assert saved is not None
    assert saved.criteria.filters[0].threshold == "tick:SILVER"
    assert [r.address for r in await run_criteria(pool, saved.criteria)] == ["0xsilver"]
