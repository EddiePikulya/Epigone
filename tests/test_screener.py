"""The screener query layer (issue #8): every screener surface goes through
run_screener, so Bot exclusion and the fine/coarse distinction hold everywhere.
The Telegram UX on top is issue #6."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from epigone.gateway import Window
from epigone.screener import Criteria, Filter, Op, run_criteria, run_screener

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def add_trader(
    pool: asyncpg.Pool,
    address: str,
    month_roi: str = "0.1",
    bot_reason: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at, bot_flagged_at, bot_reason)
        VALUES ($1, $2, $2, $3, $4)
        """,
        address,
        NOW,
        NOW if bot_reason is not None else None,
        bot_reason,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', 1000, $2, 50000, 10000, $3)
        """,
        address,
        Decimal(month_roi),
        NOW,
    )


async def add_fine_metrics(
    pool: asyncpg.Pool,
    address: str,
    win_rate: str = "0.76",
    *,
    median_trade: str = "150",
    profit_factor: str = "2.4",
    top_trade_share: str = "0.2",
) -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, median_trade, profit_factor, top_trade_share,
             realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, 104, $2, 500, 100, 3.2, 900, 2.5, 0.7, $4, $5, $6, 22000, $3, $3, $3)
        """,
        address,
        Decimal(win_rate),
        NOW,
        Decimal(median_trade),
        Decimal(profit_factor),
        Decimal(top_trade_share),
    )


async def test_results_rank_by_the_window_roi(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xmid", month_roi="0.5")
    await add_trader(pool, "0xbest", month_roi="2.0")
    await add_trader(pool, "0xworst", month_roi="-0.3")

    rows = await run_screener(pool, window=Window.MONTH, limit=10)

    assert [r.address for r in rows] == ["0xbest", "0xmid", "0xworst"]
    assert rows[0].roi == Decimal("2.0")
    assert rows[0].pnl == Decimal("1000")
    assert rows[0].coarse_computed_at == NOW


async def test_limit_caps_the_page(pool: asyncpg.Pool) -> None:
    for i in range(5):
        await add_trader(pool, f"0x{i:03d}", month_roi=str(i))

    rows = await run_screener(pool, window=Window.MONTH, limit=2)

    assert [r.address for r in rows] == ["0x004", "0x003"]


async def test_offset_pages_through_the_ranking(pool: asyncpg.Pool) -> None:
    for i in range(5):
        await add_trader(pool, f"0x{i:03d}", month_roi=str(i))

    page1 = await run_screener(pool, window=Window.MONTH, limit=2, offset=0)
    page2 = await run_screener(pool, window=Window.MONTH, limit=2, offset=2)
    page3 = await run_screener(pool, window=Window.MONTH, limit=2, offset=4)

    assert [r.address for r in page1] == ["0x004", "0x003"]
    assert [r.address for r in page2] == ["0x002", "0x001"]
    assert [r.address for r in page3] == ["0x000"]  # a partial last page


async def test_bots_never_appear_but_keep_their_rows(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xhuman", month_roi="0.1")
    await add_trader(pool, "0xbot", month_roi="99.0", bot_reason="100% win rate over 637 exits")
    await add_fine_metrics(pool, "0xbot", win_rate="1.0")

    rows = await run_screener(pool, window=Window.MONTH, limit=10)

    assert [r.address for r in rows] == ["0xhuman"]
    # Excluded from results, retained in the database (issue #8 acceptance).
    assert await pool.fetchval("SELECT count(*) FROM traders WHERE address = '0xbot'") == 1
    assert await pool.fetchval("SELECT count(*) FROM fine_metrics WHERE address = '0xbot'") == 1


async def test_fine_metrics_ride_along_when_available(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xfine")
    await add_fine_metrics(pool, "0xfine", win_rate="0.76")

    (row,) = await run_screener(pool, window=Window.MONTH, limit=10)

    assert row.fine_available
    assert row.win_rate == Decimal("0.76")
    assert row.trade_count == 104
    assert row.sharpe == Decimal("3.2")
    assert row.max_drawdown == Decimal("900")
    assert row.median_trade == Decimal("150")
    assert row.profit_factor == Decimal("2.4")
    assert row.top_trade_share == Decimal("0.2")
    assert row.fine_computed_at == NOW


async def test_the_anti_deception_trio_filters_and_sorts(pool: asyncpg.Pool) -> None:
    # A profit-factor floor keeps the edge and drops the coin-flipper, and the
    # top-trade-share sort ascends — the repeatable edge (low share) leads the
    # lottery record (#113).
    await add_trader(pool, "0xedge", month_roi="0.2")
    await add_fine_metrics(pool, "0xedge", profit_factor="2.0", top_trade_share="0.15")
    await add_trader(pool, "0xlottery", month_roi="0.9")
    await add_fine_metrics(pool, "0xlottery", profit_factor="3.0", top_trade_share="0.85")
    await add_trader(pool, "0xflipper", month_roi="0.1")
    await add_fine_metrics(pool, "0xflipper", profit_factor="0.6", top_trade_share="0.1")

    criteria = Criteria(
        filters=(Filter(metric="profit_factor", op=Op.GTE, threshold=Decimal("1.5")),),
        time_window=Window.MONTH,
        sort_key="top_trade_share",
        sort_desc=False,
    )
    rows = await run_criteria(pool, criteria, limit=10)

    # 0xflipper filtered out (PF 0.6 < 1.5); the survivors ascend by top-share.
    assert [r.address for r in rows] == ["0xedge", "0xlottery"]


async def test_coarse_only_traders_are_distinguishable(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xcoarse")

    (row,) = await run_screener(pool, window=Window.MONTH, limit=10)

    assert not row.fine_available
    assert row.win_rate is None
    assert row.trade_count is None
    assert row.fine_computed_at is None


async def test_traders_without_the_window_are_absent(pool: asyncpg.Pool) -> None:
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ('0xnew', $1, $1)",
        NOW,
    )

    assert await run_screener(pool, window=Window.MONTH, limit=10) == []
