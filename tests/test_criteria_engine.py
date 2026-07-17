"""The Criteria engine (issue #7): run_criteria turns a user-built Criteria
(filters + timeframe + sort) into a ranked query with the same guarantees as
the default screener — Bot exclusion, fine/coarse distinction, total sort.
strictest_filter explains a zero-result run. Persistence round-trips saved
Criteria so they survive restarts. The Telegram flow on top is
tests/test_criteria_ux.py."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
import pytest

from epigone.criteria_store import (
    delete_criteria,
    get_criteria,
    list_criteria,
    save_criteria,
    update_criteria,
)
from epigone.gateway import Window
from epigone.metrics.library import METRICS, Scope, format_value, parse_threshold
from epigone.screener import Criteria, Filter, Op, run_criteria, run_screener, strictest_filter

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def add_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    roi: str = "0.1",
    pnl: str = "1000",
    window: str = "month",
    bot_reason: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at, bot_flagged_at, bot_reason)
        VALUES ($1, $2, $2, $3, $4) ON CONFLICT (address) DO NOTHING
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
        VALUES ($1, $2, $3, $4, 50000, 10000, $5)
        """,
        address,
        window,
        Decimal(pnl),
        Decimal(roi),
        NOW,
    )


async def add_fine(
    pool: asyncpg.Pool,
    address: str,
    *,
    win_rate: str | None = "0.76",
    sharpe: str | None = "3.2",
    trade_count: int = 104,
) -> None:
    await pool.execute(
        """
        INSERT INTO fine_metrics
            (address, trade_count, win_rate, avg_win, avg_loss, sharpe, max_drawdown,
             avg_leverage, maker_share, realized_pnl, window_start, window_end, computed_at)
        VALUES ($1, $2, $3, 500, 100, $4, 900, 2.5, 0.7, 22000, $5, $5, $5)
        """,
        address,
        trade_count,
        Decimal(win_rate) if win_rate is not None else None,
        Decimal(sharpe) if sharpe is not None else None,
        NOW,
    )


async def set_hold(pool: asyncpg.Pool, address: str, seconds: int) -> None:
    await pool.execute(
        "UPDATE fine_metrics SET avg_hold_seconds = $2 WHERE address = $1", address, seconds
    )


def gte(metric: str, threshold: str) -> Filter:
    return Filter(metric=metric, op=Op.GTE, threshold=Decimal(threshold))


def lte(metric: str, threshold: str) -> Filter:
    return Filter(metric=metric, op=Op.LTE, threshold=Decimal(threshold))


# --- run_criteria ---


async def test_coarse_filters_narrow_the_universe(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xrich", roi="0.5", pnl="50000")
    await add_trader(pool, "0xpoor", roi="0.9", pnl="200")

    rows = await run_criteria(pool, Criteria(filters=(gte("pnl", "10000"),)))

    assert [r.address for r in rows] == ["0xrich"]


async def test_lte_filters_work(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xcalm", roi="0.2")
    await add_fine(pool, "0xcalm")  # avg_leverage 2.5
    await add_trader(pool, "0xdegen", roi="0.8")
    await add_fine(pool, "0xdegen")
    await pool.execute(
        "UPDATE fine_metrics SET avg_leverage = 20 WHERE address = '0xdegen'",
    )

    rows = await run_criteria(pool, Criteria(filters=(lte("avg_leverage", "5"),)))

    assert [r.address for r in rows] == ["0xcalm"]


async def test_filtering_by_holding_time_keeps_swing_traders(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xswing", roi="0.2")
    await add_fine(pool, "0xswing")
    await add_trader(pool, "0xscalp", roi="0.8")
    await add_fine(pool, "0xscalp")
    await set_hold(pool, "0xswing", 3 * 86400)
    await set_hold(pool, "0xscalp", 2 * 3600)
    await add_trader(pool, "0xcoarse", roi="9.9")  # no fine row: NULL never clears the filter

    # "holds >= 2 days": stored as seconds by the DURATION unit.
    rows = await run_criteria(pool, Criteria(filters=(gte("avg_hold_seconds", str(2 * 86400)),)))

    assert [r.address for r in rows] == ["0xswing"]


async def test_sorting_by_holding_time_orders_longest_first(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xslow", roi="0.1")
    await add_fine(pool, "0xslow")
    await add_trader(pool, "0xfast", roi="0.2")
    await add_fine(pool, "0xfast")
    await set_hold(pool, "0xslow", 5 * 86400)
    await set_hold(pool, "0xfast", 4 * 3600)
    await add_trader(pool, "0xpending", roi="9.9")  # no fine row: sorts last, still visible

    rows = await run_criteria(pool, Criteria(sort_key="avg_hold_seconds", sort_desc=True))

    assert [r.address for r in rows] == ["0xslow", "0xfast", "0xpending"]


async def test_a_fine_filter_excludes_coarse_only_traders(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xanalyzed", roi="0.1")
    await add_fine(pool, "0xanalyzed", win_rate="0.80")
    await add_trader(pool, "0xpending", roi="9.9")  # fine pass hasn't reached it

    rows = await run_criteria(pool, Criteria(filters=(gte("win_rate", "0.6"),)))

    # NULL fine metrics can't clear a fine filter — the filter opts into
    # fully-analyzed Traders only (the handlers.py promise for issue #7).
    assert [r.address for r in rows] == ["0xanalyzed"]


async def test_a_null_fine_metric_fails_the_filter_even_when_fine_ran(
    pool: asyncpg.Pool,
) -> None:
    await add_trader(pool, "0xnoexits", roi="0.4")
    await add_fine(pool, "0xnoexits", win_rate=None, trade_count=0)  # no closed trades

    assert await run_criteria(pool, Criteria(filters=(gte("win_rate", "0.6"),))) == []


async def test_sorting_by_a_fine_metric_puts_unanalyzed_last(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xsteady", roi="0.1")
    await add_fine(pool, "0xsteady", sharpe="4.0")
    await add_trader(pool, "0xwild", roi="0.2")
    await add_fine(pool, "0xwild", sharpe="0.5")
    await add_trader(pool, "0xpending", roi="9.9")  # no fine row: sorts last, still visible

    rows = await run_criteria(pool, Criteria(sort_key="sharpe", sort_desc=True))

    assert [r.address for r in rows] == ["0xsteady", "0xwild", "0xpending"]


async def test_ascending_sort(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xup", roi="0.9")
    await add_trader(pool, "0xdown", roi="-0.4")

    rows = await run_criteria(pool, Criteria(sort_key="roi", sort_desc=False))

    assert [r.address for r in rows] == ["0xdown", "0xup"]


async def test_the_timeframe_selects_the_matching_coarse_rows(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xweekly", roi="0.7", window="week")
    await add_trader(pool, "0xmonthly", roi="0.3", window="month")

    week = await run_criteria(pool, Criteria(time_window=Window.WEEK))
    month = await run_criteria(pool, Criteria(time_window=Window.MONTH))

    assert [r.address for r in week] == ["0xweekly"]
    assert [r.address for r in month] == ["0xmonthly"]


async def test_bots_never_reach_a_criteria_result(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xhuman", roi="0.1")
    await add_trader(pool, "0xbot", roi="99.0", bot_reason="100% win rate over 637 exits")

    rows = await run_criteria(pool, Criteria())

    assert [r.address for r in rows] == ["0xhuman"]


async def test_pagination_is_total_and_gapless(pool: asyncpg.Pool) -> None:
    for i in range(5):
        await add_trader(pool, f"0x{i:03d}", roi=str(i))

    page1 = await run_criteria(pool, Criteria(), limit=2, offset=0)
    page2 = await run_criteria(pool, Criteria(), limit=2, offset=2)
    page3 = await run_criteria(pool, Criteria(), limit=2, offset=4)

    assert [r.address for r in page1 + page2 + page3] == [
        "0x004",
        "0x003",
        "0x002",
        "0x001",
        "0x000",
    ]


async def test_run_screener_is_the_default_criteria(pool: asyncpg.Pool) -> None:
    await add_trader(pool, "0xbest", roi="2.0")
    await add_trader(pool, "0xmid", roi="0.5")

    via_screener = await run_screener(pool, window=Window.MONTH)
    via_criteria = await run_criteria(pool, Criteria())

    assert via_screener == via_criteria


async def test_unknown_metric_keys_are_rejected(pool: asyncpg.Pool) -> None:
    # Metric keys reach SQL as column expressions, so anything outside the
    # registry must die loudly, never be interpolated.
    with pytest.raises(KeyError):
        await run_criteria(pool, Criteria(filters=(gte("evil; DROP TABLE", "1"),)))
    with pytest.raises(KeyError):
        await run_criteria(pool, Criteria(sort_key="evil"))


# --- strictest_filter ---


async def test_strictest_filter_is_the_one_fewest_traders_clear_alone(
    pool: asyncpg.Pool,
) -> None:
    for i in range(3):
        await add_trader(pool, f"0x{i:03d}", roi="0.5", pnl="5000")
        await add_fine(pool, f"0x{i:03d}", win_rate="0.6")

    criteria = Criteria(filters=(gte("pnl", "1000"), gte("win_rate", "0.99")))
    assert await run_criteria(pool, criteria) == []

    strictness = await strictest_filter(pool, criteria)

    assert strictness is not None
    assert strictness.filter.metric == "win_rate"  # 0 clear it alone; pnl passes all 3
    assert strictness.solo_matches == 0


async def test_strictest_filter_none_without_filters(pool: asyncpg.Pool) -> None:
    assert await strictest_filter(pool, Criteria()) is None


# --- saved Criteria persistence ---


async def _user(pool: asyncpg.Pool, telegram_id: int) -> None:
    await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", telegram_id)


async def test_saved_criteria_round_trip(pool: asyncpg.Pool) -> None:
    await _user(pool, 111)
    criteria = Criteria(
        filters=(gte("win_rate", "0.605"), lte("avg_leverage", "5")),
        time_window=Window.WEEK,
        sort_key="sharpe",
        sort_desc=False,
    )

    saved_id = await save_criteria(pool, 111, "Steady hands", criteria, NOW)
    loaded = await get_criteria(pool, 111, saved_id)

    assert loaded is not None
    assert loaded.name == "Steady hands"
    assert loaded.criteria == criteria  # Decimals, ops, window all exact


async def test_saving_under_the_same_name_replaces(pool: asyncpg.Pool) -> None:
    await _user(pool, 111)
    first = await save_criteria(pool, 111, "Mine", Criteria(), NOW)
    second = await save_criteria(pool, 111, "Mine", Criteria(filters=(gte("roi", "0.2"),)), NOW)

    assert first == second
    saved = await list_criteria(pool, 111)
    assert len(saved) == 1
    assert saved[0].criteria.filters == (gte("roi", "0.2"),)


async def test_users_have_their_own_criteria(pool: asyncpg.Pool) -> None:
    await _user(pool, 111)
    await _user(pool, 222)
    mine = await save_criteria(pool, 111, "Mine", Criteria(), NOW)
    await save_criteria(pool, 222, "Mine", Criteria(), NOW)  # same name, other User

    assert len(await list_criteria(pool, 111)) == 1
    assert await get_criteria(pool, 222, mine) is None  # no cross-User access


async def test_update_edits_in_place_keeping_the_name(pool: asyncpg.Pool) -> None:
    await _user(pool, 111)
    saved_id = await save_criteria(pool, 111, "Mine", Criteria(), NOW)

    await update_criteria(pool, 111, saved_id, Criteria(sort_key="pnl"), NOW)

    loaded = await get_criteria(pool, 111, saved_id)
    assert loaded is not None
    assert loaded.name == "Mine"
    assert loaded.criteria.sort_key == "pnl"


async def test_delete_returns_the_name_once(pool: asyncpg.Pool) -> None:
    await _user(pool, 111)
    saved_id = await save_criteria(pool, 111, "Mine", Criteria(), NOW)

    assert await delete_criteria(pool, 111, saved_id) == "Mine"
    assert await delete_criteria(pool, 111, saved_id) is None  # stale tap
    assert await list_criteria(pool, 111) == []


# --- Metric Library ---


def test_every_metric_has_a_one_line_explanation() -> None:
    for spec in METRICS.values():
        assert spec.explanation
        assert "\n" not in spec.explanation


def test_fine_and_coarse_scopes_match_their_tables() -> None:
    for spec in METRICS.values():
        prefix = "cm." if spec.scope is Scope.COARSE else "fm."
        assert spec.sql.startswith(prefix)


def test_percent_thresholds_are_typed_as_percentages() -> None:
    win_rate = METRICS["win_rate"]
    assert parse_threshold(win_rate, "60") == Decimal("0.6")
    assert parse_threshold(win_rate, "60%") == Decimal("0.6")
    assert format_value(win_rate, Decimal("0.605")) == "60.5%"


def test_usd_thresholds_accept_shorthand() -> None:
    pnl = METRICS["pnl"]
    assert parse_threshold(pnl, "10k") == Decimal("10000")
    assert parse_threshold(pnl, "$1,500") == Decimal("1500")
    assert parse_threshold(pnl, "1.5m") == Decimal("1500000")
    assert parse_threshold(pnl, "-2000") == Decimal("-2000")
    assert format_value(pnl, Decimal("10000")) == "$10,000"
    assert format_value(pnl, Decimal("-2000")) == "-$2,000"


def test_leverage_accepts_the_x_suffix() -> None:
    assert parse_threshold(METRICS["avg_leverage"], "3x") == Decimal("3")


def test_counts_must_be_whole_numbers() -> None:
    trade_count = METRICS["trade_count"]
    assert parse_threshold(trade_count, "100") == Decimal("100")
    assert parse_threshold(trade_count, "10.5") is None


def test_garbage_thresholds_parse_to_none() -> None:
    assert parse_threshold(METRICS["pnl"], "lots") is None
    assert parse_threshold(METRICS["pnl"], "") is None


def test_duration_thresholds_parse_naturally() -> None:
    avg_hold = METRICS["avg_hold_seconds"]
    assert parse_threshold(avg_hold, "2d") == Decimal(2 * 86400)
    assert parse_threshold(avg_hold, "12h") == Decimal(12 * 3600)
    assert parse_threshold(avg_hold, "90m") == Decimal(90 * 60)  # minutes, not millions
    assert parse_threshold(avg_hold, "1d 6h") == Decimal(86400 + 6 * 3600)
    assert parse_threshold(avg_hold, "") is None
    assert parse_threshold(avg_hold, "soon") is None
    assert parse_threshold(avg_hold, "2days") is None  # stray text, not a clean 2d


def test_duration_format_is_stable_and_round_trips() -> None:
    avg_hold = METRICS["avg_hold_seconds"]
    assert format_value(avg_hold, Decimal(2 * 86400 + 4 * 3600)) == "2d 4h"
    assert format_value(avg_hold, Decimal(12 * 3600)) == "12h"
    assert format_value(avg_hold, Decimal(90 * 60)) == "1h 30m"
    # A formatted value parses back to the same seconds it came from.
    for seconds in (2 * 86400, 12 * 3600, 90 * 60, 86400 + 6 * 3600):
        assert parse_threshold(avg_hold, format_value(avg_hold, Decimal(seconds))) == Decimal(
            seconds
        )
