"""Starter Criteria presets (issue #71): three curated Criteria that appear in
every User's list ready to run, deletable per-User as a hide-for-me. The
definitions live in code (epigone.criteria_presets); only which presets a User
deleted lives in Postgres. The Telegram flow on top is exercised in
tests/test_criteria_presets_ux.py."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from epigone.criteria_presets import PRESETS, PRESETS_BY_KEY
from epigone.criteria_store import dismiss_preset, hidden_preset_keys
from epigone.gateway import Window
from epigone.metrics.library import METRICS, Unit
from epigone.screener import Filter, Op

NOW = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)


async def add_user(pool: asyncpg.Pool, telegram_id: int) -> None:
    await pool.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", telegram_id
    )


# --- The definitions (single source of truth, thresholds exact) ---


def _filters(key: str) -> tuple[Filter, ...]:
    return PRESETS_BY_KEY[key].criteria.filters


def test_the_three_presets_are_present_and_stably_keyed() -> None:
    assert [p.key for p in PRESETS] == ["steady_earners", "careful_whales", "hot_hands"]
    assert [p.name for p in PRESETS] == ["Steady earners", "Careful whales", "Hot hands"]


def test_steady_earners_matches_the_spec() -> None:
    preset = PRESETS_BY_KEY["steady_earners"]
    assert preset.criteria.time_window == Window.MONTH
    assert preset.criteria.sort_key == "pnl"
    assert preset.criteria.sort_desc is True
    assert _filters("steady_earners") == (
        Filter(metric="sharpe", op=Op.GTE, threshold=Decimal("7")),
        Filter(metric="pnl", op=Op.GTE, threshold=Decimal("50000")),
        Filter(metric="trade_count", op=Op.GTE, threshold=Decimal("20")),
    )


def test_careful_whales_matches_the_spec() -> None:
    preset = PRESETS_BY_KEY["careful_whales"]
    assert preset.criteria.time_window == Window.MONTH
    assert preset.criteria.sort_key == "pnl"
    assert preset.criteria.sort_desc is True
    assert _filters("careful_whales") == (
        Filter(metric="account_value", op=Op.GTE, threshold=Decimal("500000")),
        Filter(metric="pnl", op=Op.GTE, threshold=Decimal("100000")),
        Filter(metric="avg_leverage", op=Op.LTE, threshold=Decimal("3")),
        Filter(metric="sharpe", op=Op.GTE, threshold=Decimal("3")),
    )


def test_hot_hands_matches_the_spec_with_percent_as_a_fraction() -> None:
    preset = PRESETS_BY_KEY["hot_hands"]
    assert preset.criteria.time_window == Window.WEEK
    assert preset.criteria.sort_key == "roi"
    assert preset.criteria.sort_desc is True
    assert _filters("hot_hands") == (
        Filter(metric="roi", op=Op.GTE, threshold=Decimal("0.30")),  # 30% stored as 0.30
        Filter(metric="pnl", op=Op.GTE, threshold=Decimal("25000")),
        Filter(metric="trade_count", op=Op.GTE, threshold=Decimal("5")),
    )


def test_every_preset_filter_references_a_real_metric() -> None:
    for preset in PRESETS:
        assert preset.criteria.sort_key in METRICS
        for f in preset.criteria.filters:
            assert f.metric in METRICS


def test_roi_threshold_stays_a_fraction_not_a_whole_percent() -> None:
    # Guards the Unit.PERCENT convention: a hand-editor bumping ROI to "30"
    # instead of "0.30" would silently ask for 3000% and match no one.
    (roi_filter,) = [f for f in _filters("hot_hands") if f.metric == "roi"]
    assert METRICS["roi"].unit is Unit.PERCENT
    assert roi_filter.threshold < Decimal("1")


# --- Dismissal persistence (hide-for-me, per User) ---


async def test_a_fresh_user_has_dismissed_nothing(pool: asyncpg.Pool) -> None:
    await add_user(pool, 111)
    assert await hidden_preset_keys(pool, 111) == set()


async def test_dismissing_a_preset_hides_it_for_that_user_only(pool: asyncpg.Pool) -> None:
    await add_user(pool, 111)
    await add_user(pool, 222)

    await dismiss_preset(pool, 111, "hot_hands", NOW)

    assert await hidden_preset_keys(pool, 111) == {"hot_hands"}
    assert await hidden_preset_keys(pool, 222) == set()  # other users unaffected


async def test_dismissal_is_idempotent_and_survives_a_reread(pool: asyncpg.Pool) -> None:
    await add_user(pool, 111)

    await dismiss_preset(pool, 111, "steady_earners", NOW)
    await dismiss_preset(pool, 111, "steady_earners", NOW)  # deleting again is a no-op

    # A "restart" is just a fresh read of the same durable table.
    assert await hidden_preset_keys(pool, 111) == {"steady_earners"}
    stamp = await pool.fetchval(
        "SELECT dismissed_at FROM criteria_preset_dismissals "
        "WHERE user_telegram_id = 111 AND preset_key = 'steady_earners'"
    )
    assert stamp == NOW  # the second delete kept the original timestamp
