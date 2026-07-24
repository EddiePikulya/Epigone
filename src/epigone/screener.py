"""Screener query layer: ranked Traders from precomputed metric columns.

Every screener surface must go through run_criteria — that is what makes Bot
exclusion global (issue #8): flagged accounts keep their rows but never reach
a result. Fine metrics ride along where the fine pass has run; rows without
them are visibly coarse-only via `fine_available`. A Criteria (issue #7) is
filters over the Metric Library plus a timeframe and a sort; the default
Criteria — no filters, the chosen window's ROI best first — is what /screener
runs. Metric keys resolve through the Metric Library registry
(epigone.metrics.library), so no user input ever reaches the SQL text.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

import asyncpg

from epigone.focus_market import FOCUS_MARKET_KEY, focus_condition
from epigone.gateway import Window
from epigone.metrics.library import METRICS


@dataclass(frozen=True)
class ScreenerRow:
    """One ranked Trader: coarse metrics always, fine metrics when available.

    A None fine metric on a fine_available row means "not computable from the
    fill history" (docs/metrics.md); on a coarse-only row it means the fine
    pass hasn't reached this Trader yet."""

    address: str
    display_name: str | None
    pnl: Decimal
    roi: Decimal
    volume: Decimal
    account_value: Decimal
    coarse_computed_at: datetime
    fine_available: bool
    win_rate: Decimal | None
    avg_win: Decimal | None
    avg_loss: Decimal | None
    sharpe: Decimal | None
    max_drawdown: Decimal | None
    trade_count: int | None
    avg_leverage: Decimal | None
    maker_share: Decimal | None
    avg_hold_seconds: int | None
    effective_coins: Decimal | None
    median_trade: Decimal | None
    profit_factor: Decimal | None
    top_trade_share: Decimal | None
    fine_computed_at: datetime | None


class Op(Enum):
    """A filter's comparison. Two operators keep the guided flow guided."""

    GTE = "gte"
    LTE = "lte"

    @property
    def symbol(self) -> str:
        return "≥" if self is Op.GTE else "≤"

    @property
    def sql(self) -> str:
        return ">=" if self is Op.GTE else "<="


@dataclass(frozen=True)
class Filter:
    """metric → operator → threshold. `metric` is a Metric Library key; a
    fine-metric filter excludes coarse-only Traders (NULL never clears it).
    The focus-market filter (#108) is the one non-numeric case: its threshold
    is a cat:/tick: string (epigone.focus_market) and its op is ignored."""

    metric: str
    op: Op
    threshold: Decimal | str


@dataclass(frozen=True)
class Criteria:
    """A definition of "best trader" (CONTEXT.md). The zero-argument form is
    the default Criteria /screener runs. The timeframe scopes coarse metrics
    (per-window rows); fine metrics always cover the recent fill window."""

    filters: tuple[Filter, ...] = ()
    time_window: Window = Window.MONTH
    sort_key: str = "roi"
    sort_desc: bool = True


@dataclass(frozen=True)
class FilterStrictness:
    """A filter and how many Traders clear it on its own — the zero-result
    diagnosis: the filter with the fewest solo matches is the one to loosen."""

    filter: Filter
    solo_matches: int


_FROM = """
    FROM traders t
    JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = $1
    LEFT JOIN fine_metrics fm ON fm.address = t.address
    WHERE t.bot_reason IS NULL
"""


def _conditions(criteria: Criteria) -> tuple[str, list[object]]:
    """The WHERE tail and its parameters. Column expressions come from the
    Metric Library registry (raising KeyError on anything else); thresholds
    travel as query parameters."""
    params: list[object] = [criteria.time_window.value]
    fragments: list[str] = []
    for f in criteria.filters:
        if f.metric == FOCUS_MARKET_KEY:
            fragments.append(focus_condition(f.threshold, params))
            continue
        params.append(f.threshold)
        fragments.append(f" AND {METRICS[f.metric].sql} {f.op.sql} ${len(params)}")
    return "".join(fragments), params


async def run_criteria(
    pool: asyncpg.Pool, criteria: Criteria, limit: int = 10, offset: int = 0
) -> list[ScreenerRow]:
    """Run a Criteria: Traders with coarse metrics for its timeframe, Bots
    excluded, filters applied, ranked by its sort. `offset` pages through the
    ranking; the (sort key, address) sort is total so pages never overlap or
    skip a row. NULLS LAST keeps unanalyzed Traders visible but never on top
    when sorting by a fine metric."""
    conditions, params = _conditions(criteria)
    direction = "DESC" if criteria.sort_desc else "ASC"
    rows = await pool.fetch(
        f"""
        SELECT t.address, t.display_name,
               cm.pnl, cm.roi, cm.volume, cm.account_value,
               cm.computed_at AS coarse_computed_at,
               fm.address IS NOT NULL AS fine_available,
               fm.win_rate, fm.avg_win, fm.avg_loss, fm.sharpe, fm.max_drawdown,
               fm.trade_count, fm.avg_leverage, fm.maker_share, fm.avg_hold_seconds,
               fm.effective_coins, fm.median_trade, fm.profit_factor, fm.top_trade_share,
               fm.computed_at AS fine_computed_at
        {_FROM}{conditions}
        ORDER BY {METRICS[criteria.sort_key].sql} {direction} NULLS LAST, t.address
        LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
        """,
        *params,
        limit,
        offset,
    )
    return [ScreenerRow(**row) for row in rows]


async def run_screener(
    pool: asyncpg.Pool, window: Window = Window.MONTH, limit: int = 10, offset: int = 0
) -> list[ScreenerRow]:
    """The default Criteria: no filters, ranked by the window's ROI."""
    return await run_criteria(pool, Criteria(time_window=window), limit=limit, offset=offset)


async def strictest_filter(pool: asyncpg.Pool, criteria: Criteria) -> FilterStrictness | None:
    """After a zero-result run: which filter, applied alone, lets the fewest
    Traders through — the one worth loosening first. None without filters
    (then the Universe itself is empty). Ties keep the earliest filter."""
    strictest: FilterStrictness | None = None
    for f in criteria.filters:
        conditions, params = _conditions(Criteria(filters=(f,), time_window=criteria.time_window))
        count = await pool.fetchval(f"SELECT count(*) {_FROM}{conditions}", *params)
        if strictest is None or count < strictest.solo_matches:
            strictest = FilterStrictness(filter=f, solo_matches=count)
    return strictest
