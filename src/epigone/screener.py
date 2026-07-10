"""Screener query layer: ranked Traders from precomputed metric columns.

Every screener surface must go through run_screener — that is what makes Bot
exclusion global (issue #8): flagged accounts keep their rows but never reach
a result. Fine metrics ride along where the fine pass has run; rows without
them are visibly coarse-only via `fine_available`. The Criteria builder
(issue #7) will grow the filter surface; until then the default Criteria is
the chosen window's ROI, best first.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.gateway import Window


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
    fine_computed_at: datetime | None


async def run_screener(
    pool: asyncpg.Pool, window: Window = Window.MONTH, limit: int = 10, offset: int = 0
) -> list[ScreenerRow]:
    """The default Criteria: Traders with coarse metrics for `window`, Bots
    excluded, ranked by that window's ROI. `offset` pages through the ranking;
    the (roi, address) sort is total so pages never overlap or skip a row."""
    rows = await pool.fetch(
        """
        SELECT t.address, t.display_name,
               cm.pnl, cm.roi, cm.volume, cm.account_value,
               cm.computed_at AS coarse_computed_at,
               fm.address IS NOT NULL AS fine_available,
               fm.win_rate, fm.avg_win, fm.avg_loss, fm.sharpe, fm.max_drawdown,
               fm.trade_count, fm.avg_leverage, fm.maker_share,
               fm.computed_at AS fine_computed_at
        FROM traders t
        JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = $1
        LEFT JOIN fine_metrics fm ON fm.address = t.address
        WHERE t.bot_reason IS NULL
        ORDER BY cm.roi DESC, t.address
        LIMIT $2 OFFSET $3
        """,
        window.value,
        limit,
        offset,
    )
    return [ScreenerRow(**row) for row in rows]
