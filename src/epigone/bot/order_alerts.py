"""Order Alert delivery: the bot-side consumer of order_alerts (issue #115).

The stream's order poll queues one row per follower per wallet per poll cycle
(ADR-0002: the processes meet only in Postgres), each carrying that cycle's
whole batch of newly placed orders as JSONB (OpenOrder.to_wire). Delivery is
therefore trivially one message per row — the batching (#115's noise rule)
already happened at queue time, as did mute/min-size suppression (#10). The
shared outbox drain (epigone.bot.outbox) owns the retry rules; this module
supplies only which rows to drain and how one renders.

A very active maker can still land a huge batch in one cycle (observed live: a
wallet resting 500+ orders), so the message lists at most
format.MAX_ORDERS_SHOWN lines and counts the rest — the point of the alert is
"they're building a ladder", not the full book, which the positions
tap-through shows on demand.
"""

import json
import logging

import asyncpg
from aiogram import Bot

from epigone.bot.alerts import positions_button
from epigone.bot.format import order_lines, trader_label
from epigone.bot.outbox import MAX_DELIVERY_ATTEMPTS, drain_outbox, run_drain_loop
from epigone.clock import Clock
from epigone.gateway import OpenOrder, Position, Side

log = logging.getLogger(__name__)


async def run_order_delivery_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """The shared supervised drain loop over Order Alert delivery."""
    await run_drain_loop(
        lambda: deliver_pending_order_alerts(pool, bot, clock), clock, label="order alert"
    )


async def deliver_pending_order_alerts(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Deliver every undelivered order alert, oldest first. Returns the count."""

    async def deliver(bot: Bot, row: asyncpg.Record) -> None:
        await bot.send_message(
            chat_id=row["user_telegram_id"],
            text=await render_order_alert(pool, row),
            reply_markup=positions_button(row),
        )

    return await drain_outbox(
        pool,
        bot,
        clock,
        table="order_alerts",
        fetch=_fetch_pending,
        deliver=deliver,
    )


async def _fetch_pending(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    # Same joins as Position Alert delivery: the recipient's own per-Track
    # nickname (#86) beats the leaderboard label; NULL when they've since
    # unfollowed.
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT a.*, t.display_name, tr.name AS track_name
        FROM order_alerts a
        JOIN traders t ON t.address = a.trader_address
        LEFT JOIN tracks tr
            ON tr.trader_address = a.trader_address
            AND tr.user_telegram_id = a.user_telegram_id
        WHERE a.delivered_at IS NULL AND a.attempts < $1
        ORDER BY a.id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows


async def render_order_alert(pool: asyncpg.Pool, row: asyncpg.Record) -> str:
    """The message text for one batch row: a header naming the wallet and the
    batch size, then the shared capped order lines (format.order_lines) in the
    placement order the poll stored.

    Each line is annotated with what the order would do to the wallet's current
    position on that coin (#123) — read from the poller's snapshots at delivery
    time, the same read-time DB pattern as #121's coin-match, no new polling or
    state. A wallet with no snapshot on a coin reads as a new position."""
    label = trader_label(row["track_name"] or row["display_name"], row["trader_address"])
    orders = [OpenOrder.from_wire(entry) for entry in json.loads(row["orders"])]
    positions = await _position_snapshots(pool, row["trader_address"])
    if len(orders) == 1:
        head = f"📋 {label} placed a new order:"
    else:
        head = f"📋 {label} placed {len(orders)} new orders:"
    return "\n".join([head, *order_lines(orders, positions)])


async def _position_snapshots(pool: asyncpg.Pool, address: str) -> dict[str, Position]:
    """coin → the wallet's current open position, from the poller's snapshots
    (#123). The order annotation compares each order against these; a coin the
    wallet holds nothing on is simply absent, and the renderer reads that as a
    new position. Only the fields the annotation needs — side, notional, entry
    — are hydrated; a snapshot Position omits margin/leverage-derived extras."""
    rows = await pool.fetch(
        """
        SELECT coin, side, size_usd, leverage, entry_price, unrealized_pnl
        FROM position_snapshots
        WHERE trader_address = $1
        """,
        address,
    )
    return {
        r["coin"]: Position(
            coin=r["coin"],
            side=Side(r["side"]),
            size_usd=r["size_usd"],
            leverage=r["leverage"],
            entry_price=r["entry_price"],
            unrealized_pnl=r["unrealized_pnl"],
        )
        for r in rows
    }
