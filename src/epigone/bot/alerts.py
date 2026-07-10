"""Position Alert delivery: the bot-side consumer of position_alerts (issue #4).

The stream poller queues one row per event per follower (ADR-0002: the
processes meet only in Postgres); this loop drains undelivered rows oldest
first and stamps delivered_at only after Telegram accepts the send. Stamped
rows are never resent, so bot restarts are duplicate-free; a crash in the
instant between send and stamp re-sends that single alert — the at-least-once
residue of an outbox without delivery receipts. Sends Telegram rejects
(blocked bot, deleted chat) increment attempts, and a row is abandoned after
MAX_DELIVERY_ATTEMPTS so one dead chat cannot wedge the queue.
"""

import logging

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from epigone.bot.format import held_for, signed_pct, signed_usd, trader_label
from epigone.clock import Clock

log = logging.getLogger(__name__)

DELIVERY_INTERVAL_SECONDS = 2.0
MAX_DELIVERY_ATTEMPTS = 5


async def run_delivery_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    while True:
        await deliver_pending(pool, bot)
        await clock.sleep(DELIVERY_INTERVAL_SECONDS)


async def deliver_pending(pool: asyncpg.Pool, bot: Bot) -> int:
    """Send every undelivered alert, oldest first. Returns the delivered count."""
    rows = await pool.fetch(
        """
        SELECT a.*, t.display_name
        FROM position_alerts a
        JOIN traders t ON t.address = a.trader_address
        WHERE a.delivered_at IS NULL AND a.attempts < $1
        ORDER BY a.id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    delivered = 0
    for row in rows:
        try:
            await bot.send_message(chat_id=row["user_telegram_id"], text=render_alert(row))
        except TelegramAPIError:
            log.warning(
                "alert %d: send to user %d failed",
                row["id"],
                row["user_telegram_id"],
                exc_info=True,
            )
            await pool.execute(
                "UPDATE position_alerts SET attempts = attempts + 1 WHERE id = $1", row["id"]
            )
            continue
        await pool.execute(
            "UPDATE position_alerts SET delivered_at = now() WHERE id = $1", row["id"]
        )
        delivered += 1
    return delivered


def render_alert(row: asyncpg.Record) -> str:
    label = trader_label(row["display_name"], row["trader_address"])
    coin: str = row["coin"]
    if row["kind"] == "open":
        return f"🟢 {label} opened {coin} {_side(row['side'])} — {_new_leg(row)}"
    if row["kind"] == "close":
        return f"🔴 {label} closed {coin} {_side(row['prev_side'])} — {_closed_leg(row)}"
    return (
        f"🔄 {label} flipped {coin} {_side(row['prev_side'])} → {_side(row['side'])} — "
        f"{_closed_leg(row)}; now {_side(row['side'])} {_new_leg(row)}"
    )


def _side(side: str) -> str:
    return side.upper()


def _new_leg(row: asyncpg.Record) -> str:
    return f"${row['size_usd']:,.0f} at {row['leverage']}x, entry {row['entry_price']}"


def _closed_leg(row: asyncpg.Record) -> str:
    """Realized PnL is the poller's last-observed uPnL (see epigone.stream.poller);
    the fields are nullable at the schema level, so render what is present."""
    parts = []
    if row["realized_pnl"] is not None:
        pnl = signed_usd(row["realized_pnl"])
        if row["pct_return"] is not None:
            pnl += f" ({signed_pct(row['pct_return'])})"
        parts.append(f"PnL {pnl}")
    if row["opened_at"] is not None:
        parts.append(f"held {held_for(row['opened_at'], row['created_at'])}")
    return ", ".join(parts)
