"""Position Alert delivery: the bot-side consumer of position_alerts (issue #4).

The stream poller queues one row per event per follower (ADR-0002: the
processes meet only in Postgres); this loop drains undelivered rows oldest
first and stamps delivered_at only after Telegram accepts the send. Stamped
rows are never resent, so bot restarts are duplicate-free; a crash in the
instant between send and stamp re-sends that single alert — the at-least-once
residue of an outbox without delivery receipts.

Failures split by durability: transient trouble (network, flood control,
Telegram 5xx) pauses the run and retries everything untouched next tick,
while a per-chat reject (blocked bot, deleted chat) increments that row's
attempts until MAX_DELIVERY_ATTEMPTS abandons it — one dead chat must not
wedge the queue, but an outage must not shed alerts.
"""

import logging
from decimal import Decimal

import asyncpg
from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from epigone.bot.delete import with_delete_button
from epigone.bot.format import held_for, short_address, signed_pct, signed_usd, trader_label
from epigone.clock import Clock

log = logging.getLogger(__name__)

DELIVERY_INTERVAL_SECONDS = 2.0
MAX_DELIVERY_ATTEMPTS = 5


async def run_delivery_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """Supervised drain loop: one broken iteration (database blip, unexpected
    error) is logged and retried, never allowed to silently kill the task
    (ADR-0002's asyncio mitigation) while dialog polling carries on."""
    while True:
        try:
            await deliver_pending(pool, bot, clock)
        except Exception:
            log.exception("alert delivery iteration failed; retrying next tick")
        await clock.sleep(DELIVERY_INTERVAL_SECONDS)


async def deliver_pending(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
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
            await bot.send_message(
                chat_id=row["user_telegram_id"],
                text=render_alert(row),
                reply_markup=_positions_button(row),
            )
        except (TelegramNetworkError, TelegramRetryAfter, TelegramServerError):
            # Telegram itself is struggling, not this chat: touching attempts
            # here would bleed alerts away during an outage. Leave every
            # remaining row for the next tick.
            log.warning("alert delivery paused: Telegram transient failure", exc_info=True)
            break
        except TelegramAPIError:
            log.warning(
                "alert %d: send to user %d rejected",
                row["id"],
                row["user_telegram_id"],
                exc_info=True,
            )
            await pool.execute(
                "UPDATE position_alerts SET attempts = attempts + 1 WHERE id = $1", row["id"]
            )
            continue
        await pool.execute(
            "UPDATE position_alerts SET delivered_at = $2 WHERE id = $1",
            row["id"],
            clock.now(),
        )
        delivered += 1
    return delivered


def _positions_button(row: asyncpg.Record) -> InlineKeyboardMarkup:
    """Make the alert tap-through to the trader's live positions — the same
    on-demand view /tracked offers (the positions:<address> callback). An alert
    only ever fires for a Trader the recipient follows, which is exactly the
    relationship that handler checks, so the button always resolves."""
    address: str = row["trader_address"]
    return with_delete_button(
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"📊 {short_address(address)} — positions",
                        callback_data=f"positions:{address}",
                    )
                ]
            ]
        )
    )


def render_alert(row: asyncpg.Record) -> str:
    label = trader_label(row["display_name"], row["trader_address"])
    coin: str = row["coin"]
    kind: str = row["kind"]
    if kind == "open":
        return f"🟢 {label} opened {coin} {_side(row['side'])} — {_new_leg(row)}"
    if kind == "close":
        return f"🔴 {label} closed {coin} {_side(row['prev_side'])} — {_closed_leg(row)}"
    if kind == "scale_in":
        return f"📈 {label} added to {coin} {_side(row['side'])} — {_scale_leg(row)}"
    if kind == "scale_out":
        return f"📉 {label} trimmed {coin} {_side(row['side'])} — {_scale_leg(row)}"
    return (
        f"🔄 {label} flipped {coin} {_side(row['prev_side'])} → {_side(row['side'])} — "
        f"{_closed_leg(row)}; now {_side(row['side'])} {_new_leg(row)}"
    )


def _side(side: str) -> str:
    return side.upper()


def _new_leg(row: asyncpg.Record) -> str:
    return f"${row['size_usd']:,.0f} at {row['leverage']}x, entry {row['entry_price']}"


def _scale_leg(row: asyncpg.Record) -> str:
    """A scale alert (issue #10): the size it grew from → to at what leverage,
    plus the position's live PnL — return on margin (issue #35) — so a User sees
    at a glance whether the trade is actually winning, not just how much bigger
    it got. PnL is omitted only when it isn't available."""
    prev: Decimal = row["prev_size_usd"]
    new: Decimal = row["size_usd"]
    leg = f"${prev:,.0f} → ${new:,.0f} at {row['leverage']}x"
    if row["pct_return"] is not None:
        leg += f", PnL {signed_pct(row['pct_return'])}"
    return leg


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
