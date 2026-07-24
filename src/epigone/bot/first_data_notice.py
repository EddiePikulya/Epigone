"""First-fine-data notice delivery (issue #83): the bot drains first_data_notices.

The bot-side twin of Position Alert delivery (epigone.bot.alerts): the ingest
fine pass queues 'ready' rows (ADR-0002: the processes meet only in Postgres),
and the shared outbox drain (epigone.bot.outbox) sends them with the same
restart-safe, outage-safe delivery contract. This module supplies only what is
notice-specific: which rows to drain and how to render one. The button opens the
wallet's existing profile view via its profile:<address> callback (no new view);
the #73 🗑 delete row appends automatically.
"""

import logging

import asyncpg
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from epigone.bot.delete import with_delete_button
from epigone.bot.format import button_label, trader_label
from epigone.bot.outbox import MAX_DELIVERY_ATTEMPTS, drain_outbox, run_drain_loop
from epigone.clock import Clock

log = logging.getLogger(__name__)


async def run_first_data_notice_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """The shared supervised drain loop over first-data notice delivery."""
    await run_drain_loop(
        lambda: deliver_first_data_notices(pool, bot, clock), clock, label="first-data notice"
    )


async def deliver_first_data_notices(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Send every ready, undelivered notice, oldest first. Returns the count."""
    async def deliver(bot: Bot, row: asyncpg.Record) -> None:
        await bot.send_message(
            chat_id=row["user_telegram_id"],
            text=render_first_data_notice(row["trader_address"], row["track_name"]),
            reply_markup=_profile_button(row["trader_address"], row["track_name"]),
        )

    return await drain_outbox(
        pool,
        bot,
        clock,
        table="first_data_notices",
        fetch=_fetch_ready_notices,
        deliver=deliver,
    )


async def _fetch_ready_notices(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    # The tracks join labels the notice with the recipient's own name for the
    # wallet (#86); NULL if they've since unfollowed, so it reads as the address.
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT n.id, n.user_telegram_id, n.trader_address, tr.name AS track_name
        FROM first_data_notices n
        LEFT JOIN tracks tr
            ON tr.trader_address = n.trader_address
            AND tr.user_telegram_id = n.user_telegram_id
        WHERE n.status = 'ready' AND n.delivered_at IS NULL AND n.attempts < $1
        ORDER BY n.id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows


def render_first_data_notice(address: str, name: str | None = None) -> str:
    return f"📊 {trader_label(name, address)} now has full track-record data"


def _profile_button(address: str, name: str | None) -> InlineKeyboardMarkup:
    """Tap-through into the wallet's profile/positions view — the existing
    profile:<address> callback, no new view (issue #83). Labeled with the
    recipient's own name for the wallet when set (#86); the notice text and the
    profile carry the verifiable address."""
    return with_delete_button(
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"📊 {button_label(name, address)} — positions",
                        callback_data=f"profile:{address}",
                    )
                ]
            ]
        )
    )
