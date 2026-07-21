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
from epigone.bot.format import short_address
from epigone.bot.outbox import DELIVERY_INTERVAL_SECONDS, MAX_DELIVERY_ATTEMPTS, drain_outbox
from epigone.clock import Clock

log = logging.getLogger(__name__)


async def run_first_data_notice_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """Supervised drain loop, alongside Position Alert delivery in the bot
    process: one broken iteration is logged and retried next tick, never allowed
    to kill the task (ADR-0002's asyncio mitigation)."""
    while True:
        try:
            await deliver_first_data_notices(pool, bot, clock)
        except Exception:
            log.exception("first-data notice delivery iteration failed; retrying next tick")
        await clock.sleep(DELIVERY_INTERVAL_SECONDS)


async def deliver_first_data_notices(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Send every ready, undelivered notice, oldest first. Returns the count."""
    return await drain_outbox(
        pool,
        bot,
        clock,
        table="first_data_notices",
        fetch=_fetch_ready_notices,
        build=lambda row: (
            render_first_data_notice(row["trader_address"]),
            _profile_button(row["trader_address"]),
        ),
    )


async def _fetch_ready_notices(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT id, user_telegram_id, trader_address
        FROM first_data_notices
        WHERE status = 'ready' AND delivered_at IS NULL AND attempts < $1
        ORDER BY id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows


def render_first_data_notice(address: str) -> str:
    return f"📊 {short_address(address)} now has full track-record data"


def _profile_button(address: str) -> InlineKeyboardMarkup:
    """Tap-through into the wallet's profile/positions view — the existing
    profile:<address> callback, no new view (issue #83)."""
    return with_delete_button(
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"📊 {short_address(address)} — positions",
                        callback_data=f"profile:{address}",
                    )
                ]
            ]
        )
    )
