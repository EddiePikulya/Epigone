"""Copy-notice delivery: the bot-side consumer of copy_notices (issue #136).

The executor writes what it did and why (epigone.execute.notices); this drains
those rows into the operator's chat on the shared outbox loop, so the retry
rule — pause the whole run on a Telegram outage, burn one row's attempts on a
per-chat reject — is the one in epigone.bot.outbox and not a second copy of
it.

Nothing here understands copy semantics. The message body was rendered by the
executor, which is the only thing that knew what had just happened; this
module's whole job is getting it to a chat.
"""

import logging

import asyncpg
from aiogram import Bot

from epigone.bot.delete import with_delete_button
from epigone.bot.outbox import MAX_DELIVERY_ATTEMPTS, drain_outbox, run_drain_loop
from epigone.clock import Clock

log = logging.getLogger(__name__)


async def run_copy_notice_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """The shared supervised drain loop over copy-notice delivery."""
    await run_drain_loop(
        lambda: deliver_pending_copy_notices(pool, bot, clock), clock, label="copy notice"
    )


async def deliver_pending_copy_notices(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Deliver every undelivered copy notice, oldest first. Returns the count."""

    async def deliver(bot: Bot, row: asyncpg.Record) -> None:
        await bot.send_message(
            chat_id=row["user_telegram_id"],
            text=row["body"],
            reply_markup=with_delete_button(),
        )

    return await drain_outbox(
        pool,
        bot,
        clock,
        table="copy_notices",
        fetch=_fetch_pending,
        deliver=deliver,
    )


async def _fetch_pending(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT id, user_telegram_id, kind, body, attempts
        FROM copy_notices
        WHERE delivered_at IS NULL AND attempts < $1
        ORDER BY id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows
