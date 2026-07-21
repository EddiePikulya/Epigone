"""Shared Postgres→Telegram outbox drain (issues #4, #83).

Position Alerts (epigone.bot.alerts) and first-data notices
(epigone.bot.first_data_notice) are both outboxes with the same delivery
contract: undelivered rows carry `id`, `user_telegram_id`, `attempts`, and
`delivered_at`; a drain sends each oldest-first and stamps delivered_at only
after Telegram accepts, so a bot restart resends nothing. Failures split the one
way that matters — a transient Telegram fault (network, flood control, 5xx)
pauses the whole run untouched for the next tick, while a per-chat reject
(blocked bot, deleted chat) burns that one row's attempts up to
MAX_DELIVERY_ATTEMPTS so a single dead chat can't wedge the queue.

That send-with-retry loop lives here once, so a fix to the retry rule can never
drift between the two queues; each caller supplies only what differs — which
rows to drain (`fetch`) and how to render one (`build`).
"""

import logging
from collections.abc import Awaitable, Callable, Sequence

import asyncpg
from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.types import InlineKeyboardMarkup

from epigone.clock import Clock

log = logging.getLogger(__name__)

DELIVERY_INTERVAL_SECONDS = 2.0
MAX_DELIVERY_ATTEMPTS = 5


async def drain_outbox(
    pool: asyncpg.Pool,
    bot: Bot,
    clock: Clock,
    *,
    table: str,
    fetch: Callable[[asyncpg.Pool], Awaitable[Sequence[asyncpg.Record]]],
    build: Callable[[asyncpg.Record], tuple[str, InlineKeyboardMarkup]],
) -> int:
    """Drain one outbox pass: send every undelivered row `fetch` returns,
    stamping delivered_at as each is accepted, and return the delivered count.
    `build` renders a row to its (text, reply_markup); `table` is the outbox to
    stamp — a trusted module constant, never user input, so it is safe to inline
    into the UPDATE."""
    rows = await fetch(pool)
    delivered = 0
    for row in rows:
        text, markup = build(row)
        try:
            await bot.send_message(chat_id=row["user_telegram_id"], text=text, reply_markup=markup)
        except (TelegramNetworkError, TelegramRetryAfter, TelegramServerError):
            # Telegram itself is struggling, not this chat: touching attempts
            # here would bleed rows away during an outage. Leave every remaining
            # row for the next tick.
            log.warning("%s delivery paused: Telegram transient failure", table, exc_info=True)
            break
        except TelegramAPIError:
            log.warning(
                "%s row %d: send to user %d rejected",
                table,
                row["id"],
                row["user_telegram_id"],
                exc_info=True,
            )
            await pool.execute(
                f"UPDATE {table} SET attempts = attempts + 1 WHERE id = $1", row["id"]
            )
            continue
        await pool.execute(
            f"UPDATE {table} SET delivered_at = $2 WHERE id = $1", row["id"], clock.now()
        )
        delivered += 1
    return delivered
