import asyncio
import logging

import aiohttp
from aiogram import Bot, Dispatcher

from epigone.bot.access import install_allowlist_gate
from epigone.bot.alerts import run_delivery_loop
from epigone.bot.first_data_notice import run_first_data_notice_loop
from epigone.bot.handlers import build_router
from epigone.bot.menu import set_bot_commands
from epigone.bot.order_alerts import run_order_delivery_loop
from epigone.clock import SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.gateway.http import HttpHyperliquidGateway


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    pool = await create_pool(settings.database_url)
    await migrate(pool)

    bot = Bot(settings.require_bot_token())
    clock = SystemClock()
    async with aiohttp.ClientSession() as session:
        dp = Dispatcher()
        dp["pool"] = pool
        dp["gateway"] = HttpHyperliquidGateway(session, clock)
        dp["clock"] = clock
        dp["admin_telegram_id"] = settings.require_admin_telegram_id()  # invite-only owner (#33)
        dp["drafts"] = {}  # per-User criteria-builder drafts (bot/criteria.py)
        dp["min_size_pending"] = {}  # per-User min-size prompts (bot/controls.py)
        dp["rename_pending"] = {}  # per-User wallet-rename prompts (bot/names.py)
        # Invite-only gate (#33): the single outer-middleware seam every update
        # passes before any handler runs.
        install_allowlist_gate(dp)
        dp.include_router(build_router())

        # Position Alert delivery (issue #4) rides in the bot process — the
        # one holder of the Telegram token — alongside dialog polling.
        # Publish the Telegram command menu (admin controls scoped to the owner).
        await set_bot_commands(bot, settings.admin_telegram_id)

        delivery = asyncio.create_task(run_delivery_loop(pool, bot, clock))
        # The one-time "first fine data landed" notices (issue #83) ride the same
        # Postgres→bot seam as Position Alerts, on their own drain loop.
        first_data = asyncio.create_task(run_first_data_notice_loop(pool, bot, clock))
        # Order Alerts (issue #115): the stream's order poll queues batches into
        # order_alerts; same seam, its own drain loop.
        order_delivery = asyncio.create_task(run_order_delivery_loop(pool, bot, clock))
        logging.getLogger(__name__).info("bot: starting polling and alert delivery")
        try:
            await dp.start_polling(bot)
        finally:
            delivery.cancel()
            first_data.cancel()
            order_delivery.cancel()


if __name__ == "__main__":
    asyncio.run(main())
