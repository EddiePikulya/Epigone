import asyncio
import logging

import aiohttp
from aiogram import Bot, Dispatcher

from epigone.bot.alerts import run_delivery_loop
from epigone.bot.handlers import build_router
from epigone.clock import SystemClock
from epigone.config import Settings
from epigone.db import apply_schema, create_pool
from epigone.gateway.http import HttpHyperliquidGateway


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    pool = await create_pool(settings.database_url)
    await apply_schema(pool)

    bot = Bot(settings.require_bot_token())
    clock = SystemClock()
    async with aiohttp.ClientSession() as session:
        dp = Dispatcher()
        dp["pool"] = pool
        dp["gateway"] = HttpHyperliquidGateway(session)
        dp["clock"] = clock
        dp["drafts"] = {}  # per-User criteria-builder drafts (bot/criteria.py)
        dp.include_router(build_router())

        # Position Alert delivery (issue #4) rides in the bot process — the
        # one holder of the Telegram token — alongside dialog polling.
        delivery = asyncio.create_task(run_delivery_loop(pool, bot, clock))
        logging.getLogger(__name__).info("bot: starting polling and alert delivery")
        try:
            await dp.start_polling(bot)
        finally:
            delivery.cancel()


if __name__ == "__main__":
    asyncio.run(main())
