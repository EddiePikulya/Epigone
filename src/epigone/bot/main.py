import asyncio
import logging

from aiogram import Bot, Dispatcher

from epigone.bot.handlers import build_router
from epigone.config import Settings
from epigone.db import apply_schema, create_pool


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    pool = await create_pool(settings.database_url)
    await apply_schema(pool)

    bot = Bot(settings.require_bot_token())
    dp = Dispatcher()
    dp["pool"] = pool
    dp.include_router(build_router())

    logging.getLogger(__name__).info("bot: starting polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
