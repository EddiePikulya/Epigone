import asyncpg
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

START_TEXT = (
    "Welcome to Epigone — clone the best to be the best.\n\n"
    "Epigone finds and tracks the best Hyperliquid perp traders, where YOU "
    "define what \"best\" means:\n\n"
    "1. Define your own criteria (win rate, PnL, drawdown, and more)\n"
    "2. Find the traders that match them\n"
    "3. Track the ones you like\n"
    "4. Get a Telegram alert within seconds when they open, close, or flip a position\n\n"
    "Read-only by design: no keys, no funds, just intelligence.\n"
    "Type /help to see what you can do."
)

HELP_TEXT = (
    "Epigone commands:\n\n"
    "/start — what Epigone is and how it works\n"
    "/help — this list\n\n"
    "Coming soon: the screener, criteria builder, and trader tracking."
)


async def cmd_start(message: Message, pool: asyncpg.Pool) -> None:
    user = message.from_user
    if user is not None:
        await pool.execute(
            """
            INSERT INTO users (telegram_id, username)
            VALUES ($1, $2)
            ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user.id,
            user.username,
        )
    await message.answer(START_TEXT)


async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


def build_router() -> Router:
    """A fresh Router per Dispatcher — a Router instance can only attach once."""
    router = Router()
    router.message.register(cmd_start, Command("start"))
    router.message.register(cmd_help, Command("help"))
    return router
