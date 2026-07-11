"""The Telegram command menu — the Menu button (the square-of-dots left of the
emoji key) and the ``/`` autocomplete list.

Published once at startup via setMyCommands. Two scopes: the public commands
every User sees, and — only in the admin's own chat — those plus the invite-only
allowlist controls (#33), so an ordinary allowlisted User never sees /allow in
their menu. The gate already blocks them; hiding the commands just keeps their
menu clean and doesn't advertise controls they can't use.
"""

from aiogram import Bot
from aiogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault

# Order is the display order in Telegram's menu — most-reached first. Descriptions
# mirror HELP_TEXT (bot/handlers.py); keep the two in step.
PUBLIC_COMMANDS = [
    BotCommand(command="screener", description="Best traders now, ranked by 30-day ROI"),
    BotCommand(command="criteria", description="Build your own definition of “best”"),
    BotCommand(command="tracked", description="Your tracked traders & alert controls"),
    BotCommand(command="start", description="What Epigone is and how it works"),
    BotCommand(command="help", description="List all commands"),
]

# Owner-only runtime controls over the invite-only allowlist (#33), appended
# after the public set and shown only in the admin's chat.
ADMIN_COMMANDS = [
    BotCommand(command="allow", description="Grant a user access (by Telegram id)"),
    BotCommand(command="revoke", description="Remove a user's access"),
    BotCommand(command="allowed", description="List who has access"),
]


async def set_bot_commands(bot: Bot, admin_telegram_id: int | None) -> None:
    """Publish the command menu. Everyone gets the public commands; the admin
    also gets the allowlist controls, scoped to their own chat so no other User
    sees them. Idempotent — safe to call on every startup."""
    await bot.set_my_commands(PUBLIC_COMMANDS, scope=BotCommandScopeDefault())
    if admin_telegram_id is not None:
        await bot.set_my_commands(
            PUBLIC_COMMANDS + ADMIN_COMMANDS,
            scope=BotCommandScopeChat(chat_id=admin_telegram_id),
        )
