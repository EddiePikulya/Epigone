import re
from decimal import Decimal

import asyncpg
from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from epigone.gateway import HyperliquidGateway, Position

START_TEXT = (
    "Welcome to Epigone — clone the best to be the best.\n\n"
    "Epigone finds and tracks the best Hyperliquid perp traders, where YOU "
    'define what "best" means:\n\n'
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
    "/tracked — your tracked traders and their positions\n"
    "/help — this list\n\n"
    "Paste a wallet address (0x…) to start tracking that trader.\n\n"
    "Coming soon: the screener and criteria builder."
)

INVALID_ADDRESS_TEXT = (
    "That doesn't look like a wallet address.\n\n"
    "Paste a full Hyperliquid address — 0x followed by 40 hex characters — "
    "and I'll start tracking that trader."
)

NOT_TRACKING_TEXT = (
    "You're not tracking any traders yet.\n\nPaste a wallet address (0x…) to follow your first one."
)

UNKNOWN_COMMAND_TEXT = "I don't know that command. Type /help to see what I can do."

_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")


async def cmd_start(message: Message, pool: asyncpg.Pool) -> None:
    user = message.from_user
    if user is not None:
        await _upsert_user(pool, user.id, user.username)
    await message.answer(START_TEXT)


async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


async def follow_pasted_address(message: Message, pool: asyncpg.Pool) -> None:
    """A pasted valid address Follows the Trader; re-following is idempotent."""
    user = message.from_user
    if user is None or message.text is None:
        return
    address = message.text.strip().lower()
    async with pool.acquire() as conn, conn.transaction():
        await _upsert_user(conn, user.id, user.username)
        await conn.execute(
            "INSERT INTO traders (address) VALUES ($1) ON CONFLICT DO NOTHING", address
        )
        freshly_tracked = await conn.fetchrow(
            """
            INSERT INTO tracks (user_telegram_id, trader_address)
            VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING 1
            """,
            user.id,
            address,
        )
    if freshly_tracked is not None:
        await message.answer(
            f"Now tracking {_short(address)}.\n"
            "Paste more addresses any time — /tracked shows your whole list."
        )
    else:
        await message.answer(f"You're already tracking {_short(address)}.")


async def reject_unknown_command(message: Message) -> None:
    await message.answer(UNKNOWN_COMMAND_TEXT)


async def reject_unrecognized_input(message: Message) -> None:
    await message.answer(INVALID_ADDRESS_TEXT)


async def cmd_tracked(message: Message, pool: asyncpg.Pool, gateway: HyperliquidGateway) -> None:
    user = message.from_user
    if user is None:
        return
    text, markup = await _render_tracked_list(pool, gateway, user.id)
    await message.answer(text, reply_markup=markup)


async def on_positions(
    callback: CallbackQuery, bot: Bot, pool: asyncpg.Pool, gateway: HyperliquidGateway
) -> None:
    """On-demand current-positions view for a tracked Trader."""
    address = (callback.data or "").removeprefix("positions:")
    tracked = await pool.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    if not tracked:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    positions = await gateway.get_open_positions(address)
    view = _render_positions(address, positions)
    if isinstance(callback.message, Message):
        await callback.message.answer(view)  # the chat the button lives in
    else:
        await bot.send_message(chat_id=callback.from_user.id, text=view)
    await callback.answer()


async def on_unfollow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway
) -> None:
    """One-tap unfollow: drop the Track and refresh the list in place."""
    address = (callback.data or "").removeprefix("unfollow:")
    status = await pool.execute(
        "DELETE FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    removed = status != "DELETE 0"  # a stale button tap deletes nothing
    if isinstance(callback.message, Message):
        text, markup = await _render_tracked_list(pool, gateway, callback.from_user.id)
        await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer(
        f"Unfollowed {_short(address)}" if removed else "You weren't tracking this trader."
    )


def _is_wallet_paste(message: Message) -> bool:
    return message.text is not None and _ADDRESS_RE.fullmatch(message.text.strip()) is not None


def _is_command(message: Message) -> bool:
    return message.text is not None and message.text.startswith("/")


async def _upsert_user(
    executor: asyncpg.Pool | asyncpg.Connection, telegram_id: int, username: str | None
) -> None:
    await executor.execute(
        """
        INSERT INTO users (telegram_id, username)
        VALUES ($1, $2)
        ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username
        """,
        telegram_id,
        username,
    )


async def _render_tracked_list(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, user_id: int
) -> tuple[str, InlineKeyboardMarkup | None]:
    rows = await pool.fetch(
        "SELECT trader_address FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at",
        user_id,
    )
    if not rows:
        return NOT_TRACKING_TEXT, None

    lines = ["Your tracked traders:", ""]
    keyboard: list[list[InlineKeyboardButton]] = []
    for row in rows:
        address: str = row["trader_address"]
        positions = await gateway.get_open_positions(address)
        lines.append(f"{_short(address)} — {_summarize(positions)}")
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"📊 {_short(address)}", callback_data=f"positions:{address}"
                ),
                InlineKeyboardButton(text="✖️ Unfollow", callback_data=f"unfollow:{address}"),
            ]
        )
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard)


def _render_positions(address: str, positions: list[Position]) -> str:
    if not positions:
        return f"{_short(address)} has no open positions right now."
    blocks = [f"{_short(address)} — current positions:", ""]
    for p in positions:
        blocks.append(
            f"{p.coin} {p.side.value.upper()} — ${p.size_usd:,.0f} at {p.leverage}x\n"
            f"    entry {p.entry_price} · uPnL {_signed_usd(p.unrealized_pnl)}"
        )
    return "\n".join(blocks)


def _summarize(positions: list[Position]) -> str:
    if not positions:
        return "no open positions"
    total_upnl = sum((p.unrealized_pnl for p in positions), Decimal(0))
    noun = "position" if len(positions) == 1 else "positions"
    return f"{len(positions)} {noun}, uPnL {_signed_usd(total_upnl)}"


def _signed_usd(amount: Decimal) -> str:
    sign = "-" if amount < 0 else "+"
    return f"{sign}${abs(amount):,.0f}"


def _short(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}"


def build_router() -> Router:
    """A fresh Router per Dispatcher — a Router instance can only attach once."""
    router = Router()
    router.message.register(cmd_start, Command("start"))
    router.message.register(cmd_help, Command("help"))
    router.message.register(cmd_tracked, Command("tracked"))
    router.message.register(follow_pasted_address, _is_wallet_paste)
    router.message.register(reject_unknown_command, _is_command)
    router.message.register(reject_unrecognized_input)  # anything else: text, stickers, photos…
    router.callback_query.register(on_positions, F.data.startswith("positions:"))
    router.callback_query.register(on_unfollow, F.data.startswith("unfollow:"))
    return router
