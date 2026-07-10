"""Alert controls from the tracked list (issue #10): mute/unmute a Track and
set a minimum position size, per-Track or globally.

Mute and the per-Track floor are columns on `tracks`; the global floor is
`users.min_size_usd`. The stream poller reads all three when it fans an event
out to followers (epigone.stream.poller), dropping suppressed events at queue
time — so toggling a control here only ever affects future alerts, never a
backlog. Muting is a one-tap toggle that re-renders the list in place; setting
a floor needs one typed number, so — like the criteria builder — a per-User
pending marker in dispatcher data routes the next message here (build_router
registers this ahead of the wallet-paste handler; commands still cut through).

Callback vocabulary: mute:/unmute:{address} toggle a Track; tmin:{address}
prompts for that Track's floor; gmin prompts for the global floor; mincancel
drops a pending prompt. Payloads are client-forgeable, so every write is scoped
to the tapping User and a stale tap simply updates nothing.
"""

from decimal import Decimal, InvalidOperation

import asyncpg
from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from epigone.bot.format import short_address
from epigone.gateway import GatewayError, HyperliquidGateway

# min_size_pending[user_id] holds what floor the User is typing a value for:
# an address for a per-Track floor, or GLOBAL for the account-wide one. Absence
# means no prompt is armed. Losing it to a restart just cancels a half-set
# floor — the same cheap, in-memory tradeoff the criteria drafts make.
GLOBAL = ""
MinSizePending = dict[int, str]

CANCEL_KB = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="◀ Cancel", callback_data="mincancel")]]
)

# The prompts tell the User to "send 0" to clear a floor; these are the natural
# ways to say the same thing, so a plain-worded reply isn't read as a number.
_CLEARING_WORDS = {"0", "off", "none", "clear"}


def register(router: Router) -> None:
    """All alert-control handlers. Called by build_router before the wallet
    paste handler so a pending min-size amount is consumed here first."""
    router.message.register(on_min_size_text, awaiting_min_size)
    router.callback_query.register(on_mute, F.data.startswith("mute:"))
    router.callback_query.register(on_unmute, F.data.startswith("unmute:"))
    router.callback_query.register(on_track_min_prompt, F.data.startswith("tmin:"))
    router.callback_query.register(on_global_min_prompt, F.data == "gmin")
    router.callback_query.register(on_min_cancel, F.data == "mincancel")


def awaiting_min_size(message: Message, min_size_pending: MinSizePending) -> bool:
    """True when this User has a min-size prompt armed. Commands are never
    swallowed — /help mid-prompt still answers, the prompt stays live."""
    if message.from_user is None or message.text is None or message.text.startswith("/"):
        return False
    return message.from_user.id in min_size_pending


async def on_mute(callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway) -> None:
    await _set_muted(callback, pool, gateway, muted=True)


async def on_unmute(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway
) -> None:
    await _set_muted(callback, pool, gateway, muted=False)


async def _set_muted(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, *, muted: bool
) -> None:
    prefix = "mute:" if muted else "unmute:"
    address = (callback.data or "").removeprefix(prefix)
    status = await pool.execute(
        "UPDATE tracks SET muted = $3 WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
        muted,
    )
    changed = status != "UPDATE 0"  # a stale button on an untracked wallet
    await _refresh_list_in_place(callback, pool, gateway, _mute_toast(changed, address, muted))


async def on_track_min_prompt(
    callback: CallbackQuery, pool: asyncpg.Pool, min_size_pending: MinSizePending
) -> None:
    address = (callback.data or "").removeprefix("tmin:")
    tracked = await pool.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    if not tracked:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    min_size_pending[callback.from_user.id] = address
    if isinstance(callback.message, Message):
        await callback.message.answer(
            f"💵 Minimum position size for {short_address(address)}.\n\n"
            "Send a dollar amount — e.g. 5000 — and I'll only alert you when this "
            "trader's position is at least that big. Send 0 to clear it and fall "
            "back to your global floor.",
            reply_markup=CANCEL_KB,
        )
    await callback.answer()


async def on_global_min_prompt(callback: CallbackQuery, min_size_pending: MinSizePending) -> None:
    min_size_pending[callback.from_user.id] = GLOBAL
    if isinstance(callback.message, Message):
        await callback.message.answer(
            "⚙️ Global minimum position size.\n\n"
            "Send a dollar amount — e.g. 5000 — to silence alerts for positions "
            "smaller than that across every tracked trader (unless a trader has "
            "its own floor). Send 0 to turn it off.",
            reply_markup=CANCEL_KB,
        )
    await callback.answer()


async def on_min_cancel(callback: CallbackQuery, min_size_pending: MinSizePending) -> None:
    min_size_pending.pop(callback.from_user.id, None)
    if isinstance(callback.message, Message):
        await callback.message.edit_text("Cancelled — nothing changed.")
    await callback.answer()


async def on_min_size_text(
    message: Message,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    min_size_pending: MinSizePending,
) -> None:
    """The one typed input of these flows: a dollar floor, per-Track or global."""
    user = message.from_user
    if user is None or message.text is None:
        return
    target = min_size_pending.get(user.id)
    if target is None:
        return  # unreachable while awaiting_min_size gates registration
    ok, value = parse_min_size(message.text)
    if not ok:
        await message.answer(
            "I couldn't read that as a dollar amount. Send a number like 5000, "
            "or 0 to turn the floor off."
        )
        return
    min_size_pending.pop(user.id, None)
    if target == GLOBAL:
        await pool.execute(
            "UPDATE users SET min_size_usd = $2 WHERE telegram_id = $1", user.id, value
        )
        confirmation = (
            f"⚙️ Global minimum set to ${value:,.0f}."
            if value is not None
            else "⚙️ Global minimum turned off."
        )
    else:
        await pool.execute(
            """
            UPDATE tracks SET min_size_usd = $3
            WHERE user_telegram_id = $1 AND trader_address = $2
            """,
            user.id,
            target,
            value,
        )
        confirmation = (
            f"💵 Minimum for {short_address(target)} set to ${value:,.0f}."
            if value is not None
            else f"💵 Cleared the minimum for {short_address(target)} — "
            "it now uses your global floor."
        )
    await _send_list(message, pool, gateway, user.id, confirmation)


def parse_min_size(text: str) -> tuple[bool, Decimal | None]:
    """Parse a typed floor. Returns (ok, value): ok=False is unreadable;
    ok=True with value=None clears the floor; otherwise a non-negative Decimal.
    Accepts a bare number, a leading $, and thousands separators."""
    cleaned = text.strip().lower().replace("$", "").replace(",", "").replace("_", "")
    if cleaned in _CLEARING_WORDS:
        return True, None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return False, None
    if not value.is_finite() or value < 0:
        return False, None
    if value == 0:
        return True, None  # zero is "no floor", same as clearing
    return True, value


def _mute_toast(changed: bool, address: str, muted: bool) -> str:
    if not changed:
        return "You're not tracking this trader."
    if muted:
        return f"Muted {short_address(address)} — alerts paused until you unmute."
    return f"Unmuted {short_address(address)}."


async def _refresh_list_in_place(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, toast: str
) -> None:
    # Deferred import: handlers imports this module lazily in build_router, so a
    # top-level import here would cycle.
    from epigone.bot.handlers import _render_tracked_list

    if isinstance(callback.message, Message):
        try:
            text, markup = await _render_tracked_list(pool, gateway, callback.from_user.id)
            await callback.message.edit_text(text, reply_markup=markup)
        except GatewayError:
            pass  # the control change itself succeeded; only the redraw is stale
    await callback.answer(toast)


async def _send_list(
    message: Message,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    user_id: int,
    confirmation: str,
) -> None:
    from epigone.bot.handlers import DATA_DELAYED_TEXT, _render_tracked_list

    try:
        text, markup = await _render_tracked_list(pool, gateway, user_id)
    except GatewayError:
        await message.answer(f"{confirmation}\n\n{DATA_DELAYED_TEXT}")
        return
    await message.answer(f"{confirmation}\n\n{text}", reply_markup=markup)
