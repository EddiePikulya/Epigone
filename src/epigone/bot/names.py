"""Per-user wallet names (issue #86): a User's own nickname for a wallet they
track, set from the wallet's positions/profile view.

The name lives on the Track (`tracks.name`), so it is per-User — never shared
with, nor visible to, another tracker of the same wallet — and unfollowing
forgets it (the row deletion takes the name with it, so a refollow starts
unnamed). NULL is "unnamed": the wallet reads as its bare short address, and
clearing a name writes NULL back.

Setting a name needs one typed line, so — like the min-size control
(bot/controls.py) and the criteria builder — a per-User pending marker in
dispatcher data routes the next message here (build_router registers this ahead
of the wallet-paste handler; commands still cut through). The ✏️ Rename button
on the positions/profile view arms it; a stored name then labels the wallet
wherever this User sees it (tracked list, positions header, alerts, first-data
notice) via format.trader_label.

Callback vocabulary: rename:{address} arms the prompt; nameclear:{address}
clears an existing name; namecancel drops a pending prompt. Payloads are
client-forgeable, so every write is scoped to the tapping User and a stale tap
updates nothing.
"""

import re

import asyncpg
from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from epigone.bot.format import short_address, trader_label

# rename_pending[user_id] is the address the User is typing a name for; absence
# means no prompt is armed. Losing it to a restart just cancels a half-set name
# — the same cheap, in-memory tradeoff the criteria drafts and min-size prompts
# make.
RenamePending = dict[int, str]

# A wallet name is one short label, read inline next to the short address. The
# ~32-char cap keeps that line tight; the bot rejects a longer one rather than
# silently truncating (mirroring the criteria name flow, bot/criteria.py).
MAX_NAME_LENGTH = 32

NAME_TOO_LONG_TEXT = (
    f"That name is too long — {MAX_NAME_LENGTH} characters max. Send a shorter one."
)
NAME_EMPTY_TEXT = "That name is empty. Send some text, or tap Clear name to remove it."

_WHITESPACE_RUN = re.compile(r"\s+")


def sanitize_name(text: str) -> str | None:
    """Clean a typed wallet name into a single-line label, or None for "clear".

    Drops control/non-printable characters, collapses every whitespace run
    (newlines and tabs included) into a single space, and trims — so a name can
    never smuggle line breaks or formatting into the inline label. Empty after
    cleaning means "no name". The length cap is applied by the caller against
    MAX_NAME_LENGTH, so an overlong name is rejected rather than quietly cut."""
    kept = "".join(ch for ch in text if ch.isprintable() or ch.isspace())
    collapsed = _WHITESPACE_RUN.sub(" ", kept).strip()
    return collapsed or None


async def set_track_name(pool: asyncpg.Pool, user_id: int, address: str, name: str | None) -> bool:
    """Set (name) or clear (name=None) this User's nickname for a tracked wallet.

    Scoped to (user, wallet), so it only ever touches the caller's own Track and
    never leaks across Users. Returns whether a Track row was updated — False
    means the User isn't tracking the wallet (a stale button), so nothing
    changed."""
    status = await pool.execute(
        "UPDATE tracks SET name = $3 WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
        name,
    )
    return bool(status != "UPDATE 0")


def register(router: Router) -> None:
    """The rename flow. Called by build_router before the wallet-paste handler so
    a pending typed name is consumed here first."""
    router.message.register(on_rename_text, awaiting_rename)
    router.callback_query.register(on_rename_prompt, F.data.startswith("rename:"))
    router.callback_query.register(on_name_clear, F.data.startswith("nameclear:"))
    router.callback_query.register(on_rename_cancel, F.data == "namecancel")


def awaiting_rename(message: Message, rename_pending: RenamePending) -> bool:
    """True when this User has a rename prompt armed. Commands are never
    swallowed — /help mid-prompt still answers, the prompt stays live."""
    if message.from_user is None or message.text is None or message.text.startswith("/"):
        return False
    return message.from_user.id in rename_pending


async def on_rename_prompt(
    callback: CallbackQuery, pool: asyncpg.Pool, rename_pending: RenamePending
) -> None:
    # Deferred import: handlers imports this module lazily in build_router, so a
    # top-level import here would cycle (the same dance controls.py does).
    from epigone.bot.handlers import fetch_track

    address = (callback.data or "").removeprefix("rename:")
    track = await fetch_track(pool, callback.from_user.id, address)
    if track is None:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    current: str | None = track["name"]
    rename_pending[callback.from_user.id] = address
    if isinstance(callback.message, Message):
        await callback.message.answer(
            _prompt_text(address, current),
            reply_markup=_prompt_kb(address, named=current is not None),
        )
    await callback.answer()


async def on_name_clear(
    callback: CallbackQuery, pool: asyncpg.Pool, rename_pending: RenamePending
) -> None:
    address = (callback.data or "").removeprefix("nameclear:")
    rename_pending.pop(callback.from_user.id, None)
    cleared = await set_track_name(pool, callback.from_user.id, address, None)
    if isinstance(callback.message, Message):
        await callback.message.edit_text(
            f"🏷 Name cleared — back to {short_address(address)}."
            if cleared
            else f"You're no longer tracking {short_address(address)}."
        )
    await callback.answer("Name cleared" if cleared else "You're not tracking this trader.")


async def on_rename_cancel(callback: CallbackQuery, rename_pending: RenamePending) -> None:
    rename_pending.pop(callback.from_user.id, None)
    if isinstance(callback.message, Message):
        await callback.message.edit_text("Cancelled — the name is unchanged.")
    await callback.answer()


async def on_rename_text(
    message: Message, pool: asyncpg.Pool, rename_pending: RenamePending
) -> None:
    """The one typed input of this flow: the nickname to store for the Track."""
    user = message.from_user
    if user is None or message.text is None:
        return
    address = rename_pending.get(user.id)
    if address is None:
        return  # unreachable while awaiting_rename gates registration
    name = sanitize_name(message.text)
    if name is None:
        # Whitespace-/control-only: keep the prompt live rather than silently
        # clearing — Clear name is the deliberate clear path.
        await message.answer(NAME_EMPTY_TEXT)
        return
    if len(name) > MAX_NAME_LENGTH:
        await message.answer(NAME_TOO_LONG_TEXT)
        return
    rename_pending.pop(user.id, None)
    saved = await set_track_name(pool, user.id, address, name)
    if not saved:  # unfollowed between arming the prompt and sending the name
        await message.answer(f"You're no longer tracking {short_address(address)}.")
        return
    await message.answer(
        f"✏️ Saved — you'll see this trader as {trader_label(name, address)} from now on."
    )


def _prompt_text(address: str, current: str | None) -> str:
    if current is not None:
        return (
            f"✏️ Rename {trader_label(current, address)}.\n\n"
            f"Send a new name (up to {MAX_NAME_LENGTH} characters) — e.g. silver guy — "
            "and I'll show it wherever this trader appears. Or tap Clear name to go "
            "back to the bare address."
        )
    return (
        f"✏️ Name {short_address(address)}.\n\n"
        f"Send a name (up to {MAX_NAME_LENGTH} characters) — e.g. silver guy — and I'll "
        "show it wherever this trader appears, so your list is easy to read."
    )


def _prompt_kb(address: str, *, named: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if named:
        rows.append(
            [InlineKeyboardButton(text="🏷 Clear name", callback_data=f"nameclear:{address}")]
        )
    rows.append([InlineKeyboardButton(text="◀ Cancel", callback_data="namecancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
