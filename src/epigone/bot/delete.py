"""One-tap message deletion (issue #73): a 🗑 button appended to the
informational/terminal messages the bot sends, whose callback deletes the very
message it rides on.

No state, no DB, no payload — the callback data is a bare constant and the
message it is attached to is the message to delete. A bot may delete its own
private-chat messages, but Telegram forbids deleting one older than 48 hours; a
tap on such a stale message raises TelegramBadRequest, which we answer with a
short notice rather than failing silently.

The health monitor shares the bot token but only sends (ADR-0002); its DMs' own
delete taps arrive here, at the bot process's polling loop, so this one handler
covers both senders.

Interactive flow prompts (the criteria builder, the min-size prompts) are
deliberately left without the button — they replace themselves as the flow
advances, and deleting a mid-flow prompt would only confuse the draft state.
"""

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

# A bare constant, matched exactly. It carries no payload — "delete the message
# this button is on" — and namespaces clear of every other callback prefix.
DELETE_CALLBACK = "msgdel"

DELETE_TOO_OLD_TOAST = "Too old for me to delete — remove it manually."


def delete_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🗑", callback_data=DELETE_CALLBACK)


def with_delete_button(markup: InlineKeyboardMarkup | None = None) -> InlineKeyboardMarkup:
    """Append the delete row beneath an existing keyboard, or build a delete-only
    keyboard when a message carries no buttons of its own. Never mutates the
    input — existing rows are copied, so callers can share a base markup."""
    rows = list(markup.inline_keyboard) if markup is not None else []
    return InlineKeyboardMarkup(inline_keyboard=[*rows, [delete_button()]])


async def on_delete_message(callback: CallbackQuery) -> None:
    """Delete the message this button rides on. Telegram refuses to delete a
    message older than 48h (TelegramBadRequest); surface that as a graceful
    notice instead of a silent failure. An inaccessible message (Telegram sends
    no body for very old ones) simply can't be acted on — answer and move on."""
    message = callback.message
    if isinstance(message, Message):
        try:
            await message.delete()
        except TelegramBadRequest:
            await callback.answer(DELETE_TOO_OLD_TOAST, show_alert=True)
            return
    await callback.answer()


def register(router: Router) -> None:
    router.callback_query.register(on_delete_message, F.data == DELETE_CALLBACK)
