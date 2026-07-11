"""Invite-only gate (issue #33): Epigone is private. An aiogram outer
middleware on the Dispatcher runs before every handler and lets an update
through only if the sender is the admin (Settings.admin_telegram_id) or on the
allowlist; everyone else gets a polite refusal and no handler runs. One
middleware seam — not a per-handler check — means no command can forget the
gate.

The admin manages the allowlist at runtime with /allow, /revoke and /allowed.
Those are handlers like any other, so the gate lets the admin through first
(the admin is always allowed); the handlers then enforce owner-only and refuse
anyone else — including an ordinary allowlisted User.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
from aiogram import BaseMiddleware, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message, TelegramObject, Update
from aiogram.types import User as TgUser
from aiogram.types.update import UpdateTypeLookupError

from epigone import allowlist

REFUSAL_TEXT = "Epigone is invite-only — ask the owner for access"
ADMIN_ONLY_TEXT = "That's an owner-only command."
ALLOW_USAGE_TEXT = "Usage: /allow <telegram_id>"
REVOKE_USAGE_TEXT = "Usage: /revoke <telegram_id>"
CANT_REVOKE_ADMIN_TEXT = "You're the owner — you can't revoke your own access."


def _update_user(event: Update) -> TgUser | None:
    """The Telegram User behind an update, across every update variant that
    carries one — message, edited_message, callback_query, inline_query,
    chat-member changes, and so on. Reading it off ``Update.event`` (the
    specific sub-object) means new handler-relevant types are gated
    automatically, with no per-type branch to forget. Update types with no
    sender (channel_post, poll, …) yield None; the gate then fails closed."""
    try:
        inner = event.event
    except UpdateTypeLookupError:
        return None  # An update type this aiogram doesn't know — nothing to gate.
    # Most updates expose the sender as ``from_user``; a few (reactions, poll
    # answers, chat-boosts) use ``user``. Either way, no User → fail closed.
    user = getattr(inner, "from_user", None) or getattr(inner, "user", None)
    return user if isinstance(user, TgUser) else None


async def _refuse(event: Update) -> None:
    """Tell the sender they're not invited, on whichever surface they used."""
    if event.message is not None:
        await event.message.answer(REFUSAL_TEXT)
    elif event.callback_query is not None:
        # A toast on the tapped button — callbacks have no message to reply to.
        await event.callback_query.answer(REFUSAL_TEXT, show_alert=True)


class AllowlistGate(BaseMiddleware):
    """Outer middleware on dp.update: the single seam every update passes
    through before routing. Reads pool and admin id from the dispatcher's
    workflow data, so it needs no constructor wiring."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Update):
            return await handler(event, data)

        user = _update_user(event)
        if user is None:
            # Fail closed (#40): an update whose sender we can't resolve is
            # dropped, never routed. Epigone is private and handles only
            # messages and callbacks; any other type is ungatable and so must
            # not reach a handler. Teaching the bot a new update type is then a
            # deliberate change that must also teach _update_user its sender.
            return None

        pool: asyncpg.Pool = data["pool"]
        admin_id: int | None = data.get("admin_telegram_id")
        if _is_admin(user, admin_id) or await allowlist.is_allowed(pool, user.id):
            return await handler(event, data)

        await _refuse(event)
        return None


def install_allowlist_gate(dp: Dispatcher) -> None:
    """Wire the gate as the outer seam on every update. Call before routing;
    the admin id and pool come from the dispatcher's workflow data."""
    dp.update.outer_middleware(AllowlistGate())


def _is_admin(user: TgUser | None, admin_telegram_id: int | None) -> bool:
    return user is not None and admin_telegram_id is not None and user.id == admin_telegram_id


def _parse_target(args: str | None) -> int | None:
    """The single Telegram id argument to /allow and /revoke, or None if the
    admin gave no (or a non-numeric) argument."""
    if not args:
        return None
    try:
        return int(args.split()[0])
    except ValueError:
        return None


async def cmd_allow(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT)
        return
    target = _parse_target(command.args)
    if target is None:
        await message.answer(ALLOW_USAGE_TEXT)
        return
    assert message.from_user is not None  # _is_admin guarantees it
    await allowlist.grant(pool, target, granted_by=message.from_user.id)
    await message.answer(f"✅ {target} can now use Epigone.")


async def cmd_revoke(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT)
        return
    target = _parse_target(command.args)
    if target is None:
        await message.answer(REVOKE_USAGE_TEXT)
        return
    if target == admin_telegram_id:
        # The owner is always allowed from config; revoking them would be a
        # no-op that reads as a lockout. Refuse it outright.
        await message.answer(CANT_REVOKE_ADMIN_TEXT)
        return
    removed = await allowlist.revoke(pool, target)
    if removed:
        await message.answer(f"🚫 {target} can no longer use Epigone.")
    else:
        await message.answer(f"{target} wasn't on the allowlist.")


async def cmd_allowed(
    message: Message,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT)
        return
    ids = await allowlist.list_allowed(pool)
    lines = [f"Owner: {admin_telegram_id}"]
    if ids:
        lines.append("Allowed:")
        lines.extend(f"  • {telegram_id}" for telegram_id in ids)
    else:
        lines.append("No one else is on the allowlist yet.")
    await message.answer("\n".join(lines))


def register(router: Router) -> None:
    """Register the admin-only allowlist commands. The gate (installed on the
    dispatcher) runs ahead of these; each handler still enforces owner-only."""
    router.message.register(cmd_allow, Command("allow"))
    router.message.register(cmd_revoke, Command("revoke"))
    router.message.register(cmd_allowed, Command("allowed"))
