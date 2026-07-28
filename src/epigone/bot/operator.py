"""The operator kill switch (issue #135): /kill and /resume.

/kill halts IMMEDIATELY and unconditionally — an emergency stop takes no
confirmation dialog. It writes the halt row (epigone.safety.halt) and
returns; the bot process holds no signer (ADR-0005 keeps keys in the
signing lanes), so the actual order sweep is the watchdog's, within one
watchdog cycle, and any future executor loop stops at its next is_halted
check. Positions are HELD, not closed — the documented unwind policy
(docs/runbooks/halt-and-unwind.md); the reply says so and points at the
master-wallet escape hatch.

/resume is the opposite: deliberately two-step (an inline confirm button),
because resuming is consent to trade again, not an undo. Nothing is
re-placed, and a still-stale executor heartbeat means the watchdog will
halt again within one cycle — resume does not override the switch.

Both are owner-only, gated exactly like the allowlist commands
(bot/access.py): the admin id from config, re-checked on the callback too —
callback payloads are client-forgeable."""

from datetime import timedelta

import asyncpg
from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from epigone.bot.access import ADMIN_ONLY_TEXT, _is_admin
from epigone.bot.delete import with_delete_button
from epigone.clock import Clock
from epigone.safety import heartbeat
from epigone.safety.audit import ExecutionAudit
from epigone.safety.halt import KILL_SOURCE, active_halt, request_halt, resume

RESUME_CONFIRM_CALLBACK = "resumeconfirm"
RESUME_CANCEL_CALLBACK = "resumecancel"

KILLED_TEXT = (
    "🛑 Execution halted.\n\n"
    "The watchdog sweeps every resting order within one cycle. Open positions "
    "are HELD, not closed (docs/runbooks/halt-and-unwind.md) — to flatten, use "
    "the master wallet on app.hyperliquid.xyz; Epigone will not do it for you "
    "while halted.\n\n"
    "/resume lifts the halt (asks to confirm)."
)
# The sweep is the watchdog's; a /kill while the watchdog is silent must say
# so instead of promising a sweep nobody will run. The window is deliberately
# generous (the monitor's staleness default) — a beating watchdog is never
# accused, a dead or never-ceremonied one always is.
WATCHDOG_SILENT_AFTER = timedelta(seconds=300)
WATCHDOG_SILENT_WARNING = (
    "⚠️ The watchdog has no recent heartbeat — the order sweep will NOT run. "
    "Cancel resting orders yourself from the master wallet, and start the "
    "watchdog service (compose profile `execution`)."
)
NOT_HALTED_TEXT = "Nothing to resume — execution isn't halted."
RESUME_PROMPT_TEXT = (
    "Resume trading?\n\n"
    "The halt lifts immediately. Swept orders are NOT re-placed, and if the "
    "executor heartbeat is still stale the watchdog halts again within one "
    "cycle."
)
RESUMED_TEXT = (
    "▶️ Halt lifted. Nothing was re-placed — what happens next is the "
    "executor's decision, order by order."
)
RESUME_CANCELLED_TEXT = "Still halted — resume cancelled."
RESUME_GONE_TEXT = "Nothing to resume — the halt was already lifted."

_RESUME_CONFIRM_KB = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Resume trading", callback_data=RESUME_CONFIRM_CALLBACK),
            InlineKeyboardButton(text="◀ Cancel", callback_data=RESUME_CANCEL_CALLBACK),
        ]
    ]
)


async def cmd_kill(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
) -> None:
    user = message.from_user
    if not _is_admin(user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    assert user is not None  # _is_admin guarantees it
    reason = f"operator /kill by {user.id}"
    if command.args:
        reason += f": {command.args.strip()}"
    halt, created = await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason=reason,
        requested_by=user.id,
    )
    if not created:
        await message.answer(
            f"Already halted — {halt.source} halt since "
            f"{halt.halted_at:%Y-%m-%d %H:%M:%S} UTC ({halt.reason}). "
            f"/resume lifts it.",
            reply_markup=with_delete_button(),
        )
        return
    reply = KILLED_TEXT
    beaten = await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS)
    if beaten is None or clock.now() - beaten > WATCHDOG_SILENT_AFTER:
        reply = f"{KILLED_TEXT}\n\n{WATCHDOG_SILENT_WARNING}"
    await message.answer(reply, reply_markup=with_delete_button())


async def cmd_resume(
    message: Message,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    if await active_halt(pool) is None:
        await message.answer(NOT_HALTED_TEXT, reply_markup=with_delete_button())
        return
    await message.answer(RESUME_PROMPT_TEXT, reply_markup=_RESUME_CONFIRM_KB)


async def on_resume_confirm(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(callback.from_user, admin_telegram_id):
        await callback.answer(ADMIN_ONLY_TEXT, show_alert=True)
        return
    closed = await resume(
        pool, clock, ExecutionAudit(pool, clock), resumed_by=callback.from_user.id
    )
    text = RESUME_GONE_TEXT if closed is None else RESUMED_TEXT
    if isinstance(callback.message, Message):
        await callback.message.edit_text(text, reply_markup=with_delete_button())
    await callback.answer()


async def on_resume_cancel(callback: CallbackQuery) -> None:
    if isinstance(callback.message, Message):
        await callback.message.edit_text(
            RESUME_CANCELLED_TEXT, reply_markup=with_delete_button()
        )
    await callback.answer()


def register(router: Router) -> None:
    """Owner-only kill-switch commands; the invite-only gate runs ahead of
    these on the dispatcher, and each handler still enforces owner-only."""
    router.message.register(cmd_kill, Command("kill"))
    router.message.register(cmd_resume, Command("resume"))
    router.callback_query.register(on_resume_confirm, F.data == RESUME_CONFIRM_CALLBACK)
    router.callback_query.register(on_resume_cancel, F.data == RESUME_CANCEL_CALLBACK)
