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

import logging
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
from epigone.safety.halt import (
    KILL_SOURCE,
    HaltContentionError,
    active_halt,
    request_halt,
    resume,
)

log = logging.getLogger(__name__)

# The confirm carries the halt id it was offered for (PR #143 review): a
# stale prompt must never lift a different, later halt — halt.resume() only
# closes the exact id, and ids never recycle (halt rows are history).
RESUME_CONFIRM_PREFIX = "resumeconfirm:"
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
RESUME_SWEEP_PENDING_LINE = (
    "⚠️ Sweep PENDING — the book has not been verified empty; resting orders "
    "may still be live."
)
RESUMED_TEXT = (
    "▶️ Halt lifted. Nothing was re-placed — what happens next is the "
    "executor's decision, order by order."
)
RESUME_CANCELLED_TEXT = "Still halted — resume cancelled."
KILL_CONTENTION_TEXT = (
    "⚠️ Halt state is being contended (halts opening and resuming "
    "concurrently) — run /kill again NOW."
)
KILL_FAILED_TEXT = (
    "🚨 /kill FAILED to record the halt (database error) — Epigone is NOT "
    "halted. Retry /kill now. If Postgres is down, the watchdog blind-sweeps "
    "resting orders after its threshold (WATCHDOG_DB_BLIND_SECONDS, "
    "halt-and-unwind runbook) — but the halt itself is not recorded until "
    "the database answers."
)
RESUME_GONE_TEXT = (
    "Nothing to resume — that halt is no longer the active one. If a newer "
    "halt is standing, run /resume again for it."
)


def _resume_confirm_kb(halt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Resume trading",
                    callback_data=f"{RESUME_CONFIRM_PREFIX}{halt_id}",
                ),
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
    try:
        halt, created = await request_halt(
            pool,
            clock,
            ExecutionAudit(pool, clock),
            source=KILL_SOURCE,
            reason=reason,
            requested_by=user.id,
        )
    except HaltContentionError:
        # /kill must ALWAYS answer — even the pathological livelock gets a
        # reply telling the operator to hit it again, never a silent crash.
        await message.answer(KILL_CONTENTION_TEXT, reply_markup=with_delete_button())
        return
    except Exception:
        # The realistic outage (connection refused, timeout): same rule.
        # The halt row did NOT commit — say so plainly; a silent crash here
        # would read as "maybe halted", the worst possible answer.
        log.exception("/kill could not record the halt")
        await message.answer(KILL_FAILED_TEXT, reply_markup=with_delete_button())
        return
    if not created:
        await message.answer(
            f"Already halted — {halt.source} halt since "
            f"{halt.halted_at:%Y-%m-%d %H:%M:%S} UTC ({halt.reason}). "
            f"/resume lifts it.",
            reply_markup=with_delete_button(),
        )
        return
    # From here the halt IS committed: every path below must still confirm
    # it (round 2 item 5) — a follow-up read failure must not turn a
    # successful kill into a "kill failed" in the operator's eyes.
    try:
        beaten = await heartbeat.last_beat(pool, heartbeat.WATCHDOG_PROCESS)
        silent = beaten is None or clock.now() - beaten > WATCHDOG_SILENT_AFTER
    except Exception:
        log.exception("/kill: watchdog heartbeat unreadable after the halt committed")
        silent = True  # cannot confirm a live watchdog — warn, don't promise
    reply = KILLED_TEXT if not silent else f"{KILLED_TEXT}\n\n{WATCHDOG_SILENT_WARNING}"
    await message.answer(reply, reply_markup=with_delete_button())


async def cmd_resume(
    message: Message,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    halt = await active_halt(pool)
    if halt is None:
        await message.answer(NOT_HALTED_TEXT, reply_markup=with_delete_button())
        return
    prompt = RESUME_PROMPT_TEXT
    if halt.swept_at is None:
        prompt = f"{RESUME_PROMPT_TEXT}\n\n{RESUME_SWEEP_PENDING_LINE}"
    await message.answer(prompt, reply_markup=_resume_confirm_kb(halt.id))


async def on_resume_confirm(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(callback.from_user, admin_telegram_id):
        await callback.answer(ADMIN_ONLY_TEXT, show_alert=True)
        return
    payload = (callback.data or "").removeprefix(RESUME_CONFIRM_PREFIX)
    closed = None
    # Callback data is client-forgeable: non-decimals (isdecimal, not
    # isdigit — "²" is a digit int() refuses) AND digit strings past
    # BIGINT's 18 safe digits are garbage (an overflow at bind time would
    # crash the handler instead of answering "stale").
    if payload.isdecimal() and len(payload) <= 18:
        closed = await resume(
            pool,
            clock,
            ExecutionAudit(pool, clock),
            halt_id=int(payload),
            resumed_by=callback.from_user.id,
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
    router.callback_query.register(on_resume_confirm, F.data.startswith(RESUME_CONFIRM_PREFIX))
    router.callback_query.register(on_resume_cancel, F.data == RESUME_CANCEL_CALLBACK)
