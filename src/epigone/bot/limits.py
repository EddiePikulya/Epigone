"""The operator's window onto the risk policy (issue #137 §7): /limits.

    /limits                 — show every global knob and when it last moved
    /limits <knob> <value>  — move one, audited

OPERATOR-ONLY, hard-gated like every other copy command: the bot has other
users, and these numbers decide what Epigone will trade with the operator's own
money.

NO CONFIRM TAP, unlike /copy. The distinction is what the command DOES: /copy
moves money the moment it is confirmed, while this writes a number the executor
reads on its next loop — and the direction of a mistake is recoverable in one
message. What it does instead is state the change as old → new in the reply, so
a mistyped value is visible immediately rather than at the next open.

ONE KNOB PER COMMAND, deliberately. A multi-knob form would let a single typo
change a limit the operator was not thinking about, and the audit row would
record the pair as one intention when it was not.

EVERY CHANGE IS AUDITED, in `execution_audit` beside every other authorization
— who, which knob, old → new. The limits row itself keeps only the latest
`updated_by`/`updated_at`, because the trail is where history belongs.
"""

import logging

import asyncpg
from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from epigone.bot.access import ADMIN_ONLY_TEXT, _is_admin
from epigone.bot.delete import with_delete_button
from epigone.clock import Clock
from epigone.execute import limits as risk_limits
from epigone.safety.audit import OPERATOR_ACTOR, ExecutionAudit

log = logging.getLogger(__name__)

USAGE = (
    "Usage: /limits (show) or /limits &lt;knob&gt; &lt;value&gt;\n\n"
    + "\n".join(f"  {entry.name} — {entry.description}" for entry in risk_limits.KNOBS)
    + "\n\nExample: /limits coin_stake 250"
)


async def cmd_limits(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    assert message.from_user is not None
    parts = (command.args or "").split()
    if not parts:
        await message.answer(await _show(pool), reply_markup=with_delete_button())
        return
    if len(parts) != 2:
        await message.answer(USAGE, reply_markup=with_delete_button())
        return
    name, raw = parts
    # THE CHANGE AND ITS TRAIL COMMIT TOGETHER. A limit that moved with no
    # audit row is a limit nobody can account for afterwards, and a row
    # claiming a change that rolled back is worse — the same reason the
    # executor's claim and its attempt share one transaction (ADR-0006).
    try:
        async with pool.acquire() as conn, conn.transaction():
            before, after = await risk_limits.set_knob(
                conn, name=name, raw=raw, operator_id=message.from_user.id, now=clock.now()
            )
            entry = risk_limits.knob(name)
            old, new = risk_limits.render(before, entry), risk_limits.render(after, entry)
            await ExecutionAudit(pool, clock).record_event(
                actor=OPERATOR_ACTOR,
                action="risk_limit_changed",
                risk_decision=(
                    f"operator {message.from_user.id} set {entry.name}: {old} → {new} "
                    f"({entry.description})"
                ),
                detail={
                    "knob": entry.name,
                    "column": entry.column,
                    "old": old,
                    "new": new,
                    "operator_id": message.from_user.id,
                },
                conn=conn,
            )
    except (risk_limits.UnknownKnobError, ValueError) as exc:
        await message.answer(f"{exc}\n\n{USAGE}", reply_markup=with_delete_button())
        return
    await message.answer(
        f"✅ {entry.name}: {old} → {new}\n\n"
        f"{entry.description}. The executor re-reads these every cycle — no restart.",
        reply_markup=with_delete_button(),
    )


async def _show(pool: asyncpg.Pool) -> str:
    limits = await risk_limits.load(pool)
    lines = ["<b>Risk limits</b>", ""]
    for entry in risk_limits.KNOBS:
        lines.append(f"<b>{entry.name}</b> {risk_limits.render(limits, entry)}")
        lines.append(f"   {entry.description}")
    lines.append("")
    if limits.updated_at is None:
        # The seeded row, untouched. Worth saying: "nobody has ever moved
        # these" is different information from "someone set them and this is
        # what they chose".
        lines.append("Never changed — these are the shipped defaults.")
    else:
        lines.append(
            f"Last changed {limits.updated_at:%Y-%m-%d %H:%M:%S} UTC"
            + (f" by {limits.updated_by}" if limits.updated_by is not None else "")
        )
    lines.append("Per-sub knobs (stake, leverage mode) live on /copy, not here.")
    return "\n".join(lines)


def register(router: Router) -> None:
    """Operator-only; the invite-only gate runs ahead of this on the
    dispatcher, and the handler still enforces operator-only."""
    router.message.register(cmd_limits, Command("limits"))
