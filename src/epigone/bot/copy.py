"""Copy provisioning commands (issue #136, ADR-0007 decision 12): /copy,
/uncopy, /copies.

    /copy <leader> <allocation> <stake> <leverage> <mode> [tp% sl%]

The signature grew a LEVERAGE argument in A5 (amendment D-4) and the third
argument changed meaning under it: `<stake>` is the operator's MARGIN behind
each open, isolated per position, and the position it buys is that times the
mirrored leverage. The word in the usage text changed with the meaning
deliberately — a `/copy … 200 …` that used to open a $200 position now opens
$200 of margin at up to the backstop, and an operator re-running a habit
should read a different sentence, not the same one meaning something else.

OPERATOR-ONLY, HARD-GATED. The bot has other users; copy answers exactly one
Telegram id and everyone else gets the ordinary owner-only refusal. That gate
is one of two — the executor independently reads only mappings owned by the
operator id it was started with — because a single check in a command handler
is a check someone can route around later.

/copy CONFIRMS BEFORE ACTING. It moves money, so it takes the same
confirm-tap shape as /resume: the message states exactly what will happen and
a button commits it. Callback payloads are client-forgeable, so the operator
gate is re-checked on the tap.

WHAT /copy ACTUALLY DOES is write a row. The bot process holds no signer
(ADR-0005 keeps keys in the signing lanes), so it cannot create or fund a
sub-account; the execute process does that on its next loop and reports back
through the copy-notice queue. That is ADR-0002's seam applied to a command
that has to sign — and it is also what makes the command safe to repeat: the
mapping is the intent, the exchange state is the executor's business.

/uncopy stops event consumption and NEVER auto-flattens (decision 12,
consistent with decision 10's never-auto-fix): it reports what is still open
in the sub and leaves those positions to the operator.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import asyncpg
from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from epigone.bot.access import ADMIN_ONLY_TEXT, _is_admin
from epigone.bot.delete import with_delete_button
from epigone.clock import Clock
from epigone.execute import limits as risk_limits
from epigone.execute.episodes import live_episodes
from epigone.execute.policy import FIXED_LEVERAGE, MIRROR_LEVERAGE, RiskPolicy
from epigone.execute.subs import (
    BRACKET_MODE,
    COPY_MODES,
    DEFAULT_MODE,
    CopySubExistsError,
    all_subs,
    disable_sub,
    reenable_sub,
    register_sub,
)

log = logging.getLogger(__name__)

COPY_CONFIRM_PREFIX = "copyconfirm:"
COPY_CANCEL_CALLBACK = "copycancel"

USAGE = (
    "Usage: /copy &lt;leader&gt; &lt;allocation&gt; &lt;stake&gt; &lt;leverage&gt; "
    "&lt;mode&gt; [tp% sl%]\n\n"
    "  leader      — the wallet address to mirror\n"
    "  allocation  — dollars to fund the copy sub-account with (the hard "
    "exposure cap, enforced by the exchange)\n"
    "  stake       — YOUR margin per copied open, isolated per position. The "
    "position is stake × leverage, and the stake is the most it can lose\n"
    "  leverage    — mirror (the leader's own leverage on that position) or a "
    "number for fixed. Either way it is capped by /limits max_leverage and by "
    "the asset's own maximum\n"
    "  mode        — default (exit when the leader exits) or bracket "
    "(our own TP/SL, which ENDS the copy episode when it fires)\n\n"
    "Example: /copy 0xabc… 1000 100 mirror default\n"
    "  → $100 of margin behind each open; a leader at 10x gives a $1,000 "
    "position, and $100 is the worst case.\n"
    "Example: /copy 0xabc… 1000 100 5 bracket 10 5\n"
    "One-legged brackets are fine — use - for the leg you don't want:\n"
    "  /copy 0xabc… 1000 100 mirror bracket - 5   (stop only)"
)
NOT_COPYING_TEXT = "Not copying that wallet."
CANCELLED_TEXT = "Cancelled — nothing was funded and nothing is being copied."
# The confirm prompt's opening words. It is a mid-flow message carrying its
# own confirm/cancel keyboard, so it is exempt from the 🗑 delete row the
# way /resume's prompt is — and the exemption keys off this constant rather
# than a literal, so a wording change moves it.
CONFIRM_MARKER = "Fund a dedicated sub-account and copy"


@dataclass(frozen=True)
class CopyRequest:
    """One parsed /copy, held between the prompt and the confirm tap."""

    leader: str
    allocation: Decimal
    stake: Decimal
    leverage_mode: str
    fixed_leverage: int | None
    mode: str
    take_profit_pct: Decimal | None = None
    stop_loss_pct: Decimal | None = None

    @property
    def leverage_phrase(self) -> str:
        return (
            "mirroring the leader's own leverage"
            if self.fixed_leverage is None
            else f"a fixed {self.fixed_leverage}x"
        )


def _kb(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💸 Fund and start copying", callback_data=token),
                InlineKeyboardButton(text="◀ Cancel", callback_data=COPY_CANCEL_CALLBACK),
            ]
        ]
    )


async def cmd_copy(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
    copy_pending: dict[int, CopyRequest],
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    assert message.from_user is not None
    parsed = _parse(command.args or "")
    if isinstance(parsed, str):
        await message.answer(f"{parsed}\n\n{USAGE}", reply_markup=with_delete_button())
        return
    # Judged BEFORE the confirm, not after the tap: an operator should be told
    # the caps refuse this before being asked to approve it. Against the LIVE
    # limits row, not a hardcoded ceiling — the executor will judge the same
    # mapping against the same row on its next loop, and a command that
    # approved what the loop then declines would be a promise nobody keeps.
    verdict = RiskPolicy().judge_provisioning(
        allocation_usd=parsed.allocation,
        base_stake_usd=parsed.stake,
        limits=await risk_limits.load(pool),
    )
    if not verdict.allowed:
        await message.answer(
            f"🚫 {verdict.decision}", reply_markup=with_delete_button()
        )
        return
    copy_pending[message.from_user.id] = parsed
    await message.answer(_prompt(parsed), reply_markup=_kb(COPY_CONFIRM_PREFIX + "go"))


def _prompt(parsed: CopyRequest) -> str:
    lines = [
        f"{CONFIRM_MARKER} {parsed.leader}?",
        "",
        f"• A dedicated sub-account funded with ${parsed.allocation} — that "
        f"balance is the hard exposure cap for this leader, enforced by the "
        f"exchange, not by us.",
        f"• Every copied open puts up ${parsed.stake} of YOUR margin, isolated "
        f"per position — so ${parsed.stake} is the most any one copied "
        f"position can lose. Their scales and trims are mirrored as "
        f"percentages.",
        f"• Leverage: {parsed.leverage_phrase}, capped by /limits max_leverage "
        f"and by the asset's own maximum. The position is stake × that "
        f"leverage — "
        + (
            f"${parsed.stake} behind a 10x leader is a ${parsed.stake * 10} position."
            if parsed.fixed_leverage is None
            else f"here, ${parsed.stake * parsed.fixed_leverage} per position."
        ),
    ]
    if parsed.mode == BRACKET_MODE:
        legs = " / ".join(
            part
            for part in (
                None if parsed.take_profit_pct is None else f"TP {parsed.take_profit_pct}%",
                None if parsed.stop_loss_pct is None else f"SL {parsed.stop_loss_pct}%",
            )
            if part is not None
        )
        lines.append(
            f"• Bracket mode: {legs} on OUR fill price. If a bracket fires, that "
            f"copy episode is over — the leader's later scales and their eventual "
            f"close are skipped until they close and re-open."
        )
    else:
        lines.append("• Default mode: positions exit when the leader exits.")
    lines += [
        "",
        "Network: whichever the executor is pointed at — testnet unless "
        "EXECUTOR_ALLOW_MAINNET is set and the URL is the mainnet one "
        "(docs/runbooks/copy-execution.md).",
    ]
    return "\n".join(lines)


async def on_copy_confirm(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    clock: Clock,
    admin_telegram_id: int | None,
    copy_pending: dict[int, CopyRequest],
) -> None:
    # Re-checked on the tap: callback payloads are client-forgeable.
    if not _is_admin(callback.from_user, admin_telegram_id):
        await callback.answer(ADMIN_ONLY_TEXT, show_alert=True)
        return
    parsed = copy_pending.pop(callback.from_user.id, None)
    if parsed is None:
        text = "That copy prompt has expired — run /copy again."
    else:
        text = await _register(pool, clock, callback.from_user.id, parsed)
    if isinstance(callback.message, Message):
        await callback.message.edit_text(text, reply_markup=with_delete_button())
    await callback.answer()


async def _register(
    pool: asyncpg.Pool, clock: Clock, operator_id: int, parsed: CopyRequest
) -> str:
    leader = parsed.leader
    try:
        await register_sub(
            pool,
            operator_id=operator_id,
            leader_address=leader,
            sub_name=_sub_name(leader),
            allocation_usd=parsed.allocation,
            base_stake_usd=parsed.stake,
            leverage_mode=parsed.leverage_mode,
            fixed_leverage=parsed.fixed_leverage,
            copy_mode=parsed.mode,
            take_profit_pct=parsed.take_profit_pct,
            stop_loss_pct=parsed.stop_loss_pct,
            now=clock.now(),
        )
    except CopySubExistsError:
        # Re-copying reuses the existing mapping AND its sub-account: a second
        # sub would burn one of the master's ten slots forever (finding 10),
        # and sub-accounts cannot be deleted.
        reenabled = await reenable_sub(
            pool,
            operator_id=operator_id,
            leader_address=leader,
            allocation_usd=parsed.allocation,
            base_stake_usd=parsed.stake,
            leverage_mode=parsed.leverage_mode,
            fixed_leverage=parsed.fixed_leverage,
            copy_mode=parsed.mode,
            take_profit_pct=parsed.take_profit_pct,
            stop_loss_pct=parsed.stop_loss_pct,
        )
        if reenabled is None:  # pragma: no cover - the row exists by definition
            return "Could not re-enable that mapping — try /copy again."
        return (
            f"♻️ Re-enabled copying of {leader} on its existing sub-account "
            f"{reenabled.sub_address or '(not yet provisioned)'} — allocation "
            f"${reenabled.allocation_usd}, stake ${reenabled.base_stake_usd} per open, "
            f"{reenabled.leverage_summary}, mode {reenabled.copy_mode}."
        )
    except asyncpg.ForeignKeyViolationError:
        # copy_subs references traders: a wallet nobody has ever looked at has
        # no events to copy, so this is a typo, not a state to create.
        return (
            f"Epigone has never seen {leader}. Paste the address to open its "
            f"profile first — a wallet with no observed history has nothing to copy."
        )
    return (
        f"⏳ Copying {leader}. The executor will create and fund the sub-account on "
        f"its next loop and report back here — nothing is copied until it does."
    )


async def cmd_uncopy(
    message: Message,
    command: CommandObject,
    pool: asyncpg.Pool,
    admin_telegram_id: int | None,
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    assert message.from_user is not None
    leader = (command.args or "").strip().lower()
    if not leader:
        await message.answer(
            "Usage: /uncopy &lt;leader&gt;", reply_markup=with_delete_button()
        )
        return
    disabled = await disable_sub(
        pool, operator_id=message.from_user.id, leader_address=leader
    )
    if disabled is None:
        await message.answer(NOT_COPYING_TEXT, reply_markup=with_delete_button())
        return
    # Through the episodes module, which owns every query against that table —
    # a second copy of "what is still live" here would be one more place to
    # keep in step with the executor's own reading of it.
    open_episodes = len(await live_episodes(pool, disabled.id))
    reply = f"⏹ Stopped copying {leader}. No further events will be mirrored."
    if open_episodes:
        # Never auto-flatten (decision 12). Say what is still open and leave it
        # to the operator — the same never-auto-fix rule reconciliation obeys.
        reply += (
            f"\n\n⚠️ {open_episodes} position(s) are still OPEN in that sub-account. "
            f"They were NOT closed — disabling stops the copying, not the risk. "
            f"Close them from the master wallet if you want out."
        )
    await message.answer(reply, reply_markup=with_delete_button())


async def cmd_copies(
    message: Message, pool: asyncpg.Pool, admin_telegram_id: int | None
) -> None:
    if not _is_admin(message.from_user, admin_telegram_id):
        await message.answer(ADMIN_ONLY_TEXT, reply_markup=with_delete_button())
        return
    assert message.from_user is not None
    mappings = await all_subs(pool, message.from_user.id)
    if not mappings:
        await message.answer(
            "No copy mappings. /copy sets one up.", reply_markup=with_delete_button()
        )
        return
    lines = ["<b>Copy mappings</b>", ""]
    for sub in mappings:
        state = "▶️ copying" if sub.enabled else "⏸ stopped"
        if sub.enabled and not sub.is_provisioned:
            state = "⏳ provisioning"
        lines.append(
            f"{state} · {sub.leader_address}\n"
            f"   ${sub.allocation_usd} allocation · ${sub.base_stake_usd} stake · "
            f"{sub.leverage_summary} · {sub.copy_mode}"
        )
    await message.answer("\n".join(lines), reply_markup=with_delete_button())


async def on_copy_cancel(
    callback: CallbackQuery, copy_pending: dict[int, CopyRequest]
) -> None:
    if callback.from_user is not None:
        copy_pending.pop(callback.from_user.id, None)
    if isinstance(callback.message, Message):
        await callback.message.edit_text(CANCELLED_TEXT, reply_markup=with_delete_button())
    await callback.answer()


def _parse(raw: str) -> CopyRequest | str:
    """`<leader> <allocation> <stake> <leverage> <mode> [tp sl]`, or the
    sentence that says what is wrong with it."""
    parts = raw.split()
    if len(parts) < 5:
        return "Not enough arguments."
    leader, allocation, stake, leverage, mode = parts[:5]
    if not leader.startswith("0x") or len(leader) != 42:
        return f"{leader!r} is not a wallet address."
    if mode not in COPY_MODES:
        return f"Mode must be one of {', '.join(COPY_MODES)}."
    try:
        allocation_usd = Decimal(allocation)
        stake_usd = Decimal(stake)
    except InvalidOperation:
        return "Allocation and stake must be numbers."
    if allocation_usd <= 0 or stake_usd <= 0:
        return "Allocation and stake must be positive."
    parsed_leverage = _parse_leverage(leverage)
    if isinstance(parsed_leverage, str):
        return parsed_leverage
    leverage_mode, fixed_leverage = parsed_leverage
    if mode == DEFAULT_MODE:
        if len(parts) > 5:
            return "Default mode takes no TP/SL percentages."
        return CopyRequest(
            leader=leader.lower(),
            allocation=allocation_usd,
            stake=stake_usd,
            leverage_mode=leverage_mode,
            fixed_leverage=fixed_leverage,
            mode=mode,
        )
    if len(parts) != 7:
        return "Bracket mode takes a TP and an SL percentage (use - to omit one)."
    # ONE-LEGGED BRACKETS ARE LEGAL. Decision 6 calls them "its own OPTIONAL
    # TP% and SL%", and migration 0033 accepts either leg alone — a parser
    # that demanded both would be the only place in the system saying
    # otherwise. `-` omits a leg positionally, so the documented
    # `<tp%> <sl%>` order still reads left to right.
    tp = _optional_pct(parts[5])
    sl = _optional_pct(parts[6])
    if tp is _BAD or sl is _BAD:
        return "TP and SL must be numbers (percent), or - to omit that leg."
    assert not isinstance(tp, _Bad) and not isinstance(sl, _Bad)
    if tp is None and sl is None:
        return "Bracket mode with neither leg is just default mode — use that instead."
    if tp is not None and tp <= 0:
        return "TP must be a positive percent."
    if sl is not None and not (0 < sl < 100):
        return "SL must be between 0 and 100 percent."
    return CopyRequest(
        leader=leader.lower(),
        allocation=allocation_usd,
        stake=stake_usd,
        leverage_mode=leverage_mode,
        fixed_leverage=fixed_leverage,
        mode=mode,
        take_profit_pct=tp,
        stop_loss_pct=sl,
    )


def _parse_leverage(raw: str) -> tuple[str, int | None] | str:
    """`mirror`, or a whole number of x. Returns (mode, fixed) or the sentence
    that says what is wrong.

    WHOLE NUMBERS ONLY, because that is what the exchange takes:
    `updateLeverage` carries an integer, so a `2.5` accepted here would be
    silently truncated at the wire — a position half again the size the
    operator asked for, decided by a rounding rule nobody stated."""
    if raw == MIRROR_LEVERAGE:
        return MIRROR_LEVERAGE, None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return f"Leverage must be {MIRROR_LEVERAGE!r} or a whole number, got {raw!r}."
    if value != value.to_integral_value() or value < 1:
        return "Fixed leverage must be a whole number of at least 1."
    return FIXED_LEVERAGE, int(value)


class _Bad:
    """Sentinel: that argument was neither a number nor the `-` omission."""


_BAD = _Bad()


def _optional_pct(raw: str) -> "Decimal | None | _Bad":
    if raw == "-":
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return _BAD


def _sub_name(leader: str) -> str:
    """A stable, human-legible name for the sub-account. Derived from the
    leader's address rather than a counter: sub names cannot be recycled, and
    a name that names its leader is what the operator sees in the Hyperliquid
    UI."""
    return f"epicopy-{leader[2:10]}"


def register(router: Router) -> None:
    """Operator-only copy commands; the invite-only gate runs ahead of these
    on the dispatcher, and each handler still enforces operator-only."""
    router.message.register(cmd_copy, Command("copy"))
    router.message.register(cmd_uncopy, Command("uncopy"))
    router.message.register(cmd_copies, Command("copies"))
    router.callback_query.register(on_copy_confirm, F.data.startswith(COPY_CONFIRM_PREFIX))
    router.callback_query.register(on_copy_cancel, F.data == COPY_CANCEL_CALLBACK)
