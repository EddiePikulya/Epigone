"""Position Alert delivery: the bot-side consumer of position_alerts (issue #4).

The stream poller queues one row per event per follower (ADR-0002: the
processes meet only in Postgres); the shared outbox drain (epigone.bot.outbox)
delivers undelivered rows oldest first and stamps delivered_at only after
Telegram accepts. Stamped rows are never re-delivered, so bot restarts are
duplicate-free; a crash in the instant between the Telegram call and the stamp
re-delivers that single alert — the at-least-once residue of an outbox without
delivery receipts.

Alert-noise redesign (issue #91): only opens, closes, and flips send a new
message. An add/trim (scale_in/scale_out) instead *edits* the position's
original open (or flip) alert — its "anchor" — appending an arrow (⬆️ add,
⬇️ trim) to a changes line that accumulates the position's life story in event
order. The bot learns the anchor's Telegram message id from the send result and
persists it (0013), so arrows keep landing on the right message across restarts.
If the anchor message is gone — the recipient tapped the 🗑 (#73), deleted it by
hand, or never had one (they followed after the open) — the scale is silently
dropped: a failed edit is the expected outcome there, not an error.

My-wallet copy alerts (issue #121): the one case where a scale is urgent is when
the follower copied that position. If their own linked wallet (the /mywallet
command) is currently holding the scaled coin, delivering a scale ALSO sends them
a full standalone message with the #57-style detail (prev → new size) — on top of
the arrow edit, which still happens regardless. The match is a coin check against
their linked wallet's snapshots at delivery time (_holds_coin); no linked wallet,
or a scale on a coin they aren't holding, means the arrow edit is all they get.

This module supplies only what is alert-specific: which rows to drain and how to
deliver one (a fresh send for open/close/flip, an anchor edit for a scale plus an
optional copy alert for a holder).
"""

import logging
from decimal import Decimal

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from epigone.bot.delete import with_delete_button
from epigone.bot.format import (
    button_label,
    held_for,
    signed_pct,
    signed_usd,
    trader_label,
)
from epigone.bot.outbox import (
    DELIVERY_INTERVAL_SECONDS,
    MAX_DELIVERY_ATTEMPTS,
    drain_outbox,
    run_drain_loop,
)
from epigone.clock import Clock

log = logging.getLogger(__name__)

__all__ = [
    "DELIVERY_INTERVAL_SECONDS",
    "MAX_DELIVERY_ATTEMPTS",
    "deliver_pending",
    "render_alert",
    "render_own_scale",
]

# The arrow a scale appends to its anchor's changes line: up for an add, down
# for a trim. Concatenated in event order (e.g. '⬆️⬇️⬆️'), they are the whole of
# a scale's representation on the anchor — the pre-#91 size/PnL detail is dropped
# there by design, but restored in the standalone copy alert (#121) for a
# follower who is holding the coin (render_own_scale).
SCALE_ARROWS = {"scale_in": "⬆️", "scale_out": "⬇️"}


async def run_delivery_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """The shared supervised drain loop over Position Alert delivery."""
    await run_drain_loop(lambda: deliver_pending(pool, bot, clock), clock, label="position alert")


async def deliver_pending(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Deliver every undelivered alert, oldest first. Returns the delivered count.

    Opens, closes, and flips send a fresh message; a scale edits its anchor's
    message in place (issue #91). Oldest-first ordering means an open is always
    delivered — and its message id captured — before any scale that targets it,
    so a scale queued in the same pass as its open still lands correctly."""

    async def deliver(bot: Bot, row: asyncpg.Record) -> None:
        if row["kind"] in SCALE_ARROWS:
            await _deliver_scale(pool, bot, row)
        else:
            await _deliver_anchor(pool, bot, row)

    return await drain_outbox(
        pool,
        bot,
        clock,
        table="position_alerts",
        fetch=_fetch_pending_alerts,
        deliver=deliver,
    )


async def _deliver_anchor(pool: asyncpg.Pool, bot: Bot, row: asyncpg.Record) -> None:
    """Send an open/close/flip message and, for an open or flip, persist the
    Telegram message id so later scales can edit it (issue #91). A scale that
    landed before this open was delivered has already appended to scale_arrows,
    so the sent text carries those arrows from the start — nothing is lost."""
    message = await bot.send_message(
        chat_id=row["user_telegram_id"],
        text=render_alert(row, row["scale_arrows"]),
        reply_markup=positions_button(row),
    )
    await pool.execute(
        "UPDATE position_alerts SET telegram_message_id = $2 WHERE id = $1",
        row["id"],
        message.message_id,
    )


async def _deliver_scale(pool: asyncpg.Pool, bot: Bot, row: asyncpg.Record) -> None:
    """Represent a scale as an arrow appended to its anchor's changes line (issue
    #91). The arrow is recorded on the anchor row first — so it survives even if
    the edit can't land right now — then the anchor message is edited in place.

    Silently a no-op when the anchor never existed (the recipient followed after
    the open) or its message is gone (🗑 #73, hand-deleted): a TelegramBadRequest
    on the edit is the expected outcome there, not an error to burn attempts on."""
    # My-wallet copy alert (#121): if this follower's own linked wallet is holding
    # the scaled coin, the scale is urgent — send them a full standalone message
    # in addition to the arrow. Sent BEFORE the anchor edit so the persistent
    # arrow trail is only mutated after this external send lands; a duplicate on a
    # retry is the outbox's documented at-least-once residue. Mute/min-size need
    # no check here — a suppressed scale never became a row to deliver (they are
    # filtered at queue time in the poller), so any scale that reaches delivery
    # already cleared this follower's controls.
    if await _holds_coin(pool, row["user_telegram_id"], row["coin"]):
        await bot.send_message(
            chat_id=row["user_telegram_id"],
            text=render_own_scale(row),
            reply_markup=positions_button(row),
        )
    anchor = await _find_anchor(pool, row)
    if anchor is None:
        return
    arrows = (anchor["scale_arrows"] or "") + SCALE_ARROWS[row["kind"]]
    await pool.execute(
        "UPDATE position_alerts SET scale_arrows = $2 WHERE id = $1", anchor["id"], arrows
    )
    if anchor["telegram_message_id"] is None:
        # The anchor is not on Telegram yet (its send failed or is still pending);
        # it will render these arrows when it is finally delivered. Nothing to edit.
        return
    try:
        await bot.edit_message_text(
            chat_id=row["user_telegram_id"],
            message_id=anchor["telegram_message_id"],
            text=render_alert(anchor, arrows),
            reply_markup=positions_button(anchor),
        )
    except TelegramBadRequest:
        log.debug(
            "scale alert %d: anchor message %s gone, edit skipped",
            row["id"],
            anchor["telegram_message_id"],
        )


async def _find_anchor(pool: asyncpg.Pool, row: asyncpg.Record) -> asyncpg.Record | None:
    """The open/flip alert a scale belongs to: this follower's current instance
    of (trader, coin). We take the latest instance-boundary event queued before
    the scale — open, flip, or close — and the anchor is that row, unless it is a
    close, which means the instance the scale refers to began at a re-open that
    left no alert (floor-suppressed, or the follower joined after it). Then there
    is no message to edit and we return None, exactly the silent-drop case in the
    module docstring. Checking the latest boundary (not just the latest open)
    stops an arrow from binding to a prior, already-closed instance's message.
    The rule reads only persisted rows, so it survives restarts; the joins mirror
    _fetch_pending_alerts so render_alert can label the edited message."""
    latest = await pool.fetchrow(
        """
        SELECT a.*, t.display_name, tr.name AS track_name
        FROM position_alerts a
        JOIN traders t ON t.address = a.trader_address
        LEFT JOIN tracks tr
            ON tr.trader_address = a.trader_address
            AND tr.user_telegram_id = a.user_telegram_id
        WHERE a.user_telegram_id = $1
            AND a.trader_address = $2
            AND a.coin = $3
            AND a.kind IN ('open', 'flip', 'close')
            AND a.id < $4
        ORDER BY a.id DESC
        LIMIT 1
        """,
        row["user_telegram_id"],
        row["trader_address"],
        row["coin"],
        row["id"],
    )
    if latest is None or latest["kind"] == "close":
        return None
    return latest


async def _holds_coin(pool: asyncpg.Pool, user_telegram_id: int, coin: str) -> bool:
    """The My-wallet copy-alert match predicate (#121): is this follower's own
    linked wallet currently holding an open position on `coin`?

    Decision note — coin match, not side match. A copier who is in SOL wants to
    know their leader is scaling SOL whether they are aligned or hedged, so the
    predicate matches on coin alone. Side-matching (compare the snapshot's side to
    the scale's) is the obvious refinement if copies are known to always mirror
    the leader's direction; it is kept a coin match until that holds. The check
    reads position_snapshots live and joins through users.linked_wallet, so
    unlinking (linked_wallet → NULL) stops matches immediately, even before the
    poller prunes the now-orphan snapshot."""
    return (
        await pool.fetchval(
            """
            SELECT 1 FROM users u
            JOIN position_snapshots ps
                ON ps.trader_address = u.linked_wallet AND ps.coin = $2
            WHERE u.telegram_id = $1
            """,
            user_telegram_id,
            coin,
        )
        is not None
    )


async def _fetch_pending_alerts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    # The display_name join lets render_alert label the Trader without a second
    # query per row. The tracks join adds the recipient's own per-Track nickname
    # (#86), scoped to this alert's follower, which takes precedence over the
    # leaderboard label; NULL when they've since unfollowed.
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT a.*, t.display_name, tr.name AS track_name
        FROM position_alerts a
        JOIN traders t ON t.address = a.trader_address
        LEFT JOIN tracks tr
            ON tr.trader_address = a.trader_address
            AND tr.user_telegram_id = a.user_telegram_id
        WHERE a.delivered_at IS NULL AND a.attempts < $1
        ORDER BY a.id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows


def positions_button(row: asyncpg.Record) -> InlineKeyboardMarkup:
    """Make the alert tap-through to the trader's live positions — the same
    on-demand view /tracked offers (the positions:<address> callback). An alert
    only ever fires for a Trader the recipient follows, which is exactly the
    relationship that handler checks, so the button always resolves. The label
    is the wallet's name when the recipient gave it one (#86, else the
    leaderboard display name) — the address lives in the alert text and the
    detailed view, not on the button."""
    address: str = row["trader_address"]
    return with_delete_button(
        InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=(
                            "📊 "
                            + button_label(row["track_name"] or row["display_name"], address)
                            + " — positions"
                        ),
                        callback_data=f"positions:{address}",
                    )
                ]
            ]
        )
    )


def render_alert(row: asyncpg.Record, scale_arrows: str | None = None) -> str:
    """The message text for an open/close/flip alert. `scale_arrows` is the
    anchor's accumulated arrow trail (issue #91): when present it renders as a
    changes line beneath the alert, so an edited open reads as the position's
    life story (e.g. a line ending '⬆️⬇️⬆️'). Only opens and flips ever carry
    arrows; a close is terminal and never edited."""
    # The recipient's own name for the wallet wins over the leaderboard label (#86).
    label = trader_label(row["track_name"] or row["display_name"], row["trader_address"])
    coin: str = row["coin"]
    kind: str = row["kind"]
    if kind == "open":
        text = f"🟢 {label} opened {coin} {_side(row['side'])} — {_new_leg(row)}"
    elif kind == "close":
        text = f"🔴 {label} closed {coin} {_side(row['prev_side'])} — {_closed_leg(row)}"
    else:
        text = (
            f"🔄 {label} flipped {coin} {_side(row['prev_side'])} → {_side(row['side'])} — "
            f"{_closed_leg(row)}; now {_side(row['side'])} {_new_leg(row)}"
        )
    if scale_arrows:
        text += f"\n{scale_arrows}"
    return text


def render_own_scale(row: asyncpg.Record) -> str:
    """The standalone My-wallet copy alert (#121): the full message a follower
    gets when a wallet they track scales a coin their own linked wallet is holding
    — the urgent case #91 collapsed into a bare arrow. It restores the pre-#91
    #57 scale detail (the size it grew/shrank from → to, plus live PnL) under a
    'you're in it' banner, so the follower can act on their own copied position."""
    label = trader_label(row["track_name"] or row["display_name"], row["trader_address"])
    coin: str = row["coin"]
    verb = "added to" if row["kind"] == "scale_in" else "trimmed"
    return f"⚠️ You're in {coin} — {label} {verb} {coin} {_side(row['side'])} — {_scale_leg(row)}"


def _side(side: str) -> str:
    return side.upper()


def _new_leg(row: asyncpg.Record) -> str:
    return f"${row['size_usd']:,.0f} at {row['leverage']}x, entry {row['entry_price']}"


def _scale_leg(row: asyncpg.Record) -> str:
    """A scale's detail (issue #57, restored for #121's copy alert): the size it
    grew from → to at what leverage, plus the position's live return on margin
    (#35) so the follower sees whether the trade is actually winning, not just how
    much it moved. PnL is omitted only when it isn't available."""
    prev: Decimal = row["prev_size_usd"]
    new: Decimal = row["size_usd"]
    leg = f"${prev:,.0f} → ${new:,.0f} at {row['leverage']}x"
    if row["pct_return"] is not None:
        leg += f", PnL {signed_pct(row['pct_return'])}"
    return leg


def _closed_leg(row: asyncpg.Record) -> str:
    """Realized PnL is the poller's last-observed uPnL (see epigone.stream.poller);
    the fields are nullable at the schema level, so render what is present."""
    parts = []
    if row["realized_pnl"] is not None:
        pnl = signed_usd(row["realized_pnl"])
        if row["pct_return"] is not None:
            pnl += f" ({signed_pct(row['pct_return'])})"
        parts.append(f"PnL {pnl}")
    if row["opened_at"] is not None:
        parts.append(f"held {held_for(row['opened_at'], row['created_at'])}")
    return ", ".join(parts)
