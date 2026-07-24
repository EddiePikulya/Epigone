import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

import asyncpg
from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
)

from epigone.bot.delete import with_delete_button
from epigone.bot.format import (
    button_label,
    display_coin,
    fills_open_age,
    open_age,
    order_lines,
    short_address,
    signed_pct,
    signed_usd,
    trader_header,
    trader_label,
    usd_compact,
)
from epigone.clock import Clock
from epigone.first_data_notice import record_follow_notice_state
from epigone.gateway import (
    GatewayError,
    HyperliquidGateway,
    OpenOrder,
    Position,
    Side,
    Window,
    fetch_open_orders,
    fetch_open_positions,
)
from epigone.ingest.fine import mark_due_on_follow
from epigone.metrics.fine import RoundTrip, reduce_trips
from epigone.metrics.library import format_duration
from epigone.plays import RANKED_PLAYS_SQL
from epigone.screener import ScreenerRow, run_screener

SCREENER_PAGE_SIZE = 5

# The stream poller can only sustain ~110 distinct tracked wallets across ALL
# Users within the shared weight budget (epigone.budget, #28; halved by the xyz
# builder-DEX's second poll, #21), so an unbounded per-User follow list lets one
# User exhaust the global ceiling. A per-User cap is the first guard (#23); tune
# this constant to retune it.
MAX_TRACKED_WALLETS = 15


class TrackOutcome(Enum):
    """The result of a Follow at the shared `track_address` seam. Three outcomes
    so every caller (paste / screener / profile) can word its own reply."""

    FRESHLY_TRACKED = "freshly_tracked"
    ALREADY_TRACKING = "already_tracking"
    LIMIT_REACHED = "limit_reached"


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
    "/criteria — build and save your own definition of “best”, then run it\n"
    "/screener — the best traders right now, ranked by 30-day ROI\n"
    "/start — what Epigone is and how it works\n"
    "/tracked — your tracked traders, their positions, and alert controls\n"
    "/help — this list\n\n"
    "Paste a wallet address (0x…) to open that trader's profile — positions, "
    "track record, and a Follow button.\n"
    "From /tracked you can mute a trader or set a minimum position size so "
    "small trades stay quiet."
)

SCREENER_HEADER = "🏆 Top traders — best 30-day ROI, bots excluded"

# A row without fine metrics is usually a strong candidate the fill-history pass
# hasn't reached yet, not a weak one (weak months sink on ROI). Frame it as
# in-progress, never as a verdict. The Criteria builder (#7) will let a User
# opt into fully-analyzed-only.
SCREENER_PENDING_LABEL = "⏳ analyzing"

SCREENER_EMPTY_TEXT = (
    "No traders to rank yet — the universe is still being scanned.\n"
    "Paste a wallet address (0x…) to pull up a trader's profile in the meantime."
)

INVALID_ADDRESS_TEXT = (
    "That doesn't look like a wallet address.\n\n"
    "Paste a full Hyperliquid address — 0x followed by 40 hex characters — "
    "and I'll open that trader's profile."
)

NOT_TRACKING_TEXT = (
    "You're not tracking any traders yet.\n\n"
    "Paste a wallet address (0x…) to open a trader's profile and follow your first one."
)

UNKNOWN_COMMAND_TEXT = "I don't know that command. Type /help to see what I can do."

DATA_DELAYED_TEXT = (
    "Hyperliquid data is delayed right now — your tracked list is safe, try again in a moment."
)

# Shown when a User at the cap tries to follow one more (#23). Every Follow now
# comes through a button (the screener row or the profile's Follow tap, #111), so
# the cap surfaces through a callback answer's toast — the pasted-address path no
# longer writes a Track, so its full-message form retired with it.
TRACK_LIMIT_TOAST = f"Limit reached — {MAX_TRACKED_WALLETS} wallets max. Unfollow one first."

_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")

# Our newest-fill knowledge is only as fresh as the wallet's last fine refresh.
# A tracked wallet is due a refresh every ACTIVE_REFRESH_INTERVAL (1 day, see
# epigone.ingest.scan); once that scan is older than a day we can't imply a live
# "last trade" time, so the line hedges ("as of last scan") rather than reading
# with false precision — the same honesty spirit as the ≥ open-age marker (#72).
FILLS_FRESH_WINDOW = timedelta(days=1)

# Shown once under an untracked wallet's positions when we can't honestly date at
# least one of them — no poller snapshot (never followed) and no verified fill
# episode to fall back on (#78). A position's open time is knowable only by
# observing the wallet over time, which is exactly what following starts, so the
# honest move is to invent no age and point there. Untracked profiles only: a
# follower already gets the poller's own age.
FOLLOW_FOR_AGE_HINT = "↳ Follow to track how long these positions have been open"


@dataclass(frozen=True)
class TrackWindow:
    """One span of the track-record toggle (#102): the callback token that
    carries it, the span it reduces over, the button label, the header clause
    that keeps the #101 header honest for the window, and — for the activity
    line (#104) — the coarse leaderboard window it reads and the label that
    names that span in the PnL/ROI clause."""

    key: str  # callback token; also the "all" sentinel's absence
    span: timedelta
    label: str  # button text
    header: str  # the "(…)" clause after "Track record"
    coarse_window: str  # coarse_metrics.time_window the activity line reads (#104)
    pnl_label: str  # the activity line's PnL/ROI span prefix ("week"/"month")


# The windows the toggle offers, most-recent first. `all` is not in this list —
# it is the default (window=None), always shown as its own button whenever any
# window button is (see _track_window_row). Each carries the coarse leaderboard
# window the activity line reads for it (#104): 7d → week, 30d → month.
TRACK_WINDOWS = (
    TrackWindow("7d", timedelta(days=7), "7d", "trades from the last 7 days", "week", "week"),
    TrackWindow("30d", timedelta(days=30), "30d", "trades from the last 30 days", "month", "month"),
)
_TRACK_WINDOWS_BY_KEY = {w.key: w for w in TRACK_WINDOWS}
ALL_WINDOW_KEY = "all"

# The activity line's coarse window and label for the default (All / window=None)
# view (#104): all-time, matching the default track record's all-time span.
ALL_ACTIVITY_COARSE_WINDOW = "allTime"
ALL_ACTIVITY_PNL_LABEL = "all-time"


def _activity_coarse_window(window: TrackWindow | None) -> tuple[str, str]:
    """The coarse leaderboard window and PnL/ROI label the activity line reads for
    a toggle position (#104): All (window=None) → all-time, 7d → week, 30d →
    month, each labeled to name the span it covers."""
    if window is None:
        return ALL_ACTIVITY_COARSE_WINDOW, ALL_ACTIVITY_PNL_LABEL
    return window.coarse_window, window.pnl_label


def _parse_track_window(token: str) -> TrackWindow | None:
    """A toggle callback token → its TrackWindow, or None for `all` (the default
    all-time view) and for any unknown token (a stale/garbled callback degrades
    to the safe default rather than erroring)."""
    return _TRACK_WINDOWS_BY_KEY.get(token)


async def _available_track_windows(
    pool: asyncpg.Pool, address: str, now: datetime
) -> list[TrackWindow]:
    """Which window buttons are meaningful for this wallet (#102): a window
    shows only when the stored history reaches beyond it (a round-trip closed
    before the cutoff, so the window is a real subset) AND at least one
    round-trip closed inside it (the window is non-empty). A wallet with five
    days of history gets neither 7d nor 30d — they would equal All; a wallet
    idle this week gets no 7d."""
    closes: list[datetime] = [
        r["closed_at"]
        for r in await pool.fetch(
            "SELECT closed_at FROM fine_trades WHERE address = $1", address
        )
    ]
    available: list[TrackWindow] = []
    for window in TRACK_WINDOWS:
        cutoff = now - window.span
        if any(c >= cutoff for c in closes) and any(c < cutoff for c in closes):
            available.append(window)
    return available


async def _track_window_row(
    pool: asyncpg.Pool, address: str, now: datetime, *, prefix: str, active: TrackWindow | None
) -> list[InlineKeyboardButton]:
    """The toggle row for a wallet view, or [] when no window is meaningful.
    `prefix` is the view's callback namespace (positions vs profile) so a tap
    re-renders the same view; `active` marks the currently-shown window so the
    row reflects state and a re-tap of the same window is a real (non-identical)
    edit. All is always present once any window is."""
    available = await _available_track_windows(pool, address, now)
    if not available:
        return []
    row: list[InlineKeyboardButton] = []
    for window in available:
        marked = active is not None and active.key == window.key
        row.append(
            InlineKeyboardButton(
                text=f"• {window.label}" if marked else window.label,
                callback_data=f"{prefix}:{window.key}:{address}",
            )
        )
    row.append(
        InlineKeyboardButton(
            text="• All" if active is None else "All",
            callback_data=f"{prefix}:{ALL_WINDOW_KEY}:{address}",
        )
    )
    return row


# Callback namespaces for the toggle row on each wallet-view path (#102). Kept
# distinct so a tap re-renders the exact view it came from — the positions view
# (a follower's) and the screener profile assemble different messages/keyboards.
POSITIONS_WINDOW_PREFIX = "poswin"
PROFILE_WINDOW_PREFIX = "profwin"


def _fills_stale(fills_seen_at: datetime | None, now: datetime) -> bool:
    """Is our fills knowledge older than the fresh window (or never scanned)?
    The one staleness test both fills-derived readings share — the activity
    line's last-trade time (#72) and a position's fills-derived open age (#78) —
    so the "as of last scan" hedge fires on the same rule in both."""
    return fills_seen_at is None or now - fills_seen_at > FILLS_FRESH_WINDOW


async def cmd_start(message: Message, pool: asyncpg.Pool) -> None:
    user = message.from_user
    if user is not None:
        await upsert_user(pool, user.id, user.username)
    await message.answer(START_TEXT, reply_markup=with_delete_button())


async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT, reply_markup=with_delete_button())


async def open_pasted_profile(
    message: Message, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    """A pasted valid address opens that Trader's profile — the same rich
    _render_profile view a screener row's profile tap shows, with a Follow/Unfollow
    toggle — rather than following outright (#111). No track row is written here:
    following is the deliberate ➕ tap (pfollow: → on_profile_follow). A wallet the
    User already tracks opens showing Unfollow, a shortcut to its full view instead
    of an "already tracking" dead-end. The gateway fetch (live positions) can be
    delayed; a GatewayError answers with the existing delayed-data message rather
    than crashing, exactly as the screener profile tap."""
    user = message.from_user
    if user is None or message.text is None:
        return
    address = message.text.strip().lower()
    try:
        text, entities, markup = await _render_profile(pool, gateway, clock, user.id, address)
    except GatewayError:
        await message.answer(DATA_DELAYED_TEXT, reply_markup=with_delete_button())
        return
    await message.answer(text, reply_markup=markup, entities=entities)


async def reject_unknown_command(message: Message) -> None:
    await message.answer(UNKNOWN_COMMAND_TEXT, reply_markup=with_delete_button())


async def reject_unrecognized_input(message: Message) -> None:
    await message.answer(INVALID_ADDRESS_TEXT, reply_markup=with_delete_button())


async def cmd_tracked(message: Message, pool: asyncpg.Pool, gateway: HyperliquidGateway) -> None:
    user = message.from_user
    if user is None:
        return
    try:
        text, markup = await _render_tracked_list(pool, gateway, user.id)
    except GatewayError:
        await message.answer(DATA_DELAYED_TEXT, reply_markup=with_delete_button())
        return
    await message.answer(text, reply_markup=markup)


async def on_positions(
    callback: CallbackQuery,
    bot: Bot,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
) -> None:
    """On-demand profile for a tracked Trader: current positions + track record.
    Fine metrics where the fine pass has run; coarse-only Traders say so. Opens
    on the all-time record; the toggle row windows it in place (#102)."""
    address = (callback.data or "").removeprefix("positions:")
    try:
        rendered = await _render_positions_view(
            pool, gateway, clock, callback.from_user.id, address, window=None
        )
    except GatewayError:
        await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
        return
    if rendered is None:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    view, entities, markup = rendered
    if isinstance(callback.message, Message):
        # the chat the button lives in
        await callback.message.answer(view, reply_markup=markup, entities=entities)
    else:
        await bot.send_message(
            chat_id=callback.from_user.id, text=view, reply_markup=markup, entities=entities
        )
    await callback.answer()


async def on_positions_window(
    callback: CallbackQuery,
    bot: Bot,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
) -> None:
    """The track-record window toggle on the positions view (#102): re-render the
    same view with the record reduced over the chosen window and edit it in
    place, preserving every other button."""
    token, _, address = (callback.data or "").removeprefix(
        f"{POSITIONS_WINDOW_PREFIX}:"
    ).partition(":")
    window = _parse_track_window(token)
    try:
        rendered = await _render_positions_view(
            pool, gateway, clock, callback.from_user.id, address, window=window
        )
    except GatewayError:
        await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
        return
    if rendered is None:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    view, entities, markup = rendered
    if isinstance(callback.message, Message):
        try:
            await callback.message.edit_text(view, reply_markup=markup, entities=entities)
        except TelegramBadRequest:
            pass  # a re-tap of the current window is a no-op edit; nothing to redraw
    await callback.answer()


async def _render_positions_view(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
    user_id: int,
    address: str,
    *,
    window: TrackWindow | None,
) -> tuple[str, list[MessageEntity], InlineKeyboardMarkup] | None:
    """Assemble the follower's positions view (#102 shares it between the first
    open and every window toggle). None when the User no longer tracks the
    wallet (a stale button). Raises GatewayError if positions can't be fetched.

    Rename (✏️, #86) and an unfollow escape hatch sit right here: seeing a
    Trader's positions is exactly when a User names them or decides they've gone
    bad and wants out (posunfollow drops the Track in place). The window toggle
    row leads the keyboard when any window is meaningful."""
    track = await fetch_track(pool, user_id, address)
    if track is None:
        return None
    positions = await fetch_open_positions(gateway, address)
    # Fetched on demand like the positions, and failing the same way: a
    # GatewayError here rides the caller's existing delayed-data answer (#115).
    orders_section = _render_open_orders(await fetch_open_orders(gateway, address))
    ages = await _position_ages(pool, address)
    fills = await _fills_open_episodes(pool, address)
    positions_text, entities = _render_positions(
        address, positions, ages, clock.now(), fills, name=track["name"]
    )
    if orders_section is not None:
        positions_text += f"\n\n{orders_section}"
    view = (
        positions_text
        + "\n"
        + await _activity_block(pool, address, clock.now(), window=window)
        + "\n\n"
        + await _render_track_record(pool, address, clock.now(), window=window)
    )
    keyboard: list[list[InlineKeyboardButton]] = []
    toggle = await _track_window_row(
        pool, address, clock.now(), prefix=POSITIONS_WINDOW_PREFIX, active=window
    )
    if toggle:
        keyboard.append(toggle)
    keyboard.append([InlineKeyboardButton(text="✏️ Rename", callback_data=f"rename:{address}")])
    keyboard.append(
        [InlineKeyboardButton(text="✖️ Unfollow", callback_data=f"posunfollow:{address}")]
    )
    return view, entities, with_delete_button(InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_positions_unfollow(callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock) -> None:
    """Unfollow straight from the positions view (📊): drop the Track, confirm in
    place, and remove the button — no jump to the list or profile. For the
    "their positions look bad now, I'm out" moment."""
    address = (callback.data or "").removeprefix("posunfollow:")
    async with pool.acquire() as conn, conn.transaction():
        removed = await untrack_address(conn, callback.from_user.id, address, clock.now())
    if removed and isinstance(callback.message, Message):
        body = callback.message.text or ""
        try:
            # Preserve the header's tap-to-copy entity (#93): the confirmation is
            # appended after the header, so the existing offsets still hold.
            await callback.message.edit_text(
                f"{body}\n\n✖️ Unfollowed — you'll no longer get alerts for this trader.",
                reply_markup=None,
                entities=callback.message.entities,
            )
        except TelegramBadRequest:
            pass  # the unfollow itself succeeded; only the in-place confirmation is stale
    await callback.answer(_unfollow_toast(removed, address))


async def on_unfollow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    """One-tap unfollow: drop the Track and refresh the list in place."""
    address = (callback.data or "").removeprefix("unfollow:")
    async with pool.acquire() as conn, conn.transaction():
        removed = await untrack_address(conn, callback.from_user.id, address, clock.now())
    if isinstance(callback.message, Message):
        try:
            text, markup = await _render_tracked_list(pool, gateway, callback.from_user.id)
            await callback.message.edit_text(text, reply_markup=markup)
        except GatewayError:
            pass  # the unfollow itself succeeded; only the list refresh is stale
    await callback.answer(_unfollow_toast(removed, address))


async def cmd_screener(message: Message, pool: asyncpg.Pool, clock: Clock) -> None:
    """The default Criteria: the Universe ranked by 30-day ROI, Bots excluded.
    A pure database read — zero Hyperliquid calls (issue #6 acceptance)."""
    user = message.from_user
    if user is None:
        return
    await upsert_user(pool, user.id, user.username)
    text, markup = await _render_screener_page(pool, user.id, clock, offset=0)
    await message.answer(text, reply_markup=markup)


async def on_screener_page(callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock) -> None:
    """Page through the ranking in place. Still a pure database read."""
    offset = _parse_offset((callback.data or "").removeprefix("screen:"))
    text, markup = await _render_screener_page(pool, callback.from_user.id, clock, offset=offset)
    if isinstance(callback.message, Message):
        await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer()


async def on_screener_follow(
    callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock, admin_telegram_id: int | None
) -> None:
    """Follow straight from a results row, then re-render the page so the row
    flips to Following. The offset rides in the callback data so the re-render
    lands on the same page."""
    offset_str, _, address = (callback.data or "").removeprefix("sfollow:").partition(":")
    offset = _parse_offset(offset_str)
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(
            conn,
            callback.from_user.id,
            callback.from_user.username,
            address,
            clock.now(),
            cap_exempt=callback.from_user.id == admin_telegram_id,
        )
    if isinstance(callback.message, Message):
        text, markup = await _render_screener_page(
            pool, callback.from_user.id, clock, offset=offset
        )
        await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer(follow_toast(outcome, address))


async def on_profile(
    callback: CallbackQuery,
    bot: Bot,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
) -> None:
    """Open a Trader's profile from the screener: coarse/fine metrics, freshness,
    current positions, and a follow/unfollow toggle. Reachable for any Trader,
    tracked or not — the one screener surface that touches the gateway, and only
    on this explicit tap."""
    address = (callback.data or "").removeprefix("profile:")
    try:
        text, entities, markup = await _render_profile(
            pool, gateway, clock, callback.from_user.id, address
        )
    except GatewayError:
        await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
        return
    if isinstance(callback.message, Message):
        await callback.message.answer(text, reply_markup=markup, entities=entities)
    else:
        await bot.send_message(
            chat_id=callback.from_user.id, text=text, reply_markup=markup, entities=entities
        )
    await callback.answer()


async def on_profile_window(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    """The track-record window toggle on the screener profile (#102): re-render
    the profile with the record reduced over the chosen window and edit it in
    place, keeping the follow/unfollow, rename and #93 header entity intact."""
    token, _, address = (callback.data or "").removeprefix(
        f"{PROFILE_WINDOW_PREFIX}:"
    ).partition(":")
    window = _parse_track_window(token)
    if isinstance(callback.message, Message):
        try:
            text, entities, markup = await _render_profile(
                pool, gateway, clock, callback.from_user.id, address, window=window
            )
            await callback.message.edit_text(text, reply_markup=markup, entities=entities)
        except GatewayError:
            await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
            return
        except TelegramBadRequest:
            pass  # a re-tap of the current window is a no-op edit; nothing to redraw
    await callback.answer()


async def on_profile_follow(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
    admin_telegram_id: int | None,
) -> None:
    address = (callback.data or "").removeprefix("pfollow:")
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(
            conn,
            callback.from_user.id,
            callback.from_user.username,
            address,
            clock.now(),
            cap_exempt=callback.from_user.id == admin_telegram_id,
        )
    await _refresh_profile_in_place(
        callback, pool, gateway, clock, address, follow_toast(outcome, address)
    )


async def on_profile_unfollow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    address = (callback.data or "").removeprefix("punfollow:")
    async with pool.acquire() as conn, conn.transaction():
        removed = await untrack_address(conn, callback.from_user.id, address, clock.now())
    await _refresh_profile_in_place(
        callback, pool, gateway, clock, address, _unfollow_toast(removed, address)
    )


async def _refresh_profile_in_place(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
    address: str,
    toast: str,
) -> None:
    if isinstance(callback.message, Message):
        try:
            text, entities, markup = await _render_profile(
                pool, gateway, clock, callback.from_user.id, address
            )
            await callback.message.edit_text(text, reply_markup=markup, entities=entities)
        except GatewayError:
            pass  # the follow/unfollow itself succeeded; only the redraw is stale
    await callback.answer(toast)


def _parse_offset(raw: str) -> int:
    """Callback offsets are self-authored, but never trust one into a negative."""
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


async def _render_screener_page(
    pool: asyncpg.Pool, user_id: int, clock: Clock, *, offset: int
) -> tuple[str, InlineKeyboardMarkup]:
    # One extra row tells us whether a next page exists without a second query.
    rows = await run_screener(
        pool, window=Window.MONTH, limit=SCREENER_PAGE_SIZE + 1, offset=offset
    )
    has_next = len(rows) > SCREENER_PAGE_SIZE
    rows = rows[:SCREENER_PAGE_SIZE]
    if not rows:
        return SCREENER_EMPTY_TEXT, with_delete_button()

    addresses = [r.address for r in rows]
    tracked = await tracked_set(pool, user_id, addresses)
    unfollowed = await unfollowed_set(pool, user_id, addresses)
    lines = [SCREENER_HEADER, ""]
    keyboard: list[list[InlineKeyboardButton]] = []
    for rank, row in enumerate(rows, start=offset + 1):
        followed = row.address in tracked
        name = row.display_name or short_address(row.address)
        lines.append(f"{rank}. {name}{previously_marker(row.address, followed, unfollowed)}")
        lines.append(f"    {_screener_stats(row, clock.now())}")
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"📊 {short_address(row.address)}", callback_data=f"profile:{row.address}"
                ),
                InlineKeyboardButton(
                    text="✓ Following" if followed else "➕ Follow",
                    callback_data=f"sfollow:{offset}:{row.address}",
                ),
            ]
        )
    nav: list[InlineKeyboardButton] = []
    if offset > 0:
        prev_offset = max(0, offset - SCREENER_PAGE_SIZE)
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"screen:{prev_offset}"))
    if has_next:
        nav.append(
            InlineKeyboardButton(
                text="Next ▶", callback_data=f"screen:{offset + SCREENER_PAGE_SIZE}"
            )
        )
    if nav:
        keyboard.append(nav)
    return "\n".join(lines), with_delete_button(InlineKeyboardMarkup(inline_keyboard=keyboard))


def previously_marker(address: str, followed: bool, unfollowed: set[str]) -> str:
    """The `↩` a results row carries when this User followed the wallet before and
    dropped it (#99) — a nudge that it is a past experiment, with the detail on the
    profile. Shared by the screener and criteria row renderers so the two never
    drift. Empty while currently tracked: a live Track keeps its Following state,
    so the marker is only for previously-but-not-currently-followed."""
    if not followed and address in unfollowed:
        return f" {PREVIOUSLY_FOLLOWED_MARKER}"
    return ""


def _screener_stats(row: ScreenerRow, now: datetime) -> str:
    """One line of key stats per row: ROI and PnL always, win rate where the
    fine pass has run (else a 'still analyzing' marker — issue #8, framed as
    pending not a quality verdict), and how fresh the shown metrics are so a
    User knows whether it is today's picture or last week's (issue #11)."""
    parts = [f"ROI {signed_pct(row.roi)}", f"PnL {signed_usd(row.pnl)}"]
    if row.win_rate is not None:
        parts.append(f"{row.win_rate:.0%} win")
    elif not row.fine_available:
        parts.append(SCREENER_PENDING_LABEL)
    parts.append(f"🕒 {_relative_age(now, _row_freshness(row))}")
    return " · ".join(parts)


def _row_freshness(row: ScreenerRow) -> datetime:
    """The freshest of a row's passes — the age the User cares about. Coarse
    metrics are always present; a later fine pass supersedes them."""
    if row.fine_computed_at is not None and row.fine_computed_at > row.coarse_computed_at:
        return row.fine_computed_at
    return row.coarse_computed_at


async def fetch_track(
    pool: asyncpg.Pool, user_id: int, address: str
) -> asyncpg.Record | None:
    """A User's Track for one wallet — the row (currently just the per-User name,
    #86), or None when they don't track it. The one scoped (user, wallet) lookup
    the positions view, the profile view, and the rename flow (bot/names.py) all
    share, so "am I tracking this?" and "what did I name it?" answer from a single
    query."""
    return await pool.fetchrow(
        "SELECT name FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )


async def tracked_set(pool: asyncpg.Pool, user_id: int, addresses: list[str]) -> set[str]:
    if not addresses:
        return set()
    rows = await pool.fetch(
        """
        SELECT trader_address FROM tracks
        WHERE user_telegram_id = $1 AND trader_address = ANY($2::text[])
        """,
        user_id,
        addresses,
    )
    return {row["trader_address"] for row in rows}


async def unfollowed_set(pool: asyncpg.Pool, user_id: int, addresses: list[str]) -> set[str]:
    """The subset of `addresses` this User has unfollowed at some point (#99). The
    screener/criteria rows mark these — but only the ones not *currently* tracked;
    the caller subtracts `tracked_set`, since a re-followed wallet keeps its
    Following state and hides the marker. Per-User: the log is user-scoped."""
    if not addresses:
        return set()
    rows = await pool.fetch(
        """
        SELECT trader_address FROM unfollows
        WHERE user_telegram_id = $1 AND trader_address = ANY($2::text[])
        """,
        user_id,
        addresses,
    )
    return {row["trader_address"] for row in rows}


# The glyph that flags a previously-but-not-currently-followed wallet, shared by
# the profile line and the screener/criteria row marker so the two never drift.
PREVIOUSLY_FOLLOWED_MARKER = "↩"


async def _previously_followed_line(
    pool: asyncpg.Pool, user_id: int, address: str, now: datetime
) -> str | None:
    """The profile's "you dropped this one" line (#99): `↩ Previously followed —
    unfollowed 3d ago (as "avax")`, the name clause only when the wallet carried
    one at unfollow. None when this User never unfollowed it — the caller also
    suppresses it while the wallet is currently tracked."""
    row = await pool.fetchrow(
        "SELECT unfollowed_at, name FROM unfollows "
        "WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )
    if row is None:
        return None
    age = _relative_age(now, row["unfollowed_at"])
    line = f"{PREVIOUSLY_FOLLOWED_MARKER} Previously followed — unfollowed {age}"
    if row["name"] is not None:
        line += f' (as "{row["name"]}")'
    return line


async def _render_profile(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
    user_id: int,
    address: str,
    *,
    window: TrackWindow | None = None,
) -> tuple[str, list[MessageEntity], InlineKeyboardMarkup]:
    """The screener profile view. `window` is the track-record toggle (#102):
    None opens on the all-time record, a TrackWindow windows it in place while
    keeping every other button (follow/unfollow, rename, the #93 header
    entity)."""
    positions = await fetch_open_positions(gateway, address)  # may raise GatewayError
    # On demand for tracked and untracked wallets alike, degrading exactly like
    # the positions fetch: a GatewayError rides the caller's delayed-data answer.
    orders_section = _render_open_orders(await fetch_open_orders(gateway, address))
    track = await fetch_track(pool, user_id, address)
    followed = track is not None
    name: str | None = track["name"] if track is not None else None
    ages = await _position_ages(pool, address)
    fills = await _fills_open_episodes(pool, address)
    positions_text, entities = _render_positions(
        address, positions, ages, clock.now(), fills, name=name, offer_follow=not followed
    )
    parts = [positions_text]
    if orders_section is not None:
        parts.append(orders_section)
    # The "previously followed" note (#99) sits right under the header — but only
    # for a wallet this User dropped and hasn't re-followed; a live Track hides it.
    if not followed:
        previously = await _previously_followed_line(pool, user_id, address, clock.now())
        if previously is not None:
            parts.append(previously)
    parts.extend(
        [
            await _activity_block(pool, address, clock.now(), window=window),
            await _render_track_record(pool, address, clock.now(), window=window),
        ]
    )
    freshness = await _metric_freshness(pool, clock, address)
    if freshness is not None:
        parts.append(freshness)
    keyboard: list[list[InlineKeyboardButton]] = []
    toggle = await _track_window_row(
        pool, address, clock.now(), prefix=PROFILE_WINDOW_PREFIX, active=window
    )
    if toggle:
        keyboard.append(toggle)
    # Renaming needs a Track, so ✏️ shows only for a followed wallet (#86).
    if followed:
        keyboard.append([InlineKeyboardButton(text="✏️ Rename", callback_data=f"rename:{address}")])
        keyboard.append(
            [InlineKeyboardButton(text="✖️ Unfollow", callback_data=f"punfollow:{address}")]
        )
    else:
        keyboard.append(
            [InlineKeyboardButton(text="➕ Follow", callback_data=f"pfollow:{address}")]
        )
    return (
        "\n\n".join(parts),
        entities,
        with_delete_button(InlineKeyboardMarkup(inline_keyboard=keyboard)),
    )


async def _metric_freshness(pool: asyncpg.Pool, clock: Clock, address: str) -> str | None:
    """How stale the shown metrics are — the freshest of the coarse/fine passes.
    None when the Trader has never been scanned."""
    latest = await pool.fetchval(
        """
        SELECT greatest(
            (SELECT max(computed_at) FROM coarse_metrics WHERE address = $1),
            (SELECT computed_at FROM fine_metrics WHERE address = $1)
        )
        """,
        address,
    )
    if latest is None:
        return None
    return f"🕒 Metrics updated {_relative_age(clock.now(), latest)}"


async def _activity_block(
    pool: asyncpg.Pool, address: str, now: datetime, *, window: TrackWindow | None = None
) -> str:
    """The shared activity block both view-assembly paths render — on_positions
    (a follower's positions view) and _render_profile (the screener profile). The
    #72 last-trade/performance line always, plus the #80 most-played line
    right beneath it when the fine store has coins to rank. Assembling it once here
    keeps the two paths from drifting apart (the PR #77 regression: a line added to
    only one path).

    `window` is the track-record toggle (#104): it selects the coarse leaderboard
    window the performance line reads (All → all-time, 7d → week, 30d → month), so
    the line re-renders in step with the record beneath it. Most-played and the
    last-trade recency are window-independent."""
    lines = [await _recent_activity(pool, address, now, window=window)]
    most_played = await _most_played(pool, address)
    if most_played is not None:
        lines.append(most_played)
    return "\n".join(lines)


async def _recent_activity(
    pool: asyncpg.Pool, address: str, now: datetime, *, window: TrackWindow | None = None
) -> str:
    """The positions view's activity line: when the wallet last traded and how it
    has been doing lately (issue #72).

    Last trade is the fine store's newest folded *perp* fill (fine_metrics.window_end,
    perp-only by construction — spot fills never advance it), qualified by how fresh
    that fills scan is (computed_at). PnL/ROI are the coarse leaderboard window the
    toggle selects (#104: All → all-time, 7d → week, 30d → month), which exists even
    for wallets the fine pass hasn't reached, so the line never depends on fine
    availability. `account_value` rides the same coarse row (identical across windows
    — one leaderboard entry seeds them all), so it reads unchanged across toggles."""
    coarse_window, label = _activity_coarse_window(window)
    row = await pool.fetchrow(
        """
        SELECT fm.window_end AS last_trade_at,
               fm.computed_at AS fills_seen_at,
               cm.pnl AS pnl,
               cm.roi AS roi,
               cm.account_value AS account_value
        FROM (SELECT $1::text AS address) a
        LEFT JOIN fine_metrics fm ON fm.address = a.address
        LEFT JOIN coarse_metrics cm ON cm.address = a.address AND cm.time_window = $2
        """,
        address,
        coarse_window,
    )
    return _render_recent_activity(
        row["last_trade_at"],
        row["fills_seen_at"],
        row["pnl"],
        row["roi"],
        row["account_value"],
        now,
        label,
    )


def _render_recent_activity(
    last_trade_at: datetime | None,
    fills_seen_at: datetime | None,
    pnl: Decimal | None,
    roi: Decimal | None,
    account_value: Decimal | None,
    now: datetime,
    label: str = ALL_ACTIVITY_PNL_LABEL,
) -> str:
    """Render the last-trade + performance line from already-fetched values.

    `last_trade_at` is the newest perp fill we've folded; `fills_seen_at` is when
    that fills knowledge was last refreshed. A wallet with no captured perp fills
    (`last_trade_at is None`) says so plainly. The PnL/ROI ride along when the
    coarse leaderboard has them — ROI is a fraction (0.12 == 12%) — labeled by
    `label` to name the window they cover (#104: "week"/"month"/"all-time").
    `account_value` is the coarse denominator (#85): PnL, ROI and position sizes
    all read against it, so it trails the line when the coarse row carries it and
    is omitted when absent (it exists even without fine data)."""
    if last_trade_at is None:
        parts = ["No recent trading activity seen"]
    else:
        age = _relative_age(now, last_trade_at)
        stale = _fills_stale(fills_seen_at, now)
        parts = [f"Last trade: {age} (as of last scan)" if stale else f"Last trade: {age}"]
    if pnl is not None and roi is not None:
        parts.append(f"{label} PnL {signed_usd(pnl)} (ROI {signed_pct(roi)})")
    if account_value is not None:
        parts.append(f"account {usd_compact(account_value)}")
    return " · ".join(parts)


# How many tickers the "Most played" line names (#80). Three is enough to read a
# wallet at a glance — a SOL specialist, a BTC whale, a rotates-everything account
# — without turning the line into a coin dump.
MOST_PLAYED_LIMIT = 3


async def _most_played(pool: asyncpg.Pool, address: str) -> str | None:
    """The wallet's most-played tickers line (#80): its top coins over the fill
    window, ranked by completed round-trips with a currently-open episode counting
    toward its coin — the shared ranking (epigone.plays) the focus-market ticker
    filter (#108) also runs on. Perp-only and TWAP-complete by construction (the
    fine store, #63). None — so the caller omits the line entirely — for a wallet
    with no fine round-trips or open episodes; never an empty "Most played:"."""
    rows = await pool.fetch(
        f"""
        SELECT coin, trips, is_open
        FROM ({RANKED_PLAYS_SQL}) plays
        WHERE address = $1
        ORDER BY play_rank
        """,
        address,
    )
    return _render_most_played([(r["coin"], r["trips"], r["is_open"]) for r in rows])


def _render_most_played(plays: list[tuple[str, int, bool]]) -> str | None:
    """Render `(coin, round_trip_count, is_open)` rows — already ordered by the
    shared #80 ranking (epigone.plays: round-trips plus an open-episode point,
    coin-name tiebreak) — as "Most played: SOL · BTC · ETH". Dex-prefixed
    builder-DEX coins (xyz:SP500) render as the bare ticker (#21). When
    completed round-trips exist, the effective-coins spread (#95) trails as
    "(~2 coins)" — the coins and the number tell one story. None when there is
    nothing to rank — the line is then omitted rather than shown empty."""
    if not plays:
        return None
    top = [display_coin(coin) for coin, _, _ in plays[:MOST_PLAYED_LIMIT]]
    line = "Most played: " + " · ".join(top)
    effective = _effective_coins([count for _, count, _ in plays])
    if effective is not None:
        line += f" (~{effective} coins)"
    return line


def _effective_coins(trip_counts: list[int]) -> str | None:
    """The effective-coins annotation (#95): the inverse Herfindahl of the
    round-trip counts (`total² / Σ counts²`, matching fine._effective_coins),
    rendered to one decimal — "2", "1.6". None when there are no completed
    round-trips (open-only coins contribute 0), so the caller drops the
    annotation rather than dividing by zero."""
    total = sum(trip_counts)
    if not total:
        return None
    effective = total * total / sum(count * count for count in trip_counts)
    return f"{effective:.1f}".rstrip("0").rstrip(".")


def _relative_age(now: datetime, then: datetime) -> str:
    seconds = (now - then).total_seconds()
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def _is_wallet_paste(message: Message) -> bool:
    return message.text is not None and _ADDRESS_RE.fullmatch(message.text.strip()) is not None


def _is_command(message: Message) -> bool:
    return message.text is not None and message.text.startswith("/")


async def track_address(
    conn: asyncpg.Connection,
    telegram_id: int,
    username: str | None,
    address: str,
    now: datetime,
    *,
    cap_exempt: bool = False,
) -> TrackOutcome:
    """Follow `address` for a User; idempotent. Returns the follow's outcome.

    The single write behind every Follow — pasting (#3), the screener row, and
    the profile toggle (#6). A Track is exactly what the alert poller (#4)
    reads, so following from results feeds the alert pipeline for free.

    The per-User cap (#23) is enforced here so all three paths share one check.
    Re-touching an already-tracked wallet is always allowed (idempotent, never
    counts as a new follow), even at the cap — so the already-tracking test
    comes before the count check. `cap_exempt` waives the cap: callers pass
    `cap_exempt=<follower> == admin_telegram_id`, so only the owner (#33)
    follows without limit — every extra tracked wallet costs poller budget, so
    the waiver stays admin-only rather than a tier anyone can reach."""
    await upsert_user(conn, telegram_id, username)
    already_tracking = await conn.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        telegram_id,
        address,
    )
    if already_tracking:
        return TrackOutcome.ALREADY_TRACKING
    tracked_count = await conn.fetchval(
        "SELECT count(*) FROM tracks WHERE user_telegram_id = $1",
        telegram_id,
    )
    if not cap_exempt and tracked_count >= MAX_TRACKED_WALLETS:
        return TrackOutcome.LIMIT_REACHED
    await conn.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        now,
    )
    await conn.execute(
        """
        INSERT INTO tracks (user_telegram_id, trader_address)
        VALUES ($1, $2) ON CONFLICT DO NOTHING
        """,
        telegram_id,
        address,
    )
    # A fresh Follow makes the wallet due now, so its fine data refreshes within
    # minutes instead of on the daily cadence (issue #82) — a recently-scanned
    # wallet is left alone. Postgres-only; the ingest picks it up (ADR-0002).
    await mark_due_on_follow(conn, address, now)
    # Settle this pair's one-time "first data landed" notice (issue #83): pending
    # if the wallet is not yet scanned (notify when its first data lands),
    # suppressed if the data is already there. Only on a genuinely new Track — a
    # re-follow returned ALREADY_TRACKING above, so its row is never reset.
    await record_follow_notice_state(conn, telegram_id, address, now)
    return TrackOutcome.FRESHLY_TRACKED


async def untrack_address(
    conn: asyncpg.Connection, telegram_id: int, address: str, now: datetime
) -> bool:
    """Drop a User's Track and remember the unfollow. Returns whether a Track was
    actually removed — False on a stale button tap that deletes nothing.

    The single write behind every Unfollow — the tracked-list button, the profile
    toggle, and the positions view — the way `track_address` is the one Follow
    seam. Remembering here means all three paths log the unfollow for free (#99).

    `name` is the per-User nickname (#86) the wallet carried at this moment. #86
    forgets it by design — it rides the tracks row, so the DELETE takes it with
    it — so it is read out of the deleted row (RETURNING) and preserved into the
    log. One row per (user, wallet): the upsert keeps only the latest unfollow, so
    a re-follow → re-unfollow cycle updates the timestamp and name rather than
    piling up history. Per-User and invisible to others (the log is user-scoped)."""
    removed = await conn.fetchrow(
        "DELETE FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2 RETURNING name",
        telegram_id,
        address,
    )
    if removed is None:
        return False
    await conn.execute(
        """
        INSERT INTO unfollows (user_telegram_id, trader_address, unfollowed_at, name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_telegram_id, trader_address)
        DO UPDATE SET unfollowed_at = EXCLUDED.unfollowed_at, name = EXCLUDED.name
        """,
        telegram_id,
        address,
        now,
        removed["name"],
    )
    return True


async def upsert_user(
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
) -> tuple[str, InlineKeyboardMarkup]:
    rows = await pool.fetch(
        """
        SELECT trader_address, muted, min_size_usd, name
        FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at
        """,
        user_id,
    )
    global_min: Decimal | None = await pool.fetchval(
        "SELECT min_size_usd FROM users WHERE telegram_id = $1", user_id
    )
    if not rows:
        return NOT_TRACKING_TEXT, with_delete_button()

    lines = ["Your tracked traders:", ""]
    keyboard: list[list[InlineKeyboardButton]] = []
    for row in rows:
        address: str = row["trader_address"]
        positions = await fetch_open_positions(gateway, address)
        lines.append(f"{trader_label(row['name'], address)} — {_summarize(positions)}")
        # The alert controls (issue #10), so they are visible and editable
        # right where the User reviews the roster.
        lines.append(f"    {_controls_status(row['muted'], row['min_size_usd'], global_min)}")
        keyboard.append(
            [
                InlineKeyboardButton(
                    # Named wallets read by name on the button (#86); the list
                    # line above carries the verifiable address.
                    text=f"📊 {button_label(row['name'], address)}",
                    callback_data=f"positions:{address}",
                ),
                _mute_button(address, row["muted"]),
            ]
        )
        keyboard.append(
            [
                InlineKeyboardButton(text="💵 Min size", callback_data=f"tmin:{address}"),
                InlineKeyboardButton(text="✖️ Unfollow", callback_data=f"unfollow:{address}"),
            ]
        )
    keyboard.append(
        [InlineKeyboardButton(text=_global_min_label(global_min), callback_data="gmin")]
    )
    return "\n".join(lines), with_delete_button(InlineKeyboardMarkup(inline_keyboard=keyboard))


def _controls_status(muted: bool, track_min: Decimal | None, global_min: Decimal | None) -> str:
    """One line per Track showing its alert controls: mute state and the
    effective minimum position size (per-Track override, else the global floor,
    else none)."""
    state = "🔕 muted" if muted else "🔔 alerts on"
    if track_min is not None:
        floor = f"min ${track_min:,.0f}"
    elif global_min is not None:
        floor = f"min ${global_min:,.0f} (global)"
    else:
        floor = "no min size"
    return f"{state} · {floor}"


def _mute_button(address: str, muted: bool) -> InlineKeyboardButton:
    if muted:
        return InlineKeyboardButton(text="🔔 Unmute", callback_data=f"unmute:{address}")
    return InlineKeyboardButton(text="🔕 Mute", callback_data=f"mute:{address}")


def _global_min_label(global_min: Decimal | None) -> str:
    if global_min is not None:
        return f"⚙️ Global min: ${global_min:,.0f}"
    return "⚙️ Set global min size"


async def _render_track_record(
    pool: asyncpg.Pool, address: str, now: datetime, *, window: TrackWindow | None = None
) -> str:
    """The profile's metrics block (metric definitions: docs/metrics.md).

    `window` is the track-record toggle (#102): None is the default all-time
    record (unchanged — the #101 span header and every accumulator line), a
    TrackWindow reduces the trip-derived stats over only the round-trips closed
    inside that window, dropping the whole-history accumulator lines (maker
    share) that no window can honestly reconstruct."""
    if window is not None:
        return await _render_windowed_track_record(pool, address, now, window)
    row = await pool.fetchrow(
        """
        SELECT t.bot_reason, fm.address IS NOT NULL AS fine_available,
               fm.trade_count, fm.win_rate, fm.avg_win, fm.avg_loss, fm.sharpe,
               fm.max_drawdown, fm.avg_leverage, fm.maker_share, fm.avg_hold_seconds,
               fm.median_trade, fm.profit_factor, fm.top_trade_share,
               (SELECT min(ft.opened_at) FROM fine_trades ft WHERE ft.address = t.address)
                   AS oldest_trade_at,
               cm.pnl AS month_pnl, cm.roi AS month_roi
        FROM traders t
        LEFT JOIN fine_metrics fm ON fm.address = t.address
        LEFT JOIN coarse_metrics cm ON cm.address = t.address AND cm.time_window = 'month'
        WHERE t.address = $1
        """,
        address,
    )
    lines: list[str] = []
    if row is not None and row["bot_reason"] is not None:
        # A User may track anything, but the vetting verdict travels with it.
        lines.append(f"⚠️ Flagged as a market-maker bot: {row['bot_reason']}")
    if row is not None and row["fine_available"]:
        lines.append(f"Track record ({_trades_span_label(row['oldest_trade_at'], now)}):")
        lines.extend(_fine_lines(row))
    elif row is not None and row["month_pnl"] is not None:
        lines.append("Coarse metrics only — fine stats haven't been computed yet.")
        lines.append(f"30d PnL {signed_usd(row['month_pnl'])} · ROI {row['month_roi']:.0%}")
    else:
        lines.append("No metrics yet — this trader hasn't been scanned.")
    return "\n".join(lines)


async def _render_windowed_track_record(
    pool: asyncpg.Pool, address: str, now: datetime, window: TrackWindow
) -> str:
    """The track record reduced over the round-trips closed inside `window`
    (#102). The trip-derived stats come from the shared engine reducer
    (metrics.fine.reduce_trips) over the filtered trips, so a windowed reading
    can never drift from the all-time definitions; maker share and the other
    whole-history accumulators are omitted rather than shown mislabeled."""
    cutoff = now - window.span
    trade_rows = await pool.fetch(
        "SELECT coin, pnl, peak_notional, opened_at, closed_at, seq "
        "FROM fine_trades WHERE address = $1 AND closed_at >= $2 "
        "ORDER BY closed_at, coin, seq",
        address,
        cutoff,
    )
    trips = [
        RoundTrip(
            coin=r["coin"],
            pnl=r["pnl"],
            peak_notional=r["peak_notional"],
            opened_at=r["opened_at"],
            closed_at=r["closed_at"],
            seq=r["seq"],
        )
        for r in trade_rows
    ]
    # avg size is peak notional against today's account value (#85), so the
    # windowed reduction needs the same coarse-month anchor the all-time row uses.
    account_value = await pool.fetchval(
        "SELECT account_value FROM coarse_metrics WHERE address = $1 AND time_window = 'month'",
        address,
    )
    metrics = reduce_trips(trips, account_value)
    bot_reason = await pool.fetchval("SELECT bot_reason FROM traders WHERE address = $1", address)
    lines: list[str] = []
    if bot_reason is not None:
        lines.append(f"⚠️ Flagged as a market-maker bot: {bot_reason}")
    lines.append(f"Track record ({window.header}):")
    # The reduced TripMetrics feeds _fine_lines directly; maker_share is spliced
    # in as None because it is an accumulator with no windowed value, so
    # _fine_lines omits its clause exactly as it does when unavailable.
    lines.extend(
        _fine_lines(
            {**asdict(metrics), "maker_share": None},
            empty="No closed trades in this window",
        )
    )
    return "\n".join(lines)


def _trades_span_label(oldest_trade_at: datetime | None, now: datetime) -> str:
    """How far back the track record's trades reach, for its header: the age of
    the OLDEST completed round-trip in the store — so \"61% over 33 trades\" says
    whether those 33 span a week or a year. Varies wildly per wallet (the
    2000-fill API cap seeds days of history for a hyperactive wallet, a year+
    for a quiet one; incremental folding grows it from there). Coarse-grained
    on purpose: days under ~2 months, months beyond, so it reads at a glance.
    Falls back to the old wording when there are no stored trips (metrics rows
    whose trade_count is 0 still render the block)."""
    if oldest_trade_at is None:
        return "from recent fills"
    days = max(1, round((now - oldest_trade_at).total_seconds() / 86400))
    if days < 60:
        return f"trades from the last {days} day{'s' if days > 1 else ''}"
    return f"trades from the last ~{round(days / 30)} months"


def _fine_lines(
    row: Mapping[str, Any], *, empty: str = "No closed trades in the recent fills"
) -> list[str]:
    """The metric lines shared by the all-time record (fed the fine_metrics row)
    and the windowed record (fed a reduced TripMetrics as a mapping, #102). A
    None `maker_share` drops the maker clause — which is how the windowed view
    omits the un-windowable accumulator without a separate renderer. `empty` is
    the no-completed-trades wording, which differs per window."""
    lines: list[str] = []
    if row["win_rate"] is not None:
        lines.append(f"{row['win_rate']:.0%} win rate over {row['trade_count']} closed trades")
    else:
        lines.append(empty)
    if row["avg_win"] is not None and row["avg_loss"] is not None:
        lines.append(f"avg win ${row['avg_win']:,.0f} · avg loss ${row['avg_loss']:,.0f}")
    sharpe = f"Sharpe {row['sharpe']:.1f} · " if row["sharpe"] is not None else ""
    if row["win_rate"] is not None:
        lines.append(f"{sharpe}max drawdown ${row['max_drawdown']:,.0f}")
    # The anti-deception trio (#113): each clause appears only when computable —
    # median needs a trip, PF a loss, top-trade share a positive total — so a
    # no-losses wallet drops "PF", a net-loser drops "top trade", and a wallet
    # with no trips shows the line not at all.
    median = row["median_trade"]
    trio = [
        f"median trade {'-' if median < 0 else ''}${abs(median):,.0f}"
        if median is not None
        else None,
        f"PF {row['profit_factor']:.1f}" if row["profit_factor"] is not None else None,
        f"top trade {row['top_trade_share']:.0%}"
        if row["top_trade_share"] is not None
        else None,
    ]
    if any(trio):
        lines.append(" · ".join(part for part in trio if part is not None))
    style = [
        f"{row['maker_share']:.0%} maker" if row["maker_share"] is not None else None,
        # Estimated sizing vs the account (peak position ÷ account value), NOT the
        # exchange leverage dial the positions view shows as "at 25x" — #85.
        f"avg size ~{row['avg_leverage']:.1f}x of account"
        if row["avg_leverage"] is not None
        else None,
    ]
    if any(style):
        lines.append(" · ".join(part for part in style if part is not None))
    if row["avg_hold_seconds"] is not None:
        lines.append(f"⏱ Avg hold: {format_duration(row['avg_hold_seconds'])}")
    return lines


def _render_positions(
    address: str,
    positions: list[Position],
    ages: dict[str, tuple[datetime, bool]],
    now: datetime,
    fills: dict[str, tuple[datetime, Decimal, datetime | None]] | None = None,
    *,
    name: str | None = None,
    offer_follow: bool = False,
) -> tuple[str, list[MessageEntity]]:
    """The shared per-position view (#31): notional plus the real margin at risk,
    return-on-margin, and holding time (#35).

    Returns the rendered text and the header's `code` entities (#93) — a
    tap-to-copy span over the full address. Callers render this at the start of
    the message and pass the entities straight to the send/edit call, so the
    UTF-16 offsets carry through unchanged; text appended after the header does
    not shift them.

    `name` is the viewing User's own nickname for the wallet (#86); the header
    reads `name (0xfull…)` when set, else the bare full address.

    Age has two sources, in priority order. `ages` maps coin → (opened_at,
    baselined) from the poller's snapshots — the source for a tracked wallet,
    and always the fresher one. `fills` maps coin → (opened_at, net_position,
    fills_seen_at) from the fine store's open episodes (#78) — the fallback for
    an untracked wallet, used only where the poller has no snapshot and only
    when the episode actually matches the live position (see `_fills_open_age`).
    A coin covered by neither simply shows no age rather than a made-up one.

    `offer_follow` (an untracked profile) appends a single nudge to follow when
    at least one position was left ageless — the honest way to explain the gap:
    the open time is knowable only by observing the wallet, which following
    starts. Suppressed for a follower, who already gets the poller's own age."""
    header, addr_entity = trader_header(name, address)
    entities = [addr_entity]
    if not positions:
        return f"{header} has no open positions right now.", entities
    fills = fills or {}
    blocks = [f"{header} — current positions:", ""]
    any_ageless = False
    for p in positions:
        upnl = f"uPnL {signed_usd(p.unrealized_pnl)}"
        rom = p.return_on_margin
        if rom is not None:
            upnl += f" ({signed_pct(rom)})"
        detail = [f"entry {p.entry_price}", upnl]
        aged = ages.get(p.coin)
        if aged is not None:
            opened_at, baselined = aged
            detail.append(open_age(opened_at, now, baselined=baselined))
        else:
            fills_age = _fills_open_age(p, fills.get(p.coin), now)
            if fills_age is not None:
                detail.append(fills_age)
            else:
                any_ageless = True
        blocks.append(
            f"{p.coin} {p.side.value.upper()} — "
            f"${p.size_usd:,.0f} notional · ${p.margin:,.0f} margin at {p.leverage}x\n"
            f"    " + " · ".join(detail)
        )
    if offer_follow and any_ageless:
        blocks.append("")
        blocks.append(FOLLOW_FOR_AGE_HINT)
    return "\n".join(blocks), entities


def _render_open_orders(orders: list[OpenOrder]) -> str | None:
    """The resting-orders section both wallet views append after the positions
    block (#115): the trader's plan before it executes, tracked and untracked
    wallets alike. None — so the views show nothing extra — for a wallet with
    an empty book.

    Rows sort by coin, then by the price the order acts at (descending), so a
    ladder reads top-down; the shared order_line labels TP/SL and bares
    builder-DEX tickers. Capped at the Order Alert batch cap for the same
    reason (observed live: makers resting 500+ orders — an uncapped section
    would also gamble with Telegram's message-length limit)."""
    if not orders:
        return None
    ranked = sorted(
        orders,
        key=lambda o: (
            display_coin(o.coin),
            -(o.trigger_price if o.trigger_price is not None else o.limit_price),
        ),
    )
    return "\n".join(["Resting orders:", *order_lines(ranked)])


async def _position_ages(
    pool: asyncpg.Pool, address: str
) -> dict[str, tuple[datetime, bool]]:
    """coin → (opened_at, baselined) from the poller's snapshots for a Trader (#35).

    `opened_at` is when the poller first observed the position; a position already
    open at baseline time (#4) carries the baseline moment, not its true open — so
    `baselined` flags those (opened_at at or before the wallet's baseline) and the
    display marks them as an at-least age. Empty for a wallet never polled (an
    untracked profile), which simply omits ages."""
    rows = await pool.fetch(
        """
        SELECT s.coin, s.opened_at, s.opened_at <= p.baselined_at AS baselined
        FROM position_snapshots s
        JOIN position_poll_state p USING (trader_address)
        WHERE s.trader_address = $1
        """,
        address,
    )
    return {r["coin"]: (r["opened_at"], r["baselined"]) for r in rows}


def _fills_open_age(
    position: Position,
    episode: tuple[datetime, Decimal, datetime | None] | None,
    now: datetime,
) -> str | None:
    """The fills-derived open age for `position`, or None when the fine store
    can't honestly supply one (#78).

    The episode is a candidate only if it actually corresponds to the live
    position: it is already keyed on the coin, and its signed `net_position`
    must agree with the position's side. The live position and the fills
    snapshot can disagree — a wallet can open (or flip) a position after the
    last fine refresh — so a contradicting or never-verified (demoted, #63)
    episode lends no age, exactly like a missing snapshot. A matching episode
    reads as fills-derived and hedges staleness when the scan is old."""
    if episode is None:
        return None
    opened_at, net_position, fills_seen_at = episode
    if not _episode_matches_side(position.side, net_position):
        return None
    return fills_open_age(opened_at, now, stale=_fills_stale(fills_seen_at, now))


def _episode_matches_side(side: Side, net_position: Decimal) -> bool:
    """Does an open episode's signed net position agree with the live side?
    Positive is long, negative is short; 0 is "never verified" (the pre-#63
    demotion default), which agrees with neither and so never lends an age."""
    if net_position > 0:
        return side is Side.LONG
    if net_position < 0:
        return side is Side.SHORT
    return False


async def _fills_open_episodes(
    pool: asyncpg.Pool, address: str
) -> dict[str, tuple[datetime, Decimal, datetime | None]]:
    """coin → (opened_at, net_position, fills_seen_at) from the fine store's
    open episodes (#78) — the fallback age source when the poller has no
    snapshot (an untracked wallet).

    `opened_at` is the continuity-verified position open (#63); `net_position`
    is the signed size the walk left the coin at, so the caller can confirm the
    episode matches the live position's direction; `fills_seen_at`
    (fine_metrics.computed_at) dates the knowledge so a stale age can be hedged.
    Empty for a wallet with no fine data — which simply omits fills-derived
    ages."""
    rows = await pool.fetch(
        """
        SELECT e.coin, e.opened_at, e.net_position, fm.computed_at AS fills_seen_at
        FROM fine_open_episodes e
        LEFT JOIN fine_metrics fm ON fm.address = e.address
        WHERE e.address = $1
        """,
        address,
    )
    return {
        r["coin"]: (r["opened_at"], r["net_position"], r["fills_seen_at"]) for r in rows
    }


def _summarize(positions: list[Position]) -> str:
    if not positions:
        return "no open positions"
    total_upnl = sum((p.unrealized_pnl for p in positions), Decimal(0))
    noun = "position" if len(positions) == 1 else "positions"
    return f"{len(positions)} {noun}, uPnL {signed_usd(total_upnl)}"


def follow_toast(outcome: TrackOutcome, address: str) -> str:
    if outcome is TrackOutcome.LIMIT_REACHED:
        return TRACK_LIMIT_TOAST
    verb = "Now following" if outcome is TrackOutcome.FRESHLY_TRACKED else "Already following"
    return f"{verb} {short_address(address)}"


def _unfollow_toast(removed: bool, address: str) -> str:
    if not removed:
        return "You weren't tracking this trader."
    return f"Unfollowed {short_address(address)}"


def build_router() -> Router:
    """A fresh Router per Dispatcher — a Router instance can only attach once."""
    # Deferred import: the criteria and controls flows build on this module's
    # shared seams (track_address, _render_tracked_list, …), so importing them
    # at the top would cycle.
    from epigone.bot import access, controls, criteria, delete, names

    router = Router()
    # Invite-only admin commands (#33). The gate is a dispatcher-level outer
    # middleware (access.install_allowlist_gate); these are the owner's
    # runtime controls over it, owner-only enforced in the handlers.
    access.register(router)
    router.message.register(cmd_start, Command("start"))
    router.message.register(cmd_help, Command("help"))
    router.message.register(cmd_screener, Command("screener"))
    router.message.register(cmd_tracked, Command("tracked"))
    # Before the paste/reject handlers: each consumes its own typed input
    # (criteria thresholds/names, a min-size amount) while a prompt is pending;
    # commands still cut through.
    criteria.register(router)
    controls.register(router)
    # The rename flow (#86): consumes a pending typed wallet name before the
    # paste handler, same as the criteria/min-size prompts above.
    names.register(router)
    router.message.register(open_pasted_profile, _is_wallet_paste)
    router.message.register(reject_unknown_command, _is_command)
    router.message.register(reject_unrecognized_input)  # anything else: text, stickers, photos…
    router.callback_query.register(on_screener_page, F.data.startswith("screen:"))
    router.callback_query.register(on_screener_follow, F.data.startswith("sfollow:"))
    router.callback_query.register(on_profile, F.data.startswith("profile:"))
    router.callback_query.register(on_profile_follow, F.data.startswith("pfollow:"))
    router.callback_query.register(on_profile_unfollow, F.data.startswith("punfollow:"))
    # The track-record window toggles (#102). Distinct prefixes from every view's
    # own callbacks (poswin/profwin never collide with positions/profile), so
    # each tap re-renders the exact view it came from.
    router.callback_query.register(
        on_profile_window, F.data.startswith(f"{PROFILE_WINDOW_PREFIX}:")
    )
    router.callback_query.register(
        on_positions_window, F.data.startswith(f"{POSITIONS_WINDOW_PREFIX}:")
    )
    router.callback_query.register(on_positions_unfollow, F.data.startswith("posunfollow:"))
    router.callback_query.register(on_positions, F.data.startswith("positions:"))
    router.callback_query.register(on_unfollow, F.data.startswith("unfollow:"))
    # The one-tap 🗑 delete (#73). A bare-constant callback, so it never shadows
    # (nor is shadowed by) the prefixed callbacks above; covers the monitor's DMs
    # too — they share this polling loop (ADR-0002).
    delete.register(router)
    return router
