import re
from datetime import datetime
from decimal import Decimal
from enum import Enum

import asyncpg
from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from epigone.bot.format import open_age, short_address, signed_pct, signed_usd
from epigone.clock import Clock
from epigone.gateway import (
    GatewayError,
    HyperliquidGateway,
    Position,
    Window,
    fetch_open_positions,
)
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
    "Paste a wallet address (0x…) to start tracking that trader.\n"
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
    "Paste a wallet address (0x…) to start tracking one in the meantime."
)

INVALID_ADDRESS_TEXT = (
    "That doesn't look like a wallet address.\n\n"
    "Paste a full Hyperliquid address — 0x followed by 40 hex characters — "
    "and I'll start tracking that trader."
)

NOT_TRACKING_TEXT = (
    "You're not tracking any traders yet.\n\nPaste a wallet address (0x…) to follow your first one."
)

UNKNOWN_COMMAND_TEXT = "I don't know that command. Type /help to see what I can do."

DATA_DELAYED_TEXT = (
    "Hyperliquid data is delayed right now — your tracked list is safe, try again in a moment."
)

# Shown when a User at the cap tries to follow one more (#23). Full-message form
# for the paste path; the shorter toast for the screener/profile button paths,
# which surface through callback answers.
TRACK_LIMIT_TEXT = (
    f"You're tracking {MAX_TRACKED_WALLETS} wallets — that's the limit.\n"
    "Unfollow one from /tracked before following another."
)
TRACK_LIMIT_TOAST = f"Limit reached — {MAX_TRACKED_WALLETS} wallets max. Unfollow one first."

_ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}")


async def cmd_start(message: Message, pool: asyncpg.Pool) -> None:
    user = message.from_user
    if user is not None:
        await upsert_user(pool, user.id, user.username)
    await message.answer(START_TEXT)


async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


async def follow_pasted_address(message: Message, pool: asyncpg.Pool, clock: Clock) -> None:
    """A pasted valid address Follows the Trader; re-following is idempotent."""
    user = message.from_user
    if user is None or message.text is None:
        return
    address = message.text.strip().lower()
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(conn, user.id, user.username, address, clock.now())
    if outcome is TrackOutcome.FRESHLY_TRACKED:
        await message.answer(
            f"Now tracking {short_address(address)}.\n"
            "Paste more addresses any time — /tracked shows your whole list."
        )
    elif outcome is TrackOutcome.ALREADY_TRACKING:
        await message.answer(f"You're already tracking {short_address(address)}.")
    else:  # LIMIT_REACHED — the wallet was not added
        await message.answer(TRACK_LIMIT_TEXT)


async def reject_unknown_command(message: Message) -> None:
    await message.answer(UNKNOWN_COMMAND_TEXT)


async def reject_unrecognized_input(message: Message) -> None:
    await message.answer(INVALID_ADDRESS_TEXT)


async def cmd_tracked(message: Message, pool: asyncpg.Pool, gateway: HyperliquidGateway) -> None:
    user = message.from_user
    if user is None:
        return
    try:
        text, markup = await _render_tracked_list(pool, gateway, user.id)
    except GatewayError:
        await message.answer(DATA_DELAYED_TEXT)
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
    Fine metrics where the fine pass has run; coarse-only Traders say so."""
    address = (callback.data or "").removeprefix("positions:")
    tracked = await pool.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    if not tracked:
        await callback.answer("You're not tracking this trader.", show_alert=True)
        return
    try:
        positions = await fetch_open_positions(gateway, address)
    except GatewayError:
        await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
        return
    ages = await _position_ages(pool, address)
    view = (
        _render_positions(address, positions, ages, clock.now())
        + "\n\n"
        + await _render_track_record(pool, address)
    )
    if isinstance(callback.message, Message):
        await callback.message.answer(view)  # the chat the button lives in
    else:
        await bot.send_message(chat_id=callback.from_user.id, text=view)
    await callback.answer()


async def on_unfollow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway
) -> None:
    """One-tap unfollow: drop the Track and refresh the list in place."""
    address = (callback.data or "").removeprefix("unfollow:")
    status = await pool.execute(
        "DELETE FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    removed = status != "DELETE 0"  # a stale button tap deletes nothing
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


async def on_screener_follow(callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock) -> None:
    """Follow straight from a results row, then re-render the page so the row
    flips to Following. The offset rides in the callback data so the re-render
    lands on the same page."""
    offset_str, _, address = (callback.data or "").removeprefix("sfollow:").partition(":")
    offset = _parse_offset(offset_str)
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(
            conn, callback.from_user.id, callback.from_user.username, address, clock.now()
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
        text, markup = await _render_profile(pool, gateway, clock, callback.from_user.id, address)
    except GatewayError:
        await callback.answer(DATA_DELAYED_TEXT, show_alert=True)
        return
    if isinstance(callback.message, Message):
        await callback.message.answer(text, reply_markup=markup)
    else:
        await bot.send_message(chat_id=callback.from_user.id, text=text, reply_markup=markup)
    await callback.answer()


async def on_profile_follow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    address = (callback.data or "").removeprefix("pfollow:")
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(
            conn, callback.from_user.id, callback.from_user.username, address, clock.now()
        )
    await _refresh_profile_in_place(
        callback, pool, gateway, clock, address, follow_toast(outcome, address)
    )


async def on_profile_unfollow(
    callback: CallbackQuery, pool: asyncpg.Pool, gateway: HyperliquidGateway, clock: Clock
) -> None:
    address = (callback.data or "").removeprefix("punfollow:")
    status = await pool.execute(
        "DELETE FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        callback.from_user.id,
        address,
    )
    removed = status != "DELETE 0"  # a stale button tap deletes nothing
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
            text, markup = await _render_profile(
                pool, gateway, clock, callback.from_user.id, address
            )
            await callback.message.edit_text(text, reply_markup=markup)
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
) -> tuple[str, InlineKeyboardMarkup | None]:
    # One extra row tells us whether a next page exists without a second query.
    rows = await run_screener(
        pool, window=Window.MONTH, limit=SCREENER_PAGE_SIZE + 1, offset=offset
    )
    has_next = len(rows) > SCREENER_PAGE_SIZE
    rows = rows[:SCREENER_PAGE_SIZE]
    if not rows:
        return SCREENER_EMPTY_TEXT, None

    tracked = await tracked_set(pool, user_id, [r.address for r in rows])
    lines = [SCREENER_HEADER, ""]
    keyboard: list[list[InlineKeyboardButton]] = []
    for rank, row in enumerate(rows, start=offset + 1):
        lines.append(f"{rank}. {row.display_name or short_address(row.address)}")
        lines.append(f"    {_screener_stats(row, clock.now())}")
        followed = row.address in tracked
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
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard)


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


async def _render_profile(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    clock: Clock,
    user_id: int,
    address: str,
) -> tuple[str, InlineKeyboardMarkup]:
    positions = await fetch_open_positions(gateway, address)  # may raise GatewayError
    followed = await pool.fetchval(
        "SELECT 1 FROM tracks WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )
    ages = await _position_ages(pool, address)
    parts = [
        _render_positions(address, positions, ages, clock.now()),
        await _render_track_record(pool, address),
    ]
    freshness = await _metric_freshness(pool, clock, address)
    if freshness is not None:
        parts.append(freshness)
    button = (
        InlineKeyboardButton(text="✖️ Unfollow", callback_data=f"punfollow:{address}")
        if followed
        else InlineKeyboardButton(text="➕ Follow", callback_data=f"pfollow:{address}")
    )
    return "\n\n".join(parts), InlineKeyboardMarkup(inline_keyboard=[[button]])


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
    conn: asyncpg.Connection, telegram_id: int, username: str | None, address: str, now: datetime
) -> TrackOutcome:
    """Follow `address` for a User; idempotent. Returns the follow's outcome.

    The single write behind every Follow — pasting (#3), the screener row, and
    the profile toggle (#6). A Track is exactly what the alert poller (#4)
    reads, so following from results feeds the alert pipeline for free.

    The per-User cap (#23) is enforced here so all three paths share one check.
    Re-touching an already-tracked wallet is always allowed (idempotent, never
    counts as a new follow), even at the cap — so the already-tracking test
    comes before the count check."""
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
    if tracked_count >= MAX_TRACKED_WALLETS:
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
    return TrackOutcome.FRESHLY_TRACKED


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
) -> tuple[str, InlineKeyboardMarkup | None]:
    rows = await pool.fetch(
        """
        SELECT trader_address, muted, min_size_usd
        FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at
        """,
        user_id,
    )
    global_min: Decimal | None = await pool.fetchval(
        "SELECT min_size_usd FROM users WHERE telegram_id = $1", user_id
    )
    if not rows:
        return NOT_TRACKING_TEXT, None

    lines = ["Your tracked traders:", ""]
    keyboard: list[list[InlineKeyboardButton]] = []
    for row in rows:
        address: str = row["trader_address"]
        positions = await fetch_open_positions(gateway, address)
        lines.append(f"{short_address(address)} — {_summarize(positions)}")
        # The alert controls (issue #10), so they are visible and editable
        # right where the User reviews the roster.
        lines.append(f"    {_controls_status(row['muted'], row['min_size_usd'], global_min)}")
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"📊 {short_address(address)}", callback_data=f"positions:{address}"
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
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard)


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


async def _render_track_record(pool: asyncpg.Pool, address: str) -> str:
    """The profile's metrics block. Metric definitions: docs/metrics.md."""
    row = await pool.fetchrow(
        """
        SELECT t.bot_reason, fm.address IS NOT NULL AS fine_available,
               fm.trade_count, fm.win_rate, fm.avg_win, fm.avg_loss, fm.sharpe,
               fm.max_drawdown, fm.avg_leverage, fm.maker_share,
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
        lines.append("Track record (from recent fills):")
        lines.extend(_fine_lines(row))
    elif row is not None and row["month_pnl"] is not None:
        lines.append("Coarse metrics only — fine stats haven't been computed yet.")
        lines.append(f"30d PnL {signed_usd(row['month_pnl'])} · ROI {row['month_roi']:.0%}")
    else:
        lines.append("No metrics yet — this trader hasn't been scanned.")
    return "\n".join(lines)


def _fine_lines(row: asyncpg.Record) -> list[str]:
    lines: list[str] = []
    if row["win_rate"] is not None:
        lines.append(f"{row['win_rate']:.0%} win rate over {row['trade_count']} closed trades")
    else:
        lines.append("No closed trades in the recent fills")
    if row["avg_win"] is not None and row["avg_loss"] is not None:
        lines.append(f"avg win ${row['avg_win']:,.0f} · avg loss ${row['avg_loss']:,.0f}")
    sharpe = f"Sharpe {row['sharpe']:.1f} · " if row["sharpe"] is not None else ""
    if row["win_rate"] is not None:
        lines.append(f"{sharpe}max drawdown ${row['max_drawdown']:,.0f}")
    style = [
        f"{row['maker_share']:.0%} maker" if row["maker_share"] is not None else None,
        f"~{row['avg_leverage']:.1f}x leverage" if row["avg_leverage"] is not None else None,
    ]
    if any(style):
        lines.append(" · ".join(part for part in style if part is not None))
    return lines


def _render_positions(
    address: str,
    positions: list[Position],
    ages: dict[str, tuple[datetime, bool]],
    now: datetime,
) -> str:
    """The shared per-position view (#31): notional plus the real margin at risk,
    return-on-margin, and holding time (#35). `ages` maps coin → (opened_at,
    baselined) from the poller's snapshots; a coin absent from it (an untracked
    wallet with no snapshot) simply shows no age rather than a made-up one."""
    if not positions:
        return f"{short_address(address)} has no open positions right now."
    blocks = [f"{short_address(address)} — current positions:", ""]
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
        blocks.append(
            f"{p.coin} {p.side.value.upper()} — "
            f"${p.size_usd:,.0f} notional · ${p.margin:,.0f} margin at {p.leverage}x\n"
            f"    " + " · ".join(detail)
        )
    return "\n".join(blocks)


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
    from epigone.bot import access, controls, criteria

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
    router.message.register(follow_pasted_address, _is_wallet_paste)
    router.message.register(reject_unknown_command, _is_command)
    router.message.register(reject_unrecognized_input)  # anything else: text, stickers, photos…
    router.callback_query.register(on_screener_page, F.data.startswith("screen:"))
    router.callback_query.register(on_screener_follow, F.data.startswith("sfollow:"))
    router.callback_query.register(on_profile, F.data.startswith("profile:"))
    router.callback_query.register(on_profile_follow, F.data.startswith("pfollow:"))
    router.callback_query.register(on_profile_unfollow, F.data.startswith("punfollow:"))
    router.callback_query.register(on_positions, F.data.startswith("positions:"))
    router.callback_query.register(on_unfollow, F.data.startswith("unfollow:"))
    return router
