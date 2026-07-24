"""Shared Telegram text formatting: used by the dialog handlers and the
Position/Order Alert renderers."""

from datetime import datetime
from decimal import Decimal

from aiogram.types import MessageEntity

from epigone.gateway import OpenOrder
from epigone.metrics.library import format_duration


def short_address(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}"


def trader_label(label: str | None, address: str) -> str:
    """A wallet's inline identity: a name plus its short address, or the bare
    short address when there is no name. The name is the viewer's own per-Track
    nickname where they set one (#86), else the leaderboard display name (#4) —
    the caller resolves which to pass; None here means show just the address. The
    short address always rides along so identity stays verifiable."""
    short = short_address(address)
    return f"{label} ({short})" if label else short


def button_label(label: str | None, address: str) -> str:
    """A wallet's identity on a BUTTON: the name alone when it has one, else the
    short address. Buttons are tight and always sit on a message that carries
    the verifiable address (the alert text, the notice text, the profile header
    #93), so unlike trader_label the address doesn't ride along — the name is
    the whole label."""
    return label if label else short_address(address)


def trader_header(label: str | None, address: str) -> tuple[str, MessageEntity]:
    """The positions/profile header identity with the *full* address (#93), plus
    a `code` MessageEntity over the address span so Telegram offers tap-to-copy.

    Unlike `trader_label`'s short form (kept for scannable lists and alerts),
    the header is exactly where a viewer wants the whole address to copy — so it
    reads `name (0xfull…)` or the bare full address. A `code` entity is used in
    preference to an HTML parse mode: parse mode would force escaping every piece
    of dynamic text (a nickname may hold `<` or `&`) across the whole message.

    The entity offset is in UTF-16 code units — Telegram's unit — since a name
    may carry emoji, so the prefix is measured in UTF-16, not characters. The
    offset is relative to the start of the returned text; callers render this
    header at the very start of the message, so it carries straight through to
    the send/edit call."""
    prefix = f"{label} (" if label else ""
    suffix = ")" if label else ""
    text = f"{prefix}{address}{suffix}"
    offset = len(prefix.encode("utf-16-le")) // 2
    length = len(address.encode("utf-16-le")) // 2
    return text, MessageEntity(type="code", offset=offset, length=length)


def display_coin(coin: str) -> str:
    """A builder-DEX coin arrives namespaced `dex:COIN` (e.g. xyz:SP500, #21);
    show the bare ticker rather than leaking the venue prefix. A core coin has
    no prefix and passes through untouched."""
    return coin.rsplit(":", 1)[-1]


def order_line(order: OpenOrder) -> str:
    """One resting order, as both the wallet views and Order Alerts render it
    (issue #115) — shared here so the two can't drift (the #77 lesson).

    A plain limit reads notional-first like a position row (`LIT SELL $13,500
    @ 4.5`); a trigger order is labeled TP/SL from its orderType and reads
    against the price that arms it (its limitPx is only a slippage cap); a
    whole-position TP/SL has no order-level size, so it says what it is
    instead of inventing one. Builder-DEX coins render as the bare ticker."""
    coin = display_coin(order.coin)
    side = "BUY" if order.is_buy else "SELL"
    if order.is_position_tpsl:
        return f"{coin} {order.tpsl} @ {order.trigger_price} (whole position)"
    notional = order.notional_usd
    amount = f"${notional:,.0f}" if notional is not None else "size unknown"
    if order.is_trigger:
        return f"{coin} {side} {order.tpsl} {amount} @ trigger {order.trigger_price}"
    return f"{coin} {side} {amount} @ {order.limit_price}"


def signed_usd(amount: Decimal) -> str:
    sign = "-" if amount < 0 else "+"
    return f"{sign}${abs(amount):,.0f}"


def signed_pct(ratio: Decimal) -> str:
    sign = "-" if ratio < 0 else "+"
    return f"{sign}{abs(ratio):.0%}"


def usd_compact(amount: Decimal) -> str:
    """A rough dollar magnitude for a denominator like account value: $1.1M,
    $50k, $940. Abbreviated on purpose — the account line is a sense of scale,
    not a precise balance, so it stays short next to the PnL it contextualizes."""
    value = abs(amount)
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}k"
    return f"${value:,.0f}"


def held_for(opened_at: datetime, closed_at: datetime) -> str:
    """Compact holding time between two instants: 45s / 12m / 3h 20m / 2d 5h."""
    return format_duration(int((closed_at - opened_at).total_seconds()))


def open_age(opened_at: datetime, now: datetime, *, baselined: bool) -> str:
    """How long an open position has been held, for the position display (#35).

    A baselined position (already open when Epigone first observed the wallet,
    #4) only knows time-since-tracking, not the true on-chain open — so it reads
    `open ≥2d 4h` ("at least this long"), never a falsely precise open age. A
    position first seen opening reads plainly, `open 2d 4h`."""
    span = format_duration(int((now - opened_at).total_seconds()))
    return f"open ≥{span}" if baselined else f"open {span}"


def fills_open_age(opened_at: datetime, now: datetime, *, stale: bool) -> str:
    """How long a position has been open, derived from the fine store's open
    episode rather than a live poller observation (#78).

    Used for an untracked wallet, where no poller snapshot exists but the
    continuity-verified fill history (#63) knows when the current position
    opened. It is knowledge as of the last fills scan, not a live reading, so it
    carries a `~` approximation marker — `open ~2d 4h` — distinct from the
    poller's precise `open 2d 4h` and its `≥` baselined marker. When the scan is
    older than the fresh window it also takes the activity line's "as of last
    scan" hedge (#72): the wallet may have changed the position since."""
    span = format_duration(int((now - opened_at).total_seconds()))
    marked = f"open ~{span}"
    return f"{marked} (as of last scan)" if stale else marked
