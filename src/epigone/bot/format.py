"""Shared Telegram text formatting: used by the dialog handlers and the
Position/Order Alert renderers."""

import html
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

from aiogram.enums import ParseMode
from aiogram.types import MessageEntity

from epigone.gateway import OpenOrder, Position, Side
from epigone.metrics.library import format_duration

# --- markup, asked for one message at a time (issue #185) ---
#
# The bot speaks PLAIN TEXT by default and the Bot object carries no default
# parse mode, deliberately. Nearly every reply interpolates text Epigone does
# not control — a User's wallet nickname, a criteria name, a coin ticker, an
# address — and none of it has ever been escaped, because in plain text there
# is nothing to escape it from. A bot-wide parse mode would silently start
# reinterpreting all of that.
#
# So a handler that wants markup asks for it on the message: `parse_mode=HTML`
# at the send, and `esc()` around every dynamic value in the text. The two go
# together — the test-suite guard in tests/support/telegram.py fails a message
# that takes one without the other, in either direction.
HTML = ParseMode.HTML


def esc(value: object) -> str:
    """A dynamic value as HTML *text*: `<`, `>` and `&` become entities, so it
    reads as itself rather than as markup (or as a parse error that loses the
    whole message).

    Takes an `object` and stringifies it because the values that reach these
    messages are addresses, names, Decimals and ints alike, and a call site
    that had to remember which ones needed the wrapper would be a call site
    that eventually forgets. Quotes are left alone: nothing here builds an HTML
    attribute out of dynamic text."""
    return html.escape(str(value), quote=False)


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
    That is this message's own choice, not a bot-wide ban — parse mode is per
    message (see `HTML` above), and the operator's copy and limits replies do
    take it, because their dynamic values are few and all escaped.

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


# How many of a batch's (or a book's) orders a message lists before
# summarizing the rest. Eight rows read at a glance; beyond that the
# individual rungs stop carrying information the count doesn't — and an
# uncapped list from a maker resting 500+ orders (observed live, #115) would
# gamble with Telegram's message-length limit. Shared by the Order Alert body
# and the wallet views' resting-orders section.
MAX_ORDERS_SHOWN = 8


def order_lines(
    orders: list[OpenOrder], positions: Mapping[str, Position] | None = None
) -> list[str]:
    """One order_line per order, capped at MAX_ORDERS_SHOWN with an
    "…and N more" tail — the truncation rule shared by the Order Alert body
    and the views' resting-orders section, extracted so the two can't drift
    any more than the line format can (#77 lesson). Callers pass the orders
    already in the order they want read (placement order for alerts, ladder
    order for views).

    `positions` maps a (namespaced) coin to the wallet's current position on
    it, so each line can annotate what the order would do to that position
    (#123). Both surfaces pass one — the alert from the wallet's snapshots, the
    view from the live positions it already fetched; a coin absent from the map
    genuinely has no position and reads as a new position."""
    by_coin = positions or {}
    lines = [order_line(o, by_coin.get(o.coin)) for o in orders[:MAX_ORDERS_SHOWN]]
    hidden = len(orders) - MAX_ORDERS_SHOWN
    if hidden > 0:
        lines.append(f"…and {hidden} more")
    return lines


def is_position_tpsl_trigger(is_trigger: bool, is_position_tpsl: bool, reduce_only: bool) -> bool:
    """Whether a resting order is a take-profit/stop-loss *attached to a
    position* rather than a fresh plan (issue #125): a whole-position TP/SL
    (`is_position_tpsl`, size 0) or a reduce-only sized trigger — both protect
    an existing position. A non-reduce trigger is a stop-*entry* (it opens or
    adds), and a plain limit is neither, so both stay in the generic orders
    section; only these move onto the position line they guard.

    Keyed off the three raw flags so the one rule serves both a live OpenOrder
    (is_position_tpsl_order) and a persisted order_snapshots Record (the poller's
    anchor-enrichment diff) without the predicate drifting between them."""
    return is_trigger and (is_position_tpsl or reduce_only)


def is_position_tpsl_order(order: OpenOrder) -> bool:
    """is_position_tpsl_trigger over a live OpenOrder."""
    return is_position_tpsl_trigger(
        order.is_trigger, order.is_position_tpsl, order.reduce_only
    )


def position_tpsl_line(orders: list[OpenOrder]) -> str | None:
    """The `TP 6.50 · SL 7.10` line a position surfaces for its attached
    triggers (issue #125), shared by the wallet views' position rows and the
    open-alert anchor enrichment so the two can't drift. None when there are no
    resting triggers to show.

    Each trigger reads as its TP/SL tag and the price that arms it; a
    partial-size one (a reduce-only trigger that closes part of the position)
    also carries its notional so it is distinguishable from a whole-position
    exit (`TP 6.50 ($13,500)`). Take-profits sort ahead of stops, then by
    trigger price, so a position's ladder reads the same way every time."""
    if not orders:
        return None
    ranked = sorted(
        orders,
        key=lambda o: (o.tpsl != "TP", o.trigger_price if o.trigger_price is not None else 0),
    )
    return " · ".join(_position_tpsl_part(o) for o in ranked)


def _position_tpsl_part(order: OpenOrder) -> str:
    """One trigger in a position's TP/SL line: its tag and trigger price, plus
    the notional for a partial (sized) exit so it is not mistaken for a
    whole-position one. A whole-position TP/SL has no order-level notional and
    reads bare — its size is the position's at trigger time."""
    part = f"{order.tpsl} {order.trigger_price}"
    if not order.is_position_tpsl:
        notional = order.notional_usd
        if notional is not None:
            part += f" (${notional:,.0f})"
    return part


def order_line(order: OpenOrder, position: Position | None = None) -> str:
    """One resting order, as both the wallet views and Order Alerts render it
    (issue #115) — shared here so the two can't drift (the #77 lesson).

    A plain limit reads notional-first like a position row (`LIT SELL $13,500
    @ 4.5`); a trigger order is labeled TP/SL from its orderType and reads
    against the price that arms it (its limitPx is only a slippage cap); a
    whole-position TP/SL has no order-level size, so it says what it is
    instead of inventing one. Builder-DEX coins render as the bare ticker.

    `position` is the wallet's current position on this coin (None if it holds
    none): the line closes with what the order would do to it (#123) — the
    relationship composes with any #115 TP/SL label, it never replaces it."""
    coin = display_coin(order.coin)
    side = "BUY" if order.is_buy else "SELL"
    if order.is_position_tpsl:
        base = f"{coin} {order.tpsl} @ {order.trigger_price} (whole position)"
    else:
        notional = order.notional_usd
        amount = f"${notional:,.0f}" if notional is not None else "size unknown"
        if order.is_trigger:
            base = f"{coin} {side} {order.tpsl} {amount} @ trigger {order.trigger_price}"
        else:
            base = f"{coin} {side} {amount} @ {order.limit_price}"
    return f"{base} → {order_relationship(order, position)}"


def order_relationship(order: OpenOrder, position: Position | None) -> str:
    """What placing `order` would do to the wallet's current position on that
    coin (issue #123): add to it, reduce/close it, flip it, or open a new one.

    Same-direction orders (a buy under a long, a sell under a short) *add*, and
    the line carries the position they grow — its size and entry — so the alert
    is self-contained. Opposing orders *would reduce/close* the position, or
    *would flip* the side when the order's notional exceeds the position's.
    Sizes are only as fresh as the last poll cycle (~10s), so a flip is always
    phrased conditionally, never as fact. No position on the coin is a *new
    position* — a stop-entry gains exactly this context.

    Notional-vs-notional drives the flip call; a whole-position TP/SL has no
    order-level notional (its size is 0) and so can only reduce/close what it is
    armed against, never read as a flip."""
    if position is None:
        return "new position"
    side_label = position.side.value.upper()
    # A buy grows a long / opposes a short; a sell the mirror — so the order
    # adds exactly when its buy-ness matches the position being long.
    if order.is_buy == (position.side is Side.LONG):
        now = f"{usd_size(position.size_usd)} @ {position.entry_price}"
        return f"adds to his {side_label} (now {now})"
    notional = order.notional_usd
    if notional is not None and notional > position.size_usd:
        flipped = Side.SHORT if position.side is Side.LONG else Side.LONG
        return f"would flip him {flipped.value.upper()}"
    return f"would reduce/close his {side_label}"


def signed_usd(amount: Decimal) -> str:
    sign = "-" if amount < 0 else "+"
    return f"{sign}${abs(amount):,.0f}"


def signed_pct(ratio: Decimal, *, decimals: int = 0) -> str:
    sign = "-" if ratio < 0 else "+"
    return f"{sign}{abs(ratio):.{decimals}%}"


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


def usd_size(amount: Decimal) -> str:
    """A compact dollar size that keeps one k/M decimal — $127.6k, $6.9k, $1.1M
    — for the order-relationship position context (#123). Unlike usd_compact
    (whose whole-number k-rounding, $127.6k → $128k, only wants a sense of
    scale), this sits beside a position the reader is about to act on, so the
    tenths matter. A trailing .0 drops ($6.0k → $6k) and sub-thousand sizes read
    in full ($940)."""
    value = abs(amount)
    if value >= 1_000_000:
        return f"${_trim_tenths(value / 1_000_000)}M"
    if value >= 1_000:
        return f"${_trim_tenths(value / 1_000)}k"
    return f"${value:,.0f}"


def _trim_tenths(value: Decimal) -> str:
    """`value` to one decimal place with a trailing .0 stripped (6.9 → "6.9",
    6.0 → "6"). The :.1f guarantees exactly one fractional digit, so at most one
    trailing zero is ever removed — the integer part's zeros are safe behind the
    decimal point."""
    return f"{value:.1f}".rstrip("0").rstrip(".")


def compact_price(price: Decimal) -> str:
    """A computed price (an entry/exit VWAP, #116) rounded back onto the
    exchange's own 5-significant-digit grid — Hyperliquid quantizes perp
    prices to 5 sig figs, so a size-weighted average's longer tail is
    precision the market never had, and it would dwarf the line it sits on.
    Trailing zeros drop (79.020 → 79.02) so prices read like the exchange
    prints them."""
    if price == 0:
        return "0"
    quantized = price.quantize(Decimal(1).scaleb(price.adjusted() - 4))
    return format(quantized.normalize(), "f")


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
