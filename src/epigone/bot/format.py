"""Shared Telegram text formatting: used by the dialog handlers and the
Position Alert renderer."""

from datetime import datetime
from decimal import Decimal

from epigone.metrics.library import format_duration


def short_address(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}"


def trader_label(display_name: str | None, address: str) -> str:
    """How alerts identify a Trader: leaderboard label plus short address,
    or just the short address for unlabeled wallets."""
    short = short_address(address)
    return f"{display_name} ({short})" if display_name else short


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
