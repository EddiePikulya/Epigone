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
