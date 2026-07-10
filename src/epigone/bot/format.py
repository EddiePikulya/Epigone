"""Shared Telegram text formatting: used by the dialog handlers and the
Position Alert renderer."""

from datetime import datetime
from decimal import Decimal


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
    """Compact holding time: 45s / 12m / 3h 20m / 2d 5h."""
    seconds = int((closed_at - opened_at).total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h {minutes % 60}m" if minutes % 60 else f"{hours}h"
    days = hours // 24
    return f"{days}d {hours % 24}h" if hours % 24 else f"{days}d"
