"""The Metric Library registry (CONTEXT.md): every metric a Criteria can
filter or sort on, with the plain-language one-liner the builder shows while
a User picks. Explanations are lifted from docs/metrics.md — keep in sync.

Coarse metrics exist per timeframe (Universe-wide); fine metrics are computed
once per Trader from its recent fill history, so a fine filter quietly opts
the Criteria into fully-analyzed Traders only.
"""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum


class Unit(Enum):
    """Drives how a threshold is prompted for, parsed, and displayed."""

    PERCENT = "percent"  # User types 60, we store 0.60
    USD = "usd"
    COUNT = "count"
    NUMBER = "number"
    DURATION = "duration"  # User types 2d / 12h / 90m, we store seconds


class Scope(Enum):
    COARSE = "coarse"  # coarse_metrics: one row per Trader per timeframe
    FINE = "fine"  # fine_metrics: one row per Trader, recent fill window


@dataclass(frozen=True)
class MetricSpec:
    key: str  # ScreenerRow attribute and the stored Criteria key
    label: str
    unit: Unit
    scope: Scope
    sql: str  # column expression in the screener query — never user input
    explanation: str  # the one-liner shown during building (docs/metrics.md)
    example: str  # suggested input in the threshold prompt


_SPECS = [
    MetricSpec(
        key="roi",
        label="ROI",
        unit=Unit.PERCENT,
        scope=Scope.COARSE,
        sql="cm.roi",
        explanation=(
            "the account's percentage return over the timeframe — "
            "how hard the money worked, regardless of account size."
        ),
        example="20 for 20%",
    ),
    MetricSpec(
        key="pnl",
        label="PnL",
        unit=Unit.USD,
        scope=Scope.COARSE,
        sql="cm.pnl",
        explanation="how much money the account made or lost over the timeframe, in dollars.",
        example="10000 or 10k",
    ),
    MetricSpec(
        key="volume",
        label="Volume",
        unit=Unit.USD,
        scope=Scope.COARSE,
        sql="cm.volume",
        explanation=(
            "how much the account traded over the timeframe, in dollars — activity, not profit."
        ),
        example="1m",
    ),
    MetricSpec(
        key="account_value",
        label="Account value",
        unit=Unit.USD,
        scope=Scope.COARSE,
        sql="cm.account_value",
        explanation="what the account is worth right now, in dollars.",
        example="50k",
    ),
    MetricSpec(
        key="win_rate",
        label="Win rate",
        unit=Unit.PERCENT,
        scope=Scope.FINE,
        sql="fm.win_rate",
        explanation=(
            "out of the positions this account opened and fully closed, "
            "the share that ended in net profit."
        ),
        example="60 for 60%",
    ),
    MetricSpec(
        key="trade_count",
        label="Closed trades",
        unit=Unit.COUNT,
        scope=Scope.FINE,
        sql="fm.trade_count",
        explanation=(
            "how many positions the account opened and fully closed in its recent "
            "history — more trades, more evidence the other numbers are real."
        ),
        example="100",
    ),
    MetricSpec(
        key="avg_win",
        label="Average win",
        unit=Unit.USD,
        scope=Scope.FINE,
        sql="fm.avg_win",
        explanation="the typical profit on this account's winning trades, in dollars.",
        example="500",
    ),
    MetricSpec(
        key="avg_loss",
        label="Average loss",
        unit=Unit.USD,
        scope=Scope.FINE,
        sql="fm.avg_loss",
        explanation=(
            "the typical damage of this account's losing trades, in dollars "
            "(a positive number — smaller is better)."
        ),
        example="250",
    ),
    MetricSpec(
        key="sharpe",
        label="Sharpe",
        unit=Unit.NUMBER,
        scope=Scope.FINE,
        sql="fm.sharpe",
        explanation=(
            "how steady the daily profits are — high means smooth earning, "
            "low means a rollercoaster that happens to end up positive."
        ),
        example="2",
    ),
    MetricSpec(
        key="max_drawdown",
        label="Max drawdown",
        unit=Unit.USD,
        scope=Scope.FINE,
        sql="fm.max_drawdown",
        explanation=(
            "the deepest hole the account dug from its own peak — "
            "how much giveback you'd have sat through at worst."
        ),
        example="5000",
    ),
    MetricSpec(
        key="avg_leverage",
        label="Average leverage",
        unit=Unit.NUMBER,
        scope=Scope.FINE,
        sql="fm.avg_leverage",
        explanation="roughly how many times the account's own money it puts into a typical trade.",
        example="3",
    ),
    MetricSpec(
        key="maker_share",
        label="Maker share",
        unit=Unit.PERCENT,
        scope=Scope.FINE,
        sql="fm.maker_share",
        explanation=(
            "how often the account waits with resting orders (maker) versus paying up "
            "to take liquidity (taker) — very high maker share smells like a "
            "market-making machine."
        ),
        example="80 for 80%",
    ),
    MetricSpec(
        key="avg_hold_seconds",
        label="Avg hold",
        unit=Unit.DURATION,
        scope=Scope.FINE,
        sql="fm.avg_hold_seconds",
        explanation=(
            "how long the account typically holds a position before closing it — "
            "short means scalping, long means swinging."
        ),
        example="2d, 12h, or 90m",
    ),
]

METRICS: dict[str, MetricSpec] = {spec.key: spec for spec in _SPECS}


_DURATION_UNITS = {"d": 86400, "h": 3600, "m": 60, "s": 1}
_DURATION_TOKEN = re.compile(r"(\d+)\s*([dhms])")


def parse_duration(text: str) -> Decimal | None:
    """A holding-time threshold → seconds. Accepts one or more `<n><unit>` terms
    (`2d`, `12h`, `90m`, `1d 6h`); None if nothing parses. `m` is minutes here —
    never the millions shorthand the numeric units use."""
    matches = _DURATION_TOKEN.findall(text.strip().lower())
    if not matches:
        return None
    # Reject stray characters so "2dabc" or "2days" don't silently parse to 2d.
    if _DURATION_TOKEN.sub("", text.strip().lower()).strip():
        return None
    return Decimal(sum(int(n) * _DURATION_UNITS[unit] for n, unit in matches))


def format_duration(seconds: int) -> str:
    """A span at one or two units of resolution: 45s / 12m / 3h 20m / 2d 5h.

    The canonical compact-duration formatter — the holding-time label, the
    track-record line and the position ages (epigone.bot.format) all render
    through it, so a `2d 4h` looks the same everywhere."""
    seconds = max(0, seconds)
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


def parse_threshold(spec: MetricSpec, text: str) -> Decimal | None:
    """A User-typed threshold → the stored value, or None when unparseable.
    Forgiving on the way in: $ , % x and k/m suffixes are all accepted."""
    if spec.unit is Unit.DURATION:
        return parse_duration(text)
    raw = text.strip().lower().replace(",", "").replace("$", "").replace("%", "").replace(" ", "")
    multiplier = Decimal(1)
    if raw.endswith("k"):
        multiplier = Decimal(1_000)
        raw = raw[:-1]
    elif raw.endswith("m"):
        multiplier = Decimal(1_000_000)
        raw = raw[:-1]
    elif raw.endswith("x"):
        raw = raw[:-1]
    try:
        value = Decimal(raw) * multiplier
    except InvalidOperation:
        return None
    if spec.unit is Unit.PERCENT:
        return value / 100
    if spec.unit is Unit.COUNT and value != value.to_integral_value():
        return None
    return value


def format_value(spec: MetricSpec, value: Decimal) -> str:
    """A stored value → what the User sees, inverting parse_threshold."""
    if spec.unit is Unit.PERCENT:
        return f"{_trim(value * 100)}%"
    if spec.unit is Unit.USD:
        sign = "-" if value < 0 else ""
        return f"{sign}${_trim(abs(value), grouped=True)}"
    if spec.unit is Unit.COUNT:
        return f"{value:,.0f}"
    if spec.unit is Unit.DURATION:
        return format_duration(int(value))
    return _trim(value)


def _trim(value: Decimal, *, grouped: bool = False) -> str:
    text = f"{value:,.2f}" if grouped else f"{value:.2f}"
    return text.rstrip("0").rstrip(".") if "." in text else text
