"""The Metric Library registry (CONTEXT.md): every metric a Criteria can
filter or sort on, with the plain-language one-liner the builder shows while
a User picks. Explanations are lifted from docs/metrics.md — keep in sync.

Coarse metrics exist per timeframe (Universe-wide); fine metrics are computed
once per Trader from its recent fill history, so a fine filter quietly opts
the Criteria into fully-analyzed Traders only.
"""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum


class Unit(Enum):
    """Drives how a threshold is prompted for, parsed, and displayed."""

    PERCENT = "percent"  # User types 60, we store 0.60
    USD = "usd"
    COUNT = "count"
    NUMBER = "number"


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
        explanation="out of the trades this account closed, the share that ended in profit.",
        example="60 for 60%",
    ),
    MetricSpec(
        key="trade_count",
        label="Closed trades",
        unit=Unit.COUNT,
        scope=Scope.FINE,
        sql="fm.trade_count",
        explanation=(
            "how many trades the account closed in its recent history — "
            "more trades, more evidence the other numbers are real."
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
]

METRICS: dict[str, MetricSpec] = {spec.key: spec for spec in _SPECS}


def parse_threshold(spec: MetricSpec, text: str) -> Decimal | None:
    """A User-typed threshold → the stored value, or None when unparseable.
    Forgiving on the way in: $ , % x and k/m suffixes are all accepted."""
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
    return _trim(value)


def _trim(value: Decimal, *, grouped: bool = False) -> str:
    text = f"{value:,.2f}" if grouped else f"{value:.2f}"
    return text.rstrip("0").rstrip(".") if "." in text else text
