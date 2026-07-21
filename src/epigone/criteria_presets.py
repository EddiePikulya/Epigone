"""Starter Criteria presets (issue #71): three curated definitions of "best"
that appear in every User's criteria list — existing and new — ready to run
like a saved one, with nothing to set up.

Most Users don't know which metrics combine well or what thresholds are sane;
any single metric alone finds something degenerate (lottery winners, dust bots,
churners). Each preset teaches by example — a small, deliberate combination.

The definitions live here in code, versioned: a single source of truth, so
recalibrating a threshold is a normal PR (the survivor counts below were
calibrated live 2026-07-21, bots excluded). Per-User state — which presets a
User has deleted — lives in Postgres (criteria_preset_dismissals), keyed on the
stable `key`, never the thresholds: bumping a threshold in a future version
updates the preset for everyone who still sees it and leaves those who deleted
it deleted.

Presets are not editable in place; a User who wants a variant builds their own
Criteria as usual. They run through run_criteria exactly like a saved Criteria,
so the screener output is identical to running the same filters by hand.

PERCENT thresholds store fractions (the Unit.PERCENT convention): ROI 30% is
Decimal("0.30"), not 30.
"""

from dataclasses import dataclass
from decimal import Decimal

from epigone.gateway import Window
from epigone.screener import Criteria, Filter, Op


@dataclass(frozen=True)
class CriteriaPreset:
    """A curated Criteria shown in every list. `key` is the stable code identity
    a dismissal is recorded against — it must never change once shipped, even
    when the name or thresholds are recalibrated."""

    key: str
    name: str
    criteria: Criteria


def _gte(metric: str, threshold: str) -> Filter:
    return Filter(metric=metric, op=Op.GTE, threshold=Decimal(threshold))


def _lte(metric: str, threshold: str) -> Filter:
    return Filter(metric=metric, op=Op.LTE, threshold=Decimal(threshold))


# The guided builder offers only ≥ and ≤, so the table's strict ">"/"<" bounds
# map to GTE/LTE — the boundary difference is immaterial against continuous
# metrics and keeps a preset expressible as an ordinary Criteria.
PRESETS: tuple[CriteriaPreset, ...] = (
    # Steady earners: the smoothness trio — steadiest decile (Sharpe), real
    # money made, and enough closed trades that the numbers aren't noise.
    CriteriaPreset(
        key="steady_earners",
        name="Steady earners",
        criteria=Criteria(
            filters=(
                _gte("sharpe", "7"),
                _gte("pnl", "50000"),
                _gte("trade_count", "20"),
            ),
            time_window=Window.MONTH,
            sort_key="pnl",
            sort_desc=True,
        ),
    ),
    # Careful whales: big accounts printing with low leverage — copyable sizing.
    CriteriaPreset(
        key="careful_whales",
        name="Careful whales",
        criteria=Criteria(
            filters=(
                _gte("account_value", "500000"),
                _gte("pnl", "100000"),
                _lte("avg_leverage", "3"),
                _gte("sharpe", "3"),
            ),
            time_window=Window.MONTH,
            sort_key="pnl",
            sort_desc=True,
        ),
    ),
    # Hot hands: who's on a verified run this week. ROI 30% → 0.30 (PERCENT
    # stores fractions).
    CriteriaPreset(
        key="hot_hands",
        name="Hot hands",
        criteria=Criteria(
            filters=(
                _gte("roi", "0.30"),
                _gte("pnl", "25000"),
                _gte("trade_count", "5"),
            ),
            time_window=Window.WEEK,
            sort_key="roi",
            sort_desc=True,
        ),
    ),
)

PRESETS_BY_KEY: dict[str, CriteriaPreset] = {p.key: p for p in PRESETS}
