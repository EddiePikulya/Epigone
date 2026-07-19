"""Golden fixtures (V1 spec "Testing Decisions"): the 15 vetted wallets from
the ansem-bullpen R&D, fill histories recorded verbatim from the live info API
on 2026-07-10, run through the production parser and the fine-metric engine.

golden_metrics.json pins the expected output per wallet — regenerated on the
round-trip trade basis (issue #58), so trade counts and win rates count
completed round-trips, not closing orders. The `wallets_md_win_rate` field
keeps the independently vetted WALLETS.md numbers as provenance; they were
computed per closing order, a deliberately different statistic now, so they
are no longer asserted against directly (the pre-#58 recording cross-checked
them: the stable-window wallets reproduced within 2 points). Everything is
pinned exactly so any engine change that shifts a metric fails loudly here.
"""

import gzip
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from epigone.gateway import Fill
from epigone.gateway.http import parse_fills
from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import FineMetrics, compute_fine_metrics

FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN: dict[str, dict[str, str | int | None]] = json.loads(
    (FIXTURES / "golden_metrics.json").read_text()
)


def recorded_fills(address: str) -> list[Fill]:
    """The fixtures are verbatim userFills payloads — NEWEST-first, with array
    position the only within-millisecond execution-order signal. Reverse to
    execution order exactly as HttpHyperliquidGateway.get_fills does; feeding
    the engine as-recorded order corrupts ~every round-trip (#58 review)."""
    raw = gzip.decompress((FIXTURES / "fills" / f"{address}.json.gz").read_bytes())
    return list(reversed(parse_fills(json.loads(raw))))


def computed(address: str) -> FineMetrics:
    return compute_fine_metrics(recorded_fills(address), account_value=None)


def dec(value: str | int | None) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


@pytest.mark.parametrize("address", sorted(GOLDEN))
def test_engine_reproduces_the_pinned_profile(address: str) -> None:
    expected = GOLDEN[address]
    m = computed(address)
    assert m.trade_count == expected["trade_count"]
    assert m.window_start == datetime.fromisoformat(str(expected["window_start"]))
    assert m.window_end == datetime.fromisoformat(str(expected["window_end"]))
    for metric, places in [
        ("win_rate", "0.0001"),
        ("avg_win", "0.01"),
        ("avg_loss", "0.01"),
        ("sharpe", "0.0001"),
        ("max_drawdown", "0.01"),
        ("maker_share", "0.0001"),
        ("realized_pnl", "0.01"),
    ]:
        value: Decimal | None = getattr(m, metric)
        quantized = value.quantize(Decimal(places)) if value is not None else None
        assert quantized == dec(expected[metric]), metric


@pytest.mark.parametrize("address", sorted(GOLDEN))
def test_no_vetted_wallet_is_mistaken_for_a_bot(address: str) -> None:
    # Even alongside whale-sized coarse month PnL, every vetted human clears
    # the Bot heuristics.
    assert classify_bot(computed(address), month_pnl=Decimal("3000000")) is None


# --- Position continuity (issue #58 review) -----------------------------------
# The ground truth for fill ordering: walking a coin's perp fills in execution
# order, each fill's post position must equal the next fill's start position.
# Same-order / same-block fills share one millisecond, so a wrong within-ms
# order breaks this chain at ~every same-ms boundary (measured ~100% of
# transitions on as-recorded newest-first order vs ~0% reversed).


def _post_position(f: Fill) -> Decimal:
    if f.closes_position:
        return f.start_position + (f.size if f.start_position < 0 else -f.size)
    return f.start_position + (f.size if "Long" in f.direction else -f.size)


def _continuous(a: Fill, b: Fill) -> bool:
    # The API's own strings carry float-representation dust (~1e-10 relative,
    # e.g. startPosition "3472099.9999999998" after "3472100.0"); a genuine
    # ordering break is off by a whole fill. 1e-6 relative separates them.
    diff = abs(_post_position(a) - b.start_position)
    return diff <= max(abs(b.start_position), Decimal(1)) * Decimal("0.000001")


@pytest.mark.parametrize("address", sorted(GOLDEN))
def test_recorded_fills_satisfy_position_continuity(address: str) -> None:
    by_coin: dict[str, list[Fill]] = {}
    for f in recorded_fills(address):
        if f.is_perp:
            by_coin.setdefault(f.coin, []).append(f)
    same_ms_breaks = 0
    breaks = 0
    total = 0
    for fills in by_coin.values():
        for a, b in zip(fills, fills[1:], strict=False):
            total += 1
            if not _continuous(a, b):
                breaks += 1
                if a.time == b.time:
                    same_ms_breaks += 1
    # Ordering must be perfect: a same-ms break means execution order is lost.
    assert same_ms_breaks == 0
    # Time-gapped breaks are holes in the recorded history (fills the API never
    # served), not ordering errors; the worst golden wallet carries ~3.5%.
    assert breaks <= total * 0.05
