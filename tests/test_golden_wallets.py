"""Golden fixtures (V1 spec "Testing Decisions"): the 15 vetted wallets from
the ansem-bullpen R&D, fill histories recorded verbatim from the live info API
on 2026-07-10, run through the production parser and the fine-metric engine.

golden_metrics.json pins the expected output per wallet. Its win rates were
cross-checked at recording time against the independently vetted WALLETS.md
numbers (kept alongside as `wallets_md_win_rate`): wallets whose 2000-fill
window still covers the original scan reproduce them — #4 exactly (83.1% over
the same 71 trades) — while high-frequency wallets have drifted with their
window. The two stable-window wallets are asserted against WALLETS.md below;
everything else is pinned exactly so any engine change that shifts a metric
fails loudly here.
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

# Fill history reaches back past the 2026-07-07/08 vetting scan for these two,
# so WALLETS.md's independently verified win rates must reproduce (±2 points).
STABLE_WINDOW_WALLETS = [
    "0xfdf891f2b214a4c9374d26595ec6d4080262e381",  # #4: 83.1% over a full 27d window
    "0xf5b0af852e3dedc03b551f7050b616b5c77c7645",  # #15: 76.1% over 29 days
]


def recorded_fills(address: str) -> list[Fill]:
    raw = gzip.decompress((FIXTURES / "fills" / f"{address}.json.gz").read_bytes())
    return parse_fills(json.loads(raw))


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


@pytest.mark.parametrize("address", STABLE_WINDOW_WALLETS)
def test_stable_windows_reproduce_the_independently_vetted_win_rate(address: str) -> None:
    m = computed(address)
    assert m.win_rate is not None
    known = dec(GOLDEN[address]["wallets_md_win_rate"])
    assert known is not None
    assert abs(m.win_rate - known) <= Decimal("0.02")


@pytest.mark.parametrize("address", sorted(GOLDEN))
def test_no_vetted_wallet_is_mistaken_for_a_bot(address: str) -> None:
    # Even alongside whale-sized coarse month PnL, every vetted human clears
    # the Bot heuristics.
    assert classify_bot(computed(address), month_pnl=Decimal("3000000")) is None
