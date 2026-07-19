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


@pytest.mark.parametrize("address", sorted(GOLDEN))
def test_no_vetted_wallet_is_mistaken_for_a_bot(address: str) -> None:
    # Even alongside whale-sized coarse month PnL, every vetted human clears
    # the Bot heuristics.
    assert classify_bot(computed(address), month_pnl=Decimal("3000000")) is None
