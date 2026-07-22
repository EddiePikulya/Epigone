"""Golden fixtures (V1 spec "Testing Decisions"): the 15 vetted wallets from
the ansem-bullpen R&D, fill histories recorded verbatim from the live info API
on 2026-07-10, run through the production parser and the fine-metric engine.

golden_metrics.json pins the expected output per wallet — regenerated on the
round-trip trade basis (issue #58), so trade counts and win rates count
completed round-trips, not closing orders; regenerated again under the
startPosition continuity guard (issue #63), which demoted 4 round-trips
across 3 wallets whose walks spanned gaps in the recorded history (for the
TWAP whale 0xaf0f…e92e those gaps are its TWAP slice fills, invisible to
userFills), leaving realized_pnl identical everywhere. The `wallets_md_win_rate` field
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
from epigone.gateway.http import _merge_execution_order, parse_fills, parse_twap_fills
from epigone.metrics.bots import classify_bot
from epigone.metrics.fine import (
    FineMetrics,
    _breaks_continuity,
    _post_position,
    compute_fine_metrics,
    extract_state,
    metrics_from_state,
)

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
        ("effective_coins", "0.0001"),
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


def _continuous(a: Fill, b: Fill) -> bool:
    # The engine's own walk arithmetic and dust threshold (#63) — asserting
    # through them keeps this check and the production guard from diverging.
    return not _breaks_continuity(_post_position(a), b.start_position)


# --- TWAP slice fills merged into the stream (issue #63) ----------------------
# Hyperliquid serves TWAP slice executions only from userTwapSliceFills — they
# never appear in userFills. Recorded verbatim from the live API on 2026-07-19
# for the golden TWAP whale 0xaf0f…e92e, mid-TWAP-campaign: its 88-BTC short
# was built entirely from TWAP slices (513 BTC slices in ~2h), so the
# fills-only walk reads BTC as flat and fabricates open episodes for other
# coins. Both payloads run through the production parsers and the same merge
# the gateway performs, then through the engine — pinning what the merged
# stream restores and what the continuity guard refuses to fabricate.

TWAP_WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"


def _twap_whale_streams() -> tuple[list[Fill], list[Fill]]:
    """(regular, twap) — each normalized to execution order as get_fills does."""

    def load(name: str) -> list:
        raw = gzip.decompress((FIXTURES / "twap" / f"{TWAP_WHALE}.{name}.json.gz").read_bytes())
        return json.loads(raw)

    regular = list(reversed(parse_fills(load("user_fills"))))
    twap = list(reversed(parse_twap_fills(load("twap_slice_fills"))))
    return regular, twap


def test_twap_slices_are_disjoint_from_user_fills() -> None:
    # The premise of #63: the two endpoints overlap in time yet share nothing —
    # TWAP executions are invisible to userFills, not duplicated by it.
    regular, twap = _twap_whale_streams()
    regular_ids = {(f.coin, f.time, f.order_id) for f in regular}
    assert regular[0].time < twap[0].time < regular[-1].time  # windows overlap
    assert not any((f.coin, f.time, f.order_id) in regular_ids for f in twap)


def test_the_merged_twap_stream_satisfies_within_ms_continuity() -> None:
    # Same bar as the golden wallets: a same-ms continuity break means the
    # merge lost execution order. (Time-gapped breaks remain — the TWAP
    # endpoint's 2000-cap window is hours where userFills' is days, so older
    # TWAP activity is genuinely missing; the engine's guard owns that.)
    regular, twap = _twap_whale_streams()
    merged = _merge_execution_order(regular, twap)
    by_coin: dict[str, list[Fill]] = {}
    for f in merged:
        if f.is_perp:
            by_coin.setdefault(f.coin, []).append(f)
    same_ms_breaks = sum(
        1
        for fills in by_coin.values()
        for a, b in zip(fills, fills[1:], strict=False)
        if a.time == b.time and not _continuous(a, b)
    )
    assert same_ms_breaks == 0


def test_the_merged_stream_sees_the_twap_built_position_user_fills_cannot() -> None:
    regular, twap = _twap_whale_streams()
    merged = _merge_execution_order(regular, twap)

    # The 88-BTC short: 513 of its slices live only on the TWAP endpoint, and
    # the last merged BTC fill leaves the walk exactly at the exchange's -88.
    merged_btc = [f for f in merged if f.coin == "BTC"]
    assert len(merged_btc) == 725
    assert sum(1 for f in twap if f.coin == "BTC") == 513
    assert _post_position(merged_btc[-1]) == Decimal("-88.0")

    # userFills alone reads that campaign as silence: no BTC fill in its last
    # two days, so a fills-only walk calls the wallet flat on BTC.
    assert not [f for f in regular if f.coin == "BTC" and f.time >= twap[0].time]


def test_the_guard_demotes_the_twap_blind_episodes_instead_of_faking_them() -> None:
    # The fills-only walk invents open episodes with empty accumulators — the
    # exact "LIT open episode with pnl=0, peak_notional=0" symptom that
    # surfaced in production (#63). Under the guard, the merged stream demotes
    # every episode whose walk still spans missing TWAP history (the endpoint's
    # window is truncated) rather than persist fabricated numbers; the banked
    # money the TWAP closes reveal stays, comprehensively counted.
    regular, twap = _twap_whale_streams()
    merged = _merge_execution_order(regular, twap)

    blind = extract_state(regular)
    assert [(e.coin, e.pnl, e.peak_notional) for e in blind.open_episodes] == [
        ("HYPE", Decimal(0), Decimal(0)),
        ("LIT", Decimal(0), Decimal(0)),
    ]  # fabricated: the exchange holds far larger positions than these walks say

    state = extract_state(merged)
    assert state.open_episodes == ()  # demoted, not faked

    merged_metrics = metrics_from_state(state, None)
    blind_metrics = metrics_from_state(blind, None)
    assert merged_metrics.perp_fill_count == 2801  # vs 801 TWAP-blind
    assert blind_metrics.perp_fill_count == 801
    # The TWAP slices' closes bank ~28k the blind stream never saw.
    assert merged_metrics.realized_pnl.quantize(Decimal("0.01")) == Decimal("177513.30")
    assert blind_metrics.realized_pnl.quantize(Decimal("0.01")) == Decimal("149292.11")


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
