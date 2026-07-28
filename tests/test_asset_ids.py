"""The coin-name → asset-id mapping (issue #135): what a cancel-by-oid signs
over. Order IS the data — a coin's universe position becomes the asset id —
so the parsers fail loudly on any shape that would shift an index, and the
builder-DEX offsets follow the SDK's own arithmetic (110000 + listing
position × 10000)."""

import pytest

from epigone.gateway import GatewayError, fetch_asset_ids
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.gateway.http import parse_perp_dexs, parse_perp_universe


@pytest.fixture
def gateway() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.perp_universes[None] = ["BTC", "ETH", "SOL"]
    fake.perp_dex_listing = ["xyz", "flip", "mkts"]
    fake.perp_universes["xyz"] = ["xyz:META", "xyz:BB"]
    fake.perp_universes["mkts"] = ["mkts:US500"]
    return fake


async def test_core_and_builder_offsets(gateway: FakeHyperliquidGateway) -> None:
    ids = await fetch_asset_ids(gateway)
    assert ids["BTC"] == 0
    assert ids["SOL"] == 2
    # xyz is listing position 0 → offset 110000; mkts position 2 → 130000.
    assert ids["xyz:META"] == 110_000
    assert ids["xyz:BB"] == 110_001
    assert ids["mkts:US500"] == 130_000


async def test_covered_dex_missing_from_listing_raises(
    gateway: FakeHyperliquidGateway,
) -> None:
    gateway.perp_dex_listing = ["xyz"]  # mkts vanished — offsets would be a guess
    with pytest.raises(GatewayError, match="mkts"):
        await fetch_asset_ids(gateway)


def test_parse_perp_universe_namespaces_builder_coins() -> None:
    payload = {"universe": [{"name": "META"}, {"name": "BB"}]}
    assert parse_perp_universe(payload, "xyz") == ["xyz:META", "xyz:BB"]
    assert parse_perp_universe({"universe": [{"name": "BTC"}]}, None) == ["BTC"]


def test_parse_perp_universe_rejects_shape_surprises() -> None:
    with pytest.raises(GatewayError):
        parse_perp_universe({"universe": [{"coin": "BTC"}]}, None)
    with pytest.raises(GatewayError):
        parse_perp_universe({"assets": []}, None)


def test_parse_perp_dexs_skips_the_core_placeholder() -> None:
    payload = [None, {"name": "xyz"}, {"name": "mkts"}]
    assert parse_perp_dexs(payload) == ["xyz", "mkts"]


def test_parse_perp_dexs_refuses_to_guess_offsets() -> None:
    # No leading null → position-1-counts-from arithmetic would be wrong.
    with pytest.raises(GatewayError):
        parse_perp_dexs([{"name": "xyz"}])
    with pytest.raises(GatewayError):
        parse_perp_dexs([])
    with pytest.raises(GatewayError):
        parse_perp_dexs({"dexs": []})
