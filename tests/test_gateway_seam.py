"""The HyperliquidGateway seam: the fake honors the interface tests build on."""

from decimal import Decimal

from epigone.gateway import HyperliquidGateway, Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway

WHALE = "0xAF0FDD39E5D92499B0ED9F68693DA99C0EC1E92E"

HYPE_LONG = Position(
    coin="HYPE",
    side=Side.LONG,
    size_usd=Decimal("240000"),
    leverage=Decimal("5"),
    entry_price=Decimal("48.20"),
    unrealized_pnl=Decimal("18200"),
)


async def test_fake_returns_positions_set_for_a_trader() -> None:
    gateway: HyperliquidGateway = _fake_with_whale()
    positions = await gateway.get_open_positions(WHALE)
    assert positions == [HYPE_LONG]


async def test_address_lookup_is_case_insensitive() -> None:
    gateway: HyperliquidGateway = _fake_with_whale()
    assert await gateway.get_open_positions(WHALE.lower()) == [HYPE_LONG]


async def test_unknown_trader_has_no_positions() -> None:
    gateway: HyperliquidGateway = FakeHyperliquidGateway()
    assert await gateway.get_open_positions("0x" + "0" * 40) == []


def _fake_with_whale() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.set_positions(WHALE, [HYPE_LONG])
    return fake
