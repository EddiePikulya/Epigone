"""The HyperliquidGateway seam: the fake honors the interface tests build on."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from epigone.gateway import (
    Fill,
    GatewayError,
    HyperliquidGateway,
    LeaderboardEntry,
    PortfolioWindow,
    Position,
    Side,
    Window,
)
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.fills import fill

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


WHALE_ENTRY = LeaderboardEntry(address=WHALE.lower(), display_name="whale")

DAY_WINDOW = PortfolioWindow(
    pnl=Decimal("5000"),
    volume=Decimal("300000"),
    account_value=Decimal("1200000"),
    starting_account_value=Decimal("1195000"),
)


async def test_fake_returns_the_leaderboard_it_was_given() -> None:
    fake = FakeHyperliquidGateway()
    fake.set_leaderboard([WHALE_ENTRY])
    gateway: HyperliquidGateway = fake
    assert await gateway.get_leaderboard() == [WHALE_ENTRY]


async def test_fake_leaderboard_failure_raises_gateway_error() -> None:
    fake = FakeHyperliquidGateway()
    fake.leaderboard_error = GatewayError("stats-data is down")
    with pytest.raises(GatewayError):
        await fake.get_leaderboard()


async def test_fake_returns_portfolio_windows_for_a_trader() -> None:
    fake = FakeHyperliquidGateway()
    fake.set_portfolio(WHALE, {Window.DAY: DAY_WINDOW})
    gateway: HyperliquidGateway = fake
    assert await gateway.get_portfolio(WHALE.lower()) == {Window.DAY: DAY_WINDOW}


async def test_fake_portfolio_failure_raises_configured_error() -> None:
    fake = FakeHyperliquidGateway()
    fake.portfolio_errors[WHALE.lower()] = GatewayError("info API timeout")
    with pytest.raises(GatewayError):
        await fake.get_portfolio(WHALE)


HYPE_CLOSE_FILL = Fill(
    coin="HYPE",
    price=Decimal("52.10"),
    size=Decimal("1000"),
    direction="Close Long",
    closed_pnl=Decimal("3900"),
    start_position=Decimal("4600"),
    crossed=False,
    order_id=487750417890,
    time=datetime(2026, 7, 8, 14, 0, tzinfo=UTC),
)


async def test_fake_returns_fills_set_for_a_trader() -> None:
    fake = FakeHyperliquidGateway()
    fake.set_fills(WHALE, [HYPE_CLOSE_FILL])
    gateway: HyperliquidGateway = fake
    assert await gateway.get_fills(WHALE.lower()) == [HYPE_CLOSE_FILL]


async def test_unknown_trader_has_no_fills() -> None:
    gateway: HyperliquidGateway = FakeHyperliquidGateway()
    assert await gateway.get_fills("0x" + "0" * 40) == []


async def test_fake_fills_failure_raises_configured_error() -> None:
    fake = FakeHyperliquidGateway()
    fake.fills_errors[WHALE.lower()] = GatewayError("info API timeout")
    with pytest.raises(GatewayError):
        await fake.get_fills(WHALE)


def test_perp_fills_mention_a_leg_or_settle_a_market() -> None:
    perp_dirs = ["Open Long", "Close Short", "Long > Short", "Settlement"]
    assert [d for d in perp_dirs if fill(d).is_perp] == perp_dirs
    assert not fill("Buy", coin="PURR/USDC").is_perp
    assert not fill("Sell", coin="@107").is_perp
    assert not fill("Spot Dust Conversion", coin="@151").is_perp


def test_closing_fills_are_closes_flips_liquidations_and_settlements() -> None:
    closing = ["Close Long", "Close Short", "Long > Short", "Short > Long", "Settlement"]
    assert [d for d in closing if fill(d).closes_position] == closing
    for direction in ["Open Long", "Open Short", "Buy", "Sell", "Spot Dust Conversion"]:
        assert not fill(direction).closes_position


def _fake_with_whale() -> FakeHyperliquidGateway:
    fake = FakeHyperliquidGateway()
    fake.set_positions(WHALE, [HYPE_LONG])
    return fake
