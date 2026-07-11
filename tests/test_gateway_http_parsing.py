"""Parsing the real Hyperliquid payload shapes into gateway types.

Payload shapes recorded from the live endpoints (stats-data leaderboard,
info API `portfolio` and `userFills`) during the 2026-07-09 ecosystem
survey / Bullpen R&D and the 2026-07-10 fixture recording (issue #8).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from epigone.gateway import GatewayError, Window
from epigone.gateway.http import parse_fills, parse_leaderboard, parse_positions

LEADERBOARD_PAYLOAD = {
    "leaderboardRows": [
        {
            "ethAddress": "0xAF0FDD39E5D92499B0ED9F68693DA99C0EC1E92E",
            "accountValue": "1234567.89",
            "displayName": "whale",
            "windowPerformances": [
                ["day", {"pnl": "1000.0", "roi": "0.01", "vlm": "50000.0"}],
                # perp-only variant exists in the live payload; V1 keeps combined windows.
                ["perpDay", {"pnl": "900.0", "roi": "0.009", "vlm": "45000.0"}],
                ["allTime", {"pnl": "999999.0", "roi": "3.5", "vlm": "9000000.0"}],
            ],
            "prize": 0,
        },
        {
            "ethAddress": "0x1111111111111111111111111111111111111111",
            "accountValue": "42.5",
            "displayName": None,
            "windowPerformances": [],
            "prize": 0,
        },
    ]
}


def test_parse_leaderboard_carries_coarse_metrics_per_row() -> None:
    entries = parse_leaderboard(LEADERBOARD_PAYLOAD)
    assert len(entries) == 2
    whale = entries[0]
    assert whale.address == "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
    assert whale.display_name == "whale"
    assert whale.account_value == Decimal("1234567.89")
    # Only the four combined windows are kept; perpDay is dropped.
    assert set(whale.windows) == {Window.DAY, Window.ALL_TIME}
    day = whale.windows[Window.DAY]
    assert day.pnl == Decimal("1000.0")
    assert day.roi == Decimal("0.01")
    assert day.volume == Decimal("50000.0")
    empty = entries[1]
    assert empty.display_name is None
    assert empty.account_value == Decimal("42.5")
    assert empty.windows == {}  # a fresh account with no window performances yet


def test_parse_leaderboard_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_leaderboard({"rows": []})


def _position_payload(**overrides: object) -> dict[str, object]:
    position = {
        "coin": "BTC",
        "szi": "0.1",
        "leverage": {"type": "cross", "value": 40},
        "entryPx": "38530.0",
        "positionValue": "3853.0",
        "unrealizedPnl": "344.0",
        "returnOnEquity": "3.57",
        "marginUsed": "96.325",
        **overrides,
    }
    return {"assetPositions": [{"type": "oneWay", "position": position}]}


def test_parse_positions_falls_back_to_notional_over_leverage_without_margin() -> None:
    # An exotic position can omit marginUsed / returnOnEquity; the derived
    # margin (notional / leverage) and return (uPnL / margin) keep the line honest
    # (issue #35).
    (pos,) = parse_positions(_position_payload(marginUsed=None, returnOnEquity=None))
    assert pos.margin_used is None
    assert pos.margin == Decimal("3853.0") / Decimal("40")  # notional / leverage
    assert pos.return_on_equity is None
    assert pos.return_on_margin == Decimal("344.0") / pos.margin


def test_parse_positions_uses_exact_margin_and_return_when_present() -> None:
    (pos,) = parse_positions(_position_payload())
    assert pos.margin == Decimal("96.325")  # the API's exact marginUsed, not derived
    assert pos.return_on_margin == Decimal("3.57")


# One perp close (recorded verbatim from userFills on 2026-07-10) and one
# spot dust conversion — the two extremes of the fill shapes we store.
FILLS_PAYLOAD = [
    {
        "coin": "xyz:SPCX",
        "px": "160.6",
        "sz": "12.46",
        "side": "A",
        "time": 1783236121626,
        "startPosition": "12.46",
        "dir": "Close Long",
        "closedPnl": "24.077704",
        "hash": "0x883ac1cf738538f489b4043f401f74020d8c00b50e8857c62c036d22328912df",
        "oid": 487750417890,
        "crossed": False,
        "fee": "0.060032",
        "tid": 565337166334317,
        "feeToken": "USDC",
        "twapId": None,
    },
    {
        "coin": "@151",
        "px": "2283.4",
        "sz": "0.000068939",
        "side": "A",
        "time": 1778803200071,
        "startPosition": "0.000068939",
        "dir": "Spot Dust Conversion",
        "closedPnl": "0.0",
        "hash": "0x" + "0" * 64,
        "oid": 426350455991,
        "crossed": True,
        "fee": "0.0",
        "tid": 0,
        "feeToken": "USDC",
        "twapId": None,
    },
]


def test_parse_fills_maps_the_recorded_shape() -> None:
    fills = parse_fills(FILLS_PAYLOAD)
    assert len(fills) == 2
    close = fills[0]
    assert close.coin == "xyz:SPCX"
    assert close.price == Decimal("160.6")
    assert close.size == Decimal("12.46")
    assert close.direction == "Close Long"
    assert close.closed_pnl == Decimal("24.077704")
    assert close.start_position == Decimal("12.46")
    assert close.crossed is False
    assert close.order_id == 487750417890
    assert close.time == datetime.fromtimestamp(1783236121.626, tz=UTC)
    assert close.is_perp and close.closes_position
    dust = fills[1]
    assert dust.crossed is True
    assert not dust.is_perp and not dust.closes_position


def test_parse_fills_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_fills([{"coin": "HYPE"}])
    with pytest.raises(GatewayError):
        parse_fills({"fills": []})
