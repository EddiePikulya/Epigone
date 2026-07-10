"""Parsing the real Hyperliquid payload shapes into gateway types.

Payload shapes recorded from the live endpoints (stats-data leaderboard,
info API `portfolio` and `userFills`) during the 2026-07-09 ecosystem
survey / Bullpen R&D and the 2026-07-10 fixture recording (issue #8).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from epigone.gateway import GatewayError, Window
from epigone.gateway.http import parse_fills, parse_leaderboard, parse_portfolio

LEADERBOARD_PAYLOAD = {
    "leaderboardRows": [
        {
            "ethAddress": "0xAF0FDD39E5D92499B0ED9F68693DA99C0EC1E92E",
            "accountValue": "1234567.89",
            "displayName": "whale",
            "windowPerformances": [
                ["day", {"pnl": "1000.0", "roi": "0.01", "vlm": "50000.0"}],
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


def test_parse_leaderboard_yields_an_entry_per_row() -> None:
    entries = parse_leaderboard(LEADERBOARD_PAYLOAD)
    assert len(entries) == 2
    assert entries[0].address == "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
    assert entries[0].display_name == "whale"
    assert entries[1].display_name is None


def test_parse_leaderboard_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_leaderboard({"rows": []})


PORTFOLIO_PAYLOAD = [
    [
        "day",
        {
            "accountValueHistory": [[1752096000000, "1000.0"], [1752139200000, "1100.0"]],
            "pnlHistory": [[1752096000000, "0.0"], [1752139200000, "100.0"]],
            "vlm": "50000.0",
        },
    ],
    [
        "week",
        {
            "accountValueHistory": [[1751587200000, "900.0"], [1752139200000, "1100.0"]],
            "pnlHistory": [[1751587200000, "0.0"], [1752139200000, "200.0"]],
            "vlm": "250000.0",
        },
    ],
    [
        "month",
        {
            "accountValueHistory": [[1749716400000, "500.0"], [1752139200000, "1100.0"]],
            "pnlHistory": [[1749716400000, "0.0"], [1752139200000, "600.0"]],
            "vlm": "800000.0",
        },
    ],
    [
        "allTime",
        {
            "accountValueHistory": [[1700000000000, "100.0"], [1752139200000, "1100.0"]],
            "pnlHistory": [[1700000000000, "0.0"], [1752139200000, "1000.0"]],
            "vlm": "2000000.0",
        },
    ],
    # perp-only variants exist in the live payload; V1 stores the combined windows.
    [
        "perpDay",
        {
            "accountValueHistory": [[1752096000000, "1000.0"]],
            "pnlHistory": [[1752096000000, "0.0"]],
            "vlm": "50000.0",
        },
    ],
]


def test_parse_portfolio_maps_the_four_combined_windows() -> None:
    windows = parse_portfolio(PORTFOLIO_PAYLOAD)
    assert set(windows) == {Window.DAY, Window.WEEK, Window.MONTH, Window.ALL_TIME}
    week = windows[Window.WEEK]
    assert week.pnl == Decimal("200.0")  # latest point of the window's pnl history
    assert week.volume == Decimal("250000.0")
    assert week.account_value == Decimal("1100.0")
    assert week.starting_account_value == Decimal("900.0")


def test_parse_portfolio_treats_empty_histories_as_zero() -> None:
    payload = [["day", {"accountValueHistory": [], "pnlHistory": [], "vlm": "0.0"}]]
    day = parse_portfolio(payload)[Window.DAY]
    assert day.pnl == Decimal("0")
    assert day.account_value == Decimal("0")
    assert day.starting_account_value == Decimal("0")


def test_parse_portfolio_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_portfolio({"day": {}})


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
