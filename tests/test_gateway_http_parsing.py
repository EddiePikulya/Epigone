"""Parsing the real Hyperliquid payload shapes into gateway types.

Payload shapes recorded from the live endpoints (stats-data leaderboard,
info API `portfolio`) during the 2026-07-09 ecosystem survey / Bullpen R&D.
"""

from decimal import Decimal

import pytest

from epigone.gateway import GatewayError, Window
from epigone.gateway.http import parse_leaderboard, parse_portfolio

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
    assert entries[0].account_value == Decimal("1234567.89")
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
