"""Parsing the real Hyperliquid payload shapes into gateway types.

Payload shapes recorded from the live endpoints (stats-data leaderboard,
info API `portfolio` and `userFills`) during the 2026-07-09 ecosystem
survey / Bullpen R&D and the 2026-07-10 fixture recording (issue #8).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from epigone.gateway import GatewayError, Window
from epigone.gateway.http import (
    parse_fills,
    parse_leaderboard,
    parse_open_orders,
    parse_positions,
)

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


# Recorded verbatim from live frontendOpenOrders responses on 2026-07-24
# (issue #115): a plain resting limit, a stop-market trigger, and a
# position-wide TP (isPositionTpsl, sz "0.0" — sized to the position at
# trigger time).
OPEN_ORDERS_PAYLOAD = [
    {
        "coin": "LIT",
        "side": "A",
        "limitPx": "4.5",
        "sz": "3000.0",
        "oid": 303953272739,
        "timestamp": 1769530795995,
        "triggerCondition": "N/A",
        "isTrigger": False,
        "triggerPx": "0.0",
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": False,
        "orderType": "Limit",
        "origSz": "3000.0",
        "tif": "Alo",
        "cloid": None,
    },
    {
        "coin": "HYPE",
        "side": "B",
        "limitPx": "68.31",
        "sz": "75.0",
        "oid": 501580762100,
        "timestamp": 1784845199675,
        "triggerCondition": "Price above 63.25",
        "isTrigger": True,
        "triggerPx": "63.25",
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": False,
        "orderType": "Stop Market",
        "origSz": "75.0",
        "tif": None,
        "cloid": None,
    },
    {
        "coin": "CASHCAT",
        "side": "A",
        "limitPx": "0.10212",
        "sz": "0.0",
        "oid": 498756412140,
        "timestamp": 1784477557437,
        "triggerCondition": "Price above 0.111",
        "isTrigger": True,
        "triggerPx": "0.111",
        "children": [],
        "isPositionTpsl": True,
        "reduceOnly": True,
        "orderType": "Take Profit Market",
        "origSz": "0.0",
        "tif": None,
        "cloid": None,
    },
]


def test_parse_open_orders_maps_the_recorded_shapes() -> None:
    orders = parse_open_orders(OPEN_ORDERS_PAYLOAD)
    assert len(orders) == 3

    limit = orders[0]
    assert limit.coin == "LIT"
    assert limit.is_buy is False  # side "A" = ask/sell
    assert limit.limit_price == Decimal("4.5")
    assert limit.size == Decimal("3000.0")
    assert limit.order_id == 303953272739
    assert limit.placed_at == datetime.fromtimestamp(1769530795.995, tz=UTC)
    assert limit.order_type == "Limit"
    assert limit.is_trigger is False
    assert limit.trigger_price is None  # the API's "0.0" is a placeholder, not a price
    assert limit.is_position_tpsl is False
    assert limit.reduce_only is False
    assert limit.notional_usd == Decimal("13500.0")  # 3000 × 4.5
    assert limit.tpsl is None

    stop = orders[1]
    assert stop.is_buy is True  # side "B" = bid/buy
    assert stop.is_trigger is True
    assert stop.trigger_price == Decimal("63.25")
    assert stop.tpsl == "SL"
    # A trigger's notional reads against the price that arms it — its limitPx
    # (68.31) is only the slippage cap the fill is bounded by.
    assert stop.notional_usd == Decimal("75.0") * Decimal("63.25")

    tp = orders[2]
    assert tp.is_position_tpsl is True
    assert tp.tpsl == "TP"
    # sz "0.0" means "the whole position at trigger time": no order-level
    # notional exists, so None — never a floor-suppressible zero.
    assert tp.notional_usd is None


def test_an_unseen_trigger_family_labels_itself_rather_than_guessing_sl() -> None:
    # Only Stop/Take-Profit families were observed live (2026-07-24). If
    # Hyperliquid ships a new trigger family, its raw orderType is the label —
    # self-describing beats a silently wrong "SL", and failing the whole fetch
    # over a label would be worse.
    raw = dict(OPEN_ORDERS_PAYLOAD[1], orderType="Stop Limit")
    (order,) = parse_open_orders([raw])
    assert order.tpsl == "SL"  # the Stop family's limit variant
    raw = dict(OPEN_ORDERS_PAYLOAD[1], orderType="Trailing Stop Market")
    (order,) = parse_open_orders([raw])
    assert order.tpsl == "Trailing Stop Market"


def test_parse_open_orders_namespaces_builder_dex_coins_idempotently() -> None:
    # The live API already returns xyz coins namespaced (verified 2026-07-24);
    # prefixing must be idempotent, exactly as parse_positions (#21).
    raw = dict(OPEN_ORDERS_PAYLOAD[0], coin="xyz:BB")
    (already,) = parse_open_orders([raw], dex="xyz")
    assert already.coin == "xyz:BB"
    bare = dict(OPEN_ORDERS_PAYLOAD[0], coin="BB")
    (prefixed,) = parse_open_orders([bare], dex="xyz")
    assert prefixed.coin == "xyz:BB"


def test_parse_open_orders_rejects_unexpected_shape() -> None:
    with pytest.raises(GatewayError):
        parse_open_orders([{"coin": "HYPE"}])
    with pytest.raises(GatewayError):
        parse_open_orders({"orders": []})
    with pytest.raises(GatewayError):
        # An unknown side must fail loudly, never silently parse as a sell.
        parse_open_orders([dict(OPEN_ORDERS_PAYLOAD[0], side="X")])
