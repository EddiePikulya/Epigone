"""Shared order fixtures for the safety-layer tests.

The sweep only ever cares about a resting order's coin and oid — everything
else is filler the OpenOrder dataclass requires. Both watchdog suites build
the same shape, so it lives here rather than in each of them.
"""

from datetime import UTC, datetime
from decimal import Decimal

from epigone.gateway import OpenOrder


def open_order(coin: str, oid: int) -> OpenOrder:
    return OpenOrder(
        coin=coin,
        is_buy=True,
        limit_price=Decimal("100"),
        size=Decimal("1"),
        order_id=oid,
        placed_at=datetime(2026, 7, 10, 11, 0, tzinfo=UTC),
        order_type="Limit",
        is_trigger=False,
        trigger_price=None,
        is_position_tpsl=False,
        reduce_only=False,
    )
