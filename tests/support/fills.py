"""Synthetic Fill factory shared by the fine-metric and fine-scan tests."""

from datetime import UTC, datetime
from decimal import Decimal

from epigone.gateway import Fill

T0 = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)


def fill(
    direction: str = "Close Long",
    pnl: str = "0",
    order_id: int = 1,
    at: datetime = T0,
    coin: str = "HYPE",
    price: str = "10",
    size: str = "1",
    start_position: str = "1",
    crossed: bool = True,
) -> Fill:
    return Fill(
        coin=coin,
        price=Decimal(price),
        size=Decimal(size),
        direction=direction,
        closed_pnl=Decimal(pnl),
        start_position=Decimal(start_position),
        crossed=crossed,
        order_id=order_id,
        time=at,
    )
