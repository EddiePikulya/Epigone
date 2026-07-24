"""Issue #115: the wallet views (pasted/screener profile and the follower's
positions view) show the trader's resting orders, fetched on demand — the
plan before it executes — for tracked and untracked wallets alike.

Seam test per the house convention: dispatcher + fake transport, fake
gateway, real Postgres. The line format itself is pinned in
tests/test_order_alert_delivery.py through the shared renderer; here the
concern is where the section appears, that empty books add nothing, and that
a failing orders fetch degrades exactly like a failing positions fetch.
"""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import DATA_DELAYED_TEXT
from epigone.bot.order_alerts import MAX_ORDERS_SHOWN
from epigone.gateway import GatewayError, OpenOrder, Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.telegram import RecordingSession, feed_callback, feed_text, follow_wallet

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
PLACED_AT = datetime(2026, 7, 20, 9, 0, tzinfo=UTC)


def order(
    coin: str = "LIT",
    *,
    is_buy: bool = False,
    limit_price: str = "4.5",
    size: str = "3000",
    order_id: int = 1001,
    order_type: str = "Limit",
    is_trigger: bool = False,
    trigger_price: str | None = None,
    is_position_tpsl: bool = False,
) -> OpenOrder:
    return OpenOrder(
        coin=coin,
        is_buy=is_buy,
        limit_price=Decimal(limit_price),
        size=Decimal(size),
        order_id=order_id,
        placed_at=PLACED_AT,
        order_type=order_type,
        is_trigger=is_trigger,
        trigger_price=Decimal(trigger_price) if trigger_price is not None else None,
        is_position_tpsl=is_position_tpsl,
        reduce_only=is_position_tpsl,
    )


def hype_long() -> Position:
    return Position(
        coin="HYPE",
        side=Side.LONG,
        size_usd=Decimal("240000"),
        leverage=Decimal("5"),
        entry_price=Decimal("48.20"),
        unrealized_pnl=Decimal("18200"),
    )


async def test_an_untracked_profile_lists_resting_orders_with_tpsl_labeled(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    gateway.set_open_orders(
        WHALE,
        [
            order(),
            order(
                coin="HYPE",
                is_buy=True,
                order_id=1002,
                size="75",
                limit_price="68.31",
                order_type="Stop Market",
                is_trigger=True,
                trigger_price="63.25",
            ),
        ],
    )
    gateway.set_open_orders(WHALE, [order(coin="xyz:BB", is_buy=True, order_id=2001,
                                          size="100", limit_price="15.5")], dex="xyz")

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    assert "Resting orders:" in profile
    assert "LIT SELL $13,500 @ 4.5" in profile
    assert "HYPE BUY SL $4,744 @ trigger 63.25" in profile  # trigger labeled
    assert "BB BUY $1,550 @ 15.5" in profile  # builder-DEX order, bare ticker


async def test_a_wallet_with_no_resting_orders_shows_nothing_extra(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    gateway.set_positions(WHALE, [hype_long()])

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    assert "HYPE" in profile  # the profile itself rendered
    assert "Resting orders" not in profile


async def test_the_followers_positions_view_lists_resting_orders_too(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)
    gateway.set_open_orders(WHALE, [order()])

    await feed_callback(dp, bot, f"positions:{WHALE}", user_id=111)

    view = session.sent_messages()[-1].text or ""
    assert "Resting orders:" in view
    assert "LIT SELL $13,500 @ 4.5" in view


async def test_a_huge_ladder_is_capped_with_a_count(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    # Observed live: makers resting 500+ orders. The section must stay a
    # glanceable summary, never a Telegram-length-limit gamble.
    gateway.set_open_orders(
        WHALE,
        [order(order_id=i, limit_price=str(i)) for i in range(1, MAX_ORDERS_SHOWN + 4)],
    )

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    assert "…and 3 more" in profile


async def test_a_failing_orders_fetch_degrades_like_a_failing_positions_fetch(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway,
    pool: asyncpg.Pool,
) -> None:
    gateway.open_orders_errors[WHALE] = GatewayError("info API down")

    await feed_text(dp, bot, WHALE, user_id=111)

    # Same message the positions fetch failure answers with: the data is
    # delayed, nothing crashed, nothing half-rendered.
    assert (session.sent_messages()[-1].text or "") == DATA_DELAYED_TEXT
