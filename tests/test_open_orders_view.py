"""Issue #115: the wallet views (pasted/screener profile and the follower's
positions view) show the trader's resting orders, fetched on demand — the
plan before it executes — for tracked and untracked wallets alike.

Seam test per the house convention: dispatcher + fake transport, fake
gateway, real Postgres. The line format itself is pinned in
tests/test_order_alert_delivery.py through the shared renderer; here the
concern is where the section appears, that empty books add nothing, and the
degradation split: an orders-only failure hedges in place while a positions
failure still fails the whole view.
"""

from datetime import UTC, datetime
from decimal import Decimal

from aiogram import Bot, Dispatcher

from epigone.bot.format import MAX_ORDERS_SHOWN
from epigone.bot.handlers import DATA_DELAYED_TEXT, ORDERS_UNAVAILABLE_LINE
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


def pos(coin: str, side: Side, size_usd: str, entry_price: str = "6.7134") -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal("5"),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal("0"),
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


async def test_each_relationship_annotates_orders_against_the_live_positions(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    # The view has no snapshots for an untracked wallet, so it annotates from
    # the live positions it already fetched for the same view (#123). Each
    # order's notional is $13,500 (3000 @ 4.5); the position notional decides
    # reduce/close vs flip.
    gateway.set_positions(
        WHALE,
        [
            pos("AAA", Side.SHORT, "6900"),
            pos("BBB", Side.LONG, "20000", entry_price="41.2"),
            pos("CCC", Side.SHORT, "20000"),
            pos("DDD", Side.LONG, "20000"),
            pos("EEE", Side.SHORT, "5000"),
            pos("FFF", Side.LONG, "5000"),
        ],
    )
    gateway.set_open_orders(
        WHALE,
        [
            order(coin="AAA", order_id=1),  # SELL, SHORT → adds
            order(coin="BBB", is_buy=True, order_id=2),  # BUY, LONG → adds
            order(coin="CCC", is_buy=True, order_id=3),  # BUY $13.5k vs SHORT $20k → reduce
            order(coin="DDD", order_id=4),  # SELL $13.5k vs LONG $20k → reduce
            order(coin="EEE", is_buy=True, order_id=5),  # BUY $13.5k vs SHORT $5k → flip LONG
            order(coin="FFF", order_id=6),  # SELL $13.5k vs LONG $5k → flip SHORT
            order(coin="GGG", order_id=7),  # no position → new position
        ],
    )

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    # $6.9k keeps the k-tenth (usd_size); $20k drops a trailing .0.
    assert "AAA SELL $13,500 @ 4.5 → adds to his SHORT (now $6.9k @ 6.7134)" in profile
    assert "BBB BUY $13,500 @ 4.5 → adds to his LONG (now $20k @ 41.2)" in profile
    assert "CCC BUY $13,500 @ 4.5 → would reduce/close his SHORT" in profile
    assert "DDD SELL $13,500 @ 4.5 → would reduce/close his LONG" in profile
    assert "EEE BUY $13,500 @ 4.5 → would flip him LONG" in profile
    assert "FFF SELL $13,500 @ 4.5 → would flip him SHORT" in profile
    assert "GGG SELL $13,500 @ 4.5 → new position" in profile


async def test_a_trigger_order_composes_its_tpsl_label_with_the_relationship(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    # The #115 TP/SL label composes with the #123 relationship. A whole-position
    # TP on a LONG is a reduce-only sell with no order notional → reduce/close,
    # never a flip.
    gateway.set_positions(WHALE, [pos("HYPE", Side.LONG, "240000", entry_price="48.20")])
    gateway.set_open_orders(
        WHALE,
        [
            order(
                coin="HYPE",
                order_id=1,
                size="0",
                order_type="Take Profit Market",
                is_trigger=True,
                trigger_price="72.0",
                is_position_tpsl=True,
            ),
        ],
    )

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    assert "HYPE TP @ 72.0 (whole position) → would reduce/close his LONG" in profile


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


async def test_an_orders_only_failure_degrades_to_an_unavailable_line(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    # Orders are a garnish on the view, not its backbone: an orders-only
    # fetch failure must not blank the healthy positions beside it — but it
    # must hedge, never stay silent (silence would read as an empty book).
    gateway.set_positions(WHALE, [hype_long()])
    gateway.open_orders_errors[WHALE] = GatewayError("info API down")

    await feed_text(dp, bot, WHALE, user_id=111)

    profile = session.sent_messages()[-1].text or ""
    assert "HYPE" in profile  # positions rendered
    assert ORDERS_UNAVAILABLE_LINE in profile


async def test_a_positions_failure_still_fails_the_whole_view(
    dp: Dispatcher, bot: Bot, session: RecordingSession, gateway: FakeHyperliquidGateway
) -> None:
    # The all-venues-must-succeed rule is untouched for positions (#21/#31):
    # a partial positions read renders lies, so the view still answers with
    # the delayed-data message.
    gateway.positions_errors[WHALE] = GatewayError("info API down")

    await feed_text(dp, bot, WHALE, user_id=111)

    assert (session.sent_messages()[-1].text or "") == DATA_DELAYED_TEXT
