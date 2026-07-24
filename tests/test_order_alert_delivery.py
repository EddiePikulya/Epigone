"""Order Alert delivery: the bot drains order_alerts to Telegram (issue #115).

Seam test per the house convention: aiogram fake transport + real Postgres.
The stream side of the queue is covered in tests/test_order_poller.py. Each
row is one batch — every order a wallet placed in one poll cycle — delivered
as ONE message; the send-with-retry rules are the shared outbox drain's
(tests/test_alert_delivery.py, epigone.bot.outbox) and are not re-proven here.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from aiogram import Bot

from epigone.bot.order_alerts import MAX_ORDERS_SHOWN, deliver_pending_order_alerts
from epigone.gateway import OpenOrder
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession

T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


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
    reduce_only: bool = False,
) -> OpenOrder:
    return OpenOrder(
        coin=coin,
        is_buy=is_buy,
        limit_price=Decimal(limit_price),
        size=Decimal(size),
        order_id=order_id,
        placed_at=T0,
        order_type=order_type,
        is_trigger=is_trigger,
        trigger_price=Decimal(trigger_price) if trigger_price is not None else None,
        is_position_tpsl=is_position_tpsl,
        reduce_only=reduce_only,
    )


async def queue_batch(
    pool: asyncpg.Pool,
    orders: list[OpenOrder],
    *,
    user_id: int = 42,
    address: str = "0xaaa",
    display_name: str | None = "Ansem",
    track_name: str | None = None,
) -> None:
    """An order-alert row as the stream's order poll would have queued it."""
    await pool.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
    )
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3) ON CONFLICT (address) DO NOTHING
        """,
        address,
        display_name,
        T0,
    )
    if track_name is not None:
        await pool.execute(
            """
            INSERT INTO tracks (user_telegram_id, trader_address, name)
            VALUES ($1, $2, $3)
            """,
            user_id,
            address,
            track_name,
        )
    await pool.execute(
        """
        INSERT INTO order_alerts (user_telegram_id, trader_address, orders, created_at)
        VALUES ($1, $2, $3::jsonb, $4)
        """,
        user_id,
        address,
        json.dumps([o.to_wire() for o in orders]),
        T0,
    )


async def test_a_single_new_order_delivers_one_labeled_message(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_batch(pool, [order()])

    delivered = await deliver_pending_order_alerts(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()
    assert message.chat_id == 42
    assert "Ansem" in message.text and "0xaaa" in message.text
    assert "placed a new order" in message.text
    assert "LIT SELL $13,500 @ 4.5" in message.text
    remaining = await pool.fetchval("SELECT count(*) FROM order_alerts WHERE delivered_at IS NULL")
    assert remaining == 0


async def test_the_message_taps_through_to_positions_and_carries_the_bin(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_batch(pool, [order()])

    await deliver_pending_order_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert message.reply_markup is not None
    (button,) = message.reply_markup.inline_keyboard[0]
    assert button.callback_data == "positions:0xaaa"
    assert message.reply_markup.inline_keyboard[-1][0].callback_data == "msgdel"


async def test_a_batch_delivers_as_one_message_with_tpsl_and_bare_tickers(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_batch(
        pool,
        [
            order(coin="xyz:BB", is_buy=True, limit_price="15.5", size="100", order_id=1),
            order(
                coin="HYPE",
                is_buy=True,
                limit_price="68.31",
                size="75",
                order_id=2,
                order_type="Stop Market",
                is_trigger=True,
                trigger_price="63.25",
            ),
            order(
                coin="GRAM",
                order_id=3,
                size="0",
                order_type="Take Profit Market",
                is_trigger=True,
                trigger_price="1.38",
                is_position_tpsl=True,
                reduce_only=True,
            ),
        ],
    )

    delivered = await deliver_pending_order_alerts(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()  # ONE message for the whole batch
    assert "placed 3 new orders" in message.text
    assert "BB BUY $1,550 @ 15.5" in message.text  # xyz:BB renders bare (#21)
    assert "HYPE BUY SL $4,744 @ trigger 63.25" in message.text
    assert "GRAM TP @ 1.38 (whole position)" in message.text


async def test_an_oversized_batch_caps_its_lines_and_counts_the_rest(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_batch(
        pool,
        [order(order_id=i, limit_price=f"{i}.5") for i in range(1, MAX_ORDERS_SHOWN + 3)],
    )

    await deliver_pending_order_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert f"placed {MAX_ORDERS_SHOWN + 2} new orders" in message.text
    assert f"@ {MAX_ORDERS_SHOWN}.5" in message.text  # the last shown line
    assert f"@ {MAX_ORDERS_SHOWN + 1}.5" not in message.text  # capped
    assert "…and 2 more" in message.text


async def test_the_recipients_own_nickname_wins_over_the_leaderboard_label(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_batch(pool, [order()], track_name="silver guy")

    await deliver_pending_order_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert "silver guy" in message.text
    assert "Ansem" not in message.text
