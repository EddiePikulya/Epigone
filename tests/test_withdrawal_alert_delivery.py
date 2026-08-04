"""Withdrawal Alert delivery: the bot drains withdrawal_alerts to Telegram
(issue #171).

Seam test per the house convention: aiogram fake transport + real Postgres. The
stream side of the queue — what counts as a withdrawal at all — is covered in
tests/test_withdrawal_alerts.py, and the send-with-retry rules belong to the
shared outbox drain (tests/test_alert_delivery.py, epigone.bot.outbox) and are
not re-proven here. What is under test is the message and the at-most-once
stamping a restart depends on.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
from aiogram import Bot

from epigone.bot.withdrawal_alerts import deliver_pending_withdrawal_alerts
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession

T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"


async def queue_withdrawal(
    pool: asyncpg.Pool,
    *,
    amount: str = "120000",
    prior_equity: str = "141000",
    equity: str = "21000",
    user_id: int = 42,
    display_name: str | None = "Ansem",
    track_name: str | None = None,
) -> None:
    """A withdrawal-alert row as the poll pass would have queued it."""
    await pool.execute(
        "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
    )
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3) ON CONFLICT (address) DO NOTHING
        """,
        WHALE,
        display_name,
        T0,
    )
    if track_name is not None:
        await pool.execute(
            "INSERT INTO tracks (user_telegram_id, trader_address, name) VALUES ($1, $2, $3)",
            user_id,
            WHALE,
            track_name,
        )
    await pool.execute(
        """
        INSERT INTO withdrawal_alerts
            (user_telegram_id, trader_address, amount_usd, prior_equity, equity_usd,
             observed_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        user_id,
        WHALE,
        Decimal(amount),
        Decimal(prior_equity),
        Decimal(equity),
        T0 - timedelta(seconds=10),
        T0,
    )


async def test_a_withdrawal_says_how_much_left_what_share_and_what_remains(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_withdrawal(pool)

    delivered = await deliver_pending_withdrawal_alerts(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()
    assert message.chat_id == 42
    assert message.text == "⚠️ Ansem (0xaf0f…e92e) pulled ~$120k out (85% of equity) — $21k left"


async def test_the_recipients_own_name_for_the_wallet_wins(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # #86, exactly as the sibling alerts resolve it.
    await queue_withdrawal(pool, track_name="My whale")

    await deliver_pending_withdrawal_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert message.text.startswith("⚠️ My whale (0xaf0f…e92e) pulled")


async def test_an_unnamed_wallet_reads_as_its_address(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_withdrawal(pool, display_name=None)

    await deliver_pending_withdrawal_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert message.text.startswith("⚠️ 0xaf0f…e92e pulled")


async def test_the_message_taps_through_to_positions_and_carries_the_bin(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_withdrawal(pool)

    await deliver_pending_withdrawal_alerts(pool, bot, clock)

    (message,) = session.sent_messages()
    assert message.reply_markup is not None
    (button,) = message.reply_markup.inline_keyboard[0]
    assert button.callback_data == f"positions:{WHALE}"
    assert message.reply_markup.inline_keyboard[-1][0].callback_data == "msgdel"


async def test_a_delivered_withdrawal_is_never_sent_again(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # At-most-once across restarts: delivered_at is stamped only once Telegram
    # has accepted, and a stamped row is invisible to every later drain.
    await queue_withdrawal(pool)
    await deliver_pending_withdrawal_alerts(pool, bot, clock)

    assert await deliver_pending_withdrawal_alerts(pool, bot, clock) == 0
    assert len(session.sent_messages()) == 1
    stamped = await pool.fetchval("SELECT delivered_at FROM withdrawal_alerts")
    assert stamped == clock.now()
