"""Position Alert delivery: the bot drains position_alerts to Telegram (issue #4).

Seam test per the house convention: aiogram fake transport + real Postgres.
The stream side of the queue is covered in tests/test_position_poller.py; the
last test here walks one event through both halves.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramNetworkError
from aiogram.methods import EditMessageText, SendMessage, TelegramMethod
from aiogram.methods.base import TelegramType

from epigone.bot.alerts import MAX_DELIVERY_ATTEMPTS, deliver_pending
from epigone.gateway import Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import run_poll_pass
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, make_bot

T0 = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def queue_alert(
    pool: asyncpg.Pool,
    *,
    user_id: int = 42,
    address: str = "0xaaa",
    display_name: str | None = "Ansem",
    kind: str = "open",
    coin: str = "BTC",
    side: str | None = "long",
    size_usd: str | None = "10000",
    prev_size_usd: str | None = None,
    leverage: str | None = "5",
    entry_price: str | None = "100",
    prev_side: str | None = None,
    realized_pnl: str | None = None,
    pct_return: str | None = None,
    opened_at: datetime | None = None,
    created_at: datetime = T0,
    attempts: int = 0,
) -> None:
    """An alert row as the stream poller would have queued it."""
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
        created_at,
    )
    await pool.execute(
        """
        INSERT INTO position_alerts
            (user_telegram_id, trader_address, kind, coin, side, size_usd, prev_size_usd,
             leverage, entry_price, prev_side, realized_pnl, pct_return, opened_at,
             created_at, attempts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        """,
        user_id,
        address,
        kind,
        coin,
        side,
        Decimal(size_usd) if size_usd is not None else None,
        Decimal(prev_size_usd) if prev_size_usd is not None else None,
        Decimal(leverage) if leverage is not None else None,
        Decimal(entry_price) if entry_price is not None else None,
        prev_side,
        Decimal(realized_pnl) if realized_pnl is not None else None,
        Decimal(pct_return) if pct_return is not None else None,
        opened_at,
        created_at,
        attempts,
    )


async def test_an_open_alert_is_delivered_with_label_and_position_fields(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(
        pool, kind="open", side="short", size_usd="20000", leverage="10", entry_price="97.5"
    )

    delivered = await deliver_pending(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()
    assert message.chat_id == 42
    assert "Ansem (0xaaa…0xaaa)" not in message.text  # sanity: label renders, below
    assert "Ansem" in message.text and "0xaaa" in message.text
    assert "opened BTC SHORT" in message.text
    assert "$20,000" in message.text
    assert "10x" in message.text
    assert "entry 97.5" in message.text
    remaining = await pool.fetchval(
        "SELECT count(*) FROM position_alerts WHERE delivered_at IS NULL"
    )
    assert remaining == 0


async def test_a_close_alert_reports_pnl_return_and_holding_time(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(
        pool,
        kind="close",
        side=None,
        size_usd=None,
        leverage=None,
        entry_price=None,
        prev_side="long",
        realized_pnl="500",
        pct_return="0.25",
        opened_at=T0 - timedelta(hours=1, minutes=30),
    )

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    assert "closed BTC LONG" in message.text
    assert "+$500" in message.text
    assert "+25%" in message.text
    assert "held 1h 30m" in message.text


async def test_a_flip_alert_shows_both_legs(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(
        pool,
        kind="flip",
        side="short",
        size_usd="15000",
        leverage="3",
        entry_price="110",
        prev_side="long",
        realized_pnl="-120",
        pct_return="-0.06",
        opened_at=T0 - timedelta(hours=2),
    )

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    assert "flipped BTC LONG → SHORT" in message.text
    assert "-$120" in message.text and "-6%" in message.text  # the closed leg
    assert "$15,000" in message.text and "3x" in message.text  # the new leg
    assert "entry 110" in message.text


async def queue_scale(
    pool: asyncpg.Pool,
    *,
    kind: str = "scale_in",
    address: str = "0xaaa",
    coin: str = "BTC",
    side: str = "long",
    size_usd: str = "25000",
    prev_size_usd: str = "10000",
    leverage: str = "5",
) -> None:
    """A scale alert row (add/trim), defaulting to the same BTC-long the open
    helpers use so it resolves against a queued open anchor (issue #91)."""
    await queue_alert(
        pool,
        kind=kind,
        address=address,
        coin=coin,
        side=side,
        size_usd=size_usd,
        prev_size_usd=prev_size_usd,
        leverage=leverage,
    )


async def test_a_scale_in_edits_the_open_alert_appending_an_up_arrow(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The #91 core: an add sends no new message — it edits the open alert,
    appending ⬆️, and the open's Telegram message id was persisted at delivery."""
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    assert await deliver_pending(pool, bot, clock) == 1
    anchor_id = await pool.fetchval(
        "SELECT telegram_message_id FROM position_alerts WHERE kind = 'open'"
    )
    assert anchor_id == 1  # learned from the send result and persisted

    await queue_scale(pool, kind="scale_in")
    assert await deliver_pending(pool, bot, clock) == 1

    assert len(session.sent_messages()) == 1  # no second message
    (edit,) = session.edited_messages()
    assert edit.message_id == 1
    assert edit.chat_id == 42
    assert "opened BTC LONG" in edit.text  # the open's own text is preserved
    assert edit.text.endswith("⬆️")


async def test_a_scale_out_appends_a_down_arrow(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot, clock)

    await queue_scale(pool, kind="scale_out", size_usd="4000")
    await deliver_pending(pool, bot, clock)

    (edit,) = session.edited_messages()
    assert edit.text.endswith("⬇️")


async def test_scales_accumulate_arrows_on_the_open_in_event_order(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The message accumulates the position's life story: each scale appends its
    arrow to the changes line, in the order the events happened."""
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot, clock)

    for kind in ("scale_in", "scale_out", "scale_in"):
        await queue_scale(pool, kind=kind)
        await deliver_pending(pool, bot, clock)

    trails = [edit.text.splitlines()[-1] for edit in session.edited_messages()]
    assert trails == ["⬆️", "⬆️⬇️", "⬆️⬇️⬆️"]


async def test_a_scale_edit_preserves_the_tap_through_and_delete_keyboard(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """Editing text must not strip the buttons: the tap-through (#47) and the
    🗑 delete row (#73) ride through the edit intact."""
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot, clock)

    await queue_scale(pool, address=address)
    await deliver_pending(pool, bot, clock)

    (edit,) = session.edited_messages()
    assert edit.reply_markup is not None
    (button,) = edit.reply_markup.inline_keyboard[0]
    assert button.callback_data == f"positions:{address}"
    assert edit.reply_markup.inline_keyboard[-1][0].callback_data == "msgdel"


async def test_a_scale_with_no_open_alert_is_silently_dropped(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """Followed after the open, so no open alert ever existed: the scale does
    nothing — no message, no edit — and is stamped delivered, never retried."""
    await queue_scale(pool, kind="scale_in")

    assert await deliver_pending(pool, bot, clock) == 1
    assert session.sent_messages() == []
    assert session.edited_messages() == []
    assert await pool.fetchval("SELECT delivered_at FROM position_alerts") is not None


async def test_a_scale_on_a_deleted_open_message_is_silently_dropped(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The open message is gone (tapped 🗑, or hand-deleted): Telegram rejects
    the edit with a bad-request, which is the expected silent outcome — the
    scale is stamped delivered without burning attempts, not left to retry."""
    session = EditRejectingSession()
    bot = make_bot(session)
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    assert await deliver_pending(pool, bot, clock) == 1

    await queue_scale(pool, kind="scale_in")
    assert await deliver_pending(pool, bot, clock) == 1

    scale = await pool.fetchrow("SELECT * FROM position_alerts WHERE kind = 'scale_in'")
    assert scale is not None
    assert scale["delivered_at"] is not None
    assert scale["attempts"] == 0  # a bad-request edit is not a poison send
    await bot.session.close()


async def test_a_scale_after_a_close_with_no_new_open_is_silently_dropped(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The instance closed and re-opened, but the re-open left no alert (floor
    suppressed, or joined after it): the scale must not bind its arrow to the
    prior, now-closed instance's message — it is silently dropped instead."""
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot, clock)  # message 1: the closed instance
    await queue_alert(
        pool,
        kind="close",
        coin="BTC",
        side=None,
        size_usd=None,
        leverage=None,
        entry_price=None,
        prev_side="long",
    )
    await deliver_pending(pool, bot, clock)  # message 2: the close

    await queue_scale(pool, kind="scale_in")
    assert await deliver_pending(pool, bot, clock) == 1

    assert session.edited_messages() == []  # no arrow on the closed instance
    assert len(session.sent_messages()) == 2  # only the open and the close


async def test_scale_arrows_land_after_a_bot_restart(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Message ids survive restarts: a fresh bot process, sharing only Postgres,
    still edits the right message because the id was persisted at delivery."""
    first = RecordingSession()
    bot1 = make_bot(first)
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot1, clock)
    await bot1.session.close()

    second = RecordingSession()  # its own message-id counter starts fresh at 0
    bot2 = make_bot(second)
    await queue_scale(pool, kind="scale_in")
    assert await deliver_pending(pool, bot2, clock) == 1

    (edit,) = second.edited_messages()
    assert edit.message_id == 1  # the id the first process captured, read from Postgres
    assert edit.text.endswith("⬆️")
    await bot2.session.close()


async def test_a_scale_queued_before_its_open_is_delivered_still_lands(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """Pending-delivery race: both undelivered in one drain. Oldest-first sends
    the open (capturing its id) before the scale, so the arrow still lands."""
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await queue_scale(pool, kind="scale_in")

    assert await deliver_pending(pool, bot, clock) == 2
    (sent,) = session.sent_messages()
    (edit,) = session.edited_messages()
    assert edit.message_id == 1
    assert edit.text.endswith("⬆️")


async def test_an_arrow_survives_an_open_that_fails_its_first_send(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The append-before-send path: the open's first send is rejected, so the
    scale runs against a not-yet-delivered anchor and records its arrow on the
    row. When the open finally sends, its text carries that arrow from the start."""
    flaky = FlakySession(failures=1)  # the open (first send) is rejected once
    bot = make_bot(flaky)
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await queue_scale(pool, kind="scale_in")

    # Pass 1: open send rejected (attempts++); scale appends its arrow to the row.
    assert await deliver_pending(pool, bot, clock) == 1
    assert flaky.edited_messages() == []  # anchor not on Telegram yet, nothing to edit

    # Pass 2: the open retries and now sends with the arrow already in its text.
    assert await deliver_pending(pool, bot, clock) == 1
    (sent,) = flaky.sent_messages()
    assert sent.text.endswith("⬆️")
    await bot.session.close()


async def test_a_flip_becomes_the_anchor_for_later_scales(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """A flip closes one instance and opens a new one, so it is the anchor for
    subsequent arrows — they land on the flip message, not the original open."""
    await queue_alert(pool, kind="open", coin="BTC", side="long")
    await deliver_pending(pool, bot, clock)  # message id 1

    await queue_alert(
        pool,
        kind="flip",
        coin="BTC",
        side="short",
        prev_side="long",
        size_usd="15000",
        leverage="3",
        entry_price="110",
        realized_pnl="-120",
        pct_return="-0.06",
        opened_at=T0 - timedelta(hours=2),
    )
    await deliver_pending(pool, bot, clock)  # message id 2 — the new anchor

    await queue_scale(pool, kind="scale_in", side="short", leverage="3")
    await deliver_pending(pool, bot, clock)

    (edit,) = session.edited_messages()
    assert edit.message_id == 2  # the flip, not the original open
    assert "flipped BTC LONG → SHORT" in edit.text
    assert edit.text.endswith("⬆️")


async def test_an_xyz_market_alert_names_the_dex_qualified_coin(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """An xyz builder-DEX position (issue #21) is legible at a glance: the
    delivered text carries the namespaced `xyz:META`, not a bare `META`."""
    await queue_alert(pool, kind="open", coin="xyz:META", side="short")

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    assert "opened xyz:META SHORT" in message.text


async def test_an_unlabeled_trader_is_identified_by_short_address(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address, display_name=None)

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    assert "0x1116…3d0d" in message.text
    assert "None" not in message.text


async def test_an_alert_is_tap_through_to_the_traders_live_positions(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """UX: the alert carries a button that opens the trader's current positions —
    the same positions:<address> callback /tracked uses, so tapping the wallet in
    an alert shows what they're holding right now."""
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address)

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    assert message.reply_markup is not None
    # The tap-through row comes first; the 🗑 delete row (#73) is appended below it.
    (button,) = message.reply_markup.inline_keyboard[0]
    assert button.callback_data == f"positions:{address}"
    # The button labels the wallet by name (the leaderboard display name here);
    # the address lives in the alert text and the detailed view, not the button.
    assert "Ansem" in button.text
    assert "0x1116" not in button.text


async def test_the_tap_through_button_prefers_the_recipients_own_nickname(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The recipient's per-Track nickname (#86) beats the leaderboard display
    name on the button — same precedence as the alert text."""
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address, display_name="Ansem")
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address, name) VALUES (42, $1, $2)",
        address,
        "silver guy",
    )

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    (button,) = message.reply_markup.inline_keyboard[0]
    assert "silver guy" in button.text
    assert "Ansem" not in button.text


async def test_the_tap_through_button_falls_back_to_the_short_address(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    address = "0x1116b5fcc070945062e8879841c29807db373d0d"
    await queue_alert(pool, address=address, display_name=None)

    await deliver_pending(pool, bot, clock)

    (message,) = session.sent_messages()
    (button,) = message.reply_markup.inline_keyboard[0]
    assert "0x1116…3d0d" in button.text


async def test_delivered_alerts_are_never_resent(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The restart story: delivery marks rows in Postgres, so a bot restart
    (or the next loop iteration) resends nothing."""
    await queue_alert(pool)

    assert await deliver_pending(pool, bot, clock) == 1
    assert await deliver_pending(pool, bot, clock) == 0
    assert len(session.sent_messages()) == 1


async def test_alerts_deliver_oldest_first(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await queue_alert(pool, coin="BTC")
    await queue_alert(pool, coin="SOL")

    await deliver_pending(pool, bot, clock)

    texts = [m.text for m in session.sent_messages()]
    assert "BTC" in texts[0] and "SOL" in texts[1]


class EditRejectingSession(RecordingSession):
    """Rejects every edit the way Telegram does when the target message is gone
    (deleted, or too old) — a TelegramBadRequest — while sends succeed."""

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[TelegramType],
        timeout: int | None = None,
    ) -> TelegramType:
        if isinstance(method, EditMessageText):
            self.requests.append(method)
            raise TelegramBadRequest(method=method, message="message to edit not found")
        return cast(TelegramType, await super().make_request(bot, method, timeout))


class FlakySession(RecordingSession):
    """Fails the first N sends the way Telegram would, then recovers."""

    def __init__(
        self, failures: int, exception: type[TelegramAPIError] = TelegramBadRequest
    ) -> None:
        super().__init__()
        self.failures = failures
        self.exception = exception

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[TelegramType],
        timeout: int | None = None,
    ) -> TelegramType:
        if isinstance(method, SendMessage) and self.failures > 0:
            self.failures -= 1
            raise self.exception(method=method, message="synthetic failure")
        return cast(TelegramType, await super().make_request(bot, method, timeout))


async def test_a_failed_send_is_retried_on_the_next_run(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    flaky = FlakySession(failures=1)
    bot = make_bot(flaky)
    await queue_alert(pool)

    assert await deliver_pending(pool, bot, clock) == 0
    assert await pool.fetchval("SELECT attempts FROM position_alerts") == 1
    assert await pool.fetchval("SELECT delivered_at FROM position_alerts") is None

    assert await deliver_pending(pool, bot, clock) == 1
    assert len(flaky.sent_messages()) == 1
    await bot.session.close()


async def test_a_telegram_outage_sheds_no_alerts(pool: asyncpg.Pool, clock: FakeClock) -> None:
    """Transient failures (network, 5xx, flood control) pause the run without
    burning attempts — an outage longer than MAX_DELIVERY_ATTEMPTS ticks must
    not abandon alerts the way a dead chat does."""
    flaky = FlakySession(failures=MAX_DELIVERY_ATTEMPTS + 3, exception=TelegramNetworkError)
    bot = make_bot(flaky)
    await queue_alert(pool, user_id=42)
    await queue_alert(pool, user_id=43)

    for _ in range(MAX_DELIVERY_ATTEMPTS + 1):
        assert await deliver_pending(pool, bot, clock) == 0
    assert await pool.fetchval("SELECT max(attempts) FROM position_alerts") == 0

    flaky.failures = 0  # Telegram recovers
    assert await deliver_pending(pool, bot, clock) == 2
    await bot.session.close()


async def test_a_poison_alert_is_dropped_after_max_attempts(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """A User who blocked the bot must not wedge the queue forever."""
    await queue_alert(pool, attempts=MAX_DELIVERY_ATTEMPTS)

    assert await deliver_pending(pool, bot, clock) == 0
    assert session.sent_messages() == []


async def test_one_users_failure_does_not_block_other_users(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    flaky = FlakySession(failures=1)  # first row's send fails, second succeeds
    bot = make_bot(flaky)
    await queue_alert(pool, user_id=42)
    await queue_alert(pool, user_id=43)

    assert await deliver_pending(pool, bot, clock) == 1
    (message,) = flaky.sent_messages()
    assert message.chat_id == 43
    await bot.session.close()


async def test_a_position_change_travels_from_poll_to_telegram(
    pool: asyncpg.Pool,
    bot: Bot,
    session: RecordingSession,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
) -> None:
    """End to end across the Postgres seam: poll pass queues, delivery sends."""
    from epigone.budget import WeightBudget
    from tests.test_position_poller import position, track

    await track(pool, clock, "0xaaa", 42)
    budget = WeightBudget(1_000_000, clock)
    await run_poll_pass(pool, gateway, budget, clock)  # silent baseline

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="ETH", side=Side.LONG)])
    await run_poll_pass(pool, gateway, budget, clock)
    delivered = await deliver_pending(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()
    assert message.chat_id == 42
    assert "opened ETH LONG" in message.text
