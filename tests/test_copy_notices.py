"""The executor's own Telegram channel (issue #136, ADR-0007 decision 11).

The operator runs this product from a chat window; the audit trail is a
record, not a notification. These pin the queue's separation from Position
Alerts — the property ADR-0006 asks to hold in BOTH directions — and the
delivery it shares with every other outbox.
"""

import asyncpg
from aiogram import Bot

from epigone.bot.copy_notices import deliver_pending_copy_notices
from epigone.bot.outbox import MAX_DELIVERY_ATTEMPTS
from epigone.execute.notices import ACTION, PAGER, SKIP, notify, pending_notices
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession

OPERATOR = 4242


async def test_notices_deliver_oldest_first_and_are_stamped_once(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    for kind, body in ((ACTION, "📈 Copied open"), (SKIP, "⏭ Skipped close")):
        await notify(pool, operator_id=OPERATOR, kind=kind, body=body, now=clock.now())

    assert await deliver_pending_copy_notices(pool, bot, clock) == 2
    assert [m.text for m in session.sent_messages()] == ["📈 Copied open", "⏭ Skipped close"]
    # A bot restart resends nothing: delivery is stamped, not re-derived.
    assert await deliver_pending_copy_notices(pool, bot, clock) == 0
    assert await pending_notices(pool) == []


async def test_a_dead_chat_burns_its_own_attempts_without_wedging_the_queue(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The shared outbox rule, inherited rather than re-implemented: a
    per-chat reject burns that row's attempts and the queue keeps moving."""
    from aiogram.exceptions import TelegramForbiddenError

    await notify(pool, operator_id=OPERATOR, kind=PAGER, body="🚨 CLOSE UNFILLED", now=clock.now())

    async def refuse(*args: object, **kwargs: object) -> None:
        raise TelegramForbiddenError(method=None, message="bot was blocked")  # type: ignore[arg-type]

    original = bot.send_message
    bot.send_message = refuse  # type: ignore[method-assign]
    try:
        for _ in range(MAX_DELIVERY_ATTEMPTS):
            assert await deliver_pending_copy_notices(pool, bot, clock) == 0
    finally:
        bot.send_message = original  # type: ignore[method-assign]

    # Past the attempt cap the row stops being offered — one dead chat cannot
    # hold the whole queue.
    assert await deliver_pending_copy_notices(pool, bot, clock) == 0
    assert [n.body for n in await pending_notices(pool)] == ["🚨 CLOSE UNFILLED"]


async def test_copy_notices_are_a_separate_queue_from_position_alerts(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """ADR-0006's separation, in both directions: execution never reads
    `position_alerts` (so a mute or a min-size floor cannot change what gets
    traded), and copy status is never written onto them (so a copy report
    cannot be suppressed by an alert preference)."""
    await notify(pool, operator_id=OPERATOR, kind=ACTION, body="📈 Copied open", now=clock.now())

    assert await pool.fetchval("SELECT count(*) FROM position_alerts") == 0
    assert await pool.fetchval("SELECT count(*) FROM copy_notices") == 1
