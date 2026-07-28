"""The operator kill switch: /kill and /resume (issue #135).

/kill is immediate (an emergency stop takes no dialog); /resume is the
two-step one — resuming is consent to trade again. Both are owner-only,
and the resume-confirm callback re-checks the owner because callback
payloads are client-forgeable."""

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.access import ADMIN_ONLY_TEXT
from epigone.bot.handlers import build_router
from epigone.bot.operator import (
    KILL_CONTENTION_TEXT,
    KILL_FAILED_TEXT,
    NOT_HALTED_TEXT,
    RESUME_CANCEL_CALLBACK,
    RESUME_CANCELLED_TEXT,
    RESUME_CONFIRM_PREFIX,
    RESUME_GONE_TEXT,
    RESUME_PROMPT_TEXT,
    RESUME_SWEEP_PENDING_LINE,
    RESUMED_TEXT,
    WATCHDOG_SILENT_WARNING,
)
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.safety import heartbeat
from epigone.safety.audit import EVENT, ExecutionAudit
from epigone.safety.halt import (
    HOLD_POLICY,
    KILL_SOURCE,
    active_halt,
    is_halted,
    mark_swept,
)
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, feed_text

ADMIN = 370818090
GUEST = 111


@pytest.fixture
def admin_dp(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> Dispatcher:
    """A dispatcher with a configured owner (the conftest `dp` deliberately has
    none), so the owner-only path is drivable; no gate — gating has its own
    suite (test_invite_only)."""
    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher["gateway"] = gateway
    dispatcher["clock"] = clock
    dispatcher["admin_telegram_id"] = ADMIN
    dispatcher["drafts"] = {}
    dispatcher["min_size_pending"] = {}
    dispatcher["rename_pending"] = {}
    dispatcher.include_router(build_router())
    return dispatcher


async def test_kill_warns_when_no_watchdog_is_beating(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # No watchdog heartbeat exists at all: /kill must not promise a sweep
    # nobody will run.
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    assert WATCHDOG_SILENT_WARNING in (session.sent_messages()[-1].text or "")


async def test_kill_halts_immediately_and_audits(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await heartbeat.beat(pool, heartbeat.WATCHDOG_PROCESS, clock.now())
    await feed_text(admin_dp, bot, "/kill fat-finger ladder", user_id=ADMIN)
    assert await is_halted(pool)
    halt = await active_halt(pool)
    assert halt is not None
    assert halt.source == KILL_SOURCE
    assert halt.requested_by == ADMIN
    assert "fat-finger ladder" in halt.reason
    reply = session.sent_messages()[-1].text or ""
    assert "halted" in reply.lower()
    assert "positions" in reply.lower()  # the unwind policy is stated, not implied
    assert WATCHDOG_SILENT_WARNING not in reply  # the watchdog IS beating here
    events = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1", EVENT
    )
    assert [r["action"] for r in events] == ["halt"]


async def test_kill_is_owner_only(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=GUEST)
    assert not await is_halted(pool)
    assert session.sent_messages()[-1].text == ADMIN_ONLY_TEXT


async def test_kill_while_halted_joins_not_stacks(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/kill again", user_id=ADMIN)
    assert await pool.fetchval("SELECT count(*) FROM execution_halts") == 1
    assert "Already halted" in (session.sent_messages()[-1].text or "")


async def _active_halt_id(pool: asyncpg.Pool) -> int:
    halt = await active_halt(pool)
    assert halt is not None
    return halt.id


async def test_resume_confirm_flow(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    prompt = session.sent_messages()[-1]
    # The /kill halt is not swept yet, and the prompt must say so.
    assert prompt.text == f"{RESUME_PROMPT_TEXT}\n\n{RESUME_SWEEP_PENDING_LINE}"
    assert prompt.reply_markup is not None  # the confirm/cancel keyboard

    halt_id = await _active_halt_id(pool)
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}{halt_id}", user_id=ADMIN)
    assert not await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUMED_TEXT
    events = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    assert [r["action"] for r in events] == ["halt", "resume"]


async def test_swept_halt_prompts_without_the_pending_warning(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    halt = await active_halt(pool)
    assert halt is not None
    await mark_swept(
        pool, clock, ExecutionAudit(pool, clock),
        halt=halt, positions=[], unwind_policy=HOLD_POLICY,
    )
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    assert session.sent_messages()[-1].text == RESUME_PROMPT_TEXT


async def test_resume_cancel_keeps_the_halt(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    await feed_callback(admin_dp, bot, RESUME_CANCEL_CALLBACK, user_id=ADMIN)
    assert await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUME_CANCELLED_TEXT


async def test_resume_confirm_is_owner_only_even_as_callback(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    halt_id = await _active_halt_id(pool)
    # A forged/stray tap from a non-owner must not lift the halt.
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}{halt_id}", user_id=GUEST)
    assert await is_halted(pool)
    assert (session.callback_answers()[-1].text or "") == ADMIN_ONLY_TEXT


async def test_stale_confirm_never_lifts_a_later_halt(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """The id binding (PR #143 review): a confirm offered for halt A must be
    inert once A is closed and B stands — an old prompt in scrollback is not
    consent to resume the NEXT incident."""
    await feed_text(admin_dp, bot, "/kill first", user_id=ADMIN)
    old_id = await _active_halt_id(pool)
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}{old_id}", user_id=ADMIN)
    assert not await is_halted(pool)

    await feed_text(admin_dp, bot, "/kill second", user_id=ADMIN)
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}{old_id}", user_id=ADMIN)
    assert await is_halted(pool)  # the stale tap changed nothing
    assert session.edited_messages()[-1].text == RESUME_GONE_TEXT
    # Garbage payloads (client-forgeable) are equally inert — including a
    # digit string past BIGINT's range, which must answer stale, not crash.
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}nonsense", user_id=ADMIN)
    assert await is_halted(pool)
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}{'9' * 25}", user_id=ADMIN)
    assert await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUME_GONE_TEXT
    # And Unicode digits int() refuses ("²" is isdigit but not isdecimal —
    # round 2 item 6): stale, never a crash.
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}²", user_id=ADMIN)
    assert await is_halted(pool)
    assert session.edited_messages()[-1].text == RESUME_GONE_TEXT


async def test_kill_replies_when_the_halt_cannot_be_recorded(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 2 item 5: the REALISTIC outage (a plain database error, not the
    contrived livelock) must produce an explicit NOT-halted reply, never a
    silent crash that reads as 'maybe halted'."""
    from epigone.bot import operator as operator_module

    async def db_down(*args: object, **kwargs: object) -> object:
        raise ConnectionError("connection refused")

    monkeypatch.setattr(operator_module, "request_halt", db_down)
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    assert session.sent_messages()[-1].text == KILL_FAILED_TEXT
    assert not await is_halted(pool)


async def test_kill_still_confirms_when_the_followup_read_fails(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round 2 item 5, the other half: once the halt row COMMITTED, the
    reply must confirm it even if the watchdog-heartbeat read then fails —
    the operator must never read 'kill failed' for a kill that succeeded.
    The unreadable heartbeat degrades to the conservative silent warning."""
    async def read_down(*args: object, **kwargs: object) -> object:
        raise ConnectionError("read refused")

    monkeypatch.setattr(heartbeat, "last_beat", read_down)
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    assert await is_halted(pool)  # the halt stands…
    reply = session.sent_messages()[-1].text or ""
    assert "halted" in reply.lower()  # …and the reply says so…
    assert WATCHDOG_SILENT_WARNING in reply  # …warning, not promising


async def test_kill_answers_even_under_halt_contention(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The livelock backstop: even if request_halt exhausts its retries,
    /kill must ANSWER — a kill switch that crashes silently is the one
    unacceptable outcome."""
    from epigone.bot import operator as operator_module
    from epigone.safety.halt import HaltContentionError

    async def contended(*args: object, **kwargs: object) -> object:
        raise HaltContentionError("contended")

    monkeypatch.setattr(operator_module, "request_halt", contended)
    await feed_text(admin_dp, bot, "/kill", user_id=ADMIN)
    assert session.sent_messages()[-1].text == KILL_CONTENTION_TEXT


async def test_resume_without_a_halt(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/resume", user_id=ADMIN)
    assert session.sent_messages()[-1].text == NOT_HALTED_TEXT
    # A stale confirm tap (halt already gone) is a no-op with a clear edit.
    await feed_callback(admin_dp, bot, f"{RESUME_CONFIRM_PREFIX}999", user_id=ADMIN)
    assert session.edited_messages()[-1].text == RESUME_GONE_TEXT
