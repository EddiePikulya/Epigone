"""Monitor cycle seam test (issue #52): real Postgres + fake Telegram.

Drives `run_monitor_cycle` against synthetic DB states and asserts on the
outgoing sendMessage calls (recipient = admin, text names the failing check and
its numbers), the house convention used by test_alert_delivery.

The last section drives `monitor_loop` itself (issue #205), because the
watchdog-liveness check's CADENCE is the behaviour under test there — how soon
the operator hears about a dead dead-man's switch is not visible from inside
one cycle.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest
from aiogram import Bot

import epigone.monitor.main as monitor_main
from epigone.budget import record_rate_limit
from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import CheckThresholds
from epigone.monitor.config import MonitorConfig
from epigone.monitor.main import (
    monitor_loop,
    run_monitor_cycle,
    run_watchdog_liveness_cycle,
)
from epigone.safety import heartbeat
from epigone.safety.audit import ExecutionAudit
from epigone.safety.halt import WATCHDOG_SOURCE, request_halt
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession

ADMIN_ID = 999
T0 = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


class FakeDiskProbe:
    def __init__(self, percent: float | None) -> None:
        self._percent = percent

    def percent_used(self) -> float | None:
        return self._percent


def _config() -> MonitorConfig:
    return MonitorConfig(
        interval=timedelta(minutes=15),
        watchdog_check=timedelta(seconds=60),
        reminder=timedelta(hours=6),
        heartbeat_hour=9,  # FakeClock sits at noon, so heartbeats never interfere
        thresholds=CheckThresholds(
            ingest_stall=timedelta(minutes=30),
            coarse_stale=timedelta(minutes=120),
            alert_backlog=timedelta(minutes=5),
            rate_window=timedelta(minutes=15),
            rate_max_events=5,
            disk_percent=85,
            starvation_window=timedelta(minutes=45),
            starvation_min_due=50,
            agent_key_warn=timedelta(days=14),
            watchdog_stale=timedelta(seconds=300),
        ),
        disk_path="/",
    )


async def _add_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    fine_refreshed_at: datetime | None,
    computed_at: datetime,
    fine_attempted_at: datetime | None = None,
) -> None:
    """An eligible Trader (positive coarse month) with a given last fine refresh.
    NULL fine_refreshed_at (or one older than the active cadence) makes it due.
    `fine_attempted_at` is stamped on every attempt (success *or* failure), so a
    recent value with a stale refresh is the signature of a failing fine pass."""
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at, fine_refreshed_at, "
        "fine_attempted_at) VALUES ($1, $2, $2, $3, $4)",
        address,
        T0,
        fine_refreshed_at,
        fine_attempted_at,
    )
    await pool.execute(
        "INSERT INTO coarse_metrics (address, time_window, pnl, roi, volume, account_value, "
        "computed_at) VALUES ($1, 'month', $2, $3, $4, $5, $6)",
        address,
        Decimal(100),
        Decimal(1),
        Decimal(1000),
        Decimal(5000),
        computed_at,
    )


async def _cycle(
    pool: asyncpg.Pool,
    bot: Bot,
    monitor: Monitor,
    clock: FakeClock,
    *,
    disk: float | None = 47.0,
) -> list[str]:
    return await run_monitor_cycle(
        pool, bot, ADMIN_ID, monitor, _config(), clock, FakeDiskProbe(disk)
    )


def _monitor() -> Monitor:
    # FakeClock sits at noon; a hb hour of 23 keeps the daily digest out of these
    # alert-focused cycles (the heartbeat is covered in test_monitor_alerting).
    return Monitor(reminder=timedelta(hours=6), heartbeat_hour=23)


async def test_a_healthy_system_emits_no_alert(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # A caught-up Trader (refreshed 2 min ago → not due) with fresh coarse metrics.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )

    messages = await _cycle(pool, bot, _monitor(), clock)

    assert messages == []
    assert session.sent_messages() == []


async def test_an_idle_but_caught_up_system_does_not_false_alarm(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # No Trader is due (the one present refreshed recently), so even though the
    # last refresh could look "old" to a naive check, nothing is stuck.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    assert await _cycle(pool, bot, _monitor(), clock) == []


async def test_a_wedged_ingest_alerts_once_then_recovers(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # A due Trader whose only refresh is two days old → the fine pass is stuck.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(days=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )
    monitor = _monitor()

    first = await _cycle(pool, bot, monitor, clock)
    assert len(first) == 1
    (sent,) = session.sent_messages()
    assert sent.chat_id == ADMIN_ID
    assert "Ingest" in sent.text and "stuck" in sent.text

    # Still wedged next cycle: silent (no re-alert before the reminder interval).
    clock.advance(15 * 60)
    assert await _cycle(pool, bot, monitor, clock) == []
    assert len(session.sent_messages()) == 1

    # The fine pass makes progress: the Trader refreshes now → no longer due.
    await pool.execute(
        "UPDATE traders SET fine_refreshed_at = $2 WHERE address = $1", "0xaaa", clock.now()
    )
    recovered = await _cycle(pool, bot, monitor, clock)
    assert recovered == ["✅ Ingest recovered"]


async def test_a_constantly_failing_fine_pass_is_reported_then_recovers(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # The #61 outage shape end-to-end: a due backlog past the floor, attempts
    # still advancing (recent fine_attempted_at), but every refresh is failing
    # (last success is ancient). A low min_due keeps the fixture small.
    config = replace(
        _config(),
        thresholds=replace(_config().thresholds, starvation_min_due=2),
    )
    for i in range(3):
        await _add_trader(
            pool,
            f"0x{i:03d}",
            fine_refreshed_at=clock.now() - timedelta(days=2),
            computed_at=clock.now() - timedelta(minutes=5),
            fine_attempted_at=clock.now() - timedelta(minutes=1),
        )
    monitor = _monitor()

    first = await run_monitor_cycle(
        pool, bot, ADMIN_ID, monitor, config, clock, FakeDiskProbe(47.0)
    )
    starvation = [m for m in first if "Fine success" in m]
    assert len(starvation) == 1
    assert "failing" in starvation[0] and "3 due" in starvation[0]
    assert any(s.text == starvation[0] and s.chat_id == ADMIN_ID for s in session.sent_messages())

    # Still failing next cycle: silent (no re-alert before the reminder interval).
    clock.advance(15 * 60)
    repeat = await run_monitor_cycle(
        pool, bot, ADMIN_ID, monitor, config, clock, FakeDiskProbe(47.0)
    )
    assert not any("Fine success" in m for m in repeat)

    # The API recovers and refreshes land → the check clears with a recovery notice.
    await pool.execute("UPDATE traders SET fine_refreshed_at = $1", clock.now())
    recovered = await run_monitor_cycle(
        pool, bot, ADMIN_ID, monitor, config, clock, FakeDiskProbe(47.0)
    )
    assert "✅ Fine success recovered" in recovered


async def test_a_delivery_backlog_is_reported_with_its_count(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", 42)
    # An undelivered alert older than the backlog window.
    await pool.execute(
        """
        INSERT INTO position_alerts
            (user_telegram_id, trader_address, kind, coin, side, created_at)
        VALUES ($1, $2, 'open', 'BTC', 'long', $3)
        """,
        42,
        "0xaaa",
        clock.now() - timedelta(minutes=10),
    )

    messages = await _cycle(pool, bot, _monitor(), clock)

    assert len(messages) == 1
    (sent,) = session.sent_messages()
    assert "Alert delivery" in sent.text and "1 undelivered" in sent.text


async def test_a_rate_limit_spike_is_reported_from_the_recorded_events(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # The full read side of issue #54: escaped-429 events the passes stamped in
    # rate_limit_events, counted over the window, trip the rate check.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    for i in range(5):  # threshold is 5 within the 15-minute window
        await record_rate_limit(pool, clock.now() - timedelta(minutes=i))

    messages = await _cycle(pool, bot, _monitor(), clock)

    assert len(messages) == 1
    (sent,) = session.sent_messages()
    assert "Rate limiting" in sent.text and "throttling" in sent.text


async def test_isolated_rate_limit_events_below_the_threshold_do_not_alarm(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # A couple of absorbed-but-escaped 429s are normal pacing (user story #2):
    # under the count threshold, the cycle stays silent.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    for _ in range(2):
        await record_rate_limit(pool, clock.now())

    assert await _cycle(pool, bot, _monitor(), clock) == []


async def test_rate_events_outside_the_window_are_not_counted(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    # Events older than the window subsided long ago and must not keep alarming.
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    for _ in range(5):
        await record_rate_limit(pool, clock.now() - timedelta(minutes=30))

    assert await _cycle(pool, bot, _monitor(), clock) == []


async def test_a_full_disk_is_reported(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=1),
        computed_at=clock.now() - timedelta(minutes=1),
    )
    messages = await _cycle(pool, bot, _monitor(), clock, disk=91.0)
    assert len(messages) == 1
    assert "Disk" in messages[0] and "91%" in messages[0]


async def test_an_unreachable_database_dms_the_admin_a_critical_alert(
    database_url: str, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """DB-down is the loudest case: the monitor's own query failing must still
    reach the admin (token + admin come from env, not the DB)."""
    dead_pool = await asyncpg.create_pool(database_url)
    assert dead_pool is not None
    await dead_pool.close()  # a gather against this raises → db_down path

    messages = await run_monitor_cycle(
        dead_pool, bot, ADMIN_ID, _monitor(), _config(), clock, FakeDiskProbe(47.0)
    )

    assert len(messages) == 1
    (sent,) = session.sent_messages()
    assert sent.chat_id == ADMIN_ID
    assert "🚨" in sent.text and "unreachable" in sent.text


async def test_a_live_halt_and_a_stale_watchdog_page_the_admin(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The #135 alert path end-to-end: the watchdog trips (halt row + its own
    old heartbeat), and one cycle later the admin holds a 🚨 DM for each — the
    monitor is how a watchdog action reaches a human."""
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )
    await heartbeat.beat(pool, heartbeat.WATCHDOG_PROCESS, clock.now() - timedelta(minutes=11))
    await request_halt(
        pool, clock, ExecutionAudit(pool, clock),
        source=WATCHDOG_SOURCE, reason="executor heartbeat stale: 95s > 60s",
    )

    messages = await _cycle(pool, bot, _monitor(), clock)

    assert len(messages) == 2
    watchdog_alert = next(m for m in messages if "Watchdog" in m)
    halt_alert = next(m for m in messages if "HALTED" in m)
    assert "🚨" in watchdog_alert and "dead-man" in watchdog_alert
    assert "🚨" in halt_alert and "95s > 60s" in halt_alert and "sweep PENDING" in halt_alert


# --- the fast watchdog-liveness tick (issue #205) -----------------------------
# The full cycle is priced for its expensive checks; the watchdog's liveness is
# the one check whose subject has a deadline, because the dead-man's schedule
# it refreshes has a 300s horizon. These drive `monitor_loop` itself — the
# cadence IS the behaviour under test, so a test of one cycle cannot see it.


def _loop_config(**overrides: object) -> MonitorConfig:
    return replace(_config(), **overrides)  # type: ignore[arg-type]


async def test_a_dead_watchdog_pages_within_one_dead_man_period(
    pool: asyncpg.Pool,
    bot: Bot,
    session: RecordingSession,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 2026-08-07 incident, as a cadence. The watchdog froze at 18:20:15;
    the monitor's 18:24 pass saw ~4 minutes of staleness (under the 300s
    threshold) and the next look would have been 18:39 — by which time the
    dead-man's 300s schedule had long since fired and the operator had found
    the freeze with an ad-hoc DB query.

    The page must now land on the first liveness tick after the threshold is
    crossed, so the whole lag from freeze to human is one staleness threshold
    plus one liveness cadence — not plus one 15-minute check cycle. The send is
    timestamped here rather than inferred from where the loop stopped: WHEN the
    operator was told is the entire claim."""
    paged_at: list[datetime] = []
    real_send = monitor_main._send

    async def timestamped(bot_: Bot, admin: int, text: str) -> None:
        if "Watchdog" in text:
            paged_at.append(clock.now())
        await real_send(bot_, admin, text)

    monkeypatch.setattr(monitor_main, "_send", timestamped)
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )
    froze_at = clock.now()
    await heartbeat.beat(pool, heartbeat.WATCHDOG_PROCESS, froze_at)
    config = _loop_config()

    # Seven ticks: one full cycle at t=0 (the heartbeat is fresh then), then a
    # liveness tick every minute. The staleness threshold falls in the
    # tick-only stretch — the window the 15-minute cadence swallowed whole.
    await monitor_loop(pool, bot, ADMIN_ID, config, clock, FakeDiskProbe(47.0), max_cycles=7)

    paged = [s for s in session.sent_messages() if "Watchdog" in s.text]
    assert len(paged) == 1
    assert "🚨" in paged[0].text and "dead-man" in paged[0].text
    lag = paged_at[0] - froze_at
    assert lag <= config.thresholds.watchdog_stale + config.watchdog_check
    # Strictly better than the cadence it replaces: under the old rule the next
    # look after t=0 was a whole `interval` away.
    assert lag < config.interval


async def test_the_fast_tick_judges_only_the_watchdog(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The cadence split is the point: the expensive checks keep the 15-minute
    clock. A wedged ingest present from the start is reported by the first FULL
    cycle and then not again — the four liveness ticks that follow read one row
    and judge one check, so nothing else is re-evaluated on their timer."""
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(days=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )

    await monitor_loop(
        pool, bot, ADMIN_ID, _loop_config(), clock, FakeDiskProbe(91.0), max_cycles=5
    )

    sent = [s.text for s in session.sent_messages()]
    assert len([t for t in sent if "Ingest" in t]) == 1
    assert len([t for t in sent if "Disk" in t]) == 1
    assert not any("Watchdog" in t for t in sent)  # no execution processes: quiet


async def test_the_two_cadences_never_page_the_same_failure_twice(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """One alerting state machine serves both timers, so a watchdog the fast
    tick has already paged about is not paged again by the full cycle that
    comes after it — and the recovery, whichever timer sees it, closes the
    loop exactly once."""
    await _add_trader(
        pool, "0xaaa", fine_refreshed_at=clock.now() - timedelta(minutes=2),
        computed_at=clock.now() - timedelta(minutes=5),
    )
    await heartbeat.beat(
        pool, heartbeat.WATCHDOG_PROCESS, clock.now() - timedelta(minutes=10)
    )
    # A 3-minute full cycle: the fast tick pages first, then a full cycle runs
    # over the same still-stale row.
    config = _loop_config(interval=timedelta(minutes=3))

    await monitor_loop(pool, bot, ADMIN_ID, config, clock, FakeDiskProbe(47.0), max_cycles=5)

    assert len([s for s in session.sent_messages() if "Watchdog" in s.text]) == 1


async def test_a_watchdog_that_comes_back_is_reported_recovered_once(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The other half of the shared state: the fast tick can also CLEAR. A
    watchdog restarting mid-incident is the common case — the operator is
    watching the sweep and wants to know it is back — and hearing that a
    quarter of an hour later is the same blind spot in the other direction."""
    monitor = _monitor()
    await heartbeat.beat(
        pool, heartbeat.WATCHDOG_PROCESS, clock.now() - timedelta(minutes=10)
    )

    paged = await run_watchdog_liveness_cycle(pool, bot, ADMIN_ID, monitor, _config(), clock)
    assert len(paged) == 1 and "Watchdog" in paged[0]

    clock.advance(60)
    await heartbeat.beat(pool, heartbeat.WATCHDOG_PROCESS, clock.now())
    cleared = await run_watchdog_liveness_cycle(pool, bot, ADMIN_ID, monitor, _config(), clock)

    assert cleared == ["✅ Watchdog recovered"]
    # And it stays cleared: the recovery is a transition, not a repeat.
    clock.advance(60)
    assert await run_watchdog_liveness_cycle(pool, bot, ADMIN_ID, monitor, _config(), clock) == []


async def test_a_liveness_tick_that_cannot_read_stays_quiet(
    database_url: str, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    """The DB-down signal belongs to the full cycle, which gathers enough to
    report it with its numbers. A one-row tick that failed would page from a
    snapshot that knows one table — and, sharing the state machine, would then
    suppress the full cycle's better-informed message."""
    dead_pool = await asyncpg.create_pool(database_url)
    assert dead_pool is not None
    await dead_pool.close()

    monitor = _monitor()
    messages = await run_watchdog_liveness_cycle(
        dead_pool, bot, ADMIN_ID, monitor, _config(), clock
    )

    assert messages == []
    assert session.sent_messages() == []
    # …and the full cycle's own DB-down alert is still ahead of it, unsuppressed.
    assert "unreachable" in (
        await run_monitor_cycle(
            dead_pool, bot, ADMIN_ID, monitor, _config(), clock, FakeDiskProbe(47.0)
        )
    )[0]
