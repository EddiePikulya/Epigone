"""Monitor cycle seam test (issue #52): real Postgres + fake Telegram.

Drives `run_monitor_cycle` against synthetic DB states and asserts on the
outgoing sendMessage calls (recipient = admin, text names the failing check and
its numbers), the house convention used by test_alert_delivery.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
from aiogram import Bot

from epigone.budget import record_rate_limit
from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import CheckThresholds
from epigone.monitor.config import MonitorConfig
from epigone.monitor.main import run_monitor_cycle
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
        reminder=timedelta(hours=6),
        heartbeat_hour=9,  # FakeClock sits at noon, so heartbeats never interfere
        thresholds=CheckThresholds(
            ingest_stall=timedelta(minutes=30),
            coarse_stale=timedelta(minutes=120),
            alert_backlog=timedelta(minutes=5),
            rate_window=timedelta(minutes=15),
            rate_max_events=5,
            disk_percent=85,
        ),
        disk_path="/",
    )


async def _add_trader(
    pool: asyncpg.Pool, address: str, *, fine_refreshed_at: datetime | None, computed_at: datetime
) -> None:
    """An eligible Trader (positive coarse month) with a given last fine refresh.
    NULL fine_refreshed_at (or one older than the active cadence) makes it due."""
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at, fine_refreshed_at) "
        "VALUES ($1, $2, $2, $3)",
        address,
        T0,
        fine_refreshed_at,
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
