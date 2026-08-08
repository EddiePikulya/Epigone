"""Monitor process (issue #52): a send-only health checker.

On a short cadence it gathers one liveness snapshot, evaluates the checks, runs
the alerting state machine, and DMs the admin on Telegram when something trips,
recovers, or once a day as a positive heartbeat. It only ever calls
`sendMessage` — it never polls, so it shares the bot token with the bot process
without the two-poller conflict (ADR-0002: processes meet only where they must).

TWO CADENCES, ONE STATE MACHINE (issue #205). The full cycle stays on the
15-minute clock its expensive checks are priced for; the watchdog-liveness
check alone runs every minute, because it is the one check whose subject has a
deadline. The watchdog is the PRIMARY dead-man's switch and its exchange-side
backstop has a 300s horizon, so a quarter-hour of not-having-looked is long
enough for the schedule to fire before anyone is told the watchdog is gone —
which is what happened on 2026-08-07. Both cadences feed the same `Monitor`,
so a failure is paged once by whichever saw it first.

Notify-first, no auto-remediation: Docker's restart policy already recovers hard
crashes; this catches the silent-but-alive failures and tells a human.
"""

import asyncio
import logging
import shutil
from datetime import datetime

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from epigone.bot.delete import with_delete_button
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import (
    DiskProbe,
    evaluate_checks,
    evaluate_watchdog_liveness,
    gather_snapshot,
    gather_watchdog_snapshot,
)
from epigone.monitor.checks import db_down as db_down_snapshot
from epigone.monitor.config import MonitorConfig

log = logging.getLogger(__name__)


class SystemDiskProbe:
    """Real host disk usage. In the container the host filesystem is mounted at
    `path` (docker-compose), so this reports the server's disk, not the image's."""

    def __init__(self, path: str) -> None:
        self._path = path

    def percent_used(self) -> float | None:
        try:
            usage = shutil.disk_usage(self._path)
        except OSError:
            log.warning("disk probe: %s unreadable; skipping disk check", self._path, exc_info=True)
            return None
        return usage.used / usage.total * 100 if usage.total else None


async def run_monitor_cycle(
    pool: asyncpg.Pool,
    bot: Bot,
    admin_id: int,
    monitor: Monitor,
    config: MonitorConfig,
    clock: Clock,
    disk: DiskProbe,
) -> list[str]:
    """One check cycle: gather → evaluate → decide → send. A failed gather is
    reported as the critical DB-down check (the monitor can still DM). Returns
    the messages sent, for tests and logging."""
    try:
        snapshot = await gather_snapshot(pool, clock, disk, config.thresholds)
    except Exception:
        log.warning("monitor: snapshot gather failed; reporting DB unreachable", exc_info=True)
        snapshot = db_down_snapshot(clock.now())
    results = evaluate_checks(snapshot, config.thresholds)
    messages = monitor.evaluate(results, snapshot, clock.now())
    for text in messages:
        await _send(bot, admin_id, text)
    return messages


async def run_watchdog_liveness_cycle(
    pool: asyncpg.Pool,
    bot: Bot,
    admin_id: int,
    monitor: Monitor,
    config: MonitorConfig,
    clock: Clock,
) -> list[str]:
    """The FAST tick (issue #205): is the dead-man's switch still beating?

    One indexed single-row read and one pure check, so it can run every minute
    against a 15-minute full cycle. That gap was not a cadence but a blind
    spot: the switch's horizon is 300s, and a watchdog that died just after a
    pass went unreported until long after the schedule it exists to refresh
    had fired (live, 2026-08-07). Same check function and same alerting state
    as the full cycle, so whichever cadence sees it first pages, once.

    A read failure is logged and dropped rather than reported as DB-down: the
    database check belongs to the full cycle, which gathers enough to say so
    with its numbers, and duplicating it here would page from a snapshot that
    knows one table."""
    try:
        snapshot = await gather_watchdog_snapshot(pool, clock)
    except Exception:
        log.warning(
            "monitor: watchdog liveness read failed; the full cycle owns the "
            "DB-down signal",
            exc_info=True,
        )
        return []
    results = evaluate_watchdog_liveness(snapshot, config.thresholds)
    messages = monitor.transitions(results, snapshot.now)
    for text in messages:
        await _send(bot, admin_id, text)
    return messages


async def _send(bot: Bot, admin_id: int, text: str) -> None:
    """Best-effort DM. A send failure must not kill the loop — the next cycle
    re-evaluates and, for a still-failing check, will try again on the reminder."""
    try:
        # The 🗑 delete button (#73): the monitor only sends, but its DMs' delete
        # taps land in the bot process's polling loop, where the handler lives.
        await bot.send_message(chat_id=admin_id, text=text, reply_markup=with_delete_button())
    except TelegramAPIError:
        log.warning("monitor: failed to send admin alert %r", text, exc_info=True)


async def monitor_loop(
    pool: asyncpg.Pool,
    bot: Bot,
    admin_id: int,
    config: MonitorConfig,
    clock: Clock,
    disk: DiskProbe,
    *,
    max_cycles: int | None = None,
) -> None:
    """Supervised cadence loop. One broken cycle is logged and retried next tick,
    never allowed to kill the checker. `max_cycles` bounds it for tests; production
    leaves it None.

    TWO CADENCES ON ONE TIMER (issue #205). The loop ticks at the fast
    watchdog-liveness cadence and runs the full cycle whenever it falls due,
    rather than running a second task: everything here shares one pool, one
    Monitor state machine and one Telegram session, and a sibling task would
    have to be given a rule about racing the full cycle on all three. Ticking
    instead makes the two cadences strictly alternating by construction, and
    `max_cycles` counts TICKS.

    A full cycle that overruns simply delays the next tick — the loop is
    honest about being one thread of control. The alternative, letting a
    liveness tick fire while the expensive gather is mid-flight, buys
    punctuality with the one property this loop has always had: it can never
    be the reason Postgres has two monitor queries in the air."""
    monitor = Monitor(reminder=config.reminder, heartbeat_hour=config.heartbeat_hour)
    cycles = 0
    next_full_at: datetime | None = None  # None → due now
    while max_cycles is None or cycles < max_cycles:
        now = clock.now()
        if next_full_at is None or now >= next_full_at:
            try:
                await run_monitor_cycle(pool, bot, admin_id, monitor, config, clock, disk)
            except Exception:
                log.exception("monitor cycle failed; retrying next tick")
            # From the START of the cycle, so a slow gather does not stretch
            # the cadence the operator configured.
            next_full_at = now + config.interval
        else:
            try:
                await run_watchdog_liveness_cycle(pool, bot, admin_id, monitor, config, clock)
            except Exception:
                log.exception("monitor: watchdog liveness tick failed; retrying next tick")
        await clock.sleep(config.watchdog_check.total_seconds())
        cycles += 1


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    config = MonitorConfig.from_env(seed_interval_minutes=settings.seed_interval_minutes)
    pool = await create_pool(settings.database_url)
    await migrate(pool)
    bot = Bot(settings.require_bot_token())
    admin_id = settings.require_admin_telegram_id()
    clock = SystemClock()
    disk = SystemDiskProbe(config.disk_path)
    log.info(
        "monitor: starting health checks every %s, watchdog liveness every %s "
        "(dead-man horizon 300s, issue #205)",
        config.interval,
        config.watchdog_check,
    )
    try:
        await monitor_loop(pool, bot, admin_id, config, clock, disk)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
