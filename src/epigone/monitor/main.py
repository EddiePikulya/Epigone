"""Monitor process (issue #52): a send-only health checker.

On a short cadence it gathers one liveness snapshot, evaluates the checks, runs
the alerting state machine, and DMs the admin on Telegram when something trips,
recovers, or once a day as a positive heartbeat. It only ever calls
`sendMessage` — it never polls, so it shares the bot token with the bot process
without the two-poller conflict (ADR-0002: processes meet only where they must).

Notify-first, no auto-remediation: Docker's restart policy already recovers hard
crashes; this catches the silent-but-alive failures and tells a human.
"""

import asyncio
import logging
import shutil

import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError

from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.db import create_pool, migrate
from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import DiskProbe, evaluate_checks, gather_snapshot
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
        snapshot = await gather_snapshot(pool, clock, disk)
    except Exception:
        log.warning("monitor: snapshot gather failed; reporting DB unreachable", exc_info=True)
        snapshot = db_down_snapshot(clock.now())
    results = evaluate_checks(snapshot, config.thresholds)
    messages = monitor.evaluate(results, snapshot, clock.now())
    for text in messages:
        await _send(bot, admin_id, text)
    return messages


async def _send(bot: Bot, admin_id: int, text: str) -> None:
    """Best-effort DM. A send failure must not kill the loop — the next cycle
    re-evaluates and, for a still-failing check, will try again on the reminder."""
    try:
        await bot.send_message(chat_id=admin_id, text=text)
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
    leaves it None."""
    monitor = Monitor(reminder=config.reminder, heartbeat_hour=config.heartbeat_hour)
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        try:
            await run_monitor_cycle(pool, bot, admin_id, monitor, config, clock, disk)
        except Exception:
            log.exception("monitor cycle failed; retrying next tick")
        await clock.sleep(config.interval.total_seconds())
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
    log.info("monitor: starting health checks every %s", config.interval)
    try:
        await monitor_loop(pool, bot, admin_id, config, clock, disk)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
