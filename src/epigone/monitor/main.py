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

EVERY DB TOUCH IS BOUNDED (issue #213). This loop used to run on
`epigone.db.create_pool`'s unbounded defaults, and the failure that exposed
was not a slow page but NO page: a black-holed database host does not refuse
a query, it swallows it, so `gather_snapshot` HUNG rather than raising, the
DB-down check never evaluated, the fast tick never came round again, and the
monitor went perfectly silent — for the ~15 minutes it takes kernel TCP
retransmission to give up, or forever. A dead database host being among the
likeliest reasons the watchdog is dead too, that silence was CORRELATED with
the thing this process exists to check. So the runtime pool now carries the
watchdog's bounds (`epigone.db.create_pool`'s optional timeouts, at this
lane's numbers below), and each gather additionally sits under a hard
real-time ceiling sized from the number of touches that gather actually
makes. A hang is now an exception, and this module already knew what to do
with one: report DB-down.

That closes the SILENCE, not the failure domain — the monitor still reaches
the operator through its own process and its own Postgres reads, so a host
that takes both out still pages nobody from here. The path that does not
share the domain is the watchdog's external dead-man ping
(epigone.safety.ping, issue #213), which reports from outside; the runbook
states which failure each of the two actually covers.

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

# The monitor's per-touch DB bound (issue #213): connect, per-query, and the
# acquire/release budget alike, exactly as the watchdog's pool does it.
#
# Deliberately LOOSER than the safety lane's 5s, because the two lanes are
# priced against different mistakes. The watchdog's touches are single-row
# reads on a path where being late is being useless. The monitor's expensive
# gather is one fetchrow of ~18 correlated subqueries across five tables,
# including a count over the ~40k-row Universe, and its mistake is a FALSE
# 🚨 DB-down page — an operator woken by a database that was merely busy
# learns to distrust the pager, which is the one thing a pager cannot afford.
# 10s is two orders of magnitude above what these queries cost healthy and
# two orders below the hang timescales (asyncpg's 60s connect default, ~15min
# TCP retransmission) it replaces.
MONITOR_DB_TIMEOUT_SECONDS = 10.0
# The worst LEGAL cost of one fully-hung pool touch: an acquire, a query and a
# release, each bounded above by the per-touch bound. Conservative on purpose.
# In practice a touch costs nearer 2× — this loop is a single sequential
# consumer, so it never queues for a connection and the acquire leg is free —
# but a ceiling is priced against the case where "in practice" is wrong, and
# the two errors are not symmetric: a ceiling set too loose costs latency, a
# ceiling set too tight costs a FALSE 🚨 DB-down page, which is the mistake
# the 10s bound above is itself priced against.
WORST_TOUCH_SECONDS = 3 * MONITOR_DB_TIMEOUT_SECONDS

# How many SEQUENTIAL pool touches each gather makes. Load-bearing numbers,
# not documentation: the ceilings below are derived from them, and an earlier
# revision of this file got the full gather wrong (review of PR #216) — it
# ceilinged a TWO-touch gather at one touch's worth, so a loaded-but-answering
# database could outrun the ceiling and page DB-down for a database that was
# merely busy. `test_the_ceilings_match_what_the_gathers_actually_touch` pins
# both counts against the real functions, so adding a touch fails a test
# rather than quietly re-creating that bug.
FULL_GATHER_TOUCHES = 2  # the big fetchrow, then count_due_traders
LIVENESS_GATHER_TOUCHES = 1  # one indexed fetchrow, and deliberately only one

# The hard REAL-TIME ceilings, one per gather because the two gathers are not
# the same size and a single number would have to be wrong for one of them.
# Safe to compose because Pool.release is shielded: a cancelled gather still
# terminates its connection within the acquire budget rather than leaking it.
#
# HONESTLY, these are DEFENCE IN DEPTH rather than a leg the pool demonstrably
# cannot reach. The watchdog's equivalent ceiling exists for a specific hole —
# asyncpg awaits an UNTIMED cancel_waiter before a per-op timeout arms, so a
# transaction exit outlives every pool bound (the round-5 finding in
# epigone.safety.watchdog) — and every touch here is a bare `fetchrow`/
# `fetchval` with no transaction, so today the pool bounds should suffice on
# their own. They are kept because this codebase's whole history on this axis
# is bounds that turned out to have one more layer underneath them, and
# because the cost of being wrong is not a slow page but NO page.
#
# The liveness ceiling must also stay under the fast tick's own cadence (60s),
# and does at 30s. The full one may exceed it (60s) without harm: the full
# cycle runs on the 15-minute clock, and a full cycle that overruns simply
# delays the next tick — the loop has always been one thread of control.
MONITOR_FULL_CEILING_SECONDS = FULL_GATHER_TOUCHES * WORST_TOUCH_SECONDS
MONITOR_LIVENESS_CEILING_SECONDS = LIVENESS_GATHER_TOUCHES * WORST_TOUCH_SECONDS

# The coarse deadline on STARTUP's migration run (`open_monitor_pool`).
# Deliberately enormous next to everything else in this file, because it is
# guarding a different thing: not "is this query slow" but "is this process
# ever going to reach its first cycle". Five minutes is far longer than any
# migration this repo has shipped and far longer than a reasonable wait behind
# another process's advisory lock, while still being a bound — which unbounded
# was not, and unbounded at startup means silence nobody can distinguish from
# health.
MONITOR_MIGRATE_CEILING_SECONDS = 300.0

# No `lock_timeout`, unlike the safety pool — a deliberate omission, not an
# oversight. That bound exists there for `SELECT … FOR UPDATE` behind a holder
# that died mid-transaction; the monitor takes no row locks at all, and its
# reads' realistic lock wait is an ACCESS EXCLUSIVE held by a migration during
# a deploy — which `command_timeout` already bounds client-side, and which a
# server-side error would turn into a FALSE 🚨 DB-down page on every deploy
# that alters a table the gather reads. Three bounds here, four there.


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
    the messages sent, for tests and logging.

    A gather that HANGS is the same signal as one that fails (issue #213):
    the ceiling turns it into the TimeoutError this `except` already catches,
    so a black-holed host pages DB-down instead of stopping the loop where it
    stands. `Exception` deliberately, not `TimeoutError` — a monitor that
    cannot answer "is the database there" is reporting the same thing
    whatever the cause."""
    try:
        snapshot = await asyncio.wait_for(
            gather_snapshot(pool, clock, disk, config.thresholds), MONITOR_FULL_CEILING_SECONDS
        )
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
    knows one table.

    Which is exactly why this tick must not HANG on that read (issue #213):
    dropping a tick costs one minute of latency, but hanging on it costs the
    whole loop — including the full cycle that owns the DB-down page. The
    ceiling is what keeps a black-holed host to the first cost."""
    try:
        snapshot = await asyncio.wait_for(
            gather_watchdog_snapshot(pool, clock), MONITOR_LIVENESS_CEILING_SECONDS
        )
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


async def open_monitor_pool(database_url: str) -> asyncpg.Pool:
    """Migrations on a plain unbounded pool, then the bounded pool the loop
    runs on (issue #213) — the shape `epigone.safety.coldstart` already uses
    for the watchdog, for the same reason: a migration legitimately runs long
    and must not inherit a timeout tuned for a process with a deadline, while
    everything after startup must.

    Every process calls `migrate` at startup (ADR-0002), so the monitor's
    usually applies nothing at all — but "usually" is not a bound, and this
    process is the one that has to survive being pointed at a database that
    just took a schema change.

    STARTUP GETS A COARSE DEADLINE OF ITS OWN (review of PR #216). Bounding
    the loop while leaving startup unbounded left the issue's own failure
    shape alive at the one moment nothing is watching: a host that black-holes
    while the monitor is migrating hangs it here, before the first cycle, so
    there is no DB-down page and no daily digest — silence indistinguishable
    from health. Crashing instead is strictly better: the container
    crash-loops under `restart: unless-stopped`, which is visible in
    `docker compose ps` and in the logs. Be honest about the size of that
    improvement, though — a crash loop is VISIBLE, not a page. Nothing pages
    for a dead monitor at all, however it died; that is the residual issue
    #220 tracks."""
    startup_pool = await create_pool(database_url)
    # NOT `asyncio.wait_for`, and the difference is the whole point. wait_for
    # CANCELS what it times out and then AWAITS that cancellation — and a
    # migration cancelled mid-transaction rolls back through the untimed
    # cancel_waiter that this codebase has now met four times
    # (epigone.safety.db's module docstring), so a wait_for here would hang in
    # exactly the way it was added to prevent. `asyncio.wait` returns on the
    # timeout WITHOUT touching the task; `terminate()` then rips the sockets
    # down synchronously, which is what actually ends the hung migration and
    # releases its advisory lock server-side.
    task = asyncio.create_task(migrate(startup_pool))
    done, _ = await asyncio.wait({task}, timeout=MONITOR_MIGRATE_CEILING_SECONDS)
    if not done:
        log.error(
            "monitor: migrations did not finish within %.0fs — refusing to start rather "
            "than hang unwatched before the first check cycle (issue #213). The database "
            "may be unreachable, or another process may be holding the migration lock.",
            MONITOR_MIGRATE_CEILING_SECONDS,
        )
        startup_pool.terminate()
        task.cancel()  # not awaited: the transport is already gone and we are exiting
        raise TimeoutError(
            f"monitor migrations exceeded {MONITOR_MIGRATE_CEILING_SECONDS:.0f}s"
        )
    try:
        await task  # a genuine migration failure still propagates, as it always did
    finally:
        await startup_pool.close()
    return await create_pool(database_url, timeout_seconds=MONITOR_DB_TIMEOUT_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    config = MonitorConfig.from_env(seed_interval_minutes=settings.seed_interval_minutes)
    pool = await open_monitor_pool(settings.database_url)
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
