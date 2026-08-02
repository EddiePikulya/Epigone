"""Watchdog process (issue #135): the primary dead-man's switch, standing
alone.

Wiring notes, each load-bearing:

- FAIL-FAST ON MISSING SIGNER: no watchdog-lane agent key — in the keystore
  when Postgres answers, in the encrypted local cache when it does not —
  means this process cannot cancel anything, and a watchdog that beats its
  heartbeat but can't act would be worse than none (false safety), so it
  refuses to start instead. Run the ceremony with `--lane watchdog`
  (docs/runbooks/agent-key-ceremony.md) before enabling the service.
- STARTUP DOES NOT REQUIRE POSTGRES (issue #145). epigone.safety.coldstart
  probes the database on a bounded connect for a grace window; if it never
  answers, the process COLD-STARTS BLIND — cached watchdog-lane key,
  deferred migration check, an incident declared at launch so the first
  cycle cancels — and reconciles into the trail when Postgres returns,
  launch stamp included, with the actual launch time.
- CANCEL-ONLY BY CONSTRUCTION: the real gateway is wrapped in
  CancelOnlyExecutionGateway before the audit wrapper, so no path through
  this process — the blind cold-start path least of all — can place,
  modify, or re-lever. The watchdog's authority is subtractive; that is now
  a property of the wiring rather than of what the code happens to call.
- Phase A is operator-only: the account under protection is the operator's
  (ADMIN_TELEGRAM_ID names the keystore row). Multi-account is Phase B.
- The rate budget is a FallbackBudget over a SharedWeightBudget at
  reserve 0 — execution-lane priority (issue #133) while Postgres answers,
  in-process pacing when it doesn't (PR #143 round 2): a dead rate_budget
  row must never queue a cancel. Safety lane only; the executor's order
  lane keeps the shared bucket un-degraded.
- The deadman upgrade path shares the cycle but not the cycle's fate: its
  ambiguity is logged and retried, and any cycle error is logged and
  retried — a broken cycle must never stop the loop (the monitor alerts on
  a stale watchdog heartbeat if the breakage persists).
- Mainnet is refused by construction in HttpExecutionGateway (the A5 gate);
  this process never passes allow_mainnet.
"""

import asyncio
import logging
import os
from pathlib import Path

import aiohttp

from epigone.budget import SharedWeightBudget
from epigone.clock import Clock, SystemClock
from epigone.config import Settings
from epigone.gateway.execution import AmbiguousExecutionError, ExecutionGateway
from epigone.gateway.execution_http import HttpExecutionGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.keystore import KeystoreError, load_kek
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.budget import PRIMARY_ATTEMPT_CEILING_SECONDS, FallbackBudget
from epigone.safety.cancel_only import CancelOnlyExecutionGateway
from epigone.safety.coldstart import open_startup
from epigone.safety.config import WatchdogConfig
from epigone.safety.deadman import DeadMansSwitch
from epigone.safety.keycache import WatchdogKeyCache
from epigone.safety.watchdog import Watchdog

log = logging.getLogger(__name__)


async def watchdog_loop(
    watchdog: Watchdog,
    deadman: DeadMansSwitch,
    clock: Clock,
    interval_seconds: float,
    *,
    max_cycles: int | None = None,
) -> None:
    """Supervised cadence loop (the monitor-loop shape): one broken cycle is
    logged and retried next tick, never allowed to stop the switch.
    `max_cycles` bounds it for tests; production leaves it None."""
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        try:
            await watchdog.run_cycle()
        except Exception:
            log.exception("watchdog cycle failed; retrying next tick")
        try:
            await deadman.maintain()
        except AmbiguousExecutionError:
            # On the audit trail already; a repeated set replaces the
            # schedule, so next cycle's retry is the reconciliation.
            log.warning("dead-man's switch push was ambiguous; retrying next tick")
        except Exception:
            log.exception("dead-man's switch maintenance failed; retrying next tick")
        await clock.sleep(interval_seconds)
        cycles += 1


def safety_gateway(
    inner: ExecutionGateway,
    audit: ExecutionAudit,
    *,
    master_address: str,
    signer_address: str,
) -> AuditedExecutionGateway:
    """The watchdog's write path, in one place so the tests can assert its
    SHAPE rather than re-describe it (review of PR for issue #145):

    - CANCEL-ONLY INSIDE the audit wrapper: a refused placement never reaches
      the signer, and still leaves its error row on the trail. This is what
      makes "a blind cold start cannot open risk" structural.
    - BEST-EFFORT AUDIT (PR #143 review, both rounds): a Postgres outage must
      never suppress a protective cancel. This flag is the audit leg;
      FallbackBudget is the pacing leg; the watchdog's blind trip is the
      decision leg (watchdog.py's module docstring states the full guarantee).
    """
    return AuditedExecutionGateway(
        CancelOnlyExecutionGateway(inner),
        audit,
        actor=WATCHDOG_ACTOR,
        master_address=master_address,
        signer_address=signer_address,
        best_effort_audit=True,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    config = WatchdogConfig.from_env()
    clock = SystemClock()
    launched_at = clock.now()

    kek_path = os.environ.get("KEYSTORE_KEK_FILE")
    if not kek_path:
        raise KeystoreError("KEYSTORE_KEK_FILE must name the KEK file (outside git)")
    kek = load_kek(Path(kek_path))
    operator_id = settings.require_admin_telegram_id()

    # Where startup used to hard-depend on Postgres (pool → migrations →
    # keystore) it now DECIDES (issue #145, epigone.safety.coldstart): a
    # DB-backed start migrates, loads the keystore's watchdog-lane key and
    # refreshes the local cache from it; a start whose grace window expires
    # unanswered takes the cached key instead and hands back a pool that
    # keeps trying to become real. `record_start` is stamped inside — at
    # launch when Postgres answers, retroactively on the reconnect when it
    # does not (the #52 grace clock must not restart because of a cold start).
    startup = await open_startup(
        database_url=settings.database_url,
        clock=clock,
        kek=kek,
        cache=WatchdogKeyCache(config.key_cache_path, kek),
        operator_id=operator_id,
        launched_at=launched_at,
        grace=config.coldstart_grace,
        retry_every=config.interval,
    )
    pool = startup.pool
    signer = startup.signer
    master_address = startup.master_address

    audit = ExecutionAudit(pool, clock)
    budget = FallbackBudget(
        SharedWeightBudget(
            pool, clock, reserve=0, attempt_ceiling=PRIMARY_ATTEMPT_CEILING_SECONDS
        ),
        clock,
    )
    async with aiohttp.ClientSession() as session:
        read_gateway = HttpHyperliquidGateway(session, clock, info_url=config.info_url)
        exec_gateway = safety_gateway(
            HttpExecutionGateway(
                session,
                clock,
                budget,
                signer=signer,
                master_address=master_address,
                exchange_url=config.exchange_url,
            ),
            audit,
            master_address=master_address,
            signer_address=signer.address,
        )
        deadman = DeadMansSwitch(
            exec_gateway,
            audit,
            clock,
            horizon=config.deadman_horizon,
            reprobe=config.deadman_reprobe,
            master_address=master_address,
        )
        watchdog = Watchdog(
            pool,
            clock,
            read_gateway,
            exec_gateway,
            audit,
            budget,
            master_address=master_address,
            signer_address=signer.address,
            executor_stale=config.executor_stale,
            db_blind_after=config.db_blind_after,
            capability_interval=config.capability_interval,
        )
        if startup.cold_start_reason is not None:
            # The incident opens HERE, not after a threshold: the startup
            # grace already spent the blind threshold's worth of unbroken
            # unreachability, so the first cycle cancels (issue #145).
            watchdog.declare_cold_start(launched_at, startup.cold_start_reason)
        # "Which mechanism(s) are active" starts here on the trail: the
        # watchdog is primary the moment it runs; the deadman announces its
        # own eligibility transitions as it probes.
        try:
            await audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="watchdog_started",
                risk_decision="process start",
                detail={
                    "primary": "watchdog process (scheduleCancel is volume-gated, PR #141)",
                    "exchange_url": config.exchange_url,
                    "executor_stale_seconds": int(config.executor_stale.total_seconds()),
                    "interval_seconds": int(config.interval.total_seconds()),
                    "cold_start": startup.cold_start_reason is not None,
                },
                master_address=master_address,
            )
        except Exception:
            # A cold start has no database to announce itself to, and the
            # announcement must never be what stops the switch from running.
            # The reconnect's own event and the blind-window reconcile carry
            # the story into the trail once Postgres answers.
            log.exception("watchdog: could not record the process-start event; continuing")
        log.info(
            "watchdog: guarding %s every %ss (executor stale after %ss, %s)",
            master_address,
            int(config.interval.total_seconds()),
            int(config.executor_stale.total_seconds()),
            config.exchange_url,
        )
        await watchdog_loop(watchdog, deadman, clock, config.interval.total_seconds())


if __name__ == "__main__":
    asyncio.run(main())
