"""Watchdog process (issue #135): the primary dead-man's switch, standing
alone.

Wiring notes, each load-bearing:

- FAIL-FAST ON MISSING SIGNER: no watchdog-lane agent key in the keystore
  means this process cannot cancel anything — a watchdog that beats its
  heartbeat but can't act would be worse than none (false safety), so it
  refuses to start instead. Run the ceremony with `--lane watchdog`
  (docs/runbooks/agent-key-ceremony.md) before enabling the service.
- Phase A is operator-only: the account under protection is the operator's
  (ADMIN_TELEGRAM_ID names the keystore row). Multi-account is Phase B.
- One SharedWeightBudget spender at reserve 0 — the execution lane's
  priority (issue #133): a kill sweep never queues behind backfill.
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
from epigone.db import create_pool, migrate
from epigone.gateway.execution import AmbiguousExecutionError
from epigone.gateway.execution_http import HttpExecutionGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.keystore import WATCHDOG_LANE, AgentKeystore, KeystoreError, load_kek
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.config import WatchdogConfig
from epigone.safety.deadman import DeadMansSwitch
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


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    config = WatchdogConfig.from_env()
    pool = await create_pool(settings.database_url)
    await migrate(pool)
    clock = SystemClock()

    kek_path = os.environ.get("KEYSTORE_KEK_FILE")
    if not kek_path:
        raise KeystoreError("KEYSTORE_KEK_FILE must name the KEK file (outside git)")
    keystore = AgentKeystore(pool, load_kek(Path(kek_path)), clock)
    operator_id = settings.require_admin_telegram_id()
    signer = await keystore.signer(operator_id, WATCHDOG_LANE)  # fail fast (module docstring)
    record = await keystore.active_record(operator_id, WATCHDOG_LANE)
    assert record is not None  # signer() above proved it
    master_address = record.master_address

    audit = ExecutionAudit(pool, clock)
    budget = SharedWeightBudget(pool, clock, reserve=0)
    async with aiohttp.ClientSession() as session:
        read_gateway = HttpHyperliquidGateway(session, clock, info_url=config.info_url)
        exec_gateway = AuditedExecutionGateway(
            HttpExecutionGateway(
                session,
                clock,
                budget,
                signer=signer,
                master_address=master_address,
                exchange_url=config.exchange_url,
            ),
            audit,
            actor=WATCHDOG_ACTOR,
            master_address=master_address,
            signer_address=signer.address,
            # The safety path's discipline (PR #143 review): a Postgres
            # outage must never suppress a protective cancel — best-effort
            # audit, guaranteed action.
            best_effort_audit=True,
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
            capability_interval=config.capability_interval,
        )
        # "Which mechanism(s) are active" starts here on the trail: the
        # watchdog is primary the moment it runs; the deadman announces its
        # own eligibility transitions as it probes.
        await audit.record_event(
            actor=WATCHDOG_ACTOR,
            action="watchdog_started",
            risk_decision="process start",
            detail={
                "primary": "watchdog process (scheduleCancel is volume-gated, PR #141)",
                "exchange_url": config.exchange_url,
                "executor_stale_seconds": int(config.executor_stale.total_seconds()),
                "interval_seconds": int(config.interval.total_seconds()),
            },
            master_address=master_address,
        )
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
