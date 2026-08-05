"""Execute process (issue #136): the operator copy executor, standing beside
ingest / stream / bot / watchdog on the Postgres seam (ADR-0002).

Wiring notes, each load-bearing:

- FAIL-FAST ON MISSING SIGNER: no executor-lane agent key means this process
  cannot place anything, and a process that beats a heartbeat but cannot act
  would be worse than none — the watchdog would read it as a healthy
  executor. Run the ceremony with `--lane executor`
  (docs/runbooks/agent-key-ceremony.md) before enabling the service.
- STARTUP REQUIRES POSTGRES, deliberately, unlike the watchdog's cold start
  (#145). The asymmetry is the whole point: a watchdog that cannot reach the
  database must still cancel, because cancelling is subtractive and the
  outage is exactly when orders go unattended. An EXECUTOR that cannot reach
  the database must NOT trade — it has no halt state, no mappings, no claims,
  and no way to write the audit row that must precede the wire.
- ONE GATEWAY INSTANCE, TWO PROTOCOLS: `HttpExecutionGateway` serves both the
  order surface and sub-account provisioning, wrapped separately
  (AuditedExecutionGateway / AuditedProvisioning). One instance because
  nonces are per SIGNER — a second instance for provisioning would run a
  second counter over the same signer and collide.
- The rate budget is the shared bucket at reserve 0: signed orders are the
  execution lane and outrank every poller (issue #133), which is what the
  stream's EXECUTION_RESERVE_WEIGHT floor exists to guarantee.
- Phase A is operator-only: ADMIN_TELEGRAM_ID names both the keystore row and
  the only operator whose copy mappings this process will read (ADR-0007
  decision 12's second gate).
- A broken cycle is logged and retried; only the loop dying is fatal, and the
  #52 monitor sees that as a stale executor heartbeat within a minute.
- Mainnet is refused by construction in HttpExecutionGateway unless this
  process passes the capability, and `EXECUTOR_ALLOW_MAINNET` is the only
  thing that makes it do so (issue #137's live wiring). Default off; the
  mainnet URL alone is still refused. The start-up audit row records which
  network this process came up on, so the trail answers "when did it go live"
  without anyone having to remember.
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
from epigone.execute.config import ExecutorConfig
from epigone.execute.executor import CopyExecutor
from epigone.execute.policy import RiskPolicy
from epigone.gateway.execution_http import HttpExecutionGateway
from epigone.gateway.http import HttpHyperliquidGateway
from epigone.keystore import EXECUTOR_LANE, AgentKeystore, KeystoreError, load_kek
from epigone.safety import heartbeat
from epigone.safety.audit import (
    EXECUTOR_ACTOR,
    AuditedExecutionGateway,
    AuditedProvisioning,
    ExecutionAudit,
)

log = logging.getLogger(__name__)


async def executor_loop(
    executor: CopyExecutor,
    clock: Clock,
    interval_seconds: float,
    *,
    max_cycles: int | None = None,
) -> None:
    """Supervised cadence loop (the watchdog-loop shape): one broken cycle is
    logged and retried next tick, never allowed to stop the executor.
    `max_cycles` bounds it for tests; production leaves it None."""
    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        try:
            await executor.run_cycle()
        except Exception:
            log.exception("copy executor cycle failed; retrying next tick")
        await clock.sleep(interval_seconds)
        cycles += 1


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    config = ExecutorConfig.from_env()
    clock = SystemClock()

    kek_path = os.environ.get("KEYSTORE_KEK_FILE")
    if not kek_path:
        raise KeystoreError("KEYSTORE_KEK_FILE must name the KEK file (outside git)")
    kek = load_kek(Path(kek_path))
    operator_id = settings.require_admin_telegram_id()

    pool = await create_pool(settings.database_url)
    await migrate(pool)
    keystore = AgentKeystore(pool, kek, clock)
    # Fail-fast: no key, no process (module docstring). Both calls raise
    # KeystoreError when the lane was never ceremonied.
    record = await keystore.active_record(operator_id, EXECUTOR_LANE)
    if record is None:
        raise KeystoreError(
            f"no active {EXECUTOR_LANE}-lane agent key for operator {operator_id} — run "
            f"the ceremony (docs/runbooks/agent-key-ceremony.md) before starting this "
            f"service; a beating executor that cannot place is worse than none"
        )
    signer = await keystore.signer(operator_id, EXECUTOR_LANE)
    master_address = record.master_address

    audit = ExecutionAudit(pool, clock)
    budget = SharedWeightBudget(pool, clock, reserve=0)
    async with aiohttp.ClientSession() as session:
        read_gateway = HttpHyperliquidGateway(session, clock, info_url=config.info_url)
        http_exec = HttpExecutionGateway(
            session,
            clock,
            budget,
            signer=signer,
            master_address=master_address,
            exchange_url=config.exchange_url,
            allow_mainnet=config.allow_mainnet,
        )
        exec_gateway = AuditedExecutionGateway(
            http_exec,
            audit,
            actor=EXECUTOR_ACTOR,
            master_address=master_address,
            signer_address=signer.address,
        )
        provisioning = AuditedProvisioning(
            http_exec,
            audit,
            actor=EXECUTOR_ACTOR,
            master_address=master_address,
            signer_address=signer.address,
        )
        executor = CopyExecutor(
            pool,
            clock,
            read_gateway,
            exec_gateway,
            provisioning,
            audit,
            budget,
            RiskPolicy(),
            operator_id=operator_id,
            master_address=master_address,
            signer_address=signer.address,
        )
        await heartbeat.record_start(pool, heartbeat.EXECUTOR_PROCESS, clock.now())
        await audit.record_event(
            actor=EXECUTOR_ACTOR,
            action="executor_started",
            risk_decision="process start",
            detail={
                "exchange_url": config.exchange_url,
                "interval_seconds": int(config.interval.total_seconds()),
                "operator_id": operator_id,
                "risk_policy": "A5 (#137): liquidity floor, stake caps, mirrored leverage",
                "network": "MAINNET" if config.is_mainnet else "testnet",
            },
            master_address=master_address,
        )
        log.info(
            "copy executor: trading %s every %ss (%s, %s)",
            master_address,
            int(config.interval.total_seconds()),
            config.exchange_url,
            "MAINNET — REAL MONEY" if config.is_mainnet else "testnet",
        )
        await executor_loop(executor, clock, config.interval.total_seconds())


if __name__ == "__main__":
    asyncio.run(main())
