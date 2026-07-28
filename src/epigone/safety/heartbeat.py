"""Process heartbeats: the liveness seam the dead-man's switch reads
(issue #135; ADR-0002 — processes meet only in Postgres).

The (future, A4+) executor upserts EXECUTOR_PROCESS every loop; the watchdog
trips when that row goes stale and upserts WATCHDOG_PROCESS itself so the
#52 health monitor can watch the watcher. Rows are current state, not
history: one per process, overwritten in place. Decommissioning a process
for good means deleting its row (docs/runbooks/halt-and-unwind.md) —
otherwise its staleness reads as death, which is exactly the fail-safe
default a dead-man's switch wants.
"""

from datetime import datetime

import asyncpg

EXECUTOR_PROCESS = "executor"
WATCHDOG_PROCESS = "watchdog"


async def beat(pool: asyncpg.Pool, process: str, now: datetime) -> None:
    await pool.execute(
        """
        INSERT INTO process_heartbeats (process, beaten_at) VALUES ($1, $2)
        ON CONFLICT (process) DO UPDATE SET beaten_at = EXCLUDED.beaten_at
        """,
        process,
        now,
    )


async def last_beat(pool: asyncpg.Pool, process: str) -> datetime | None:
    """When the process last beat, or None if it never ran (a legitimate
    pre-deploy state, distinct from stale — callers decide what each means)."""
    beaten: datetime | None = await pool.fetchval(
        "SELECT beaten_at FROM process_heartbeats WHERE process = $1", process
    )
    return beaten


async def record_start(pool: asyncpg.Pool, process: str, now: datetime) -> None:
    """Stamp the process's launch (migration 0026): the #52 monitor measures
    the never-verified capability grace period from here, so a probe that has
    never succeeded in this process's life eventually escalates instead of
    reading as healthy forever (PR #143 round 2)."""
    await pool.execute(
        """
        INSERT INTO process_heartbeats (process, beaten_at, started_at)
        VALUES ($1, $2, $2)
        ON CONFLICT (process) DO UPDATE
        SET started_at = EXCLUDED.started_at, beaten_at = EXCLUDED.beaten_at
        """,
        process,
        now,
    )


async def record_capability(
    pool: asyncpg.Pool, process: str, *, capable: bool, detail: str, now: datetime
) -> None:
    """The on-chain capability verdict (migration 0025): can this process's
    agent key actually act? Stored beside the heartbeat so the #52 monitor
    can tell a beating-but-impotent watchdog from a healthy one (PR #143
    review). Upserts like beat(): the verdict must land even if a check runs
    before the first beat of a cycle."""
    await pool.execute(
        """
        INSERT INTO process_heartbeats
            (process, beaten_at, capable, capability_detail, capability_checked_at)
        VALUES ($1, $2, $3, $4, $2)
        ON CONFLICT (process) DO UPDATE
        SET capable = EXCLUDED.capable,
            capability_detail = EXCLUDED.capability_detail,
            capability_checked_at = EXCLUDED.capability_checked_at
        """,
        process,
        now,
        capable,
        detail,
    )
