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
