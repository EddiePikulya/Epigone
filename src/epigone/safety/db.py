"""Bounded-latency Postgres access for the safety process (issue #135,
PR #143 round 3).

Every leg of the DB-degradation design — the blind trip, the best-effort
beat, FallbackBudget — engages when an await RAISES. But the classic
correlated-infrastructure failure this design exists to survive does not
raise; it HANGS: a partitioned or powered-off database host leaves a pooled
connection blocking on TCP retransmission for ~15 minutes, a fresh connect
waits out asyncpg's 60s default, and a `SELECT … FOR UPDATE` behind a lock
holder that died mid-transaction blocks forever (Postgres ships with
idle_in_transaction_session_timeout off). A watchdog stuck in any of those
never reaches the blind logic at all.

So the safety process gets its own pool, where every touch is bounded and a
hang becomes the exception the design already handles:

- CONNECT: asyncpg's per-connection `timeout`, so a dead host refuses fast;
- PER QUERY: `command_timeout`, enforced CLIENT-side by asyncpg — which is
  what makes it hold even when the server is a black hole;
- LOCK WAITS: a server-side `lock_timeout`, so a wedged lock row answers
  with an error instead of queueing the kill switch behind a corpse.

With these, WATCHDOG_DB_BLIND_SECONDS is a real bound (threshold plus a few
bounded touches), not an aspiration.

Only the safety process uses this. The scanner processes keep
epigone.db.create_pool's unbounded defaults on purpose: fine backfills and
migrations run legitimately long queries and must not inherit an
incident-tuned timeout — which is also why the watchdog runs its MIGRATIONS
on a plain pool that closes before this one takes over
(epigone.safety.main). Everything after that — the keystore's single-row
reads included — runs bounded here: startup failing fast inside the bound
is exactly the fail-fast the watchdog wants.
"""

import asyncpg

# One bound for connect and per-query alike: far above any healthy safety
# query (single-row reads/upserts) and far below the blind threshold, so a
# fully hung cycle still costs a handful of touches × this, not minutes.
SAFETY_DB_TIMEOUT_SECONDS = 5.0
# The FOR UPDATE bound (rate_budget, execution_halts): same reasoning.
SAFETY_LOCK_TIMEOUT = "5s"


async def create_safety_pool(
    database_url: str,
    *,
    timeout_seconds: float = SAFETY_DB_TIMEOUT_SECONDS,
    lock_timeout: str = SAFETY_LOCK_TIMEOUT,
) -> asyncpg.Pool:
    """The watchdog's runtime pool. The overrides exist for tests, which
    prove the bounds with sub-second values; production takes the defaults."""
    pool = await asyncpg.create_pool(
        database_url,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
        server_settings={"lock_timeout": lock_timeout},
    )
    assert pool is not None
    return pool
