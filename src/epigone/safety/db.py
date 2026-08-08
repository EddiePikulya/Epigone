"""Bounded-latency Postgres access for the safety process (issue #135,
PR #143 rounds 3–4).

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
  what makes it fire even when the server is a black hole;
- LOCK WAITS: a server-side `lock_timeout`, so a wedged lock row answers
  with an error instead of queueing the kill switch behind a corpse;
- THE CANCEL-WAIT ON RELEASE (round 4 — the trap): when `command_timeout`
  fires, asyncpg sends a CancelRequest over a FRESH TCP connection opened
  with NO timeout (`connect_utils._cancel`: bare `loop.create_connection`,
  then an unbounded wait for the server to hang up — verified against the
  installed asyncpg 0.31.0), and the pool's release then AWAITS that
  cancellation with the holder's budget — which is the ACQUIRE timeout,
  `None` for every plain `pool.execute`/`pool.acquire()` in this codebase.
  Against a black-holed host that turns "≤5s per query" into 5s + a kernel
  SYN timeout (~2 minutes on the Linux production host) PER TOUCH, because
  `async with pool.acquire()` runs release in `__aexit__` before the
  TimeoutError even propagates. The fix: this pool's `acquire()` carries a
  DEFAULT acquire timeout, which asyncpg stamps onto the holder
  (`ch._timeout`) and applies as the release budget — a cancellation that
  cannot complete within it gets the connection TERMINATED (asyncpg's own
  release-failure path) instead of awaited. The stuck cancel socket dies in
  the background; the caller's touch stays bounded at roughly
  command_timeout + the release budget (~2× TIMEOUT).

HONEST SCOPE (round 5): these timeouts bound plain execute/fetch/acquire
touches. They can NOT reach a transaction exit — every asyncpg protocol op
awaits an UNTIMED cancel_waiter before its own command timeout arms, so a
ROLLBACK issued after a black-holed statement hangs past every bound here.
That is exactly why the safety lane stopped depending on them for the
incident path: once an incident is declared the cancel reaches the wire
with ZERO Postgres work (watchdog.py, FallbackBudget.incident_mode), and
every durable/state block — normal operation included — additionally runs
under a hard asyncio.wait_for ceiling (watchdog.DB_BLOCK_CEILING_SECONDS;
safe because Pool.release is shielded, so a cancelled block's connection is
still terminated within the acquire budget). These pool bounds remain the
NORMAL-operation guarantee and the reason a ceilinged block usually exits
in ~2× TIMEOUT instead of at the ceiling.

The scanner processes keep epigone.db.create_pool's unbounded defaults on
purpose: fine backfills and migrations run legitimately long queries and must
not inherit an incident-tuned timeout — which is also why the watchdog runs
its MIGRATIONS on a plain pool that closes before this one takes over
(epigone.safety.main). Everything after that — the keystore's single-row
reads included — runs bounded here: startup failing fast inside the bound
is exactly the fail-fast the watchdog wants.

WHERE THE MECHANISM LIVES (issue #213). This module used to build the
bounded pool itself. It no longer does: the #52 health monitor turned out to
need the same bounds — a black-holed database hung its loop instead of
tripping its DB-down check, so the checker went silent in the one failure it
shares with the watchdog it checks — and a second copy of the release-budget
subclass would be two places to get an asyncpg upgrade wrong. So the four
bounds above are `epigone.db.create_pool`'s optional arguments, documented
there, and this module is now the SAFETY LANE'S NUMBERS: what a watchdog's
deadline can afford, and why.
"""

import asyncpg

from epigone.db import create_pool

# One bound for connect, per-query, and the acquire/release budget alike:
# far above any healthy safety query (single-row reads/upserts) and far
# below the blind threshold, so a fully hung cycle costs a handful of
# touches × ~2× this (query bound + release's bounded cancel-wait).
SAFETY_DB_TIMEOUT_SECONDS = 5.0
# The FOR UPDATE bound (rate_budget, execution_halts): same reasoning.
SAFETY_LOCK_TIMEOUT = "5s"


async def create_safety_pool(
    database_url: str,
    *,
    timeout_seconds: float = SAFETY_DB_TIMEOUT_SECONDS,
    lock_timeout: str = SAFETY_LOCK_TIMEOUT,
) -> asyncpg.Pool:
    """The watchdog's runtime pool: all four bounds of the module docstring,
    at the safety lane's numbers. The overrides exist for tests, which prove
    the bounds with sub-second values; production takes the defaults."""
    return await create_pool(
        database_url, timeout_seconds=timeout_seconds, lock_timeout=lock_timeout
    )
