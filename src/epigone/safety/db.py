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
    """The watchdog's runtime pool. The overrides exist for tests, which
    prove the bounds with sub-second values; production takes the defaults."""
    pool = await asyncpg.create_pool(
        database_url,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
        server_settings={"lock_timeout": lock_timeout},
    )
    assert pool is not None

    # The acquire-timeout default (module docstring, round 4). asyncpg 0.31
    # offers no pool_class hook and Pool declares __slots__, so the least
    # invasive seam is a layout-compatible subclass swapped onto the live
    # pool: acquire() gains a default timeout, which asyncpg itself then
    # carries into the holder as the RELEASE budget ("Record the timeout, as
    # we will apply it by default in release()" — asyncpg/pool.py). Every
    # caller — pool.execute, pool.fetchrow, bare pool.acquire() in the
    # shared state modules — inherits the bound with no call-site changes.
    class _BoundedAcquirePool(type(pool)):  # type: ignore[misc]
        # Empty slots keep the layout identical to Pool (which declares
        # __slots__), which is what makes the __class__ assignment legal —
        # and is also why the per-pool timeout lives in this closure rather
        # than on the instance. HONESTLY (round 5): this construction can
        # NEVER fail loudly on an asyncpg upgrade — deriving from type(pool)
        # with empty slots is layout-compatible with whatever Pool becomes,
        # and every realistic drift (helpers no longer routing through
        # self.acquire(), the acquire timeout no longer becoming the release
        # budget) degrades SILENTLY. The real guard is the version pin
        # (pyproject: asyncpg>=0.31,<0.32 — an upgrade is a deliberate visit
        # to this seam) plus the black-holed-connection tests in CI, which
        # exercise the actual behavior.
        __slots__ = ()

        def acquire(self, *, timeout: float | None = None) -> "asyncpg.pool.PoolAcquireContext":
            return super().acquire(
                timeout=timeout_seconds if timeout is None else timeout
            )

    pool.__class__ = _BoundedAcquirePool
    return pool
