"""Weight budgeting for Hyperliquid's ~1200 weight/min per-IP limit (issues #28, #41).

Ingest and stream used to run uncoordinated per-process buckets whose shares
summed to exactly the cap (400 + 800), so under real load both tripped 429s.
Now both processes draw from one Postgres-backed token bucket (they meet only
in Postgres, ADR-0002), pacing their combined spend to SHARED_WEIGHT_PER_MINUTE
with real headroom below the cap.

Stream priority: ingest passes a `reserve` of STREAM_RESERVE_WEIGHT, so it can
never draw the bucket below that floor. Whenever the bucket runs low the refill
serves the stream first — background backfill waits, Position Alerts don't.

Issue #41 closed the two gaps that still leaked a trickle of 429s:

- **A send gate** (`next_send_at`): each grant reserves an exclusive window of
  `weight / SMOOTHING_WEIGHT_PER_SECOND`, so even a full bucket cannot be
  emptied in a sub-second spike — no window ever exceeds the per-IP cap's own
  uniform per-second pace. The gate is a window, not a debt: idle time clears
  it, it never accumulates.
- **Post-response reconciliation** (`settle`): some responses reveal weight the
  nominal pre-call bill missed (userFills adds weight per 20 fills returned —
  up to ~+100 on a full response). settle() deducts that truth unconditionally;
  the bucket may go negative, and later spends pace the debt off. Without this
  the "under budget" average was a fiction and the real spend blew the cap.
- **Metering**: the shared row counts real consumed weight per rolling minute;
  whichever spender crosses a minute boundary logs actual-vs-limit, so the
  calibration is observable in production logs.

The in-memory WeightBudget is the same token bucket without the Postgres seam,
the send gate, or the metering; tests of the passes use it where cross-process
coordination is not the point (the fake-in-src convention of
epigone.gateway.fake).
"""

import logging
from datetime import datetime, timedelta
from typing import Protocol

import asyncpg

from epigone.clock import Clock

log = logging.getLogger(__name__)

# Hyperliquid's per-IP allowance, as observed/documented — the hard ceiling.
PER_IP_WEIGHT_PER_MINUTE = 1200

# Our combined refill rate: 75% of the cap, leaving 25% steady-state headroom
# (plus whatever the bot process's rare user-triggered calls need).
SHARED_WEIGHT_PER_MINUTE = 900

# Bucket capacity. Worst rolling minute = one full burst + a minute of refill
# = 240 + 900 = 1140, still under the 1200 cap even in the pathological case.
BURST_WEIGHT = 240

# Ingest never draws the bucket below this floor: the stream's instant claim
# (30 wallets' worth of weight-4 polls). Beyond it the stream still wins — a
# low bucket blocks ingest entirely, so refill accrues to the stream first.
STREAM_RESERVE_WEIGHT = 120

# The send gate's pace (issue #41): a grant of weight w reserves the next
# w / SMOOTHING_WEIGHT_PER_SECOND seconds, so instantaneous spend never exceeds
# this rate. 20/s is the per-IP cap spread uniformly (1200/60) — never send
# faster than the cap's own per-second pace, even transiently — and sits above
# the 15/s refill so short bursts still clear quicker than steady-state pacing.
# It also bounds how long a stream poll can wait behind ingest: one call's
# window plus its settled surcharge — a full ~2000-fill response is ~120 real
# weight, so ~6s worst case, well inside the 30s poll interval. Tune here after
# watching production; the final value is calibrated empirically against the
# server IP (issue #41 notes).
SMOOTHING_WEIGHT_PER_SECOND = PER_IP_WEIGHT_PER_MINUTE / 60

# Never pace-sleep shorter than this. Clock arithmetic quantizes to whole
# microseconds, so a deficit-exact sleep can refill fractionally short and
# leave a dust shortfall whose next sleep rounds to zero — a busy loop. The
# floor guarantees every blocked retry makes real progress.
PACING_SLEEP_FLOOR_SECONDS = 0.05

# The consumption meter rolls (and logs) after this window...
METER_WINDOW_SECONDS = 60.0
# ...but a window that ran far past it means an idle stretch, not a measured
# minute — reset silently rather than log a figure diluted over hours.
METER_STALE_SECONDS = 120.0

# Rate-limit events (issue #54) older than this are pruned as new ones land: the
# health monitor only ever asks about a recent window (default 15m), so a day of
# retention is ample and keeps the append-only log tiny without a separate
# sweeper. Comfortably larger than any sane HEALTHCHECK_RATE_WINDOW_MINUTES.
RATE_EVENT_RETENTION = timedelta(days=1)


class Budget(Protocol):
    """The pacing seam the passes spend against.

    `spend` blocks until the nominal pre-call weight fits; `settle` bills
    weight that only the response revealed (issue #41), never blocking.
    """

    async def spend(self, weight: int) -> None: ...

    async def settle(self, weight: int) -> None: ...


class WeightBudget:
    """In-process token bucket: starts full (one burst), refills continuously."""

    def __init__(self, weight_per_minute: int, clock: Clock) -> None:
        self._capacity = float(weight_per_minute)
        self._rate_per_second = weight_per_minute / 60.0
        self._clock = clock
        self._available = self._capacity
        self._last_refill = clock.now()

    async def spend(self, weight: int) -> None:
        """Block (via the injected clock) until `weight` fits in the budget, then take it."""
        if weight > self._capacity:
            raise ValueError(f"weight {weight} exceeds per-minute capacity {self._capacity}")
        while True:
            self._refill()
            if self._available >= weight:
                self._available -= weight
                return
            deficit = weight - self._available
            await self._clock.sleep(
                max(deficit / self._rate_per_second, PACING_SLEEP_FLOOR_SECONDS)
            )

    async def settle(self, weight: int) -> None:
        """Bill already-consumed weight (issue #41): deduct unconditionally, even
        into debt — the truth of a response is not capped by the bucket."""
        if weight <= 0:
            return
        self._refill()
        self._available -= weight

    def _refill(self) -> None:
        now = self._clock.now()
        elapsed = (now - self._last_refill).total_seconds()
        self._last_refill = now
        self._available = min(self._capacity, self._available + elapsed * self._rate_per_second)


class SharedWeightBudget:
    """The cross-process token bucket: one `rate_budget` row all spenders share.

    Each spend refills and draws the row in one short transaction (the row lock
    serializes concurrent spenders); a spend that does not fit sleeps until its
    blocker — token deficit or the send gate — clears, and tries again.
    `reserve` is the floor this spender must leave in the bucket — 0 for the
    stream, STREAM_RESERVE_WEIGHT for ingest — which is what gives the stream
    its priority. The send gate applies to every spender alike: it caps the
    IP's instantaneous rate, which 429s regardless of who is sending.
    """

    def __init__(self, pool: asyncpg.Pool, clock: Clock, *, reserve: int = 0) -> None:
        self._pool = pool
        self._clock = clock
        self._reserve = float(reserve)
        self._capacity = float(BURST_WEIGHT)
        self._rate_per_second = SHARED_WEIGHT_PER_MINUTE / 60.0

    async def spend(self, weight: int) -> None:
        """Block (via the injected clock) until `weight` fits above this spender's
        reserve floor and the send gate is open, then take it."""
        if weight + self._reserve > self._capacity:
            raise ValueError(
                f"weight {weight} plus reserve {self._reserve} "
                f"exceeds bucket capacity {self._capacity}"
            )
        while True:
            wait = await self._try_spend(weight)
            if wait <= 0:
                return
            await self._clock.sleep(max(wait, PACING_SLEEP_FLOOR_SECONDS))

    async def settle(self, weight: int) -> None:
        """Bill weight that only the response revealed (issue #41), without
        blocking: the bucket may go negative (debt) and later spends pace it
        off. The gate advances too, so the smoothed rate reflects real weight."""
        if weight <= 0:
            return
        now = self._clock.now()
        async with self._pool.acquire() as conn, conn.transaction():
            row = await self._locked_row(conn, now)
            available = self._refilled(row, now) - weight
            gate = max(row["next_send_at"], now) + _gate_window(weight)
            started_at, spent = _roll_meter(now, row["minute_started_at"], row["minute_spent"])
            await self._store(conn, available, now, row, gate, started_at, spent + weight)

    async def _try_spend(self, weight: int) -> float:
        """Refill the shared row and take `weight` if it fits above the reserve
        with the send gate open; returns the seconds until the blocker clears
        (0 when granted). The refill and meter roll are persisted either way."""
        now = self._clock.now()
        async with self._pool.acquire() as conn, conn.transaction():
            row = await self._locked_row(conn, now)
            available = self._refilled(row, now)
            token_wait = max(0.0, (weight + self._reserve - available) / self._rate_per_second)
            gate_wait = max(0.0, (row["next_send_at"] - now).total_seconds())
            granted = token_wait <= 0 and gate_wait <= 0
            started_at, spent = _roll_meter(now, row["minute_started_at"], row["minute_spent"])
            if granted:
                available -= weight
                spent += weight
                gate = now + _gate_window(weight)
            else:
                gate = row["next_send_at"]
            await self._store(conn, available, now, row, gate, started_at, spent)
            return 0.0 if granted else max(token_wait, gate_wait)

    async def _locked_row(self, conn: asyncpg.Connection, now: datetime) -> asyncpg.Record:
        await conn.execute(
            """
            INSERT INTO rate_budget
                (id, available, last_refill, next_send_at, minute_started_at, minute_spent)
            VALUES (TRUE, $1, $2, $2, $2, 0)
            ON CONFLICT (id) DO NOTHING
            """,
            self._capacity,
            now,
        )
        row = await conn.fetchrow("SELECT * FROM rate_budget FOR UPDATE")
        assert row is not None
        return row

    def _refilled(self, row: asyncpg.Record, now: datetime) -> float:
        # Guard against clock skew between processes: never refill backwards.
        elapsed: float = max(0.0, (now - row["last_refill"]).total_seconds())
        available: float = row["available"]
        return min(self._capacity, available + elapsed * self._rate_per_second)

    async def _store(
        self,
        conn: asyncpg.Connection,
        available: float,
        now: datetime,
        row: asyncpg.Record,
        gate: datetime,
        meter_started_at: datetime,
        meter_spent: float,
    ) -> None:
        await conn.execute(
            """
            UPDATE rate_budget
            SET available = $1, last_refill = $2, next_send_at = $3,
                minute_started_at = $4, minute_spent = $5
            """,
            available,
            max(now, row["last_refill"]),
            gate,
            meter_started_at,
            meter_spent,
        )


async def record_rate_limit(pool: asyncpg.Pool, occurred_at: datetime) -> None:
    """Stamp a sustained rate-limit event for the health monitor (issue #54).

    Called only when a `RateLimitedError` escapes the gateway — the gateway
    already backed off and retried through a full 429 streak (issue #28), so each
    event is real limiting, never a lone backoff-absorbed 429 (which never gets
    here; user story #2). Writes an append-only `rate_limit_events` row rather
    than touching the hot `rate_budget` bucket every spender locks, and prunes
    stale rows as it goes. Best-effort by design: a failed health-signal write
    must never disturb the ingest/stream pass it rides on."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO rate_limit_events (occurred_at) VALUES ($1)", occurred_at
            )
            await conn.execute(
                "DELETE FROM rate_limit_events WHERE occurred_at < $1",
                occurred_at - RATE_EVENT_RETENTION,
            )
    except Exception:
        log.warning("failed to record rate-limit event", exc_info=True)


def _gate_window(weight: int) -> timedelta:
    """The exclusive send window a grant (or settled surcharge) of this weight
    reserves behind the gate."""
    return timedelta(seconds=weight / SMOOTHING_WEIGHT_PER_SECOND)


def _roll_meter(now: datetime, started_at: datetime, spent: float) -> tuple[datetime, float]:
    """Advance the consumption meter across a minute boundary, logging the
    finished minute's real weight against the limits (issue #41 observability).
    Within the window the meter just accumulates."""
    window = (now - started_at).total_seconds()
    if window < METER_WINDOW_SECONDS:
        return started_at, spent
    if window <= METER_STALE_SECONDS:
        log.info(
            "rate budget: %.0f weight consumed in the last %.0fs "
            "(pacing %d/min, per-IP cap %d/min)",
            spent,
            window,
            SHARED_WEIGHT_PER_MINUTE,
            PER_IP_WEIGHT_PER_MINUTE,
        )
    return now, 0.0
