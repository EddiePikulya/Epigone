"""Weight budgeting for Hyperliquid's ~1200 weight/min per-IP limit (issue #28).

Ingest and stream used to run uncoordinated per-process buckets whose shares
summed to exactly the cap (400 + 800), so under real load both tripped 429s.
Now both processes draw from one Postgres-backed token bucket (they meet only
in Postgres, ADR-0002), pacing their combined spend to SHARED_WEIGHT_PER_MINUTE
with real headroom below the cap.

Stream priority: ingest passes a `reserve` of STREAM_RESERVE_WEIGHT, so it can
never draw the bucket below that floor. The stream's poll bursts always find
tokens instantly, and whenever the bucket runs low the refill serves the stream
first — background backfill waits, Position Alerts don't.

The in-memory WeightBudget is the same token bucket without the Postgres seam;
tests of the passes use it where cross-process coordination is not the point
(the fake-in-src convention of epigone.gateway.fake).
"""

from typing import Protocol

import asyncpg

from epigone.clock import Clock

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

# Never pace-sleep shorter than this. Clock arithmetic quantizes to whole
# microseconds, so a deficit-exact sleep can refill fractionally short and
# leave a dust shortfall whose next sleep rounds to zero — a busy loop. The
# floor guarantees every blocked retry makes real progress.
PACING_SLEEP_FLOOR_SECONDS = 0.05


class Budget(Protocol):
    """The pacing seam the passes spend against."""

    async def spend(self, weight: int) -> None: ...


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

    def _refill(self) -> None:
        now = self._clock.now()
        elapsed = (now - self._last_refill).total_seconds()
        self._last_refill = now
        self._available = min(self._capacity, self._available + elapsed * self._rate_per_second)


class SharedWeightBudget:
    """The cross-process token bucket: one `rate_budget` row all spenders share.

    Each spend refills and draws the row in one short transaction (the row lock
    serializes concurrent spenders); a spend that does not fit sleeps for the
    exact deficit's refill time and tries again. `reserve` is the floor this
    spender must leave in the bucket — 0 for the stream, STREAM_RESERVE_WEIGHT
    for ingest — which is what gives the stream its priority.
    """

    def __init__(self, pool: asyncpg.Pool, clock: Clock, *, reserve: int = 0) -> None:
        self._pool = pool
        self._clock = clock
        self._reserve = float(reserve)
        self._capacity = float(BURST_WEIGHT)
        self._rate_per_second = SHARED_WEIGHT_PER_MINUTE / 60.0

    async def spend(self, weight: int) -> None:
        """Block (via the injected clock) until `weight` fits above this spender's
        reserve floor, then take it."""
        if weight + self._reserve > self._capacity:
            raise ValueError(
                f"weight {weight} plus reserve {self._reserve} "
                f"exceeds bucket capacity {self._capacity}"
            )
        while True:
            shortfall = await self._try_spend(weight)
            if shortfall <= 0:
                return
            await self._clock.sleep(
                max(shortfall / self._rate_per_second, PACING_SLEEP_FLOOR_SECONDS)
            )

    async def _try_spend(self, weight: int) -> float:
        """Refill the shared row and take `weight` if it fits; returns the weight
        still missing (0 when granted). The refill is persisted either way."""
        now = self._clock.now()
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """
                INSERT INTO rate_budget (id, available, last_refill)
                VALUES (TRUE, $1, $2)
                ON CONFLICT (id) DO NOTHING
                """,
                self._capacity,
                now,
            )
            row = await conn.fetchrow("SELECT available, last_refill FROM rate_budget FOR UPDATE")
            assert row is not None
            # Guard against clock skew between processes: never refill backwards.
            elapsed = max(0.0, (now - row["last_refill"]).total_seconds())
            available = min(self._capacity, row["available"] + elapsed * self._rate_per_second)
            granted = available >= weight + self._reserve
            await conn.execute(
                "UPDATE rate_budget SET available = $1, last_refill = $2",
                available - weight if granted else available,
                max(now, row["last_refill"]),
            )
            return 0.0 if granted else weight + self._reserve - available
