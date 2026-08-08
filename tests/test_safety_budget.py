"""The safety lane's degrading rate budget (issue #135, PR #143 round 2):
shared Postgres bucket while the database answers, in-process pacing when it
doesn't — a dead rate_budget row must never queue a protective cancel."""

import asyncpg

from epigone.budget import SharedWeightBudget
from epigone.safety.budget import FallbackBudget
from tests.support.clock import FakeClock
from tests.test_shared_budget import _seed_bucket


async def test_spend_survives_a_dead_shared_bucket(
    database_url: str, clock: FakeClock
) -> None:
    dead_pool = await asyncpg.create_pool(database_url)
    assert dead_pool is not None
    await dead_pool.close()
    budget = FallbackBudget(SharedWeightBudget(dead_pool, clock, reserve=0), clock)
    # The FOR UPDATE seam is dead; the spend must still be granted (paced by
    # the in-process fallback) — this is the seam the round-1 test missed.
    await budget.spend(20)
    await budget.settle(5)


async def test_healthy_shared_bucket_is_actually_used(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    budget = FallbackBudget(SharedWeightBudget(pool, clock, reserve=0), clock)
    await budget.spend(20)
    row = await pool.fetchrow("SELECT available FROM rate_budget")
    assert row is not None  # the shared row exists and took the spend…
    from epigone.budget import BURST_WEIGHT

    assert row["available"] <= BURST_WEIGHT - 20  # …for the full weight


class _RecordingDeadPrimary:
    """A shared-bucket stand-in that records every attempt and then fails —
    normal mode must ATTEMPT it (and degrade); incident mode must not touch
    it at all."""

    def __init__(self) -> None:
        self.calls = 0

    async def spend(self, weight: int) -> None:
        self.calls += 1
        raise ConnectionError("shared bucket unreachable")

    async def settle(self, weight: int) -> None:
        self.calls += 1
        raise ConnectionError("shared bucket unreachable")


async def test_incident_mode_never_touches_the_shared_bucket(clock: FakeClock) -> None:
    """Round 5: during a declared incident the shared Postgres bucket is not
    attempted AT ALL — Postgres-free by construction, not by exception
    handling. Normal mode still tries it first and degrades."""
    primary = _RecordingDeadPrimary()
    budget = FallbackBudget(primary, clock)

    budget.incident_mode = True
    await budget.spend(20)
    await budget.settle(5)
    assert primary.calls == 0  # zero Postgres on the incident path

    budget.incident_mode = False
    await budget.spend(20)  # attempted, failed, degraded — the normal shape
    assert primary.calls == 1


async def test_the_safety_lane_outranks_ingest_at_the_shared_bucket(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Issue #201 deliverable 3, pinned at the seam: the kill path spends at
    the EXECUTION lane's priority — reserve 0, the floor every other spender
    leaves untouched (issue #133) — so a sweep never queues behind ingest's
    backfill for tokens. With the bucket drained to exactly the execution
    floor the sweep's enumeration is granted instantly while ingest, which
    must leave both floors intact, waits out a refill."""
    from epigone.budget import (
        EXECUTION_RESERVE_WEIGHT,
        SHARED_WEIGHT_PER_MINUTE,
        STREAM_RESERVE_WEIGHT,
    )
    from epigone.safety.budget import safety_budget
    from epigone.safety.watchdog import ORDERS_WEIGHT

    await _seed_bucket(pool, clock.now(), available=EXECUTION_RESERVE_WEIGHT)
    safety = safety_budget(pool, clock)
    ingest = SharedWeightBudget(
        pool, clock, reserve=EXECUTION_RESERVE_WEIGHT + STREAM_RESERVE_WEIGHT
    )

    await safety.spend(ORDERS_WEIGHT)  # one sweep enumeration
    assert clock.slept == []  # granted without waiting on anyone

    await ingest.spend(20)
    assert sum(clock.slept) >= STREAM_RESERVE_WEIGHT / (SHARED_WEIGHT_PER_MINUTE / 60)
