"""The safety lane's degrading rate budget (issue #135, PR #143 round 2):
shared Postgres bucket while the database answers, in-process pacing when it
doesn't — a dead rate_budget row must never queue a protective cancel."""

import asyncpg

from epigone.budget import SharedWeightBudget
from epigone.safety.budget import FallbackBudget
from tests.support.clock import FakeClock


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
