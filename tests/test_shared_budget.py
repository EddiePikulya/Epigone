"""The shared weight budget: ingest + stream draw from one Postgres bucket (issue #28).

The two processes previously ran uncoordinated per-process buckets summing to
exactly Hyperliquid's 1200/min per-IP cap; under real load both tripped 429s.
The shared bucket paces their combined spend to a rate with real headroom, and
a reserve floor gives the stream (user-facing alerts) priority over ingest
(background backfill).
"""

import asyncpg
import pytest

from epigone.budget import (
    BURST_WEIGHT,
    PER_IP_WEIGHT_PER_MINUTE,
    SHARED_WEIGHT_PER_MINUTE,
    STREAM_RESERVE_WEIGHT,
    SharedWeightBudget,
)
from tests.support.clock import FakeClock


async def test_spending_within_the_burst_does_not_wait(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    budget = SharedWeightBudget(pool, clock)
    for _ in range(BURST_WEIGHT // 20):
        await budget.spend(20)
    assert clock.slept == []


async def test_both_processes_drain_the_same_bucket(pool: asyncpg.Pool, clock: FakeClock) -> None:
    stream = SharedWeightBudget(pool, clock)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    for _ in range(BURST_WEIGHT // 2):
        await stream.spend(2)
    assert clock.slept == []
    # The stream spent the whole burst, so ingest has nothing left to draw.
    await ingest.spend(20)
    assert clock.slept != []


async def test_sustained_combined_spend_is_paced_to_the_shared_rate(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    stream = SharedWeightBudget(pool, clock)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    start = clock.now()
    for _ in range(50):  # 50 x 24 = 1200 weight = the 240 burst + 960 refilled
        await ingest.spend(20)
        await stream.spend(2)
        await stream.spend(2)
    elapsed = (clock.now() - start).total_seconds()
    assert elapsed >= (1200 - BURST_WEIGHT) / (SHARED_WEIGHT_PER_MINUTE / 60)


async def test_worst_case_rolling_minute_stays_under_the_per_ip_cap(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # The worst rolling minute starts from a full bucket: the whole burst plus
    # a minute of refill must still clear the per-IP cap with margin.
    budget = SharedWeightBudget(pool, clock)
    start = clock.now()
    granted = 0
    while True:
        await budget.spend(2)
        if (clock.now() - start).total_seconds() > 60:
            break
        granted += 2
    assert granted <= BURST_WEIGHT + SHARED_WEIGHT_PER_MINUTE
    assert BURST_WEIGHT + SHARED_WEIGHT_PER_MINUTE < PER_IP_WEIGHT_PER_MINUTE


async def test_ingest_cannot_draw_down_the_stream_reserve(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    stream = SharedWeightBudget(pool, clock)
    # Ingest drains freely down to the reserve floor...
    for _ in range((BURST_WEIGHT - STREAM_RESERVE_WEIGHT) // 20):
        await ingest.spend(20)
    assert clock.slept == []
    # ...the floor belongs to the stream: its polls proceed without waiting...
    await stream.spend(20)
    assert clock.slept == []
    # ...while ingest waits for refill to rise back above the floor.
    await ingest.spend(20)
    assert clock.slept != []


async def test_an_idle_bucket_refills_only_to_the_burst_capacity(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    budget = SharedWeightBudget(pool, clock)
    for _ in range(BURST_WEIGHT // 20):
        await budget.spend(20)
    clock.advance(3600)  # an idle hour must not bank an hour of weight
    for _ in range(BURST_WEIGHT // 20):
        await budget.spend(20)
    assert clock.slept == []
    await budget.spend(20)
    assert clock.slept != []


async def test_the_budget_survives_a_process_restart(pool: asyncpg.Pool, clock: FakeClock) -> None:
    before = SharedWeightBudget(pool, clock)
    for _ in range(BURST_WEIGHT // 20):
        await before.spend(20)
    restarted = SharedWeightBudget(pool, clock)  # same table, fresh process
    await restarted.spend(20)
    assert clock.slept != []


async def test_a_spend_that_could_never_be_granted_is_rejected(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    stream = SharedWeightBudget(pool, clock)
    with pytest.raises(ValueError):
        await stream.spend(BURST_WEIGHT + 1)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    with pytest.raises(ValueError):
        await ingest.spend(BURST_WEIGHT - STREAM_RESERVE_WEIGHT + 1)
