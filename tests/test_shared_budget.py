"""The shared weight budget: ingest + stream draw from one Postgres bucket (issue #28),
smoothed against sub-minute spikes and reconciled to real response weights (issue #41).

The two processes previously ran uncoordinated per-process buckets summing to
exactly Hyperliquid's 1200/min per-IP cap; under real load both tripped 429s.
The shared bucket paces their combined spend to a rate with real headroom, and
a reserve floor gives the stream (user-facing alerts) priority over ingest
(background backfill).

Issue #41 tightened two things. A send gate spaces grants proportionally to
their weight (SMOOTHING_WEIGHT_PER_SECOND), so even a full bucket cannot be
emptied in a sub-second spike. And settle() bills weight that only the response
revealed (userFills adds weight per 20 fills returned), driving the bucket into
debt that later spends pace off — the budget tracks real consumption, not the
nominal pre-call estimate.
"""

import logging
from datetime import datetime

import asyncpg
import pytest

from epigone.budget import (
    BURST_WEIGHT,
    PER_IP_WEIGHT_PER_MINUTE,
    SHARED_WEIGHT_PER_MINUTE,
    SMOOTHING_WEIGHT_PER_SECOND,
    STREAM_RESERVE_WEIGHT,
    SharedWeightBudget,
)
from tests.support.clock import FakeClock

REFILL_PER_SECOND = SHARED_WEIGHT_PER_MINUTE / 60


async def _seed_bucket(pool: asyncpg.Pool, now: datetime, *, available: float) -> None:
    """Pin the shared row to a known level with the send gate open, so a test
    can stage a bucket state (e.g. drained to the reserve floor) without the
    refill that real spends' pacing sleeps would accrue."""
    await pool.execute(
        """
        INSERT INTO rate_budget
            (id, available, last_refill, next_send_at, minute_started_at, minute_spent)
        VALUES (TRUE, $1, $2, $2, $2, 0)
        ON CONFLICT (id) DO UPDATE
            SET available = $1, last_refill = $2, next_send_at = $2,
                minute_started_at = $2, minute_spent = 0
        """,
        float(available),
        now,
    )


async def test_a_burst_sized_spend_is_granted_instantly(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    budget = SharedWeightBudget(pool, clock)
    await budget.spend(BURST_WEIGHT)
    assert clock.slept == []


async def test_a_full_bucket_cannot_be_emptied_in_a_sub_minute_spike(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # The send gate is the issue-#41 fix: no window — not just no rolling
    # minute — may ever exceed the cap's uniform per-second rate.
    budget = SharedWeightBudget(pool, clock)
    start = clock.now()
    granted = 0
    while (clock.now() - start).total_seconds() < 10.0:
        await budget.spend(20)
        granted += 20
    assert granted <= 10 * SMOOTHING_WEIGHT_PER_SECOND + 20  # + one in-flight grant


async def test_the_send_gate_scales_with_request_weight(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # Light stream polls (weight 2) get proportionally short windows: smoothing
    # must not slow the stream down to heavyweight-call cadence.
    budget = SharedWeightBudget(pool, clock)
    start = clock.now()
    for _ in range(6):
        await budget.spend(2)
    elapsed = (clock.now() - start).total_seconds()
    assert 5 * 2 / SMOOTHING_WEIGHT_PER_SECOND <= elapsed <= 5 * 3 / SMOOTHING_WEIGHT_PER_SECOND


async def test_the_send_gate_is_a_window_not_a_debt(pool: asyncpg.Pool, clock: FakeClock) -> None:
    budget = SharedWeightBudget(pool, clock)
    await budget.spend(200)  # gates the next send 10s out
    clock.advance(60)  # ...but an idle minute clears it entirely
    await budget.spend(20)
    assert clock.slept == []


async def test_settle_bills_weight_the_response_revealed(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # A userFills response of 2000 fills really cost ~120, not the nominal 20
    # (weight per 20 items returned): the surcharge must hold the gate shut.
    budget = SharedWeightBudget(pool, clock)
    await budget.spend(20)
    await budget.settle(100)
    before = sum(clock.slept)
    await budget.spend(20)
    assert sum(clock.slept) - before >= 100 / SMOOTHING_WEIGHT_PER_SECOND


async def test_settle_can_drive_the_bucket_into_debt(pool: asyncpg.Pool, clock: FakeClock) -> None:
    budget = SharedWeightBudget(pool, clock)
    # Larger than the whole bucket — and, unlike spend, never rejected: the
    # weight is already consumed truth, not a request.
    await budget.settle(BURST_WEIGHT + 60)
    assert await pool.fetchval("SELECT available FROM rate_budget") == -60
    assert await pool.fetchval("SELECT minute_spent FROM rate_budget") == BURST_WEIGHT + 60
    # The next spend pays the debt off before drawing again.
    await budget.spend(2)
    assert sum(clock.slept) >= 60 / REFILL_PER_SECOND


async def test_the_reserve_floor_belongs_to_the_stream(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await _seed_bucket(pool, clock.now(), available=STREAM_RESERVE_WEIGHT)
    stream = SharedWeightBudget(pool, clock)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    # At the floor the stream's claim is instant...
    await stream.spend(20)
    assert clock.slept == []
    # ...while ingest waits for refill to rise back above the floor.
    await ingest.spend(20)
    assert sum(clock.slept) >= 20 / REFILL_PER_SECOND


async def test_the_stream_waits_at_most_one_send_window_during_an_ingest_wave(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # The no-starvation bound (issue #41 acceptance): smoothing spaces the
    # stream's polls but never queues them behind refill-scale waits — the
    # worst case is the tail of one heavyweight ingest window.
    await _seed_bucket(pool, clock.now(), available=BURST_WEIGHT)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    stream = SharedWeightBudget(pool, clock)
    for _ in range(5):
        await ingest.spend(20)
        before = sum(clock.slept)
        await stream.spend(2)
        assert sum(clock.slept) - before <= 20 / SMOOTHING_WEIGHT_PER_SECOND + 1e-6


async def test_the_streams_worst_wait_includes_a_settled_surcharge(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # A settled surcharge advances the gate too (real weight is what must be
    # smoothed), so the stream's true worst case is one ingest call plus a
    # full fills response's surcharge — ~6s, still well inside the 30s poll.
    await _seed_bucket(pool, clock.now(), available=BURST_WEIGHT)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    stream = SharedWeightBudget(pool, clock)
    await ingest.spend(20)
    await ingest.settle(100)  # the response carried ~2000 fills
    before = sum(clock.slept)
    await stream.spend(2)
    assert sum(clock.slept) - before <= (20 + 100) / SMOOTHING_WEIGHT_PER_SECOND + 1e-6


async def test_both_processes_drain_the_same_bucket(pool: asyncpg.Pool, clock: FakeClock) -> None:
    stream = SharedWeightBudget(pool, clock)
    ingest = SharedWeightBudget(pool, clock, reserve=STREAM_RESERVE_WEIGHT)
    await stream.spend(BURST_WEIGHT)  # the stream takes the whole burst
    assert clock.slept == []
    await ingest.spend(20)  # so ingest finds nothing left to draw
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
    assert elapsed >= (1200 - BURST_WEIGHT) / REFILL_PER_SECOND


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


async def test_an_idle_bucket_refills_only_to_the_burst_capacity(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    budget = SharedWeightBudget(pool, clock)
    await budget.spend(BURST_WEIGHT)
    clock.advance(3600)  # an idle hour banks one burst, never an hour of weight
    await budget.spend(BURST_WEIGHT)
    assert clock.slept == []
    await budget.spend(BURST_WEIGHT)
    # Refill-bound (16s at 15/s), not merely gate-bound (12s at 20/s): the
    # stronger wait proves the idle hour banked nothing beyond the burst.
    assert sum(clock.slept) >= BURST_WEIGHT / REFILL_PER_SECOND - 0.1


async def test_the_budget_survives_a_process_restart(pool: asyncpg.Pool, clock: FakeClock) -> None:
    before = SharedWeightBudget(pool, clock)
    await before.spend(BURST_WEIGHT)
    restarted = SharedWeightBudget(pool, clock)  # same table, fresh process
    await restarted.spend(BURST_WEIGHT)
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


async def test_actual_consumed_weight_is_logged_each_minute(
    pool: asyncpg.Pool, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    # The observability half of issue #41: whichever spender crosses a minute
    # boundary logs the finished minute's real consumption against the limits.
    budget = SharedWeightBudget(pool, clock)
    with caplog.at_level(logging.INFO, logger="epigone.budget"):
        for _ in range(70):  # paced past the minute boundary
            await budget.spend(20)
    lines = [r.getMessage() for r in caplog.records if "rate budget" in r.getMessage()]
    assert lines, "expected a per-minute consumption log line"
    assert any(str(PER_IP_WEIGHT_PER_MINUTE) in line for line in lines)


async def test_a_stale_minute_window_resets_silently(
    pool: asyncpg.Pool, clock: FakeClock, caplog: pytest.LogCaptureFixture
) -> None:
    # An idle process waking hours later must not log a bogus "minute".
    budget = SharedWeightBudget(pool, clock)
    await budget.spend(20)
    clock.advance(3600)
    with caplog.at_level(logging.INFO, logger="epigone.budget"):
        await budget.spend(20)
    assert not [r for r in caplog.records if "rate budget" in r.getMessage()]
