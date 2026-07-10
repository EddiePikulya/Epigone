"""The weight budgeter paces API spend to the ingest share of the 1200/min limit."""

import pytest

from epigone.budget import WeightBudget
from tests.support.clock import FakeClock


async def test_spending_within_the_per_minute_allowance_does_not_wait() -> None:
    clock = FakeClock()
    budget = WeightBudget(weight_per_minute=400, clock=clock)
    for _ in range(20):  # 20 portfolio calls x weight 20 = exactly the allowance
        await budget.spend(20)
    assert clock.slept == []


async def test_spending_beyond_the_allowance_waits_for_refill() -> None:
    clock = FakeClock()
    budget = WeightBudget(weight_per_minute=400, clock=clock)
    for _ in range(21):
        await budget.spend(20)
    # The 21st call needs 20 weight refilled at 400/min: at least 3 seconds pass.
    assert sum(clock.slept) >= 3.0


async def test_sustained_spend_is_paced_to_the_configured_rate() -> None:
    clock = FakeClock()
    budget = WeightBudget(weight_per_minute=400, clock=clock)
    start = clock.now()
    for _ in range(60):  # 1200 weight = the initial 400 burst + 800 refilled
        await budget.spend(20)
    elapsed = (clock.now() - start).total_seconds()
    assert elapsed >= 800 / (400 / 60)  # refill of 800 weight takes >= 2 minutes


async def test_waiting_is_credited_by_elapsed_time() -> None:
    clock = FakeClock()
    budget = WeightBudget(weight_per_minute=400, clock=clock)
    for _ in range(20):
        await budget.spend(20)
    clock.advance(60)  # a full minute idle refills the full allowance
    await budget.spend(20)
    assert clock.slept == []


async def test_a_single_spend_larger_than_the_allowance_is_rejected() -> None:
    budget = WeightBudget(weight_per_minute=400, clock=FakeClock())
    with pytest.raises(ValueError):
        await budget.spend(401)
