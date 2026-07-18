"""Ingest loop cadence (issue #50): the coarse Universe re-seed runs on the
configured interval, gated by elapsed time on the injected clock, while the fine
pass runs every cycle regardless of whether a seed happened. Exercised at the
loop seam with the existing fakes (fake gateway + injected clock)."""

from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import (
    GatewayError,
    LeaderboardEntry,
    LeaderboardWindow,
    RateLimitedError,
    Window,
)
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.main import CYCLE_PAUSE_SECONDS, ingest_loop
from epigone.ingest.scan import seed_universe
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000


def entry(address: str) -> LeaderboardEntry:
    return LeaderboardEntry(
        address=address,
        display_name=None,
        account_value=Decimal("1000"),
        windows={
            Window.WEEK: LeaderboardWindow(
                pnl=Decimal("100"), roi=Decimal("0.1"), volume=Decimal("5000")
            )
        },
    )


async def _seed_count_after(pool: asyncpg.Pool, interval: timedelta, max_cycles: int) -> int:
    """Run the loop for a fixed number of cycles and report how many times it
    downloaded the leaderboard (i.e. re-seeded)."""
    gateway = FakeHyperliquidGateway()
    gateway.set_leaderboard([entry("0xaaa")])
    clock = FakeClock()
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await ingest_loop(pool, gateway, budget, clock, interval, max_cycles=max_cycles)
    return gateway.leaderboard_calls


@pytest.mark.parametrize("interval_cycles", [2, 5])
async def test_reseeds_only_once_the_configured_interval_elapses(
    pool: asyncpg.Pool, interval_cycles: int
) -> None:
    # Each cycle sleeps CYCLE_PAUSE_SECONDS, so an interval of N cycles' worth of
    # sleep re-seeds again only on the Nth cycle: advance just under → one seed,
    # advance one cycle past → two. A non-default interval is honoured the same
    # way (2- vs 5-cycle interval), so the cadence tracks the configured value.
    interval = timedelta(seconds=CYCLE_PAUSE_SECONDS * interval_cycles)
    assert await _seed_count_after(pool, interval, max_cycles=interval_cycles) == 1
    assert await _seed_count_after(pool, interval, max_cycles=interval_cycles + 1) == 2


async def test_seeds_on_the_first_cycle(pool: asyncpg.Pool) -> None:
    # Cold start (no prior seed) must seed immediately, not wait an interval.
    assert await _seed_count_after(pool, timedelta(minutes=60), max_cycles=1) == 1


async def test_failed_reseed_leaves_universe_intact(pool: asyncpg.Pool) -> None:
    # A failed leaderboard download must never wipe the Universe (unchanged
    # from #26): the existing Traders survive and the loop retries next cycle.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    gateway.set_leaderboard([entry("0xaaa")])
    await seed_universe(pool, gateway, clock)

    gateway.leaderboard_error = GatewayError("stats-data is down")
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await ingest_loop(pool, gateway, budget, clock, timedelta(minutes=60), max_cycles=1)

    rows = await pool.fetch("SELECT address FROM traders")
    assert [r["address"] for r in rows] == ["0xaaa"]


async def test_fine_pass_runs_every_cycle_regardless_of_reseed(pool: asyncpg.Pool) -> None:
    # The fine pass runs each cycle even when no seed happened this cycle. A
    # tracked, eligible Trader that keeps getting rate-limited stays due, so a
    # fills fetch fires every cycle while the leaderboard is downloaded only once.
    gateway = FakeHyperliquidGateway()  # empty leaderboard: seeding is a no-op upsert
    clock = FakeClock()
    await pool.execute(
        "INSERT INTO traders (address, refresh_tier, first_seen_at, last_seen_at) "
        "VALUES ('0xaaa', 'active', $1, $1)",
        clock.now(),
    )
    await pool.execute(
        "INSERT INTO coarse_metrics (address, time_window, pnl, roi, volume, account_value, "
        "computed_at) VALUES ('0xaaa', 'month', 5000, 0, 100000, 1000, $1)",
        clock.now(),
    )
    gateway.fills_errors["0xaaa"] = RateLimitedError("paced")

    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await ingest_loop(pool, gateway, budget, clock, timedelta(days=1), max_cycles=3)

    assert gateway.leaderboard_calls == 1  # seeded once (interval far exceeds 3 cycles)
    assert gateway.fills_calls == ["0xaaa"] * 3  # fine pass ran on every cycle
