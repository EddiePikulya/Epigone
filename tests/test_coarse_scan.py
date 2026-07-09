"""The coarse metric pass: portfolio stats per Trader, budget-aware and resumable."""

from decimal import Decimal

import asyncpg

from epigone.gateway import GatewayError, LeaderboardEntry, PortfolioWindow, Window
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.budget import WeightBudget
from epigone.ingest.scan import run_coarse_pass, seed_universe
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000


def window(
    pnl: str = "100", volume: str = "5000", value: str = "1100", start: str = "1000"
) -> PortfolioWindow:
    return PortfolioWindow(
        pnl=Decimal(pnl),
        volume=Decimal(volume),
        account_value=Decimal(value),
        starting_account_value=Decimal(start),
    )


def full_portfolio(week_volume: str = "5000") -> dict[Window, PortfolioWindow]:
    return {
        Window.DAY: window(pnl="10", volume="900"),
        Window.WEEK: window(pnl="100", volume=week_volume),
        Window.MONTH: window(pnl="400", volume="20000"),
        Window.ALL_TIME: window(pnl="2000", volume="90000"),
    }


async def seed_traders(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock, addresses: list[str]
) -> None:
    gateway.set_leaderboard(
        [LeaderboardEntry(address=a, display_name=None) for a in addresses]
    )
    await seed_universe(pool, gateway, clock)


async def test_coarse_pass_stores_all_windows_with_computed_at(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xaaa"])
    gateway.set_portfolio("0xaaa", full_portfolio())

    result = await run_coarse_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.refreshed == 1 and result.failed == 0
    rows = await pool.fetch("SELECT * FROM coarse_metrics WHERE address = '0xaaa'")
    by_window = {r["time_window"]: r for r in rows}
    assert set(by_window) == {"day", "week", "month", "allTime"}
    week = by_window["week"]
    assert week["pnl"] == Decimal("100")
    assert week["volume"] == Decimal("5000")
    assert week["account_value"] == Decimal("1100")
    assert week["roi"] == Decimal("0.1")  # pnl 100 on a 1000 starting stack
    assert all(r["computed_at"] == clock.now() for r in rows)


async def test_roi_is_zero_when_the_window_started_from_nothing(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xaaa"])
    gateway.set_portfolio("0xaaa", {Window.DAY: window(pnl="50", start="0")})

    await run_coarse_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    roi = await pool.fetchval("SELECT roi FROM coarse_metrics WHERE time_window = 'day'")
    assert roi == Decimal("0")


async def test_bookkeeping_tiers_traders_by_recent_activity(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xactive", "0xdormant"])
    gateway.set_portfolio("0xactive", full_portfolio(week_volume="5000"))
    gateway.set_portfolio("0xdormant", full_portfolio(week_volume="0"))

    await run_coarse_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    rows = await pool.fetch("SELECT * FROM traders ORDER BY address")
    tiers = {r["address"]: r["refresh_tier"] for r in rows}
    assert tiers == {"0xactive": "active", "0xdormant": "dormant"}
    assert all(r["coarse_refreshed_at"] == clock.now() for r in rows)


async def test_fresh_traders_are_not_rescanned(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xaaa"])
    gateway.set_portfolio("0xaaa", full_portfolio())
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_coarse_pass(pool, gateway, budget, clock)

    clock.advance(3600)  # an hour later: still fresh for every tier
    result = await run_coarse_pass(pool, gateway, budget, clock)

    assert result.refreshed == 0
    assert gateway.portfolio_calls == ["0xaaa"]


async def test_stale_traders_are_rescanned_on_their_tier_cadence(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xactive", "0xdormant"])
    gateway.set_portfolio("0xactive", full_portfolio(week_volume="5000"))
    gateway.set_portfolio("0xdormant", full_portfolio(week_volume="0"))
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_coarse_pass(pool, gateway, budget, clock)

    clock.advance(2 * 24 * 3600)  # two days: past the active cadence, inside the dormant one
    result = await run_coarse_pass(pool, gateway, budget, clock)
    assert result.refreshed == 1
    assert gateway.portfolio_calls.count("0xactive") == 2
    assert gateway.portfolio_calls.count("0xdormant") == 1

    clock.advance(6 * 24 * 3600)  # eight days since the dormant Trader's last refresh
    await run_coarse_pass(pool, gateway, budget, clock)
    assert gateway.portfolio_calls.count("0xdormant") == 2


async def test_scan_is_paced_by_the_weight_budget(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    addresses = [f"0x{i:03d}" for i in range(30)]
    await seed_traders(pool, gateway, clock, addresses)
    for a in addresses:
        gateway.set_portfolio(a, full_portfolio())

    start = clock.now()
    # Ingest share is 400 weight/min: 30 portfolio calls x 20 = 600 weight,
    # so the pass must wait for at least 200 weight = 30s of refill.
    await run_coarse_pass(pool, gateway, WeightBudget(400, clock), clock)

    assert (clock.now() - start).total_seconds() >= 30
    refreshed = "SELECT count(*) FROM traders WHERE coarse_refreshed_at IS NOT NULL"
    assert await pool.fetchval(refreshed) == 30


async def test_one_failing_trader_does_not_stop_the_pass(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed_traders(pool, gateway, clock, ["0xaaa", "0xbbb", "0xccc"])
    for a in ("0xaaa", "0xccc"):
        gateway.set_portfolio(a, full_portfolio())
    gateway.portfolio_errors["0xbbb"] = GatewayError("info API timeout")

    result = await run_coarse_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.refreshed == 2 and result.failed == 1 and not result.aborted
    unrefreshed = await pool.fetch("SELECT address FROM traders WHERE coarse_refreshed_at IS NULL")
    assert [r["address"] for r in unrefreshed] == ["0xbbb"]


async def test_interrupted_scan_resumes_where_it_left_off(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    healthy = ["0x001", "0x002"]
    outage = [f"0x{i:03d}" for i in range(3, 9)]  # scans after 0x002 in stale-first order
    await seed_traders(pool, gateway, clock, healthy + outage)
    for a in healthy + outage:
        gateway.set_portfolio(a, full_portfolio())
    for a in outage:
        gateway.portfolio_errors[a] = GatewayError("connection reset")
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)

    result = await run_coarse_pass(pool, gateway, budget, clock)
    assert result.aborted  # sustained failures stop the pass instead of burning budget
    assert result.refreshed == 2
    first_pass_time = clock.now()

    gateway.portfolio_errors.clear()
    clock.advance(60)
    resumed = await run_coarse_pass(pool, gateway, budget, clock)

    assert resumed.refreshed == len(outage)
    # The healthy traders were not re-fetched: their metrics kept the first pass's timestamp.
    healthy_times = await pool.fetch(
        "SELECT DISTINCT computed_at FROM coarse_metrics WHERE address = ANY($1::text[])", healthy
    )
    assert [r["computed_at"] for r in healthy_times] == [first_pass_time]
    assert gateway.portfolio_calls.count("0x001") == 1
    assert gateway.portfolio_calls.count("0x002") == 1


async def test_persistently_failing_traders_do_not_block_the_scan(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    poison = [f"0x{i:03d}" for i in range(5)]  # first in scan order, always failing
    healthy = ["0x900", "0x901"]
    await seed_traders(pool, gateway, clock, poison + healthy)
    for a in healthy:
        gateway.set_portfolio(a, full_portfolio())
    for a in poison:
        gateway.portfolio_errors[a] = GatewayError("malformed payload")
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)

    first = await run_coarse_pass(pool, gateway, budget, clock)
    assert first.aborted and first.refreshed == 0

    # The failed attempts were recorded, so next cycle the poison addresses
    # rotate to the back and the healthy Traders get their turn.
    clock.advance(60)
    second = await run_coarse_pass(pool, gateway, budget, clock)
    assert second.refreshed == 2
    query = "SELECT address FROM traders WHERE coarse_refreshed_at IS NOT NULL"
    assert sorted(r["address"] for r in await pool.fetch(query)) == healthy
