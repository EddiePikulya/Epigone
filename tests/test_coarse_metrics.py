"""Coarse metrics land straight from the leaderboard download (issue #26):
seeding populates coarse_metrics for the whole Universe with zero per-account
API calls, and re-seeding is the only "refresh"."""

from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.gateway import LeaderboardEntry, LeaderboardWindow, Window
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.fine import run_fine_pass
from epigone.ingest.scan import seed_universe
from tests.support.clock import FakeClock
from tests.support.fills import T0, fill

WIDE_OPEN_BUDGET = 1_000_000


def win(pnl: str = "100", roi: str = "0.1", volume: str = "5000") -> LeaderboardWindow:
    return LeaderboardWindow(pnl=Decimal(pnl), roi=Decimal(roi), volume=Decimal(volume))


def all_windows(week_volume: str = "5000") -> dict[Window, LeaderboardWindow]:
    return {
        Window.DAY: win(pnl="10", roi="0.01", volume="900"),
        Window.WEEK: win(pnl="100", roi="0.1", volume=week_volume),
        Window.MONTH: win(pnl="400", roi="0.4", volume="20000"),
        Window.ALL_TIME: win(pnl="2000", roi="2.0", volume="90000"),
    }


def entry(
    address: str,
    account_value: str = "1100",
    windows: dict[Window, LeaderboardWindow] | None = None,
) -> LeaderboardEntry:
    return LeaderboardEntry(
        address=address,
        display_name=None,
        account_value=Decimal(account_value),
        windows=all_windows() if windows is None else windows,
    )


async def seed(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    entries: list[LeaderboardEntry],
) -> None:
    gateway.set_leaderboard(entries)
    await seed_universe(pool, gateway, clock)


async def test_seeding_populates_all_coarse_windows_with_computed_at(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed(pool, gateway, clock, [entry("0xaaa", account_value="1100")])

    rows = await pool.fetch("SELECT * FROM coarse_metrics WHERE address = '0xaaa'")
    by_window = {r["time_window"]: r for r in rows}
    assert set(by_window) == {"day", "week", "month", "allTime"}
    week = by_window["week"]
    assert week["pnl"] == Decimal("100")
    assert week["volume"] == Decimal("5000")
    assert week["account_value"] == Decimal("1100")  # the row's account-wide value
    assert all(r["computed_at"] == clock.now() for r in rows)


async def test_roi_is_stored_verbatim_from_the_leaderboard(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    # A net-deposit-adjusted roi no pnl-over-stack proxy would reproduce.
    windows = {Window.MONTH: win(pnl="100", roi="-0.25", volume="5000")}
    await seed(pool, gateway, clock, [entry("0xaaa", account_value="1100", windows=windows)])

    roi = await pool.fetchval("SELECT roi FROM coarse_metrics WHERE time_window = 'month'")
    assert roi == Decimal("-0.25")


async def test_refresh_tier_comes_from_the_leaderboard_week_volume(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed(
        pool,
        gateway,
        clock,
        [
            entry("0xactive", windows=all_windows(week_volume="5000")),
            entry("0xdormant", windows=all_windows(week_volume="0")),
            entry("0xnowindows", windows={}),  # no week window at all -> dormant
        ],
    )

    rows = await pool.fetch("SELECT address, refresh_tier FROM traders ORDER BY address")
    assert {r["address"]: r["refresh_tier"] for r in rows} == {
        "0xactive": "active",
        "0xdormant": "dormant",
        "0xnowindows": "dormant",
    }


async def test_seeding_makes_no_per_account_calls(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed(pool, gateway, clock, [entry("0xaaa"), entry("0xbbb")])

    # The whole point of issue #26: coarse is populated with zero per-account I/O.
    assert gateway.fills_calls == []
    assert gateway.positions_calls == []
    populated = await pool.fetchval("SELECT count(*) FROM coarse_metrics")
    assert populated == 8  # two Traders x four windows


async def test_reseeding_upserts_coarse_metrics_without_duplicating(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await seed(pool, gateway, clock, [entry("0xaaa")])

    clock.advance(3600)
    updated = {Window.MONTH: win(pnl="999", roi="1.5", volume="30000")}
    await seed(pool, gateway, clock, [entry("0xaaa", windows=updated)])

    month = await pool.fetchrow(
        "SELECT * FROM coarse_metrics WHERE address = '0xaaa' AND time_window = 'month'"
    )
    assert month is not None
    assert month["pnl"] == Decimal("999")
    assert month["roi"] == Decimal("1.5")
    assert month["computed_at"] == clock.now()
    # No duplicate rows: the month window is still a single row (PK upsert).
    count = "SELECT count(*) FROM coarse_metrics WHERE address = '0xaaa' AND time_window = 'month'"
    assert await pool.fetchval(count) == 1


async def test_only_present_windows_get_coarse_rows(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    partial = {Window.DAY: win(), Window.ALL_TIME: win()}
    entries = [entry("0xaaa", windows=partial), entry("0xfresh", windows={})]
    await seed(pool, gateway, clock, entries)

    windows = await pool.fetch("SELECT time_window FROM coarse_metrics WHERE address = '0xaaa'")
    assert {r["time_window"] for r in windows} == {"day", "allTime"}
    # A row with no window performances is still a seeded Trader, just no metrics.
    assert await pool.fetchval("SELECT count(*) FROM coarse_metrics WHERE address = '0xfresh'") == 0
    assert await pool.fetchval("SELECT count(*) FROM traders WHERE address = '0xfresh'") == 1


async def test_seeded_coarse_feeds_the_fine_pass_eligibility(pool: asyncpg.Pool) -> None:
    """The preserved contract: fine-pass eligibility reads the leaderboard-sourced
    coarse rows unchanged — profitable-with-volume month survives, a loser doesn't."""
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    loser = {
        Window.WEEK: win(volume="5000"),
        Window.MONTH: win(pnl="-50", roi="-0.1", volume="1000"),
    }
    await seed(pool, gateway, clock, [entry("0xsurvivor"), entry("0xloser", windows=loser)])
    gateway.set_fills("0xsurvivor", [fill(pnl="100", order_id=1, at=T0)])

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    # Only the profitable-with-volume month reaches the fine pass.
    assert gateway.fills_calls == ["0xsurvivor"]
