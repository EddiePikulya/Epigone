"""Universe seeding: leaderboard entries become Trader rows, idempotently."""

import logging
from decimal import Decimal

import asyncpg
import pytest

from epigone.gateway import GatewayError, LeaderboardEntry, LeaderboardWindow, Window
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.scan import seed_universe
from tests.support.clock import FakeClock


def entry(
    address: str,
    name: str | None = None,
    account_value: str = "1000",
    week_volume: str = "5000",
) -> LeaderboardEntry:
    return LeaderboardEntry(
        address=address,
        display_name=name,
        account_value=Decimal(account_value),
        windows={
            Window.WEEK: LeaderboardWindow(
                pnl=Decimal("100"), roi=Decimal("0.1"), volume=Decimal(week_volume)
            )
        },
    )


async def test_seed_creates_a_trader_per_leaderboard_entry(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    gateway.set_leaderboard([entry("0xaaa", "alice"), entry("0xbbb")])
    clock = FakeClock()

    assert await seed_universe(pool, gateway, clock) == 2

    rows = await pool.fetch("SELECT * FROM traders ORDER BY address")
    seeded = [(r["address"], r["display_name"]) for r in rows]
    assert seeded == [("0xaaa", "alice"), ("0xbbb", None)]
    assert all(r["first_seen_at"] == clock.now() for r in rows)
    assert all(r["last_seen_at"] == clock.now() for r in rows)
    # refresh_tier is set from the leaderboard's week volume in the same pass.
    assert all(r["refresh_tier"] == "active" for r in rows)


async def test_addresses_are_stored_lowercased(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    gateway.set_leaderboard([entry("0xAbCd")])

    await seed_universe(pool, gateway, FakeClock())

    assert await pool.fetchval("SELECT address FROM traders") == "0xabcd"


async def test_reseeding_upserts_instead_of_duplicating(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    gateway.set_leaderboard([entry("0xaaa", "alice")])
    clock = FakeClock()
    await seed_universe(pool, gateway, clock)
    first_seen = clock.now()

    clock.advance(3600)
    gateway.set_leaderboard([entry("0xaaa", "alice-renamed"), entry("0xbbb")])
    await seed_universe(pool, gateway, clock)

    rows = await pool.fetch("SELECT * FROM traders ORDER BY address")
    assert len(rows) == 2
    assert rows[0]["display_name"] == "alice-renamed"
    assert rows[0]["first_seen_at"] == first_seen  # original discovery time survives
    assert rows[0]["last_seen_at"] == clock.now()


async def test_leaderboard_failure_leaves_universe_intact_and_logs(
    pool: asyncpg.Pool, caplog: pytest.LogCaptureFixture
) -> None:
    gateway = FakeHyperliquidGateway()
    gateway.set_leaderboard([entry("0xaaa", "alice")])
    await seed_universe(pool, gateway, FakeClock())

    gateway.leaderboard_error = GatewayError("stats-data is down")
    with caplog.at_level(logging.ERROR):
        assert await seed_universe(pool, gateway, FakeClock()) is None

    assert "leaderboard" in caplog.text
    rows = await pool.fetch("SELECT * FROM traders")
    assert [(r["address"], r["display_name"]) for r in rows] == [("0xaaa", "alice")]
