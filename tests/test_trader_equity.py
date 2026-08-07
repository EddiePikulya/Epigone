"""Equity capture in the poll pass (issue #170).

Seam test per the house convention: fake HyperliquidGateway, fake clock, real
Postgres. What is under test is that every poll pass leaves each tracked
Trader's covered-venue equity in the database, freshly observed, at no extra
cost to the exchange — and that the observation it replaces stays readable for
the length of the pass, which is what the withdrawal follow-up (#171) computes
its delta from.
"""

from decimal import Decimal

import asyncpg
import pytest

from epigone import position_publish
from epigone.budget import WeightBudget
from epigone.clock import Clock
from epigone.gateway import POSITION_VENUES, GatewayError, Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import POLL_INTERVAL_SECONDS, POSITIONS_WEIGHT, run_poll_pass
from epigone.trader_equity import EquityObservation, record_equity
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000
WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"


def position(coin: str = "BTC", size_usd: str = "10000") -> Position:
    return Position(
        coin=coin,
        side=Side.LONG,
        size_usd=Decimal(size_usd),
        leverage=Decimal("5"),
        entry_price=Decimal("100"),
        unrealized_pnl=Decimal("0"),
    )


async def track(pool: asyncpg.Pool, clock: Clock, address: str, *user_ids: int) -> None:
    """A Trader in the Universe, tracked by each given User."""
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        clock.now(),
    )
    for user_id in user_ids:
        await pool.execute(
            "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )
        await pool.execute(
            "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)",
            user_id,
            address,
        )


async def equity(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM trader_equity ORDER BY trader_address")


async def poll(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)


async def test_a_poll_pass_records_the_traders_covered_venue_equity(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Both polled venues collateralise separately, so the Trader's equity on the
    # venues Epigone watches is the sum — recorded at the instant of the pass.
    await track(pool, clock, WHALE, 1)
    gateway.set_positions(WHALE, [position()])
    gateway.set_account_value(WHALE, Decimal("400000"))
    gateway.set_account_value(WHALE, Decimal("25000"), dex="xyz")

    await poll(pool, gateway, clock)

    (row,) = await equity(pool)
    assert row["trader_address"] == WHALE
    assert row["account_value"] == Decimal("425000")
    assert row["observed_at"] == clock.now()


async def test_consecutive_passes_refresh_the_stored_equity(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The figure the operator sees is the last poll's, not the first one's: one
    # row per Trader, overwritten every pass, on the poll cadence.
    await track(pool, clock, WHALE, 1)
    gateway.set_positions(WHALE, [position()])
    gateway.set_account_value(WHALE, Decimal("100000"))
    await poll(pool, gateway, clock)

    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_account_value(WHALE, Decimal("60000"))  # a withdrawal, say
    await poll(pool, gateway, clock)

    (row,) = await equity(pool)  # still ONE row: the latest observation
    assert row["account_value"] == Decimal("60000")
    assert row["observed_at"] == clock.now()


async def test_a_pass_can_read_the_observation_it_replaces(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # The #171 prefactor: withdrawal alerts are a delta between consecutive
    # observations, so the previous one has to be legible at the moment the new
    # one lands. record_equity hands it back from inside the writing
    # transaction — the one moment both figures exist.
    await track(pool, clock, WHALE)
    first_seen = clock.now()
    async with pool.acquire() as conn, conn.transaction():
        assert await record_equity(conn, WHALE, Decimal("100000"), first_seen) is None

    clock.advance(POLL_INTERVAL_SECONDS)
    async with pool.acquire() as conn, conn.transaction():
        previous = await record_equity(conn, WHALE, Decimal("60000"), clock.now())

    assert previous == EquityObservation(
        account_value=Decimal("100000"), observed_at=first_seen
    )  # a $40k drop, for #171 to judge against the pass's realized PnL


async def test_capturing_equity_costs_the_exchange_nothing_extra(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The account value has always ridden the poller's own clearinghouseState
    # response. Capturing it must therefore add no request and no weight: one
    # call per covered venue, and the pass's spend unchanged.
    await track(pool, clock, WHALE, 1)
    gateway.set_positions(WHALE, [position()])
    gateway.set_account_value(WHALE, Decimal("100000"))
    # A budget with room for exactly one wallet's venue calls and not a token
    # more: an extra request would have to wait for a refill, and the pacing
    # sleep would show as the fake clock moving.
    budget = WeightBudget(POSITIONS_WEIGHT * len(POSITION_VENUES), clock)
    start = clock.now()

    await run_poll_pass(pool, gateway, budget, clock)

    assert gateway.positions_calls == [(WHALE, dex) for dex in POSITION_VENUES]
    assert clock.now() == start  # nothing waited: the pass spent what it always did
    assert (await equity(pool))[0]["account_value"] == Decimal("100000")


async def test_a_failed_venue_leaves_the_previous_equity_standing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # A venue that did not answer contributes nothing to the sum, so recording a
    # partial total would invent the exact signal #171 alerts on. The wallet is
    # simply retried next pass, its last good observation untouched.
    await track(pool, clock, WHALE, 1)
    gateway.set_positions(WHALE, [position()])
    gateway.set_account_value(WHALE, Decimal("400000"))
    gateway.set_account_value(WHALE, Decimal("25000"), dex="xyz")
    await poll(pool, gateway, clock)

    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.positions_errors_by_dex[(WHALE, "xyz")] = GatewayError("xyz venue down")
    await poll(pool, gateway, clock)

    (row,) = await equity(pool)
    assert row["account_value"] == Decimal("425000")  # the last complete look
    assert row["observed_at"] != clock.now()


async def test_a_trader_who_leaves_the_poll_set_leaves_no_stale_equity(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The re-follow rule, applied to equity: an observation nobody was watching
    # the wallet during is not a baseline to compare against. Kept, it would
    # hand #171 a months-old figure and a withdrawal alert for money that moved
    # while Epigone was not looking.
    await track(pool, clock, WHALE, 1)
    gateway.set_account_value(WHALE, Decimal("400000"))
    await poll(pool, gateway, clock)
    assert await equity(pool) != []

    await pool.execute("DELETE FROM tracks WHERE trader_address = $1", WHALE)
    await poll(pool, gateway, clock)

    assert await equity(pool) == []


async def test_an_interrupted_pass_leaves_the_equity_where_the_snapshots_are(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Equity rides the poll apply's transaction, so a pass killed partway
    through leaves the equity as untouched as the snapshots it was recorded
    beside. A figure that outlived the pass that observed it would describe a
    look nothing else in the database remembers — and #171 would read the drop
    between it and the next pass as a withdrawal."""
    await track(pool, clock, WHALE, 1)
    gateway.set_positions(WHALE, [position()])
    gateway.set_account_value(WHALE, Decimal("400000"))
    await poll(pool, gateway, clock)

    clock.advance(POLL_INTERVAL_SECONDS)
    gateway.set_positions(WHALE, [])  # BTC closes
    gateway.set_account_value(WHALE, Decimal("60000"))

    async def die(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("interrupted mid-pass")

    # Patched where the write lives since the cutover (#158): recording an
    # event moved to epigone.position_publish so both lanes publish alike.
    monkeypatch.setattr(position_publish, "record_events", die)
    with pytest.raises(RuntimeError):
        await poll(pool, gateway, clock)

    (row,) = await equity(pool)
    assert row["account_value"] == Decimal("400000")  # rolled back with the snapshot
