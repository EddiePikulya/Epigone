"""The fine metric pass: fills per eligible Trader, budget-aware and resumable.

Eligible = coarse-pass survivors (profitable, active month) plus every tracked
Trader. The pass also runs Bot vetting: flagged accounts keep their rows and
metrics but carry bot_reason (screener exclusion is tested in test_screener.py).
"""

from datetime import timedelta
from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.gateway import GatewayError, RateLimitedError
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.fine import run_fine_pass
from tests.support.clock import FakeClock
from tests.support.fills import T0, fill

WIDE_OPEN_BUDGET = 1_000_000


async def add_trader(
    pool: asyncpg.Pool,
    clock: FakeClock,
    address: str,
    month_pnl: str | None = "5000",
    month_volume: str = "100000",
    account_value: str = "1000",
    tracked_by: int | None = None,
    refresh_tier: str = "active",
) -> None:
    """A Trader in the Universe; with coarse month metrics unless month_pnl is
    None; tracked by a User when tracked_by is set."""
    await pool.execute(
        """
        INSERT INTO traders (address, refresh_tier, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3)
        """,
        address,
        refresh_tier,
        clock.now(),
    )
    if month_pnl is not None:
        await pool.execute(
            """
            INSERT INTO coarse_metrics
                (address, time_window, pnl, roi, volume, account_value, computed_at)
            VALUES ($1, 'month', $2, 0, $3, $4, $5)
            """,
            address,
            Decimal(month_pnl),
            Decimal(month_volume),
            Decimal(account_value),
            clock.now(),
        )
    if tracked_by is not None:
        await pool.execute(
            "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", tracked_by
        )
        await pool.execute(
            "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)",
            tracked_by,
            address,
        )


def human_fills() -> list:
    """A modest human profile: 3 trades, 2 wins, one maker fill."""
    return [
        fill(pnl="100", order_id=1, at=T0, crossed=False, start_position="50"),
        fill(pnl="-40", order_id=2, at=T0 + timedelta(days=1)),
        fill(pnl="60", order_id=3, at=T0 + timedelta(days=2)),
    ]


async def test_fine_pass_stores_metrics_for_a_coarse_survivor(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa", account_value="1000")
    gateway.set_fills("0xaaa", human_fills())

    result = await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.refreshed == 1 and result.failed == 0 and not result.aborted
    row = await pool.fetchrow("SELECT * FROM fine_metrics WHERE address = '0xaaa'")
    assert row is not None
    assert row["trade_count"] == 3
    assert row["win_rate"] == Decimal(2) / Decimal(3)
    assert row["avg_win"] == Decimal("80")
    assert row["avg_loss"] == Decimal("40")
    assert row["realized_pnl"] == Decimal("120")
    assert row["maker_share"] == Decimal(1) / Decimal(3)
    # peak notionals 500, 10, 10 against the $1000 coarse account value
    assert row["avg_leverage"] == Decimal("520") / Decimal("3000")
    assert row["window_start"] == T0
    assert row["computed_at"] == clock.now()
    trader = await pool.fetchrow("SELECT * FROM traders WHERE address = '0xaaa'")
    assert trader is not None
    assert trader["fine_refreshed_at"] == clock.now()
    assert trader["bot_reason"] is None


async def test_only_survivors_and_tracked_traders_get_the_fine_pass(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xsurvivor", month_pnl="5000")
    await add_trader(pool, clock, "0xloser", month_pnl="-5000")
    await add_trader(pool, clock, "0xidle", month_pnl="5000", month_volume="0")
    await add_trader(pool, clock, "0xunscanned", month_pnl=None)
    await add_trader(pool, clock, "0xtrackedloser", month_pnl="-5000", tracked_by=42)
    await add_trader(pool, clock, "0xtrackedfresh", month_pnl=None, tracked_by=42)

    result = await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.refreshed == 3
    assert sorted(gateway.fills_calls) == ["0xsurvivor", "0xtrackedfresh", "0xtrackedloser"]


async def test_a_market_maker_is_flagged_as_bot_but_keeps_its_rows(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xbot")
    # 150 exits, every one a winner: the ~100%-win-rate heuristic.
    gateway.set_fills(
        "0xbot",
        [fill(pnl="5", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 151)],
    )

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    trader = await pool.fetchrow("SELECT * FROM traders WHERE address = '0xbot'")
    assert trader is not None
    assert trader["bot_reason"] is not None and "win rate" in trader["bot_reason"]
    assert trader["bot_flagged_at"] == clock.now()
    # Retained, not deleted: the row and its metrics stay in the database.
    trade_count = "SELECT trade_count FROM fine_metrics WHERE address = '0xbot'"
    assert await pool.fetchval(trade_count) == 150


async def test_a_reformed_bot_is_unflagged_on_refresh(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xbot")
    await pool.execute(
        "UPDATE traders SET bot_flagged_at = $1, bot_reason = 'stale' WHERE address = '0xbot'",
        clock.now() - timedelta(days=30),
    )
    gateway.set_fills("0xbot", human_fills())

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    trader = await pool.fetchrow("SELECT * FROM traders WHERE address = '0xbot'")
    assert trader is not None
    assert trader["bot_reason"] is None and trader["bot_flagged_at"] is None


async def test_fresh_traders_are_not_refetched(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.set_fills("0xaaa", human_fills())
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    clock.advance(3600)  # an hour: inside the active cadence
    result = await run_fine_pass(pool, gateway, budget, clock)

    assert result.refreshed == 0
    assert gateway.fills_calls == ["0xaaa"]

    clock.advance(2 * 24 * 3600)  # past the active cadence
    result = await run_fine_pass(pool, gateway, budget, clock)
    assert result.refreshed == 1
    # Refetched, but incrementally now (issue #11): the first pass set a
    # checkpoint, so the second pulls only fills since it — no second full pull.
    assert gateway.fills_calls == ["0xaaa"]
    assert [addr for addr, _ in gateway.fills_since_calls] == ["0xaaa"]


async def test_one_failing_trader_does_not_stop_the_pass(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    for address in ("0xaaa", "0xbbb", "0xccc"):
        await add_trader(pool, clock, address)
        gateway.set_fills(address, human_fills())
    gateway.fills_errors["0xbbb"] = GatewayError("info API timeout")

    result = await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.refreshed == 2 and result.failed == 1 and not result.aborted
    unrefreshed = await pool.fetch("SELECT address FROM traders WHERE fine_refreshed_at IS NULL")
    assert [r["address"] for r in unrefreshed] == ["0xbbb"]
    # The failed attempt was recorded so 0xbbb rotates to the back next cycle.
    attempted = await pool.fetchval("SELECT fine_attempted_at FROM traders WHERE address = '0xbbb'")
    assert attempted == clock.now()


async def test_sustained_failures_abort_the_pass(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    addresses = [f"0x{i:03d}" for i in range(7)]
    for address in addresses:
        await add_trader(pool, clock, address)
        gateway.fills_errors[address] = GatewayError("connection reset")

    result = await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.aborted
    assert result.failed == 5  # stops at the failure streak, not the full list


async def test_rate_limit_streaks_do_not_abort_the_pass(pool: asyncpg.Pool) -> None:
    # Rate limiting is pacing, not an outage (issue #28): even a streak longer
    # than the abort threshold must leave the pass making forward progress.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    limited = [f"0x{i:03d}" for i in range(6)]
    for address in limited:
        await add_trader(pool, clock, address)
        gateway.fills_errors[address] = RateLimitedError("still 429 after retries")
    await add_trader(pool, clock, "0xhealthy")
    gateway.set_fills("0xhealthy", human_fills())

    result = await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert not result.aborted
    assert result.failed == 6 and result.refreshed == 1
    # The rate-limited attempt still rotates the Trader to the back of the scan.
    attempted = await pool.fetchval(
        "SELECT fine_attempted_at FROM traders WHERE address = $1", limited[0]
    )
    assert attempted == clock.now()


async def test_the_pass_is_paced_by_the_weight_budget(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    addresses = [f"0x{i:03d}" for i in range(30)]
    for address in addresses:
        await add_trader(pool, clock, address)
        gateway.set_fills(address, human_fills())

    start = clock.now()
    # 30 userFills calls x 20 weight = 600 against a 400/min budget: >= 30s of refill.
    await run_fine_pass(pool, gateway, WeightBudget(400, clock), clock)

    assert (clock.now() - start).total_seconds() >= 30
    assert await pool.fetchval("SELECT count(*) FROM fine_metrics") == 30


class RecordingBudget:
    """Grants everything instantly, recording the billing calls."""

    def __init__(self) -> None:
        self.spends: list[int] = []
        self.settles: list[int] = []

    async def spend(self, weight: int) -> None:
        self.spends.append(weight)

    async def settle(self, weight: int) -> None:
        self.settles.append(weight)


async def test_the_fills_surcharge_is_settled_after_the_response(pool: asyncpg.Pool) -> None:
    # userFills costs its nominal 20 plus weight per 20 fills returned (issue
    # #41): the size of the response is billed once it is known.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.set_fills(
        "0xaaa", [fill(pnl="5", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 46)]
    )
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [20]
    assert budget.settles == [3]  # 45 fills -> ceil(45 / 20)


async def test_an_empty_fills_response_settles_nothing(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.set_fills("0xaaa", [])
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [20]
    assert budget.settles == []


async def test_a_failed_fetch_settles_no_surcharge(pool: asyncpg.Pool) -> None:
    # No response arrived, so there is no revealed weight to reconcile.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.fills_errors["0xaaa"] = RateLimitedError("still 429 after retries")
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [20]
    assert budget.settles == []


# --- Incremental refresh (issue #11) -----------------------------------------


async def test_incremental_refresh_folds_new_fills_into_the_stored_metrics(
    pool: asyncpg.Pool,
) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa", account_value="1000")
    initial = [
        fill(pnl="100", order_id=1, at=T0, crossed=False, start_position="50"),
        fill(pnl="-40", order_id=2, at=T0 + timedelta(days=1)),
    ]
    gateway.set_fills("0xaaa", initial)
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    # A new closing trade lands after the checkpoint; the next pass folds it in
    # without re-pulling the earlier fills.
    new_fill = fill(pnl="60", order_id=3, at=T0 + timedelta(days=1, hours=2))
    gateway.set_fills("0xaaa", initial + [new_fill])
    clock.advance(2 * 24 * 3600)  # past the active cadence
    result = await run_fine_pass(pool, gateway, budget, clock)

    assert result.refreshed == 1
    # Full pull happened once; the second refresh fetched only fills since the
    # checkpoint (one fill-tick past the last folded fill).
    assert gateway.fills_calls == ["0xaaa"]
    assert gateway.fills_since_calls == [("0xaaa", T0 + timedelta(days=1, milliseconds=1))]

    row = await pool.fetchrow("SELECT * FROM fine_metrics WHERE address = '0xaaa'")
    assert row is not None
    assert row["trade_count"] == 3  # the two seeded trades plus the folded one
    assert row["realized_pnl"] == Decimal("120")  # 100 - 40 + 60
    assert row["win_rate"] == Decimal(2) / Decimal(3)
    # The full history is persisted as trades, not just the last pull's window.
    assert await pool.fetchval("SELECT count(*) FROM fine_trades WHERE address = '0xaaa'") == 3
    checkpoint = await pool.fetchval(
        "SELECT fine_checkpoint_at FROM traders WHERE address = '0xaaa'"
    )
    assert checkpoint == T0 + timedelta(days=1, hours=2)


async def test_incremental_refresh_preserves_history_beyond_a_single_pull(
    pool: asyncpg.Pool,
) -> None:
    # The point of the fold: a later pull's narrow window does not shrink the
    # metrics back to just what that pull returned — history accumulates, so it
    # survives past the ~2000-fill cap a full re-pull would truncate to (#11).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    seeded = [fill(pnl="10", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 6)]
    gateway.set_fills("0xaaa", seeded)
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    # The incremental window returns ONLY the single new fill (the fake filters
    # by time, exactly as userFillsByTime would) — the five seeded ones are gone.
    late = fill(pnl="10", order_id=6, at=T0 + timedelta(hours=6))
    gateway.set_fills("0xaaa", [late])
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)

    row = await pool.fetchrow(
        "SELECT trade_count, realized_pnl FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert row is not None
    assert row["trade_count"] == 6  # all six, though the last pull saw only one
    assert row["realized_pnl"] == Decimal("60")


async def test_incremental_refresh_settles_only_the_new_fills_surcharge(
    pool: asyncpg.Pool,
) -> None:
    # A fast-tier refresh is cheaper: the surcharge bills the few new fills, not
    # a full ~2000-fill re-pull (issue #11 / #41).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    initial = [fill(pnl="5", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 46)]
    gateway.set_fills("0xaaa", initial)
    budget = RecordingBudget()
    await run_fine_pass(pool, gateway, budget, clock)  # full: settle ceil(45/20)=3

    new = [fill(pnl="5", order_id=100 + i, at=T0 + timedelta(hours=45 + i)) for i in range(1, 4)]
    gateway.set_fills("0xaaa", initial + new)
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)  # incremental: settle ceil(3/20)=1

    assert budget.spends == [20, 20]  # base weight each time
    assert budget.settles == [3, 1]  # full pull's surcharge, then just the new fills'


async def test_a_refresh_with_no_new_fills_does_not_double_count(pool: asyncpg.Pool) -> None:
    # A boundary re-fetch (nothing new since the checkpoint) must leave the
    # accumulators untouched — the counters are running totals (#11).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa", account_value="1000")
    gateway.set_fills("0xaaa", human_fills())
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    cols = "trade_count, realized_pnl, maker_share, perp_fill_count, maker_fill_count, window_start"
    before = await pool.fetchrow(f"SELECT {cols} FROM fine_metrics WHERE address = '0xaaa'")
    checkpoint_before = await pool.fetchval(
        "SELECT fine_checkpoint_at FROM traders WHERE address = '0xaaa'"
    )

    clock.advance(2 * 24 * 3600)  # due again, but no fills have arrived since
    result = await run_fine_pass(pool, gateway, budget, clock)

    assert result.refreshed == 1
    after = await pool.fetchrow(f"SELECT {cols} FROM fine_metrics WHERE address = '0xaaa'")
    assert dict(after) == dict(before)  # metrics and counters unchanged
    assert await pool.fetchval("SELECT count(*) FROM fine_trades WHERE address = '0xaaa'") == 3
    checkpoint_after = await pool.fetchval(
        "SELECT fine_checkpoint_at FROM traders WHERE address = '0xaaa'"
    )
    assert checkpoint_after == checkpoint_before  # nothing new, checkpoint holds
