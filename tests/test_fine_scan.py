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
    """A modest human profile: 3 completed round-trips, 2 wins, one maker fill."""
    return [
        fill("Open Long", order_id=1, at=T0, start_position="0", size="50", crossed=False),
        fill(pnl="100", order_id=2, at=T0 + timedelta(hours=1), start_position="50", size="50"),
        fill("Open Long", order_id=3, at=T0 + timedelta(days=1), start_position="0"),
        fill(pnl="-40", order_id=4, at=T0 + timedelta(days=1, hours=1)),
        fill("Open Long", order_id=5, at=T0 + timedelta(days=2), start_position="0"),
        fill(pnl="60", order_id=6, at=T0 + timedelta(days=2, hours=1)),
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
    assert row["maker_share"] == Decimal(1) / Decimal(6)
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
    # 150 completed round-trips, every one a winner: the ~100%-win-rate heuristic.
    gateway.set_fills(
        "0xbot",
        [
            f
            for i in range(1, 151)
            for f in (
                fill("Open Long", at=T0 + timedelta(hours=2 * i), start_position="0"),
                fill(pnl="5", at=T0 + timedelta(hours=2 * i + 1)),
            )
        ],
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


async def test_rate_limits_are_recorded_for_the_health_monitor(pool: asyncpg.Pool) -> None:
    # Each escaped RateLimitedError stamps a rate_limit_events row (issue #54) so
    # the monitor can alert on sustained limiting; the healthy fetch stamps none.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    for address in ("0xaaa", "0xbbb"):
        await add_trader(pool, clock, address)
        gateway.fills_errors[address] = RateLimitedError("still 429 after retries")
    await add_trader(pool, clock, "0xhealthy")
    gateway.set_fills("0xhealthy", human_fills())

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert await pool.fetchval("SELECT count(*) FROM rate_limit_events") == 2


async def test_the_pass_is_paced_by_the_weight_budget(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    addresses = [f"0x{i:03d}" for i in range(30)]
    for address in addresses:
        await add_trader(pool, clock, address)
        gateway.set_fills(address, human_fills())

    start = clock.now()
    # 30 fetches x 40 weight (two fills endpoints each, #63) = 1200 against a
    # 400/min budget: >= 120s of refill.
    await run_fine_pass(pool, gateway, WeightBudget(400, clock), clock)

    assert (clock.now() - start).total_seconds() >= 120
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
    # Each fills endpoint costs its nominal 20 — a fetch hits two (userFills
    # plus userTwapSliceFills, #63) — plus weight per 20 fills returned (issue
    # #41): the size of the response is billed once it is known.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.set_fills(
        "0xaaa", [fill(pnl="5", order_id=i, at=T0 + timedelta(hours=i)) for i in range(1, 46)]
    )
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [40]  # 20 per fills endpoint
    # 45 fills -> ceil(45 / 20), plus one for the unknown two-endpoint split
    # (each endpoint's own ceil can round up): the conservative settle.
    assert budget.settles == [4]


async def test_an_empty_fills_response_settles_nothing(pool: asyncpg.Pool) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.set_fills("0xaaa", [])
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [40]
    assert budget.settles == []


async def test_a_failed_fetch_settles_no_surcharge(pool: asyncpg.Pool) -> None:
    # No response arrived, so there is no revealed weight to reconcile.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    gateway.fills_errors["0xaaa"] = RateLimitedError("still 429 after retries")
    budget = RecordingBudget()

    await run_fine_pass(pool, gateway, budget, clock)

    assert budget.spends == [40]
    assert budget.settles == []


# --- Incremental refresh (issue #11) -----------------------------------------


async def test_incremental_refresh_folds_new_fills_into_the_stored_metrics(
    pool: asyncpg.Pool,
) -> None:
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa", account_value="1000")
    initial = human_fills()[:4]  # two completed round-trips (+100, -40)
    gateway.set_fills("0xaaa", initial)
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    # A new round-trip lands after the checkpoint; the next pass folds it in
    # without re-pulling the earlier fills.
    new_trip = [
        fill("Open Long", order_id=5, at=T0 + timedelta(days=2), start_position="0"),
        fill(pnl="60", order_id=6, at=T0 + timedelta(days=2, hours=1)),
    ]
    gateway.set_fills("0xaaa", initial + new_trip)
    clock.advance(2 * 24 * 3600)  # past the active cadence
    result = await run_fine_pass(pool, gateway, budget, clock)

    assert result.refreshed == 1
    # Full pull happened once; the second refresh fetched only fills since the
    # checkpoint (one fill-tick past the last folded fill).
    assert gateway.fills_calls == ["0xaaa"]
    assert gateway.fills_since_calls == [("0xaaa", T0 + timedelta(days=1, hours=1, milliseconds=1))]

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
    assert checkpoint == T0 + timedelta(days=2, hours=1)


async def test_incremental_refresh_preserves_history_beyond_a_single_pull(
    pool: asyncpg.Pool,
) -> None:
    # The point of the fold: a later pull's narrow window does not shrink the
    # metrics back to just what that pull returned — history accumulates, so it
    # survives past the ~2000-fill cap a full re-pull would truncate to (#11).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    seeded = [
        f
        for i in range(1, 6)
        for f in (
            fill("Open Long", at=T0 + timedelta(hours=2 * i), start_position="0"),
            fill(pnl="10", at=T0 + timedelta(hours=2 * i + 1)),
        )
    ]
    gateway.set_fills("0xaaa", seeded)
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    # The incremental window returns ONLY the new round-trip (the fake filters
    # by time, exactly as userFillsByTime would) — the five seeded ones are gone.
    late = [
        fill("Open Long", at=T0 + timedelta(hours=12), start_position="0"),
        fill(pnl="10", at=T0 + timedelta(hours=13)),
    ]
    gateway.set_fills("0xaaa", late)
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
    await run_fine_pass(pool, gateway, budget, clock)  # full: settle ceil(45/20)+1 = 4

    new = [fill(pnl="5", order_id=100 + i, at=T0 + timedelta(hours=45 + i)) for i in range(1, 4)]
    gateway.set_fills("0xaaa", initial + new)
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)  # incremental: ceil(3/20)+1 = 2

    assert budget.spends == [40, 40]  # base weight per fills endpoint, each time
    assert budget.settles == [4, 2]  # full pull's surcharge, then just the new fills'


async def test_holding_time_folds_across_an_incremental_refresh(pool: asyncpg.Pool) -> None:
    # A position opened before the checkpoint and closed in the next batch: the
    # open-time survived the fold in fine_open_episodes, so the incremental close
    # resolves to the right holding time (issue #48).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    opened = fill("Open Long", order_id=1, at=T0, start_position="0")
    gateway.set_fills("0xaaa", [opened])
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    await run_fine_pass(pool, gateway, budget, clock)

    # Nothing has closed yet: no average, but the open episode persists.
    assert (
        await pool.fetchval("SELECT avg_hold_seconds FROM fine_metrics WHERE address = '0xaaa'")
        is None
    )
    episodes = "SELECT count(*) FROM fine_open_episodes WHERE address = '0xaaa'"
    assert await pool.fetchval(episodes) == 1

    closed = fill("Close Long", order_id=2, at=T0 + timedelta(hours=3), start_position="1")
    gateway.set_fills("0xaaa", [opened, closed])
    clock.advance(2 * 24 * 3600)  # past the active cadence; only the close is new
    await run_fine_pass(pool, gateway, budget, clock)

    row = await pool.fetchrow(
        "SELECT avg_hold_seconds, trade_count FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert row is not None
    assert row["avg_hold_seconds"] == 3 * 3600  # T0 -> T0+3h, resolved across the fold
    assert row["trade_count"] == 1  # the resolved episode is a completed round-trip
    assert await pool.fetchval(episodes) == 0  # the episode closed, its open row is gone


async def test_a_round_trip_accumulates_net_pnl_across_refreshes(pool: asyncpg.Pool) -> None:
    # Opened in one refresh, trimmed in the next, fully closed in a third: one
    # trade whose net PnL spans all three batches (issue #58) — the open
    # episode's PnL/peak accumulators persist in fine_open_episodes between
    # passes, so the final close completes the whole round-trip.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    opened = fill("Open Long", at=T0, start_position="0", size="2")
    gateway.set_fills("0xaaa", [opened])
    await run_fine_pass(pool, gateway, budget, clock)

    trim = fill(pnl="20", at=T0 + timedelta(hours=1), start_position="2")
    gateway.set_fills("0xaaa", [opened, trim])
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)

    # Mid-life: the trim banked into the open episode, not into any trade.
    episode = await pool.fetchrow(
        "SELECT pnl, peak_notional FROM fine_open_episodes WHERE address = '0xaaa'"
    )
    assert episode is not None
    assert episode["pnl"] == Decimal("20")
    assert episode["peak_notional"] == Decimal("20")  # |start 2| x price 10
    mid = await pool.fetchrow(
        "SELECT trade_count, win_rate, realized_pnl FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert mid is not None
    assert mid["trade_count"] == 0  # still open: not a trade yet
    assert mid["win_rate"] is None
    assert mid["realized_pnl"] == Decimal("20")  # but the money is banked

    closed = fill(pnl="-50", at=T0 + timedelta(hours=2), start_position="1")
    gateway.set_fills("0xaaa", [opened, trim, closed])
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)

    row = await pool.fetchrow("SELECT * FROM fine_metrics WHERE address = '0xaaa'")
    assert row is not None
    assert row["trade_count"] == 1
    assert row["win_rate"] == Decimal(0)  # trimmed in profit, net a loss
    assert row["avg_loss"] == Decimal("30")
    assert row["realized_pnl"] == Decimal("-30")
    assert row["avg_hold_seconds"] == 2 * 3600
    trade = await pool.fetchrow("SELECT * FROM fine_trades WHERE address = '0xaaa'")
    assert trade is not None
    assert trade["pnl"] == Decimal("-30")
    assert trade["opened_at"] == T0
    assert trade["closed_at"] == T0 + timedelta(hours=2)
    assert await pool.fetchval("SELECT count(*) FROM fine_open_episodes") == 0


async def test_a_twap_built_round_trip_is_captured(pool: asyncpg.Pool) -> None:
    # A position accumulated by TWAP slices and closed by one regular order.
    # The gateway contract (#63) delivers the union of both fill endpoints as
    # one execution-order stream — the fake's set_fills list — so the engine
    # walks the whole life: without the slices this history would read as a
    # lone pre-window close and yield no trade at all.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa", account_value="1000")
    slices = [
        fill("Open Long", order_id=i, at=T0 + timedelta(minutes=i), start_position=str(i), size="1")
        for i in range(3)  # three TWAP slices: 0 -> 3
    ]
    close = fill(pnl="90", order_id=9, at=T0 + timedelta(hours=1), start_position="3", size="3")
    gateway.set_fills("0xaaa", [*slices, close])

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    row = await pool.fetchrow(
        "SELECT trade_count, win_rate, realized_pnl, avg_leverage "
        "FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert row is not None
    assert row["trade_count"] == 1
    assert row["win_rate"] == Decimal(1)
    assert row["realized_pnl"] == Decimal("90")
    assert row["avg_leverage"] == Decimal("30") / Decimal("1000")  # peak 3 x price 10


async def test_a_stored_episode_that_missed_executions_demotes_across_refreshes(
    pool: asyncpg.Pool,
) -> None:
    # The #63 self-healing path: a stored open episode whose walk missed
    # executions (folded TWAP-blind before the merged stream shipped, or
    # history truncated at the cap). The next incremental's first fill
    # startPosition disagrees with the persisted net_position, so the episode
    # demotes to untracked — no round-trip is credited, the money still banks,
    # and no checkpoint reset was needed.
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    budget = WeightBudget(WIDE_OPEN_BUDGET, clock)
    opened = fill("Open Long", at=T0, start_position="0")  # walked net: 1
    gateway.set_fills("0xaaa", [opened])
    await run_fine_pass(pool, gateway, budget, clock)

    # The next batch says the position was 5 when it closed: four coins of
    # executions this pass never saw (they were TWAP slices, pre-#63).
    closed = fill(pnl="50", at=T0 + timedelta(hours=1), start_position="5", size="5")
    gateway.set_fills("0xaaa", [opened, closed])
    clock.advance(2 * 24 * 3600)
    await run_fine_pass(pool, gateway, budget, clock)

    row = await pool.fetchrow(
        "SELECT trade_count, win_rate, realized_pnl FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert row is not None
    assert row["trade_count"] == 0  # demoted, never a reconstructed trade
    assert row["win_rate"] is None
    assert row["realized_pnl"] == Decimal("50")  # closes still bank
    assert await pool.fetchval("SELECT count(*) FROM fine_trades WHERE address = '0xaaa'") == 0
    # The corrupt episode is dropped, not carried to poison later folds.
    assert await pool.fetchval("SELECT count(*) FROM fine_open_episodes") == 0


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


async def test_same_ms_trades_both_persist(pool: asyncpg.Pool) -> None:
    # A same-block close->reopen->close completes two round-trips on one
    # timestamp; the fine_trades primary key carries the seq ordinal so
    # neither row silently vanishes (issue #58 review).
    gateway = FakeHyperliquidGateway()
    clock = FakeClock()
    await add_trader(pool, clock, "0xaaa")
    t1 = T0 + timedelta(hours=1)
    gateway.set_fills(
        "0xaaa",
        [
            fill("Open Long", at=T0, start_position="0"),
            fill(pnl="10", at=t1),
            fill("Open Long", at=t1, start_position="0"),
            fill(pnl="-5", at=t1),
        ],
    )

    await run_fine_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    rows = await pool.fetch("SELECT pnl, seq FROM fine_trades WHERE address = '0xaaa' ORDER BY seq")
    assert [(r["pnl"], r["seq"]) for r in rows] == [(Decimal("10"), 0), (Decimal("-5"), 1)]
    metrics = await pool.fetchrow(
        "SELECT trade_count, win_rate FROM fine_metrics WHERE address = '0xaaa'"
    )
    assert metrics is not None
    assert metrics["trade_count"] == 2
    assert metrics["win_rate"] == Decimal("0.5")
