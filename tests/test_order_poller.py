"""The stream order-poll pass: resting-order diffing for Order Alerts (issue #115).

Seam test per the house convention: fake HyperliquidGateway, fake clock,
real Postgres. The diff semantics under test (documented in
epigone.stream.orders):

- first poll of a Trader baselines silently (a pre-existing ladder is not news)
- a new order id -> alerted; a disappeared id (cancel or fill) -> silent prune
  (fills already alert as position events)
- ONE alert row per follower per wallet per cycle, batching every new order —
  never one row per order (#115's noise rule)
- mute and min-size floors suppress at queue time, judged on order notional;
  a whole-position TP/SL (no order-level notional) is never floor-suppressed
- snapshots and alert rows share one transaction, so a restart neither
  re-alerts nor loses orders
"""

import json
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.clock import Clock
from epigone.gateway import GatewayError, OpenOrder, RateLimitedError
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.orders import ORDERS_WEIGHT, run_order_poll_pass
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000

PLACED_AT = datetime(2026, 7, 20, 9, 0, tzinfo=UTC)


def order(
    coin: str = "LIT",
    *,
    is_buy: bool = False,
    limit_price: str = "4.5",
    size: str = "3000",
    order_id: int = 1001,
    placed_at: datetime = PLACED_AT,
    order_type: str = "Limit",
    is_trigger: bool = False,
    trigger_price: str | None = None,
    is_position_tpsl: bool = False,
    reduce_only: bool = False,
) -> OpenOrder:
    return OpenOrder(
        coin=coin,
        is_buy=is_buy,
        limit_price=Decimal(limit_price),
        size=Decimal(size),
        order_id=order_id,
        placed_at=placed_at,
        order_type=order_type,
        is_trigger=is_trigger,
        trigger_price=Decimal(trigger_price) if trigger_price is not None else None,
        is_position_tpsl=is_position_tpsl,
        reduce_only=reduce_only,
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


async def alerts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM order_alerts ORDER BY id")


def batch(row: asyncpg.Record) -> list[dict[str, object]]:
    orders = json.loads(row["orders"])
    assert isinstance(orders, list)
    return orders


async def run_pass(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> object:
    return await run_order_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)


async def baseline(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    """First pass: establish the known-order-id set; asserts it stayed silent."""
    await run_pass(pool, gateway, clock)
    assert await alerts(pool) == []


async def test_first_poll_baselines_the_existing_ladder_without_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [order(order_id=1001), order(order_id=1002)])

    result = await run_order_poll_pass(
        pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock
    )

    assert result.polled == 1 and result.new_orders == 0 and result.failed == 0
    assert await alerts(pool) == []
    ids = await pool.fetch("SELECT order_id FROM order_snapshots ORDER BY order_id")
    assert [r["order_id"] for r in ids] == [1001, 1002]
    state = await pool.fetchrow("SELECT * FROM order_poll_state")
    assert state is not None and state["trader_address"] == "0xaaa"
    # Every covered venue is queried, per the per-dex empirical contract (#115).
    assert gateway.open_orders_calls == [("0xaaa", None), ("0xaaa", "xyz"), ("0xaaa", "mkts")]


async def test_a_new_order_alerts_every_follower_with_its_details(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42, 43)
    gateway.set_open_orders("0xaaa", [order(order_id=1001)])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    gateway.set_open_orders(
        "0xaaa",
        [
            order(order_id=1001),
            order(
                coin="HYPE",
                is_buy=True,
                limit_price="68.31",
                size="75",
                order_id=2002,
                order_type="Stop Market",
                is_trigger=True,
                trigger_price="63.25",
            ),
        ],
    )
    result = await run_pass(pool, gateway, clock)

    assert result.new_orders == 1
    rows = await alerts(pool)
    assert sorted(r["user_telegram_id"] for r in rows) == [42, 43]
    for row in rows:
        assert row["trader_address"] == "0xaaa"
        assert row["created_at"] == clock.now()
        assert row["delivered_at"] is None
        (entry,) = batch(row)
        assert entry["coin"] == "HYPE"
        assert entry["is_buy"] is True
        assert entry["is_trigger"] is True
        assert entry["trigger_price"] == "63.25"
        assert entry["order_type"] == "Stop Market"
        assert Decimal(entry["notional_usd"]) == Decimal("75") * Decimal("63.25")


async def test_all_new_orders_of_a_cycle_batch_into_one_alert_per_follower(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    later = order(order_id=3002, limit_price="4.2", placed_at=PLACED_AT.replace(minute=30))
    earlier = order(order_id=3001, limit_price="4.5", placed_at=PLACED_AT)
    gateway.set_open_orders("0xaaa", [later, earlier])
    result = await run_pass(pool, gateway, clock)

    assert result.new_orders == 2
    (row,) = await alerts(pool)  # ONE row carrying both orders, never two rows
    entries = batch(row)
    # Batched in placement order, so the message reads chronologically.
    assert [e["order_id"] for e in entries] == [3001, 3002]


async def test_cancels_and_fills_prune_silently_and_never_realert(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [order(order_id=1001), order(order_id=1002)])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    gateway.set_open_orders("0xaaa", [order(order_id=1002)])  # 1001 cancelled or filled
    result = await run_pass(pool, gateway, clock)

    assert result.new_orders == 0
    assert await alerts(pool) == []
    ids = await pool.fetch("SELECT order_id FROM order_snapshots")
    assert [r["order_id"] for r in ids] == [1002]

    # An unchanged book on the next pass stays silent too (restart-safety: the
    # ids are persisted, not in-memory).
    clock.advance(300)
    result = await run_pass(pool, gateway, clock)
    assert result.new_orders == 0
    assert await alerts(pool) == []


async def test_a_muted_track_receives_no_order_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42, 43)
    await pool.execute("UPDATE tracks SET muted = TRUE WHERE user_telegram_id = 42")
    gateway.set_open_orders("0xaaa", [])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    gateway.set_open_orders("0xaaa", [order(order_id=1001)])
    await run_pass(pool, gateway, clock)

    (row,) = await alerts(pool)
    assert row["user_telegram_id"] == 43


async def test_min_size_floor_judges_each_order_by_its_notional(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    # 3000 × 4.5 = $13,500 notional; the floor sits above it.
    await pool.execute("UPDATE users SET min_size_usd = 20000 WHERE telegram_id = 42")
    gateway.set_open_orders("0xaaa", [])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    small = order(order_id=1001)  # $13,500 — below the floor
    big = order(order_id=1002, size="10000", limit_price="4.5")  # $45,000 — above
    gateway.set_open_orders("0xaaa", [small, big])
    await run_pass(pool, gateway, clock)

    (row,) = await alerts(pool)
    entries = batch(row)
    assert [e["order_id"] for e in entries] == [1002]


async def test_a_batch_entirely_below_the_floor_queues_no_row(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await pool.execute("UPDATE users SET min_size_usd = 20000 WHERE telegram_id = 42")
    gateway.set_open_orders("0xaaa", [])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    gateway.set_open_orders("0xaaa", [order(order_id=1001)])
    await run_pass(pool, gateway, clock)

    assert await alerts(pool) == []
    # The order is still remembered: suppression is per-follower delivery
    # filtering, never a gap in the known-id set.
    ids = await pool.fetch("SELECT order_id FROM order_snapshots")
    assert [r["order_id"] for r in ids] == [1001]


async def test_a_whole_position_tpsl_is_never_floor_suppressed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await pool.execute("UPDATE users SET min_size_usd = 20000 WHERE telegram_id = 42")
    gateway.set_open_orders("0xaaa", [])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    # sz 0: sized to the position at trigger time — no order-level notional, so
    # the floor cannot honestly judge it and must let it through.
    tpsl = order(
        coin="GRAM",
        order_id=1001,
        size="0",
        order_type="Stop Market",
        is_trigger=True,
        trigger_price="1.38",
        is_position_tpsl=True,
        reduce_only=True,
    )
    gateway.set_open_orders("0xaaa", [tpsl])
    await run_pass(pool, gateway, clock)

    (row,) = await alerts(pool)
    (entry,) = batch(row)
    assert entry["is_position_tpsl"] is True
    assert entry["notional_usd"] is None


async def test_budget_is_billed_per_venue_per_wallet(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    # A budget with room for exactly one wallet's three venue calls: the pass
    # must bill ORDERS_WEIGHT per POSITION_VENUES entry, in lockstep with the
    # calls the shared fetch makes (the #31 rule), and no more.
    budget = WeightBudget(ORDERS_WEIGHT * 3, clock)
    await run_order_poll_pass(pool, gateway, budget, clock)
    assert gateway.open_orders_calls == [("0xaaa", None), ("0xaaa", "xyz"), ("0xaaa", "mkts")]


async def test_gateway_failure_leaves_snapshots_untouched_for_retry(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [order(order_id=1001)])
    await baseline(pool, gateway, clock)

    clock.advance(300)
    gateway.open_orders_errors["0xaaa"] = GatewayError("info API down")
    result = await run_pass(pool, gateway, clock)

    assert result.failed == 1 and result.new_orders == 0
    ids = await pool.fetch("SELECT order_id FROM order_snapshots")
    assert [r["order_id"] for r in ids] == [1001]  # not pruned into a re-alert setup


async def test_a_sustained_failure_streak_aborts_the_pass(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    for i in range(6):
        address = f"0x{i:040x}"
        await track(pool, clock, address, 42)
        gateway.open_orders_errors[address] = GatewayError("down")

    result = await run_pass(pool, gateway, clock)

    assert result.aborted is True
    assert result.failed == 5  # stopped at the streak threshold, not all six


async def test_rate_limiting_is_pacing_not_outage(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    for i in range(6):
        address = f"0x{i:040x}"
        await track(pool, clock, address, 42)
        gateway.open_orders_errors[address] = RateLimitedError("still 429")

    result = await run_pass(pool, gateway, clock)

    assert result.aborted is False  # never counts toward the abort streak
    assert result.failed == 6
    events = await pool.fetchval("SELECT count(*) FROM rate_limit_events")
    assert events == 6  # feeds the health monitor's sustained-limiting signal (#54)


async def test_unfollowed_wallets_prune_and_refollow_rebaselines(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [order(order_id=1001)])
    await baseline(pool, gateway, clock)

    await pool.execute("DELETE FROM tracks")
    clock.advance(300)
    result = await run_pass(pool, gateway, clock)
    assert result.polled == 0
    assert await pool.fetchval("SELECT count(*) FROM order_snapshots") == 0
    assert await pool.fetchval("SELECT count(*) FROM order_poll_state") == 0

    # Refollow: the wallet re-baselines silently instead of diffing against a
    # stale (now pruned) id set and re-alerting its whole standing ladder.
    await track(pool, clock, "0xaaa", 42)
    gateway.set_open_orders("0xaaa", [order(order_id=1001), order(order_id=1002)])
    clock.advance(300)
    result = await run_pass(pool, gateway, clock)
    assert result.new_orders == 0
    assert await alerts(pool) == []
