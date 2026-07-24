"""The stream poll pass: snapshot diffing for Position Alerts (issue #4).

Seam test per the house convention: fake HyperliquidGateway, fake clock,
real Postgres. The diff semantics under test (documented in
epigone.stream.poller):

- first poll of a Trader baselines silently (pre-existing positions are not news)
- coin appears -> OPEN; coin disappears -> CLOSE; side changes -> FLIP
- same-side size change >= SCALE_SIGNIFICANCE_THRESHOLD -> SCALE-IN/SCALE-OUT
  (issue #10); smaller drift (entry/leverage/partial-close) stays silent
- one alert row per event per follower, in the same transaction as the
  snapshot update, so a restart neither re-alerts nor loses events

Alert-control suppression (mute, min-size) lives in tests/test_alert_controls.py.
"""

from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.clock import Clock
from epigone.gateway import GatewayError, Position, RateLimitedError, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import POSITIONS_WEIGHT, run_poll_pass
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000


def position(
    coin: str = "BTC",
    side: Side = Side.LONG,
    size_usd: str = "10000",
    leverage: str = "5",
    entry_price: str = "100",
    unrealized_pnl: str = "0",
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal(leverage),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal(unrealized_pnl),
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
    return await pool.fetch("SELECT * FROM position_alerts ORDER BY id")


async def baseline(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    """First pass: establish snapshots; asserts it stayed silent."""
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
    assert await alerts(pool) == []


async def test_first_poll_baselines_existing_positions_without_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="ETH", unrealized_pnl="250")])

    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.polled == 1 and result.events == 0 and result.failed == 0
    assert await alerts(pool) == []
    snapshot = await pool.fetchrow("SELECT * FROM position_snapshots")
    assert snapshot is not None
    assert snapshot["trader_address"] == "0xaaa"
    assert snapshot["coin"] == "ETH"
    assert snapshot["side"] == "long"
    assert snapshot["unrealized_pnl"] == Decimal("250")
    assert snapshot["opened_at"] == clock.now()


async def test_a_new_position_emits_an_open_alert_to_every_follower(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42, 43)
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions(
        "0xaaa", [position(coin="BTC", side=Side.SHORT, size_usd="20000", leverage="10")]
    )
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 1
    rows = await alerts(pool)
    assert sorted(r["user_telegram_id"] for r in rows) == [42, 43]
    for row in rows:
        assert row["kind"] == "open"
        assert row["trader_address"] == "0xaaa"
        assert row["coin"] == "BTC"
        assert row["side"] == "short"
        assert row["size_usd"] == Decimal("20000")
        assert row["leverage"] == Decimal("10")
        assert row["entry_price"] == Decimal("100")
        assert row["created_at"] == clock.now()
        assert row["delivered_at"] is None
    # Deduped across Users, and each pass polls every covered venue (core +
    # xyz + mkts): baseline pass then this pass, three calls apiece.
    assert gateway.positions_calls == [
        ("0xaaa", None),
        ("0xaaa", "xyz"),
        ("0xaaa", "mkts"),
        ("0xaaa", None),
        ("0xaaa", "xyz"),
        ("0xaaa", "mkts"),
    ]


async def test_a_disappeared_position_emits_a_close_alert_with_pnl_and_holding(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(size_usd="10000", leverage="5")])
    await baseline(pool, gateway, clock)
    opened = clock.now()

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(unrealized_pnl="500")])
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [])
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    (row,) = await alerts(pool)
    assert row["kind"] == "close"
    assert row["coin"] == "BTC"
    assert row["prev_side"] == "long"
    # Realized PnL approximated by the last observed uPnL (weight-2 budget:
    # the exact figure would cost a weight-20 userFills call per close).
    assert row["realized_pnl"] == Decimal("500")
    # Return on margin: 500 against 10000/5x.
    assert row["pct_return"] == Decimal("500") / Decimal("2000")
    assert row["opened_at"] == opened
    assert row["created_at"] == clock.now()
    assert await pool.fetchval("SELECT count(*) FROM position_snapshots") == 0


async def test_a_side_change_emits_one_flip_alert_with_both_legs(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions(
        "0xaaa", [position(side=Side.LONG, size_usd="10000", leverage="5", unrealized_pnl="300")]
    )
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions(
        "0xaaa",
        [position(side=Side.SHORT, size_usd="15000", leverage="3", entry_price="110")],
    )
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 1
    (row,) = await alerts(pool)
    assert row["kind"] == "flip"
    assert row["coin"] == "BTC"
    assert row["prev_side"] == "long"
    assert row["realized_pnl"] == Decimal("300")
    assert row["pct_return"] == Decimal("300") / Decimal("2000")
    assert row["side"] == "short"
    assert row["size_usd"] == Decimal("15000")
    assert row["leverage"] == Decimal("3")
    assert row["entry_price"] == Decimal("110")
    # The snapshot now carries the new leg, opened at flip time.
    snapshot = await pool.fetchrow("SELECT * FROM position_snapshots")
    assert snapshot is not None
    assert snapshot["side"] == "short"
    assert snapshot["opened_at"] == clock.now()


async def test_subthreshold_size_and_entry_changes_are_silent(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Below SCALE_SIGNIFICANCE_THRESHOLD, same-side drift (small partial close,
    entry/leverage change) updates the snapshot without alerting — the pre-#10
    silent-update behavior, now bounded by the threshold."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(size_usd="10000")])
    await baseline(pool, gateway, clock)
    opened = clock.now()

    clock.advance(30)  # 10% partial close, well under the 25% threshold
    gateway.set_positions(
        "0xaaa",
        [position(size_usd="9000", entry_price="105", leverage="8", unrealized_pnl="120")],
    )
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 0
    assert await alerts(pool) == []
    snapshot = await pool.fetchrow("SELECT * FROM position_snapshots")
    assert snapshot is not None
    assert snapshot["size_usd"] == Decimal("9000")
    assert snapshot["entry_price"] == Decimal("105")
    assert snapshot["leverage"] == Decimal("8")
    assert snapshot["unrealized_pnl"] == Decimal("120")
    assert snapshot["opened_at"] == opened  # holding time survives resizes
    assert snapshot["updated_at"] == clock.now()


async def test_simultaneous_open_and_close_both_alert(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="SOL", side=Side.SHORT)])
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 2
    kinds = {(r["kind"], r["coin"]) for r in await alerts(pool)}
    assert kinds == {("close", "BTC"), ("open", "SOL")}


async def test_unchanged_positions_stay_silent_across_passes(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Also the restart story: every pass reads persisted snapshots, so a
    fresh process seeing the same positions re-alerts nothing."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(unrealized_pnl="50")])
    await baseline(pool, gateway, clock)

    for _ in range(3):
        clock.advance(30)
        result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
        assert result.events == 0
    assert await alerts(pool) == []


async def test_a_new_follower_of_a_baselined_trader_gets_no_backfill_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position()])
    await baseline(pool, gateway, clock)

    clock.advance(30)
    await track(pool, clock, "0xaaa", 43)  # second User follows later
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert await alerts(pool) == []


async def test_a_refollowed_trader_rebaselines_instead_of_replaying_stale_diffs(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Losing the last follower prunes the bookkeeping, so changes that happen
    while nobody watches never surface as stale alerts on re-follow."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await baseline(pool, gateway, clock)

    await pool.execute("DELETE FROM tracks")  # the last follower leaves
    clock.advance(30)
    gateway.set_positions("0xaaa", [])  # ...and the position closes unwatched
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
    # Only the baseline's three calls (core + xyz + mkts); the pruned wallet isn't polled.
    assert gateway.positions_calls == [("0xaaa", None), ("0xaaa", "xyz"), ("0xaaa", "mkts")]
    assert await pool.fetchval("SELECT count(*) FROM position_snapshots") == 0
    assert await pool.fetchval("SELECT count(*) FROM position_poll_state") == 0

    clock.advance(30)
    await track(pool, clock, "0xaaa", 42)  # re-follow
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert await alerts(pool) == []  # a silent fresh baseline, no stale CLOSE


async def test_untracked_traders_are_not_polled(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ('0xidle', $1, $1)",
        clock.now(),
    )
    await track(pool, clock, "0xaaa", 42)

    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.polled == 1
    assert gateway.positions_calls == [("0xaaa", None), ("0xaaa", "xyz"), ("0xaaa", "mkts")]


async def test_one_failing_wallet_does_not_stop_the_pass(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await track(pool, clock, "0xbbb", 42)
    gateway.set_positions("0xbbb", [position()])
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.positions_errors["0xaaa"] = GatewayError("info API timeout")
    gateway.set_positions("0xbbb", [])
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.failed == 1 and result.polled == 1 and result.events == 1
    (row,) = await alerts(pool)
    assert row["trader_address"] == "0xbbb" and row["kind"] == "close"
    # The failed wallet's bookkeeping is untouched: next pass diffs, not re-baselines.
    state = await pool.fetchrow("SELECT * FROM position_poll_state WHERE trader_address = '0xaaa'")
    assert state is not None
    assert state["last_polled_at"] < clock.now()


async def test_sustained_failures_abort_the_pass(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    addresses = [f"0x{i:03d}" for i in range(7)]
    for address in addresses:
        await track(pool, clock, address, 42)
        gateway.positions_errors[address] = GatewayError("connection reset")

    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.aborted
    assert result.failed == 5  # stops at the failure streak, not the full list


async def test_rate_limit_streaks_do_not_abort_the_pass(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Rate limiting is pacing, not an outage (issue #28): even a streak longer
    # than the abort threshold must leave the pass polling the rest.
    limited = [f"0x{i:03d}" for i in range(6)]
    for address in limited:
        await track(pool, clock, address, 42)
        gateway.positions_errors[address] = RateLimitedError("still 429 after retries")
    await track(pool, clock, "0xhealthy", 42)
    gateway.set_positions("0xhealthy", [position()])

    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert not result.aborted
    assert result.failed == 6 and result.polled == 1
    # Each escaped RateLimitedError feeds the health monitor's signal (issue #54).
    assert await pool.fetchval("SELECT count(*) FROM rate_limit_events") == 6


async def test_the_pass_is_paced_by_the_weight_budget(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    for i in range(10):
        await track(pool, clock, f"0x{i:03d}", 42)

    start = clock.now()
    # 10 wallets x 3 calls (core + xyz + mkts) x 2 weight = 60 against a 4/min
    # budget: the burst covers the first 2 calls, each of the other 28 refills 30s.
    await run_poll_pass(pool, gateway, WeightBudget(4, clock), clock)

    assert (clock.now() - start).total_seconds() >= 28 * 30
    assert POSITIONS_WEIGHT == 2
    assert len(gateway.positions_calls) == 30


# --- xyz builder DEX coverage (issue #21) -----------------------------------
#
# The poller polls each Trader on both the core venue and the xyz HIP-3 builder
# DEX per pass, merging the two position lists before diffing. xyz coins are
# namespaced (`xyz:META`), so the same diff machinery tracks them independently
# of core, with the market/DEX legible in every alert's coin field.


async def test_each_pass_polls_both_the_core_and_the_xyz_venue(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)

    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert gateway.positions_calls == [("0xaaa", None), ("0xaaa", "xyz"), ("0xaaa", "mkts")]


async def test_first_poll_baselines_xyz_positions_without_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    gateway.set_positions("0xaaa", [position(coin="xyz:META")], dex="xyz")

    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 0
    assert await alerts(pool) == []
    coins = {r["coin"] for r in await pool.fetch("SELECT coin FROM position_snapshots")}
    assert coins == {"BTC", "xyz:META"}  # both venues baselined under one Trader


async def test_an_xyz_open_emits_an_alert_naming_the_market(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42, 43)
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions(
        "0xaaa", [position(coin="xyz:META", side=Side.SHORT, size_usd="8000")], dex="xyz"
    )
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 1
    rows = await alerts(pool)
    assert sorted(r["user_telegram_id"] for r in rows) == [42, 43]
    for row in rows:
        assert row["kind"] == "open"
        assert row["coin"] == "xyz:META"  # the market/DEX is legible in the alert
        assert row["side"] == "short"
        assert row["size_usd"] == Decimal("8000")


async def test_an_xyz_close_emits_an_alert(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions(
        "0xaaa", [position(coin="xyz:BB", size_usd="6000", leverage="3")], dex="xyz"
    )
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions(
        "0xaaa",
        [position(coin="xyz:BB", size_usd="6000", leverage="3", unrealized_pnl="900")],
        dex="xyz",
    )
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [], dex="xyz")
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    (row,) = await alerts(pool)
    assert row["kind"] == "close"
    assert row["coin"] == "xyz:BB"
    assert row["prev_side"] == "long"
    assert row["realized_pnl"] == Decimal("900")
    assert await pool.fetchval("SELECT count(*) FROM position_snapshots") == 0


async def test_an_xyz_flip_emits_one_flip_alert(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions(
        "0xaaa", [position(coin="xyz:SNDK", side=Side.LONG, unrealized_pnl="200")], dex="xyz"
    )
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions(
        "0xaaa", [position(coin="xyz:SNDK", side=Side.SHORT, size_usd="12000")], dex="xyz"
    )
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 1
    (row,) = await alerts(pool)
    assert row["kind"] == "flip"
    assert row["coin"] == "xyz:SNDK"
    assert row["prev_side"] == "long"
    assert row["side"] == "short"
    assert row["size_usd"] == Decimal("12000")


async def test_xyz_and_core_positions_on_one_trader_track_independently(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A change on one venue must not read as an open/close on the other — the
    namespaced coin keeps the two snapshot sets from mixing."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    gateway.set_positions("0xaaa", [position(coin="xyz:META", side=Side.SHORT)], dex="xyz")
    await baseline(pool, gateway, clock)

    clock.advance(30)
    # Core opens SOL and is otherwise unchanged; xyz closes META. Neither venue's
    # move should touch the other's still-open position.
    gateway.set_positions("0xaaa", [position(coin="BTC"), position(coin="SOL")])
    gateway.set_positions("0xaaa", [], dex="xyz")
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 2
    kinds = {(r["kind"], r["coin"]) for r in await alerts(pool)}
    assert kinds == {("open", "SOL"), ("close", "xyz:META")}
    # BTC (core) survived untouched; only SOL was added and xyz:META removed.
    coins = {r["coin"] for r in await pool.fetch("SELECT coin FROM position_snapshots")}
    assert coins == {"BTC", "SOL"}


async def test_same_symbol_on_core_and_xyz_never_collides(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Even if a symbol existed on both venues, the `xyz:` namespace keeps the
    snapshots separate — closing one leaves the other alerting-clean."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="META", side=Side.LONG)])
    gateway.set_positions("0xaaa", [position(coin="xyz:META", side=Side.SHORT)], dex="xyz")
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [], dex="xyz")  # only the xyz leg closes
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.events == 1
    (row,) = await alerts(pool)
    assert row["kind"] == "close" and row["coin"] == "xyz:META"
    coins = {r["coin"] for r in await pool.fetch("SELECT coin FROM position_snapshots")}
    assert coins == {"META"}  # the core leg is still open and un-alerted


async def test_a_partial_fetch_alerts_nothing_and_keeps_the_baseline(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """If the xyz call fails after the core call succeeds, the pass must not
    apply a half-poll — that would read xyz positions as all-closed. The wallet
    is counted failed and retried next pass, its snapshots untouched."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    gateway.set_positions("0xaaa", [position(coin="xyz:META")], dex="xyz")
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.positions_errors_by_dex[("0xaaa", "xyz")] = GatewayError("xyz info API timeout")
    result = await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert result.failed == 1 and result.polled == 0 and result.events == 0
    assert await alerts(pool) == []
    coins = {r["coin"] for r in await pool.fetch("SELECT coin FROM position_snapshots")}
    assert coins == {"BTC", "xyz:META"}  # baseline intact, ready to diff next pass


# --- scale-in / scale-out (issue #10) ---------------------------------------
#
# A same-coin/same-side size change at or above SCALE_SIGNIFICANCE_THRESHOLD
# alerts as SCALE-IN (bigger) or SCALE-OUT (smaller); anything below stays a
# silent snapshot update. The alert carries the size it grew/shrank from.


async def _scale_from(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    *,
    before: str,
    after: str,
    pnl: str = "0",
) -> list[asyncpg.Record]:
    """Baseline a BTC-long at `before`, then repoll it at `after` (carrying `pnl`
    of unrealized PnL); return alerts."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(size_usd=before)])
    await baseline(pool, gateway, clock)
    clock.advance(30)
    gateway.set_positions("0xaaa", [position(size_usd=after, unrealized_pnl=pnl)])
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)
    return await alerts(pool)


async def test_a_large_add_emits_a_scale_in_with_both_sizes(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    rows = await _scale_from(pool, gateway, clock, before="10000", after="20000", pnl="3000")

    (row,) = rows
    assert row["kind"] == "scale_in"
    assert row["coin"] == "BTC"
    assert row["side"] == "long"
    assert row["prev_size_usd"] == Decimal("10000")
    assert row["size_usd"] == Decimal("20000")
    # The position's live return on margin rides along (3000 against 20000/5x),
    # so the alert can show whether the trade is winning (issue #35).
    assert row["pct_return"] == Decimal("3000") / Decimal("4000")
    # The snapshot now carries the new size but keeps the original opened_at.
    snapshot = await pool.fetchrow("SELECT * FROM position_snapshots")
    assert snapshot is not None
    assert snapshot["size_usd"] == Decimal("20000")


async def test_a_large_trim_emits_a_scale_out(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    rows = await _scale_from(pool, gateway, clock, before="10000", after="6000")

    (row,) = rows
    assert row["kind"] == "scale_out"
    assert row["prev_size_usd"] == Decimal("10000")
    assert row["size_usd"] == Decimal("6000")


async def test_a_change_just_below_the_threshold_stays_silent(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # +24% < 25% threshold: still an ordinary silent update.
    rows = await _scale_from(pool, gateway, clock, before="10000", after="12400")

    assert rows == []
    snapshot = await pool.fetchrow("SELECT * FROM position_snapshots")
    assert snapshot is not None
    assert snapshot["size_usd"] == Decimal("12400")  # snapshot still tracks the drift


async def test_a_change_exactly_at_the_threshold_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # +25% == threshold fires (>= is the boundary).
    rows = await _scale_from(pool, gateway, clock, before="10000", after="12500")

    (row,) = rows
    assert row["kind"] == "scale_in"


async def test_scale_respects_the_snapshot_baseline_not_the_original_open(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Each poll measures against the last snapshot, so gradual drift that never
    clears the threshold in one step stays silent even as it accumulates."""
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(size_usd="10000")])
    await baseline(pool, gateway, clock)

    for size in ("11500", "13000", "14500"):  # ~13-15% steps, each below 25%
        clock.advance(30)
        gateway.set_positions("0xaaa", [position(size_usd=size)])
        await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    assert await alerts(pool) == []  # drift never clears the threshold in one poll


async def test_scale_fires_on_the_xyz_venue_too(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    gateway.set_positions("0xaaa", [position(coin="xyz:META", size_usd="8000")], dex="xyz")
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="xyz:META", size_usd="16000")], dex="xyz")
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    (row,) = await alerts(pool)
    assert row["kind"] == "scale_in" and row["coin"] == "xyz:META"


async def test_an_mkts_open_emits_an_alert_naming_the_market(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The mkts venue (Markets by Kinetiq: index perps like mkts:US500) is
    covered exactly like xyz — namespaced coins, same diff machinery."""
    await track(pool, clock, "0xaaa", 42)
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="mkts:US500")], dex="mkts")
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)

    rows = await alerts(pool)
    assert len(rows) == 1
    assert rows[0]["kind"] == "open"
    assert rows[0]["coin"] == "mkts:US500"
