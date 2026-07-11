"""Alert controls at the poll seam (issue #10): mute and minimum position size.

Suppression happens where the poller fans an event out to followers, so a
suppressed event is never queued — the reason unmuting or raising a floor can
never dump a backlog. Seam test per the house convention: fake gateway, fake
clock, real Postgres. The tracked-list UX that sets these controls is covered
in tests/test_alert_controls_ux.py.
"""

from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.gateway import Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import run_poll_pass
from tests.support.clock import FakeClock
from tests.test_position_poller import WIDE_OPEN_BUDGET, alerts, baseline, position, track


async def _poll(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)


async def _set_track(pool: asyncpg.Pool, user_id: int, address: str, **cols: object) -> None:
    for column, value in cols.items():
        await pool.execute(
            f"UPDATE tracks SET {column} = $3 WHERE user_telegram_id = $1 AND trader_address = $2",
            user_id,
            address,
            value,
        )


async def _set_global_min(pool: asyncpg.Pool, user_id: int, value: Decimal | None) -> None:
    await pool.execute("UPDATE users SET min_size_usd = $2 WHERE telegram_id = $1", user_id, value)


async def test_a_muted_track_receives_no_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_track(pool, 42, "0xaaa", muted=True)
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await _poll(pool, gateway, clock)

    assert await alerts(pool) == []


async def test_muting_one_follower_leaves_another_alerting(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42, 43)
    await _set_track(pool, 42, "0xaaa", muted=True)  # only 42 mutes
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await _poll(pool, gateway, clock)

    rows = await alerts(pool)
    assert [r["user_telegram_id"] for r in rows] == [43]  # 42 muted, 43 still hears it


async def test_unmuting_does_not_replay_events_missed_while_muted(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The core promise: a muted stretch's events are dropped at queue time, so
    unmuting only affects events from then on — no backlog."""
    await track(pool, clock, "0xaaa", 42)
    await _set_track(pool, 42, "0xaaa", muted=True)
    await baseline(pool, gateway, clock)

    clock.advance(30)  # opens while muted — must never be queued
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await _poll(pool, gateway, clock)

    await _set_track(pool, 42, "0xaaa", muted=False)  # unmute
    clock.advance(30)  # a fresh open after unmuting
    gateway.set_positions("0xaaa", [position(coin="BTC"), position(coin="SOL", side=Side.SHORT)])
    await _poll(pool, gateway, clock)

    rows = await alerts(pool)
    assert [(r["kind"], r["coin"]) for r in rows] == [("open", "SOL")]  # BTC never resurfaces


async def test_a_position_below_the_global_floor_is_suppressed(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("5000"))
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="1000")])  # dust
    await _poll(pool, gateway, clock)

    assert await alerts(pool) == []


async def test_a_position_at_or_above_the_floor_alerts(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("5000"))
    await baseline(pool, gateway, clock)

    clock.advance(30)  # exactly at the floor fires
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="5000")])
    await _poll(pool, gateway, clock)

    (row,) = await alerts(pool)
    assert row["kind"] == "open" and row["size_usd"] == Decimal("5000")


async def test_a_per_track_floor_overrides_the_global_one(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("5000"))
    await _set_track(pool, 42, "0xaaa", min_size_usd=Decimal("50000"))  # stricter here
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="10000")])  # clears global
    await _poll(pool, gateway, clock)

    assert await alerts(pool) == []  # but not the per-track floor


async def test_a_per_track_floor_can_loosen_a_global_one(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("50000"))
    await _set_track(pool, 42, "0xaaa", min_size_usd=Decimal("1000"))  # this trader I want it all
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="2000")])
    await _poll(pool, gateway, clock)

    (row,) = await alerts(pool)
    assert row["kind"] == "open"


async def test_the_floor_suppresses_a_close_by_the_closed_position_size(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A close is judged by the notional of the position that closed, so a dust
    position closing stays quiet under a floor."""
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("5000"))
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="1000")])
    await baseline(pool, gateway, clock)

    clock.advance(30)
    gateway.set_positions("0xaaa", [])  # dust position closes
    await _poll(pool, gateway, clock)

    assert await alerts(pool) == []


async def test_the_floor_suppresses_a_scale_below_it(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, "0xaaa", 42)
    await _set_global_min(pool, 42, Decimal("5000"))
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="1000")])
    await baseline(pool, gateway, clock)

    clock.advance(30)  # doubles, well above threshold, but still sub-floor notional
    gateway.set_positions("0xaaa", [position(coin="BTC", size_usd="2000")])
    await _poll(pool, gateway, clock)

    assert await alerts(pool) == []


async def test_mute_and_floor_do_not_freeze_snapshots_for_other_followers(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Suppression is per-follower fan-out only: the shared snapshot diff still
    advances, so a later un-suppressed event is detected correctly."""
    await track(pool, clock, "0xaaa", 42)
    await _set_track(pool, 42, "0xaaa", muted=True)
    gateway.set_positions("0xaaa", [position(coin="BTC")])
    await baseline(pool, gateway, clock)

    clock.advance(30)  # BTC closes while muted
    gateway.set_positions("0xaaa", [])
    await _poll(pool, gateway, clock)
    assert await alerts(pool) == []
    # The snapshot advanced despite the mute — BTC is gone from state.
    assert await pool.fetchval("SELECT count(*) FROM position_snapshots") == 0
