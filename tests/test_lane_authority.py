"""Who produces position events, and how ownership moves (issue #158).

The cutover's whole safety argument is one rule: **exactly one lane writes
authoritative events for a (Trader, coin) at any instant**, and ownership moves
between the lanes on evidence rather than on a human's judgement. These tests
drive the seam that decides it — `epigone.lane_authority` — through the four
transitions that matter:

- websocket healthy → it owns production, the poller reads and stays quiet;
- websocket silent → the poller escalates fast, without a supervisor;
- websocket back → ownership returns only after SUSTAINED health AND after the
  lane has re-established absolute state, never on the first fresh heartbeat;
- the operator's kill switch → the poller owns, permanently and immediately.

Seam per the house convention: fake clock, real Postgres. The heartbeat the
decision reads is the existing `process_heartbeats` row (ADR-0002 — processes
meet only in Postgres), written here exactly as the ws lane writes it.
"""

from datetime import UTC, datetime

import asyncpg
import pytest

from epigone.lane_authority import (
    POLL_OWNER,
    WS_HEARTBEAT_STALE_SECONDS,
    WS_OWNER,
    WS_RECOVERY_SECONDS,
    evaluate_authority,
    read_authority,
)
from epigone.ws.lane import WS_LANE_PROCESS
from tests.support.clock import FakeClock


async def ws_beating(pool: asyncpg.Pool, when: datetime) -> None:
    """The ws lane's heartbeat, written the way the lane writes it."""
    from epigone.safety.heartbeat import beat

    await beat(pool, WS_LANE_PROCESS, when)


async def track(pool: asyncpg.Pool, clock: FakeClock, address: str) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        clock.now(),
    )
    await pool.execute("INSERT INTO users (telegram_id) VALUES (1) ON CONFLICT DO NOTHING")
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address) VALUES (1, $1)", address
    )


async def resynced(pool: asyncpg.Pool, address: str, when: datetime) -> None:
    """The ws lane's per-Trader resync stamp — its half of the handback."""
    await pool.execute(
        """
        INSERT INTO ws_lane_state (trader_address, baselined_at, resynced_at)
        VALUES ($1, $2, $2)
        ON CONFLICT (trader_address) DO UPDATE SET resynced_at = EXCLUDED.resynced_at
        """,
        address,
        when,
    )


async def test_the_poller_owns_production_before_any_websocket_has_beaten(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """A deploy with no ws lane running is the pre-cutover world, and it must be
    the state the system boots into: REST owns, nothing waits for a promotion."""
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == POLL_OWNER


async def test_a_silent_websocket_escalates_the_poller_within_the_staleness_window(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The failover: ownership follows the heartbeat, and the poller decides for
    itself — no supervisor, no second piece of state."""
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)
    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    assert (await evaluate_authority(pool, clock)).owner == WS_OWNER

    clock.advance(WS_HEARTBEAT_STALE_SECONDS + 1)
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == POLL_OWNER
    assert "heartbeat" in authority.reason


async def test_a_freshly_recovered_websocket_does_not_get_ownership_back_at_once(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Escalate fast, de-escalate slow: one fresh heartbeat is a flap, not
    health, and thrashing ownership is worse than staying degraded."""
    await ws_beating(pool, clock.now())

    authority = await evaluate_authority(pool, clock)

    assert authority.owner == POLL_OWNER


async def test_sustained_websocket_health_returns_ownership(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)

    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == WS_OWNER


async def test_a_flap_inside_the_recovery_window_restarts_the_clock(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Sustained means uninterrupted. A lane that goes quiet halfway through its
    probation serves the whole probation again."""
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)

    clock.advance(WS_RECOVERY_SECONDS - 1)
    await evaluate_authority(pool, clock)  # the beat is now stale: probation resets
    clock.advance(WS_HEARTBEAT_STALE_SECONDS)
    await ws_beating(pool, clock.now())
    clock.advance(2)
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == POLL_OWNER


async def test_ownership_returns_only_after_the_lane_re_established_absolute_state(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The handback's other half. A websocket delivers what happens from the
    moment you subscribe, so a lane that streamed through a degraded window
    without re-reading absolute state may be diffing against memory the world
    moved past. Its resync stamp — per Trader, in its own table — is the
    evidence, and ownership waits for it."""
    await track(pool, clock, "0xaaa")
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)
    took_over_at = (await read_authority(pool)).since

    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    stale_resync = await evaluate_authority(pool, clock)
    assert stale_resync.owner == POLL_OWNER

    await resynced(pool, "0xaaa", took_over_at)
    clock.advance(1)
    await ws_beating(pool, clock.now())
    authority = await evaluate_authority(pool, clock)

    assert authority.owner == WS_OWNER


async def test_the_kill_switch_keeps_the_poller_authoritative(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """One operator-facing escape hatch: the cutover can be undone without a
    code change, and undoing it must be immediate — not another probation."""
    await ws_beating(pool, clock.now())
    await evaluate_authority(pool, clock)
    clock.advance(WS_RECOVERY_SECONDS + 1)
    await ws_beating(pool, clock.now())
    assert (await evaluate_authority(pool, clock)).owner == WS_OWNER

    authority = await evaluate_authority(pool, clock, enabled=False)

    assert authority.owner == POLL_OWNER
    assert "disabled" in authority.reason


@pytest.mark.parametrize("beaten_at", [None, datetime(2020, 1, 1, tzinfo=UTC)])
async def test_a_websocket_that_never_ran_is_not_a_websocket_that_is_healthy(
    pool: asyncpg.Pool, clock: FakeClock, beaten_at: datetime | None
) -> None:
    if beaten_at is not None:
        await ws_beating(pool, beaten_at)

    assert (await evaluate_authority(pool, clock)).owner == POLL_OWNER
