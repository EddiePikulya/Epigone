"""Issue #82: following a wallet marks it due-now for an immediate fine refresh,
unless its fine data is already fresh (~15 min). The bot only touches Postgres
(ADR-0002); the chunked, tracked-first pass (#65/#66) picks the wallet up within
minutes without any process talking to Hyperliquid."""

from datetime import datetime, timedelta

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.ingest.fine import FOLLOW_REFRESH_FRESHNESS, mark_due_on_follow
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_callback, follow_wallet

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
OTHER = "0x" + "1" * 40


async def _scan_state(pool: asyncpg.Pool, address: str) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT fine_refreshed_at, fine_attempted_at FROM traders WHERE address = $1",
        address,
    )


async def _seed_scanned_trader(
    pool: asyncpg.Pool,
    address: str,
    *,
    first_seen: datetime,
    refreshed_at: datetime | None,
    attempted_at: datetime | None,
) -> None:
    """A Trader already in the Universe with a prior fine scan history — so a
    later Follow either bumps or leaves it, depending on freshness."""
    await pool.execute(
        """
        INSERT INTO traders
            (address, refresh_tier, first_seen_at, last_seen_at,
             fine_refreshed_at, fine_attempted_at)
        VALUES ($1, 'active', $2, $2, $3, $4)
        """,
        address,
        first_seen,
        refreshed_at,
        attempted_at,
    )


# --- the store seam, exercised directly with the injected clock --------------


async def test_mark_due_clears_both_columns_for_a_stale_wallet(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    now = clock.now()
    await _seed_scanned_trader(
        pool,
        WHALE,
        first_seen=now - timedelta(days=30),
        refreshed_at=now - timedelta(hours=6),
        attempted_at=now - timedelta(hours=6),
    )

    bumped = await mark_due_on_follow(pool, WHALE, now)

    assert bumped is True
    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] is None  # due now
    assert state["fine_attempted_at"] is None  # sorts first


async def test_mark_due_skips_a_recently_refreshed_wallet(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    now = clock.now()
    fresh = now - timedelta(minutes=5)  # inside the freshness window
    await _seed_scanned_trader(
        pool, WHALE, first_seen=now - timedelta(days=30), refreshed_at=fresh, attempted_at=fresh
    )

    bumped = await mark_due_on_follow(pool, WHALE, now)

    assert bumped is False
    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] == fresh  # left untouched
    assert state["fine_attempted_at"] == fresh


async def test_mark_due_bumps_exactly_at_the_freshness_boundary(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # At exactly the window edge the data is no longer "fresh" — bump it.
    now = clock.now()
    edge = now - FOLLOW_REFRESH_FRESHNESS
    await _seed_scanned_trader(
        pool, WHALE, first_seen=now - timedelta(days=30), refreshed_at=edge, attempted_at=edge
    )

    bumped = await mark_due_on_follow(pool, WHALE, now)

    assert bumped is True
    state = await _scan_state(pool, WHALE)
    assert state is not None and state["fine_refreshed_at"] is None


# --- the follow paths, end to end through the handlers -----------------------


async def test_following_an_address_marks_the_wallet_due_now(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await follow_wallet(dp, bot, WHALE, user_id=111)

    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] is None
    assert state["fine_attempted_at"] is None


async def test_following_a_recently_scanned_wallet_does_not_rebump_it(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # A wallet another User already tracks: on the active cadence, scanned 5m ago.
    now = clock.now()
    fresh = now - timedelta(minutes=5)
    await _seed_scanned_trader(
        pool, WHALE, first_seen=now - timedelta(days=30), refreshed_at=fresh, attempted_at=fresh
    )

    await follow_wallet(dp, bot, WHALE, user_id=111)

    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] == fresh  # not re-bumped
    assert state["fine_attempted_at"] == fresh


async def test_following_a_stale_wallet_from_a_button_marks_it_due(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # The profile follow button (pfollow:) is the follow entry point now (#111);
    # the shared track_address seam means it bumps a stale wallet due.
    now = clock.now()
    stale = now - timedelta(days=2)
    await _seed_scanned_trader(
        pool, WHALE, first_seen=now - timedelta(days=30), refreshed_at=stale, attempted_at=stale
    )

    await feed_callback(dp, bot, f"pfollow:{WHALE}", user_id=111)

    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] is None
    assert state["fine_attempted_at"] is None


async def test_unfollow_leaves_scan_state_untouched(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # Follow makes it due; a later scan refreshes it; unfollowing must not re-bump.
    await follow_wallet(dp, bot, WHALE, user_id=111)
    scanned = clock.now()
    await pool.execute(
        "UPDATE traders SET fine_refreshed_at = $2, fine_attempted_at = $2 WHERE address = $1",
        WHALE,
        scanned,
    )

    await feed_callback(dp, bot, f"unfollow:{WHALE}", user_id=111)

    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] == scanned  # unchanged
    assert state["fine_attempted_at"] == scanned


async def test_refollowing_an_already_tracked_wallet_does_not_rebump(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    # First follow bumps it; a scan lands; re-tapping Follow is idempotent
    # (ALREADY_TRACKING) and must not force another refresh.
    await follow_wallet(dp, bot, WHALE, user_id=111)
    scanned = clock.now()
    await pool.execute(
        "UPDATE traders SET fine_refreshed_at = $2, fine_attempted_at = $2 WHERE address = $1",
        WHALE,
        scanned,
    )

    await follow_wallet(dp, bot, WHALE, user_id=111)  # re-follow, idempotent

    state = await _scan_state(pool, WHALE)
    assert state is not None
    assert state["fine_refreshed_at"] == scanned  # left on the cadence
    assert state["fine_attempted_at"] == scanned
