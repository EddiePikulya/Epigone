"""One-time "first fine data landed" notice (issue #83).

Three seams, real Postgres throughout (the house convention):
- the DB store (record_follow_notice_state, mark_first_data_ready) exercised
  directly with the injected clock,
- the two writers wired end to end — Follow via the shared track_address seam,
  first data via run_fine_pass — proving a notice is queued exactly once and
  survives refollow / restart / wipe→reseed,
- delivery (deliver_first_data_notices) to the aiogram fake transport, with the
  profile:<address> button and the #73 delete row.
"""

from datetime import timedelta

import asyncpg
from aiogram import Bot

from epigone.bot.first_data_notice import deliver_first_data_notices
from epigone.bot.handlers import track_address
from epigone.budget import WeightBudget
from epigone.first_data_notice import mark_first_data_ready, record_follow_notice_state
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.ingest.fine import run_fine_pass
from tests.support.clock import FakeClock
from tests.support.fills import T0, fill
from tests.support.telegram import RecordingSession

WALLET = "0x94cc0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e2fbc"
OTHER = "0x" + "b" * 40
BUDGET = 1_000_000


# --- fixtures/helpers --------------------------------------------------------


def human_fills() -> list:
    """Enough fills for a fine scan to persist real metrics (mirrors test_fine_scan)."""
    return [
        fill("Open Long", order_id=1, at=T0, start_position="0", size="50", crossed=False),
        fill(pnl="100", order_id=2, at=T0 + timedelta(hours=1), start_position="50", size="50"),
        fill("Open Long", order_id=3, at=T0 + timedelta(days=1), start_position="0"),
        fill(pnl="-40", order_id=4, at=T0 + timedelta(days=1, hours=1)),
    ]


async def _add_trader(pool: asyncpg.Pool, clock: FakeClock, address: str) -> None:
    await pool.execute(
        "INSERT INTO traders (address, refresh_tier, first_seen_at, last_seen_at) "
        "VALUES ($1, 'active', $2, $2)",
        address,
        clock.now(),
    )


async def _seed_scanned(pool: asyncpg.Pool, clock: FakeClock, address: str) -> None:
    """A wallet that already carries real fine data — as if a prior scan saw
    fills: a fine_metrics row plus fine_checkpoint_at (the "has data" predicate)."""
    await _add_trader(pool, clock, address)
    await pool.execute(
        "INSERT INTO fine_metrics (address, trade_count, max_drawdown, realized_pnl, computed_at) "
        "VALUES ($1, 1, 0, 0, $2)",
        address,
        clock.now(),
    )
    await pool.execute(
        "UPDATE traders SET fine_checkpoint_at = $2 WHERE address = $1", address, clock.now()
    )


async def _follow(pool: asyncpg.Pool, clock: FakeClock, user_id: int, address: str) -> None:
    """A Follow through the shared track_address seam (records notice state)."""
    async with pool.acquire() as conn, conn.transaction():
        await track_address(conn, user_id, None, address, clock.now())


async def _notice(pool: asyncpg.Pool, user_id: int, address: str) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT * FROM first_data_notices WHERE user_telegram_id = $1 AND trader_address = $2",
        user_id,
        address,
    )


async def _scan(pool: asyncpg.Pool, clock: FakeClock, address: str, fills: list | None) -> None:
    gateway = FakeHyperliquidGateway()
    gateway.set_fills(address, human_fills() if fills is None else fills)
    await run_fine_pass(pool, gateway, WeightBudget(BUDGET, clock), clock)


# --- the store seam, directly ------------------------------------------------


async def test_follow_before_data_records_pending(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _add_trader(pool, clock, WALLET)
    await pool.execute("INSERT INTO users (telegram_id) VALUES (7)")

    async with pool.acquire() as conn:
        await record_follow_notice_state(conn, 7, WALLET, clock.now())

    row = await _notice(pool, 7, WALLET)
    assert row is not None and row["status"] == "pending"


async def test_follow_after_data_records_suppressed(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _seed_scanned(pool, clock, WALLET)
    await pool.execute("INSERT INTO users (telegram_id) VALUES (7)")

    async with pool.acquire() as conn:
        await record_follow_notice_state(conn, 7, WALLET, clock.now())

    row = await _notice(pool, 7, WALLET)
    assert row is not None and row["status"] == "suppressed"


async def test_record_is_idempotent_and_never_resets(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _add_trader(pool, clock, WALLET)
    await pool.execute("INSERT INTO users (telegram_id) VALUES (7)")
    async with pool.acquire() as conn:
        await record_follow_notice_state(conn, 7, WALLET, clock.now())
        await mark_first_data_ready(conn, WALLET)  # notice becomes ready
        # A re-follow (data now present) must not overwrite the ready row.
        await record_follow_notice_state(conn, 7, WALLET, clock.now())

    row = await _notice(pool, 7, WALLET)
    assert row is not None and row["status"] == "ready"


async def test_mark_ready_flips_only_pending(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _seed_scanned(pool, clock, WALLET)
    for uid in (1, 2, 3):
        await pool.execute("INSERT INTO users (telegram_id) VALUES ($1)", uid)
    # user 1 pending, user 2 suppressed, user 3 already delivered-ready
    await pool.execute(
        "INSERT INTO first_data_notices (user_telegram_id, trader_address, status, created_at) "
        "VALUES (1, $1, 'pending', $2), (2, $1, 'suppressed', $2), (3, $1, 'ready', $2)",
        WALLET,
        clock.now(),
    )
    await pool.execute(
        "UPDATE first_data_notices SET delivered_at = $1 WHERE user_telegram_id = 3",
        clock.now(),
    )

    async with pool.acquire() as conn:
        await mark_first_data_ready(conn, WALLET)

    assert (await _notice(pool, 1, WALLET))["status"] == "ready"
    assert (await _notice(pool, 2, WALLET))["status"] == "suppressed"
    row3 = await _notice(pool, 3, WALLET)
    assert row3["status"] == "ready" and row3["delivered_at"] is not None


# --- the two writers, end to end --------------------------------------------


async def test_first_data_after_follow_queues_one_ready_notice(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    assert (await _notice(pool, 111, WALLET))["status"] == "pending"

    await _scan(pool, clock, WALLET, None)

    assert (await _notice(pool, 111, WALLET))["status"] == "ready"


async def test_an_empty_first_scan_does_not_announce_data(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """A first scan that finds no fills writes a fine_metrics row but isn't "full
    track-record data" — the tracker stays 'pending' until real fills land, then
    is notified once."""
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)

    await _scan(pool, clock, WALLET, [])  # scanned, but no fills
    assert (await _notice(pool, 111, WALLET))["status"] == "pending"

    clock.advance(60 * 60 * 25)  # past the active refresh interval, so it's due again
    await _scan(pool, clock, WALLET, None)  # real fills now
    assert (await _notice(pool, 111, WALLET))["status"] == "ready"


async def test_following_an_already_scanned_wallet_never_notifies(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await _seed_scanned(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    assert (await _notice(pool, 111, WALLET))["status"] == "suppressed"

    await _scan(pool, clock, WALLET, None)  # a routine refresh

    assert (await _notice(pool, 111, WALLET))["status"] == "suppressed"


async def test_routine_refreshes_never_re_notify(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    await _scan(pool, clock, WALLET, None)  # first data → ready
    await pool.execute(
        "UPDATE first_data_notices SET status = 'ready', delivered_at = $1 "
        "WHERE user_telegram_id = 111",  # pretend delivered
        clock.now(),
    )

    clock.advance(60 * 60 * 24)
    await _scan(pool, clock, WALLET, None)  # a later routine refresh

    row = await _notice(pool, 111, WALLET)
    assert row["status"] == "ready" and row["delivered_at"] is not None  # untouched


async def test_multiple_trackers_each_get_one(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    await _follow(pool, clock, 222, WALLET)

    await _scan(pool, clock, WALLET, None)

    assert (await _notice(pool, 111, WALLET))["status"] == "ready"
    assert (await _notice(pool, 222, WALLET))["status"] == "ready"


async def test_refollow_does_not_duplicate(pool: asyncpg.Pool, clock: FakeClock) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    await _scan(pool, clock, WALLET, None)  # first data landed → ready
    await pool.execute(
        "DELETE FROM tracks WHERE user_telegram_id = 111"  # unfollow
    )
    await _follow(pool, clock, 111, WALLET)  # refollow (data present now)

    rows = await pool.fetch(
        "SELECT status FROM first_data_notices WHERE user_telegram_id = 111 "
        "AND trader_address = $1",
        WALLET,
    )
    assert [r["status"] for r in rows] == ["ready"]  # one row, still ready


async def test_wipe_and_reseed_does_not_re_notify_existing_trackers(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The 0008-style edge: fine data deleted and re-seeded must not re-notify a
    tracker who already saw the data (their row is not 'pending')."""
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 111, WALLET)
    await _scan(pool, clock, WALLET, None)  # first data → user 111 ready
    await pool.execute(
        "UPDATE first_data_notices SET delivered_at = $1 WHERE user_telegram_id = 111",
        clock.now(),
    )

    # Wipe the fine data (0008 scenario), then a later scan (due again past the
    # active refresh interval) re-seeds it — the flip runs but finds no 'pending'.
    await pool.execute("DELETE FROM fine_metrics WHERE address = $1", WALLET)
    await pool.execute("UPDATE traders SET fine_checkpoint_at = NULL WHERE address = $1", WALLET)
    clock.advance(60 * 60 * 25)
    await _scan(pool, clock, WALLET, None)

    row = await _notice(pool, 111, WALLET)
    assert row["status"] == "ready" and row["delivered_at"] is not None  # not re-queued


async def test_a_pre_feature_tracker_gets_no_notice(pool: asyncpg.Pool, clock: FakeClock) -> None:
    """No backfill: a Track that exists without a notice row (followed before this
    shipped) is never swept into a notice by a later scan."""
    await _add_trader(pool, clock, WALLET)
    await pool.execute("INSERT INTO users (telegram_id) VALUES (111)")
    await pool.execute(
        "INSERT INTO tracks (user_telegram_id, trader_address) VALUES (111, $1)", WALLET
    )

    await _scan(pool, clock, WALLET, None)

    assert await _notice(pool, 111, WALLET) is None


# --- delivery ----------------------------------------------------------------


async def test_a_ready_notice_is_delivered_with_a_profile_button(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 42, WALLET)
    await _scan(pool, clock, WALLET, None)

    delivered = await deliver_first_data_notices(pool, bot, clock)

    assert delivered == 1
    (message,) = session.sent_messages()
    assert message.chat_id == 42
    assert "full track-record data" in message.text
    assert "0x94cc…2fbc" in message.text
    # The profile tap-through comes first; the #73 🗑 delete row appends below it.
    (button,) = message.reply_markup.inline_keyboard[0]
    assert button.callback_data == f"profile:{WALLET}"
    assert message.reply_markup.inline_keyboard[-1][0].callback_data == "msgdel"


async def test_suppressed_and_pending_rows_are_not_delivered(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await _add_trader(pool, clock, OTHER)
    await _seed_scanned(pool, clock, WALLET)
    await _follow(pool, clock, 1, WALLET)  # suppressed
    await _follow(pool, clock, 2, OTHER)  # pending (no scan yet)

    assert await deliver_first_data_notices(pool, bot, clock) == 0
    assert session.sent_messages() == []


async def test_delivered_notices_are_never_resent(
    pool: asyncpg.Pool, bot: Bot, session: RecordingSession, clock: FakeClock
) -> None:
    await _add_trader(pool, clock, WALLET)
    await _follow(pool, clock, 42, WALLET)
    await _scan(pool, clock, WALLET, None)

    assert await deliver_first_data_notices(pool, bot, clock) == 1
    assert await deliver_first_data_notices(pool, bot, clock) == 0
    assert len(session.sent_messages()) == 1
