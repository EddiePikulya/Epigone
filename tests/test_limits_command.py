"""/limits — the operator's window onto the risk policy (issue #137 §7).

Operator-only; one knob per command; every change audited old → new; and the
executor re-reads the row each cycle, so a change lands without a restart.
"""

from decimal import Decimal

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.access import ADMIN_ONLY_TEXT
from epigone.bot.handlers import build_router
from epigone.execute import limits as risk_limits
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.telegram import RecordingSession, feed_text

ADMIN = 370818090
GUEST = 111


@pytest.fixture
def admin_dp(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher["pool"] = pool
    dispatcher["gateway"] = gateway
    dispatcher["clock"] = clock
    dispatcher["admin_telegram_id"] = ADMIN
    dispatcher["drafts"] = {}
    dispatcher["min_size_pending"] = {}
    dispatcher["rename_pending"] = {}
    dispatcher["copy_pending"] = {}
    dispatcher.include_router(build_router())
    return dispatcher


async def test_limits_is_operator_only(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, "/limits coin_stake 900", user_id=GUEST)
    assert (session.sent_messages()[-1].text or "") == ADMIN_ONLY_TEXT
    assert (await risk_limits.load(pool)).max_coin_stake_usd == Decimal("300")


async def test_bare_limits_shows_every_knob_and_says_nobody_has_moved_them(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(admin_dp, bot, "/limits", user_id=ADMIN)
    listing = session.sent_messages()[-1].text or ""
    for entry in risk_limits.KNOBS:
        assert entry.name in listing
    assert "$300" in listing and "20x" in listing
    # "Nobody has ever moved these" is different information from "someone set
    # them and this is what they chose".
    assert "shipped defaults" in listing


async def test_setting_a_knob_reports_old_to_new_and_audits_it(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
) -> None:
    await feed_text(admin_dp, bot, "/limits coin_stake 250", user_id=ADMIN)

    reply = session.sent_messages()[-1].text or ""
    assert "$300 → $250" in reply
    assert (await risk_limits.load(pool)).max_coin_stake_usd == Decimal("250")

    row = await pool.fetchrow(
        "SELECT * FROM execution_audit WHERE action = 'risk_limit_changed'"
    )
    assert row is not None
    assert "coin_stake: $300 → $250" in row["risk_decision"]
    assert row["actor"] == "operator"


async def test_a_floor_can_be_turned_off_but_a_stake_cap_cannot_be_zeroed(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
) -> None:
    """Zero means something for a floor — the floor is a default stance, not a
    cage — and nothing for a stake cap: "copy nothing" is what /uncopy says,
    and it says it reversibly."""
    await feed_text(admin_dp, bot, "/limits floor_volume 0", user_id=ADMIN)
    assert "(off)" in (session.sent_messages()[-1].text or "")
    assert (await risk_limits.load(pool)).floor_day_notional_usd == Decimal("0")

    await feed_text(admin_dp, bot, "/limits sub_stake 0", user_id=ADMIN)
    assert "must be at least" in (session.sent_messages()[-1].text or "")
    assert (await risk_limits.load(pool)).max_sub_stake_usd == Decimal("900")


async def test_a_leverage_backstop_must_be_a_whole_number(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # updateLeverage carries an integer on the wire; a 2.5 accepted here would
    # be truncated by a rounding rule nobody stated.
    await feed_text(admin_dp, bot, "/limits max_leverage 7.5", user_id=ADMIN)
    assert "whole number" in (session.sent_messages()[-1].text or "")
    assert (await risk_limits.load(pool)).backstop_leverage == 20


async def test_an_unknown_knob_names_the_ones_that_exist(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(admin_dp, bot, "/limits max_drawdown 5", user_id=ADMIN)
    reply = session.sent_messages()[-1].text or ""
    assert "unknown limit" in reply and "coin_stake" in reply


async def test_a_missing_limits_row_reads_as_the_defaults_not_as_no_limits(
    pool: asyncpg.Pool,
) -> None:
    """Absence is not permission: a policy that treated a vanished row as "no
    limits" would turn a bad migration into an unbounded order."""
    await pool.execute("DELETE FROM risk_limits")
    limits = await risk_limits.load(pool)
    assert limits.max_coin_stake_usd == risk_limits.DEFAULT_MAX_COIN_STAKE_USD
    assert limits.backstop_leverage == risk_limits.DEFAULT_BACKSTOP_LEVERAGE


async def test_a_knob_set_on_a_missing_row_recreates_it(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # The command that configures the policy is also the way back from a row
    # that went missing — repairable from Telegram, not only from psql.
    await pool.execute("DELETE FROM risk_limits")
    await feed_text(admin_dp, bot, "/limits sub_stake 500", user_id=ADMIN)
    limits = await risk_limits.load(pool)
    assert limits.max_sub_stake_usd == Decimal("500")
    assert limits.max_coin_stake_usd == risk_limits.DEFAULT_MAX_COIN_STAKE_USD
