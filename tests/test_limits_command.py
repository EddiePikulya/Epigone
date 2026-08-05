"""/limits — the operator's window onto the risk policy (issue #137 §7).

Operator-only; one knob per command; every change audited old → new; and the
executor re-reads the row each cycle, so a change lands without a restart.
"""

import json
from decimal import Decimal

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.access import ADMIN_ONLY_TEXT
from epigone.bot.handlers import build_router
from epigone.execute import limits as risk_limits
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.telegram import HTML_PARSE_MODE, RecordingSession, feed_text

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


async def test_a_rejected_value_leaves_neither_the_row_nor_the_trail_touched(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
) -> None:
    """The change and its audit row commit together, so the two can never
    disagree about whether a limit moved."""
    await feed_text(admin_dp, bot, "/limits max_leverage 7.5", user_id=ADMIN)

    assert (await risk_limits.load(pool)).backstop_leverage == 20
    assert await pool.fetchval(
        "SELECT count(*) FROM execution_audit WHERE action = 'risk_limit_changed'"
    ) == 0


async def test_setting_one_knob_leaves_the_others_exactly_as_they_were(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
) -> None:
    """One knob per command means one COLUMN per command: the write names the
    knob that moved and re-states nothing else, so it cannot carry a stale copy
    of a value someone changed in between."""
    await feed_text(admin_dp, bot, "/limits floor_oi 250000", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/limits coin_stake 275", user_id=ADMIN)

    limits = await risk_limits.load(pool)
    assert limits.floor_open_interest_usd == Decimal("250000")
    assert limits.max_coin_stake_usd == Decimal("275")
    assert limits.floor_day_notional_usd == risk_limits.DEFAULT_FLOOR_DAY_NOTIONAL_USD
    assert limits.max_sub_stake_usd == risk_limits.DEFAULT_MAX_SUB_STAKE_USD
    assert limits.backstop_leverage == risk_limits.DEFAULT_BACKSTOP_LEVERAGE


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


async def test_a_round_dollar_knob_reads_as_a_plain_number_not_scientific_notation(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    """Postgres hands a round NUMERIC back as an exponent-carrying Decimal
    (100000 decodes to Decimal('1.0E+5')), so a knob rendered with plain str()
    reaches the operator as `$1.0E+5` — observed live on the first /limits."""
    await feed_text(admin_dp, bot, "/limits floor_volume 100000", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/limits", user_id=ADMIN)

    listing = session.sent_messages()[-1].text or ""
    assert "$100000" in listing
    assert "E+" not in listing


async def test_the_audit_trail_carries_the_plain_number_the_reply_carries(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """The old value in `risk_limit_changed` is read back from the row, so it
    is exactly where the exponent form gets in — and the trail is the copy
    nobody can re-read in chat to work out what `$1.0E+5` meant."""
    await feed_text(admin_dp, bot, "/limits floor_volume 100000", user_id=ADMIN)
    await feed_text(admin_dp, bot, "/limits floor_volume 50000", user_id=ADMIN)

    row = await pool.fetchrow(
        "SELECT * FROM execution_audit WHERE action = 'risk_limit_changed' "
        "ORDER BY id DESC LIMIT 1"
    )
    assert row is not None
    assert "floor_volume: $100000 → $50000" in row["risk_decision"]
    detail = json.loads(row["detail"])
    assert (detail["old"], detail["new"]) == ("$100000", "$50000")


async def test_the_knob_listing_reads_as_labels_and_values_not_as_markup(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    """The listing is written in HTML and, before #185, sent with no parse
    mode — so the operator's first /limits after A5 arrived as literal
    `<b>floor_volume</b>`."""
    await feed_text(admin_dp, bot, "/limits", user_id=ADMIN)

    reply = session.sent_messages()[-1]
    assert reply.parse_mode == HTML_PARSE_MODE
    assert "floor_volume $100000" in session.rendered(reply)
    assert "<b>" not in session.rendered(reply)


async def test_the_usage_line_reads_as_placeholders_not_as_entities(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(admin_dp, bot, "/limits coin_stake 250 extra", user_id=ADMIN)

    usage = session.rendered(session.sent_messages()[-1])
    assert "/limits <knob> <value>" in usage
    assert "&lt;" not in usage


async def test_an_operators_own_markup_comes_back_as_text_not_as_markup(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """The error replies quote what was typed, so the operator's words reach an
    HTML message. Escaped, they read back as themselves; unescaped, Telegram
    would either swallow them or reject the whole reply."""
    await feed_text(admin_dp, bot, "/limits <b>coin_stake</b> 250", user_id=ADMIN)

    reply = session.sent_messages()[-1]
    assert "unknown limit '<b>coin_stake</b>'" in session.rendered(reply)
    assert (await risk_limits.load(pool)).max_coin_stake_usd == Decimal("300")
