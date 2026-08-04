"""/copy, /uncopy and /copies (issue #136, ADR-0007 decision 12).

Operator-only and hard-gated; /copy confirms before acting because it moves
money; and what it writes is INTENT — the bot process holds no signer, so the
sub-account is created and funded by the execute process, not here.
"""

from decimal import Decimal

import asyncpg
import pytest
from aiogram import Bot, Dispatcher

from epigone.bot.access import ADMIN_ONLY_TEXT
from epigone.bot.copy import COPY_CANCEL_CALLBACK, COPY_CONFIRM_PREFIX, NOT_COPYING_TEXT
from epigone.bot.handlers import build_router
from epigone.execute import episodes as ep
from epigone.execute import subs as subs_store
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock
from tests.support.copy import LEADER, SUB, seed_trader
from tests.support.telegram import RecordingSession, feed_callback, feed_text

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


async def test_copy_is_operator_only_on_the_command_and_on_the_tap(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """Two gates, not one: the bot has other users, and a callback payload is
    client-forgeable — so a guest who somehow taps the confirm button is
    refused there too."""
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=GUEST)
    assert (session.sent_messages()[-1].text or "") == ADMIN_ONLY_TEXT

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=GUEST)
    assert (session.callback_answers()[-1].text or "") == ADMIN_ONLY_TEXT
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_copy_confirms_before_writing_anything(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    # It moves money, so it takes the /resume shape: state what will happen,
    # then commit on a tap. Nothing is written until the tap.
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=ADMIN)

    prompt = session.sent_messages()[-1].text or ""
    assert "hard exposure cap" in prompt
    assert "TESTNET only" in prompt
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["leader_address"] == LEADER
    assert row["allocation_usd"] == Decimal("1000")
    assert row["base_notional_usd"] == Decimal("200")
    assert row["enabled"] is True
    # INTENT, not exchange state: the bot holds no signer, so provisioning is
    # the execute process's job and this row says so.
    assert row["sub_address"] is None and row["provisioned_at"] is None
    assert "next loop" in (session.edited_messages()[-1].text or "")


async def test_cancelling_the_prompt_writes_nothing(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CANCEL_CALLBACK, user_id=ADMIN)
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_a_bracket_copy_records_its_percentages_and_says_what_they_do(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 bracket 10 5", user_id=ADMIN)
    # The episode rule (g1) is surprising enough that the prompt states it.
    assert "copy episode is over" in (session.sent_messages()[-1].text or "")

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["copy_mode"] == "bracket"
    assert row["take_profit_pct"] == Decimal("10")
    assert row["stop_loss_pct"] == Decimal("5")


async def test_the_v0_ceilings_refuse_before_the_operator_is_asked_to_approve(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, f"/copy {LEADER} 500000 200 default", user_id=ADMIN)
    assert "v0 policy DECLINED" in (session.sent_messages()[-1].text or "")
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


@pytest.mark.parametrize(
    "args",
    [
        "0xnope 1000 200 default",  # not an address
        f"{LEADER} 1000 200 turbo",  # unknown mode
        f"{LEADER} -5 200 default",  # non-positive money
        f"{LEADER} 1000 200 bracket",  # bracket needs both percentages
        f"{LEADER} 1000 200 default 10 5",  # default takes none
    ],
)
async def test_a_malformed_copy_explains_itself_and_writes_nothing(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    args: str,
) -> None:
    await feed_text(admin_dp, bot, f"/copy {args}", user_id=ADMIN)
    assert "Usage:" in (session.sent_messages()[-1].text or "")
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_copying_an_unknown_wallet_is_refused_rather_than_created(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # copy_subs references traders. A wallet Epigone has never observed
    # produces no events, so there is nothing to copy — that is a typo.
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    assert "never seen" in (session.edited_messages()[-1].text or "")
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_recopying_reuses_the_existing_sub_account(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    """Sub-accounts cannot be deleted and a master holds at most ten (finding
    10), so a second /copy for the same leader must re-enable the existing
    mapping rather than mint another sub."""
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    sub_id = await pool.fetchval("SELECT id FROM copy_subs")
    await subs_store.record_sub_address(pool, sub_id, SUB)
    await subs_store.disable_sub(pool, operator_id=ADMIN, leader_address=LEADER)

    await feed_text(admin_dp, bot, f"/copy {LEADER} 800 100 default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)

    rows = await pool.fetch("SELECT * FROM copy_subs")
    assert len(rows) == 1
    assert rows[0]["id"] == sub_id and rows[0]["sub_address"] == SUB
    assert rows[0]["enabled"] is True
    assert rows[0]["allocation_usd"] == Decimal("800")
    assert "Re-enabled" in (session.edited_messages()[-1].text or "")


async def test_uncopy_stops_copying_and_never_flattens(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    """Decision 12, consistent with decision 10's never-auto-fix: disabling
    stops the copying, not the risk — and the reply says exactly that, with
    the count, so the operator can act."""
    await seed_trader(pool, clock)
    sub = await subs_store.register_sub(
        pool,
        operator_id=ADMIN,
        leader_address=LEADER,
        sub_name="epicopy-1",
        allocation_usd=Decimal("1000"),
        base_notional_usd=Decimal("200"),
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        now=clock.now(),
    )
    await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )

    await feed_text(admin_dp, bot, f"/uncopy {LEADER}", user_id=ADMIN)

    reply = session.sent_messages()[-1].text or ""
    assert "Stopped copying" in reply
    assert "were NOT closed" in reply
    assert "1 position(s) are still OPEN" in reply
    assert await subs_store.enabled_subs(pool, ADMIN) == []


async def test_uncopying_a_wallet_we_never_copied_says_so(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession
) -> None:
    await feed_text(admin_dp, bot, f"/uncopy {LEADER}", user_id=ADMIN)
    assert session.sent_messages()[-1].text or "" == NOT_COPYING_TEXT


async def test_copies_lists_the_mappings_and_their_state(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)

    await feed_text(admin_dp, bot, "/copies", user_id=ADMIN)
    listing = session.sent_messages()[-1].text or ""
    assert "provisioning" in listing  # pending until the executor funds it
    assert LEADER in listing
