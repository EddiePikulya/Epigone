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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=GUEST)
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=ADMIN)

    prompt = session.sent_messages()[-1].text or ""
    assert "hard exposure cap" in prompt
    assert "isolated" in prompt  # the stake is the worst case, and the prompt says so
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["leader_address"] == LEADER
    assert row["allocation_usd"] == Decimal("1000")
    assert row["base_stake_usd"] == Decimal("200")
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=ADMIN)
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror bracket 10 5", user_id=ADMIN)
    # The episode rule (g1) is surprising enough that the prompt states it.
    assert "copy episode is over" in (session.sent_messages()[-1].text or "")

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["copy_mode"] == "bracket"
    assert row["take_profit_pct"] == Decimal("10")
    assert row["stop_loss_pct"] == Decimal("5")


async def test_the_caps_refuse_before_the_operator_is_asked_to_approve(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    # Judged against the LIVE limits row, before the confirm — the executor
    # will judge the same mapping against the same row on its next loop, and a
    # command that approved what the loop then declines would be a promise
    # nobody keeps.
    await feed_text(admin_dp, bot, f"/copy {LEADER} 500000 200 mirror default", user_id=ADMIN)
    assert "DECLINED" in (session.sent_messages()[-1].text or "")
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_a_stake_over_the_per_coin_cap_is_refused_rather_than_clamped_forever(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """A mapping configured above its own cap would be clamped on EVERY open —
    a copy that never does what the operator asked for. Better said now than
    absorbed silently at each entry."""
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 900 mirror default", user_id=ADMIN)
    reply = session.sent_messages()[-1].text or ""
    assert "per-coin stake cap" in reply and "/limits coin_stake" in reply
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


async def test_a_one_legged_bracket_is_accepted(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    """Decision 6 calls them "its own OPTIONAL TP% and SL%", and migration
    0033 accepts either leg alone — so the parser must too, or it would be the
    only place in the system saying otherwise. `-` omits a leg positionally,
    keeping the documented `<tp%> <sl%>` order readable."""
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror bracket - 5", user_id=ADMIN)
    prompt = session.sent_messages()[-1].text or ""
    assert "SL 5%" in prompt and "TP" not in prompt

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["take_profit_pct"] is None
    assert row["stop_loss_pct"] == Decimal("5")


async def test_the_leverage_argument_records_the_mode_and_says_what_it_means(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    """Amendment D-4: the configured dollars are MARGIN now, and the position
    is that times the mirrored leverage. Both halves are stated before the tap
    — an operator re-running a pre-A5 habit must read a different sentence,
    not the same one meaning something else."""
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 100 mirror default", user_id=ADMIN)
    prompt = session.sent_messages()[-1].text or ""
    assert "$100 of YOUR margin" in prompt
    assert "mirroring the leader" in prompt
    assert "$1000 position" in prompt  # $100 behind a 10x leader, spelled out

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["base_stake_usd"] == Decimal("100")
    assert row["leverage_mode"] == "mirror"
    assert row["fixed_leverage"] is None


async def test_a_fixed_leverage_is_recorded_as_a_number(
    admin_dp: Dispatcher,
    bot: Bot,
    session: RecordingSession,
    pool: asyncpg.Pool,
    clock: FakeClock,
) -> None:
    await seed_trader(pool, clock)
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 100 5 default", user_id=ADMIN)
    assert "a fixed 5x" in (session.sent_messages()[-1].text or "")

    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    row = await pool.fetchrow("SELECT * FROM copy_subs")
    assert row is not None
    assert row["leverage_mode"] == "fixed"
    assert row["fixed_leverage"] == 5


async def test_a_bracket_with_neither_leg_is_refused_as_default_in_disguise(
    admin_dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror bracket - -", user_id=ADMIN)
    assert "just default mode" in (session.sent_messages()[-1].text or "")
    assert await pool.fetchval("SELECT count(*) FROM copy_subs") == 0


@pytest.mark.parametrize(
    "args",
    [
        "0xnope 1000 200 mirror default",  # not an address
        f"{LEADER} 1000 200 mirror turbo",  # unknown mode
        f"{LEADER} -5 200 mirror default",  # non-positive money
        f"{LEADER} 1000 200 mirror bracket",  # bracket takes two positions, - to omit
        f"{LEADER} 1000 200 mirror bracket 10 nope",  # not a number and not -
        f"{LEADER} 1000 200 mirror default 10 5",  # default takes none
        f"{LEADER} 1000 200 2.5 default",  # updateLeverage takes an integer
        f"{LEADER} 1000 200 copy default",  # not `mirror` and not a number
        f"{LEADER} 1000 200 default",  # the pre-A5 signature, missing leverage
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=ADMIN)
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)
    sub_id = await pool.fetchval("SELECT id FROM copy_subs")
    await subs_store.record_sub_address(pool, sub_id, SUB)
    await subs_store.disable_sub(pool, operator_id=ADMIN, leader_address=LEADER)

    await feed_text(admin_dp, bot, f"/copy {LEADER} 800 100 4 default", user_id=ADMIN)
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
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
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
    await feed_text(admin_dp, bot, f"/copy {LEADER} 1000 200 mirror default", user_id=ADMIN)
    await feed_callback(admin_dp, bot, COPY_CONFIRM_PREFIX + "go", user_id=ADMIN)

    await feed_text(admin_dp, bot, "/copies", user_id=ADMIN)
    listing = session.sent_messages()[-1].text or ""
    assert "provisioning" in listing  # pending until the executor funds it
    assert LEADER in listing
