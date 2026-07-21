"""Ticket #23 acceptance: a User is capped at 15 tracked wallets. The cap is
enforced at the one shared seam (`_track_address`), so it holds identically
across all three follow paths — paste, screener row, and trader profile. Tested
here at the handler seam, one path per acceptance bullet."""

import asyncpg
from aiogram import Bot, Dispatcher

from epigone.bot.handlers import MAX_TRACKED_WALLETS
from tests.support.telegram import RecordingSession, feed_callback, feed_text
from tests.test_screener_ux import _button_texts, _callback_data, add_trader

USER = 111


def _addr(i: int) -> str:
    """A distinct, well-formed lowercase address (0x + 40 hex chars)."""
    return "0x" + f"{i:040x}"


async def _tracked(pool: asyncpg.Pool, user_id: int) -> list[str]:
    rows = await pool.fetch(
        "SELECT trader_address FROM tracks WHERE user_telegram_id = $1 ORDER BY tracked_at",
        user_id,
    )
    return [r["trader_address"] for r in rows]


async def _fill_to_cap(dp: Dispatcher, bot: Bot, user_id: int = USER) -> list[str]:
    """Follow exactly MAX_TRACKED_WALLETS distinct wallets by pasting them."""
    addresses = [_addr(i) for i in range(MAX_TRACKED_WALLETS)]
    for address in addresses:
        await feed_text(dp, bot, address, user_id=user_id)
    return addresses


def test_the_limit_is_a_single_named_constant_set_to_15() -> None:
    assert MAX_TRACKED_WALLETS == 15


async def test_following_the_fifteenth_wallet_still_succeeds(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    addresses = await _fill_to_cap(dp, bot)

    assert await _tracked(pool, USER) == addresses  # all 15 landed
    assert "tracking" in (session.sent_messages()[-1].text or "").lower()


async def test_pasting_a_sixteenth_wallet_is_refused_and_not_added(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _fill_to_cap(dp, bot)

    await feed_text(dp, bot, _addr(99), user_id=USER)

    tracked = await _tracked(pool, USER)
    assert len(tracked) == MAX_TRACKED_WALLETS  # the 16th was not added
    assert _addr(99) not in tracked
    text = (session.sent_messages()[-1].text or "").lower()
    assert "limit" in text
    assert "unfollow" in text  # tells the User how to make room


async def test_refollowing_an_already_tracked_wallet_at_the_cap_is_never_blocked(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    addresses = await _fill_to_cap(dp, bot)

    # Re-touch a wallet already tracked while at the cap — idempotent, allowed.
    await feed_text(dp, bot, addresses[0], user_id=USER)

    assert await _tracked(pool, USER) == addresses  # unchanged, still 15
    assert "already" in (session.sent_messages()[-1].text or "").lower()


async def test_unfollowing_frees_a_slot_for_a_new_follow(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    addresses = await _fill_to_cap(dp, bot)

    await feed_callback(dp, bot, f"unfollow:{addresses[0]}", user_id=USER)
    await feed_text(dp, bot, _addr(99), user_id=USER)  # the newly-freed slot

    tracked = await _tracked(pool, USER)
    assert len(tracked) == MAX_TRACKED_WALLETS
    assert _addr(99) in tracked
    assert addresses[0] not in tracked


async def test_screener_follow_at_the_cap_is_refused_and_not_added(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _fill_to_cap(dp, bot)
    await add_trader(pool, "0xstar", month_roi="2.0")  # the 16th, via the screener

    await feed_text(dp, bot, "/screener", user_id=USER)
    data = _callback_data(session.sent_messages()[-1].reply_markup)
    follow_data = next(d for d in data if d.startswith("sfollow:"))
    await feed_callback(dp, bot, follow_data, user_id=USER)

    assert "0xstar" not in await _tracked(pool, USER)  # not added
    assert "limit" in (session.callback_answers()[-1].text or "").lower()
    # The row still offers a Follow (nothing changed), not "Following".
    assert not any(
        "✓ Following" in t for t in _button_texts(session.edited_messages()[-1].reply_markup)
    )


async def test_profile_follow_at_the_cap_is_refused_and_not_added(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _fill_to_cap(dp, bot)
    await add_trader(pool, "0xstar", month_roi="2.0")

    await feed_callback(dp, bot, "profile:0xstar", user_id=USER)
    await feed_callback(dp, bot, "pfollow:0xstar", user_id=USER)

    assert "0xstar" not in await _tracked(pool, USER)  # not added
    assert "limit" in (session.callback_answers()[-1].text or "").lower()
    # Still not tracked, so the profile keeps offering a Follow.
    assert "pfollow:0xstar" in _callback_data(session.edited_messages()[-1].reply_markup)


async def test_the_admin_follows_past_the_cap(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    """The owner (#33) is cap-exempt: a sixteenth paste lands instead of being
    refused. Only the admin id — the exemption follows the id, not a flag a
    user could reach."""
    dp["admin_telegram_id"] = USER
    await _fill_to_cap(dp, bot)

    await feed_text(dp, bot, _addr(99), user_id=USER)

    tracked = await _tracked(pool, USER)
    assert len(tracked) == MAX_TRACKED_WALLETS + 1
    assert _addr(99) in tracked
    assert "tracking" in (session.sent_messages()[-1].text or "").lower()


async def test_a_non_admin_stays_capped_while_an_admin_exists(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    other = 222
    dp["admin_telegram_id"] = USER  # someone else is the admin

    await _fill_to_cap(dp, bot, user_id=other)
    await feed_text(dp, bot, _addr(99), user_id=other)

    tracked = await _tracked(pool, other)
    assert len(tracked) == MAX_TRACKED_WALLETS  # still refused
    assert _addr(99) not in tracked
