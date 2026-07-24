"""The focus-market builder flow (issue #108): the first non-numeric filter.
Picking Focus market shows a category-button row plus a Specific-ticker
prompt; a category tap completes the filter in one step, a typed ticker is
normalized and checked against coins the universe has actually played, and
the filter is never offered as a sort."""

from datetime import UTC, datetime, timedelta

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import InlineKeyboardMarkup

from epigone.criteria_store import list_criteria
from tests.support.telegram import RecordingSession, feed_callback, feed_text

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


async def add_trader(
    pool: asyncpg.Pool, address: str, *, display_name: str | None = None
) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $3)
        """,
        address,
        display_name,
        NOW,
    )
    await pool.execute(
        """
        INSERT INTO coarse_metrics
            (address, time_window, pnl, roi, volume, account_value, computed_at)
        VALUES ($1, 'month', 1000, 0.1, 50000, 10000, $2)
        """,
        address,
        NOW,
    )


async def add_round_trips(pool: asyncpg.Pool, address: str, *coins: str) -> None:
    for seq, coin in enumerate(coins):
        await pool.execute(
            """
            INSERT INTO fine_trades
                (address, coin, pnl, peak_notional, opened_at, closed_at, seq)
            VALUES ($1, $2, 100, 10000, $3, $3, $4)
            """,
            address,
            coin,
            NOW - timedelta(hours=seq),
            seq,
        )


def _callback_data(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.callback_data or "" for row in markup.inline_keyboard for b in row]


def _button_texts(markup: InlineKeyboardMarkup | None) -> list[str]:
    assert markup is not None
    return [b.text for row in markup.inline_keyboard for b in row]


async def _open_market_picker(dp: Dispatcher, bot: Bot, *, user_id: int) -> None:
    await feed_text(dp, bot, "/criteria", user_id=user_id)
    await feed_callback(dp, bot, "cnew", user_id=user_id)
    await feed_callback(dp, bot, "cfadd", user_id=user_id)
    await feed_callback(dp, bot, "cfm:focus_market", user_id=user_id)


async def test_focus_market_offers_categories_and_a_ticker_prompt(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _open_market_picker(dp, bot, user_id=111)

    picker = session.edited_messages()[-1]
    data = _callback_data(picker.reply_markup)
    assert ["cfc:CRYPTO", "cfc:STOCKS", "cfc:METALS", "cfc:ENERGY", "cft"] == [
        d for d in data if d.startswith(("cfc:", "cft"))
    ]
    assert "🥇 Metals" in _button_texts(picker.reply_markup)
    # No operator step — a market is picked, not compared.
    assert not any(d.startswith("cfo:") for d in data)


async def test_a_category_tap_completes_the_filter(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xmetals", display_name="Silverback")
    await add_round_trips(pool, "0xmetals", "xyz:SILVER", "xyz:GOLD", "BTC")
    await add_trader(pool, "0xcrypto", display_name="Cryptonaut")
    await add_round_trips(pool, "0xcrypto", "BTC", "ETH")

    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cfc:METALS", user_id=111)

    builder = session.edited_messages()[-1]
    assert "Focus market: Metals" in (builder.text or "")

    await feed_callback(dp, bot, "crun:d:0", user_id=111)
    results = session.edited_messages()[-1].text or ""
    assert "Silverback" in results
    assert "Cryptonaut" not in results


async def test_a_typed_ticker_completes_the_filter(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xsilver", display_name="Hi-yo")
    await add_round_trips(pool, "0xsilver", "xyz:SILVER", "xyz:SILVER", "BTC")

    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cft", user_id=111)
    prompt = session.edited_messages()[-1]
    assert "SILVER, BTC or SP500" in (prompt.text or "")

    await feed_text(dp, bot, " silver ", user_id=111)  # trimmed, uppercased
    built = session.sent_messages()[-1]
    assert "Focus market: SILVER" in (built.text or "")

    await feed_callback(dp, bot, "crun:d:0", user_id=111)
    assert "Hi-yo" in (session.edited_messages()[-1].text or "")


async def test_a_never_played_ticker_is_answered_helpfully(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xsilver")
    await add_round_trips(pool, "0xsilver", "xyz:SILVER")

    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cft", user_id=111)
    await feed_text(dp, bot, "PLUTONIUM", user_id=111)

    reply = session.sent_messages()[-1].text or ""
    assert "PLUTONIUM" in reply
    assert "match nobody" in reply

    # The prompt stays live — a corrected ticker still lands.
    await feed_text(dp, bot, "silver", user_id=111)
    assert "Focus market: SILVER" in (session.sent_messages()[-1].text or "")


async def test_gibberish_ticker_input_is_rejected_readably(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cft", user_id=111)
    await feed_text(dp, bot, "not a ticker!!", user_id=111)

    assert "couldn't read that as a ticker" in (session.sent_messages()[-1].text or "")


async def test_focus_market_is_not_a_sort_option(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    await feed_callback(dp, bot, "csort", user_id=111)

    picker = session.edited_messages()[-1]
    assert "csm:focus_market" not in _callback_data(picker.reply_markup)
    # The filter picker still offers it — the asymmetry is the point.
    await feed_callback(dp, bot, "cfadd", user_id=111)
    assert "cfm:focus_market" in _callback_data(session.edited_messages()[-1].reply_markup)


async def test_forged_focus_market_sort_and_operator_callbacks_are_rejected(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await feed_text(dp, bot, "/criteria", user_id=111)
    await feed_callback(dp, bot, "cnew", user_id=111)
    before = session.edited_messages()[-1]

    # Old or forged keyboards must not smuggle the non-numeric metric into the
    # numeric flows: no threshold prompt, no sort by focus market.
    await feed_callback(dp, bot, "csm:focus_market", user_id=111)
    await feed_callback(dp, bot, "csd:focus_market:d", user_id=111)
    await feed_callback(dp, bot, "cfo:focus_market:gte", user_id=111)

    assert session.edited_messages()[-1] is before  # nothing re-rendered


async def test_saved_focus_criteria_re_run_from_home(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xmetals", display_name="Silverback")
    await add_round_trips(pool, "0xmetals", "xyz:SILVER", "xyz:GOLD", "BTC")

    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cfc:METALS", user_id=111)
    await feed_callback(dp, bot, "csave", user_id=111)
    await feed_text(dp, bot, "Metal heads", user_id=111)

    (saved,) = await list_criteria(pool, 111)
    assert saved.criteria.filters[0].threshold == "cat:METALS"

    await feed_callback(dp, bot, f"crun:{saved.id}:0", user_id=111)
    assert "Silverback" in (session.edited_messages()[-1].text or "")


async def test_zero_results_name_the_focus_filter_as_strictest(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await add_trader(pool, "0xcrypto", display_name="Cryptonaut")
    await add_round_trips(pool, "0xcrypto", "BTC", "ETH")

    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cfc:ENERGY", user_id=111)
    await feed_callback(dp, bot, "crun:d:0", user_id=111)

    text = session.edited_messages()[-1].text or ""
    assert "No traders match" in text
    assert "Focus market: Energy" in text


async def test_focus_filter_can_be_removed_like_any_other(
    dp: Dispatcher, bot: Bot, session: RecordingSession, pool: asyncpg.Pool
) -> None:
    await _open_market_picker(dp, bot, user_id=111)
    await feed_callback(dp, bot, "cfc:METALS", user_id=111)
    builder = session.edited_messages()[-1]
    assert "Focus market: Metals" in (builder.text or "")

    await feed_callback(dp, bot, "cfdel:0:focus_market", user_id=111)
    assert "No filters yet" in (session.edited_messages()[-1].text or "")
