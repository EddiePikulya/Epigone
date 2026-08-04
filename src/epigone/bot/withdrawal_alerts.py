"""Withdrawal Alert delivery: the bot-side consumer of withdrawal_alerts
(issue #171).

The stream's poll pass queues one row per follower when a tracked Trader's
equity falls by more than that pass's PnL explains (epigone.withdrawals);
ADR-0002 has the two processes meet only in Postgres, so this side knows
nothing about detection. The shared outbox drain (epigone.bot.outbox) owns the
retry rules and the delivered_at stamping that makes delivery at-most-once
across restarts; this module supplies only which rows to drain and how one
renders.

Formatted as its siblings are — the recipient's own name for the wallet ahead
of the leaderboard label (#86), the positions tap-through, the 🗑 delete row
(#73) — because a withdrawal arrives in the same stream of messages as the
Position Alerts about the same wallet, and the one that says "they left" should
not be the one that looks foreign.
"""

import asyncpg
from aiogram import Bot

from epigone.bot.alerts import positions_button
from epigone.bot.format import trader_label, usd_compact
from epigone.bot.outbox import MAX_DELIVERY_ATTEMPTS, drain_outbox, run_drain_loop
from epigone.clock import Clock


async def run_withdrawal_delivery_loop(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> None:
    """The shared supervised drain loop over Withdrawal Alert delivery."""
    await run_drain_loop(
        lambda: deliver_pending_withdrawal_alerts(pool, bot, clock), clock, label="withdrawal alert"
    )


async def deliver_pending_withdrawal_alerts(pool: asyncpg.Pool, bot: Bot, clock: Clock) -> int:
    """Deliver every undelivered withdrawal alert, oldest first. Returns the count."""

    async def deliver(bot: Bot, row: asyncpg.Record) -> None:
        await bot.send_message(
            chat_id=row["user_telegram_id"],
            text=render_withdrawal_alert(row),
            reply_markup=positions_button(row),
        )

    return await drain_outbox(
        pool,
        bot,
        clock,
        table="withdrawal_alerts",
        fetch=_fetch_pending,
        deliver=deliver,
    )


async def _fetch_pending(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    # The same joins as its sibling queues: the recipient's own per-Track
    # nickname (#86) beats the leaderboard label, and is NULL once they have
    # unfollowed — an alert queued before an unfollow still owes delivery, and
    # reads as the bare address.
    rows: list[asyncpg.Record] = await pool.fetch(
        """
        SELECT a.*, t.display_name, tr.name AS track_name
        FROM withdrawal_alerts a
        JOIN traders t ON t.address = a.trader_address
        LEFT JOIN tracks tr
            ON tr.trader_address = a.trader_address
            AND tr.user_telegram_id = a.user_telegram_id
        WHERE a.delivered_at IS NULL AND a.attempts < $1
        ORDER BY a.id
        """,
        MAX_DELIVERY_ATTEMPTS,
    )
    return rows


def render_withdrawal_alert(row: asyncpg.Record) -> str:
    """The message: how much left, what share of the account that was, and what
    is still there.

    The share is the headline because it is the part that changes what a
    follower should do — $120k out of $10M is housekeeping and $120k out of
    $140k is the Trader leaving — and the remaining equity is beside it because
    the next question after "they pulled out" is always "how much is left".

    `~` on the amount is not decoration: nothing here observed a transfer. The
    figure is an equity drop with this pass's PnL taken out (see
    epigone.withdrawals), so funding and fees ride inside it."""
    label = trader_label(row["track_name"] or row["display_name"], row["trader_address"])
    share = row["amount_usd"] / row["prior_equity"]
    return (
        f"⚠️ {label} pulled ~{usd_compact(row['amount_usd'])} out "
        f"({share:.0%} of equity) — {usd_compact(row['equity_usd'])} left"
    )
