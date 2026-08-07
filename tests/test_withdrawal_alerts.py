"""Withdrawal detection inside the poll pass (issue #171).

Seam test per the house convention: fake HyperliquidGateway, fake clock, real
Postgres. What is under test is the judgement — an equity drop the pass's own
PnL cannot explain becomes one queued alert per follower, and a drop PnL DOES
explain becomes nothing at all. The delivery half lives in
tests/test_withdrawal_alert_delivery.py.

Every test drives two consecutive `run_poll_pass` calls, because that is the
shape of the signal: a withdrawal exists only as the delta between two
observations of the same wallet, and the first pass of a Trader's life is a
baseline that can never fire.
"""

from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.clock import Clock
from epigone.gateway import Position, Side
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.stream.poller import POLL_INTERVAL_SECONDS, run_poll_pass
from epigone.withdrawals import (
    MAX_OBSERVATION_GAP_INTERVALS,
    WITHDRAWAL_FLOOR_USD,
    WITHDRAWAL_FRACTION_THRESHOLD,
)
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000
WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"


def position(
    coin: str = "BTC",
    *,
    size_usd: str = "100000",
    size_coin: str | None = "10",
    unrealized_pnl: str = "0",
    side: Side = Side.LONG,
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal("5"),
        entry_price=Decimal("100"),
        unrealized_pnl=Decimal(unrealized_pnl),
        size_coin=Decimal(size_coin) if size_coin is not None else None,
    )


async def track(pool: asyncpg.Pool, clock: Clock, address: str, *user_ids: int) -> None:
    """A Trader in the Universe, tracked by each given User."""
    await pool.execute(
        """
        INSERT INTO traders (address, display_name, first_seen_at, last_seen_at)
        VALUES ($1, 'Ansem', $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        clock.now(),
    )
    for user_id in user_ids:
        await pool.execute(
            "INSERT INTO users (telegram_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )
        await pool.execute(
            "INSERT INTO tracks (user_telegram_id, trader_address) VALUES ($1, $2)",
            user_id,
            address,
        )


async def alerts(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM withdrawal_alerts ORDER BY user_telegram_id")


async def poll(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> None:
    await run_poll_pass(pool, gateway, WeightBudget(WIDE_OPEN_BUDGET, clock), clock)


async def observe(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    *,
    equity: str,
    positions: list[Position] | None = None,
) -> None:
    """One poll pass seeing this equity and these positions."""
    gateway.set_positions(WHALE, positions if positions is not None else [position()])
    gateway.set_account_value(WHALE, Decimal(equity))
    await poll(pool, gateway, clock)


async def test_an_unexplained_equity_drop_alerts_every_follower_once(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Nothing about the positions changed and no PnL moved: the account is
    # simply smaller than it was, which is money leaving the wallet.
    await track(pool, clock, WHALE, 1, 2)
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")

    first, second = await alerts(pool)
    assert [row["user_telegram_id"] for row in (first, second)] == [1, 2]
    assert first["trader_address"] == WHALE
    assert first["amount_usd"] == Decimal("300000")
    assert first["prior_equity"] == Decimal("400000")
    assert first["equity_usd"] == Decimal("100000")
    assert first["created_at"] == clock.now()
    assert first["delivered_at"] is None


async def test_a_drop_that_is_pure_drawdown_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The same headline drop as above, every dollar of it unrealized loss on a
    # position still open. A losing trader is not a leaving trader.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(
        pool,
        gateway,
        clock,
        equity="100000",
        positions=[position(unrealized_pnl="-300000")],
    )

    assert await alerts(pool) == []


async def test_a_close_and_a_withdrawal_in_one_pass_attribute_separately(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The Trader closes a position holding a $50k unrealized loss AND pulls
    # $250k out, in the window between two passes. Realizing a loss moves money
    # from unrealized to realized without changing what the account is worth, so
    # the alert must name the $250k that left — not the $300k the equity fell.
    await track(pool, clock, WHALE, 1)
    await observe(
        pool, gateway, clock, equity="400000", positions=[position(unrealized_pnl="-50000")]
    )

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="150000", positions=[])

    (row,) = await alerts(pool)
    assert row["amount_usd"] == Decimal("250000")


async def test_a_partial_close_of_a_losing_position_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Half a losing position is closed: $60k of the $120k unrealized loss
    # becomes realized, the rest stays on the book, and the account is worth
    # exactly what it was. Attributing the realized half to the size that left
    # is what keeps this silent — counting only the change in unrealized PnL
    # would read the realization as a $60k outflow, 30% of the account.
    await track(pool, clock, WHALE, 1)
    await observe(
        pool,
        gateway,
        clock,
        equity="200000",
        positions=[position(size_usd="100000", size_coin="10", unrealized_pnl="-120000")],
    )

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(
        pool,
        gateway,
        clock,
        equity="200000",
        positions=[position(size_usd="50000", size_coin="5", unrealized_pnl="-60000")],
    )

    assert await alerts(pool) == []


async def test_a_leveraged_position_closed_into_a_falling_market_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The residual the arithmetic cannot see: a close prices itself at a moment
    # between two passes, so it can realize far more loss than the last observed
    # mark showed. Here $8M of notional is closed after a 1.25% adverse move,
    # realizing $150k against the $50k on the books — the account falls $100k,
    # a clean 25% of it, with nothing having left the wallet. The departed size
    # buys an allowance big enough to cover an ordinary move on itself, so the
    # blown-up trader is not reported as a leaving one.
    await track(pool, clock, WHALE, 1)
    await observe(
        pool,
        gateway,
        clock,
        equity="400000",
        positions=[position(size_usd="8000000", size_coin="80", unrealized_pnl="-50000")],
    )

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="300000", positions=[])

    assert await alerts(pool) == []


async def test_a_flip_and_a_withdrawal_in_one_pass_attribute_separately(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # A flip closes the whole prior leg and opens the other side, so the prior
    # leg's unrealized PnL is realized in full — and the $120k that left on top
    # of it is the news. The departed notional here is small enough that an
    # ordinary move on it cannot account for the drop.
    await track(pool, clock, WHALE, 1)
    await observe(
        pool,
        gateway,
        clock,
        equity="200000",
        positions=[position(size_usd="100000", size_coin="10", unrealized_pnl="20000")],
    )

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(
        pool,
        gateway,
        clock,
        equity="80000",
        positions=[position(size_usd="100000", size_coin="10", side=Side.SHORT)],
    )

    (row,) = await alerts(pool)
    assert row["amount_usd"] == Decimal("120000")


async def test_a_scale_in_holds_all_of_its_prior_pnl(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Size added enters at its own price carrying no PnL of its own, so
    # doubling a winning position leaves every dollar of the old leg's
    # unrealized PnL exactly where it was. Scaling the prior PnL by the size
    # ratio instead of capping it would credit the new leg with $40k it never
    # made, and name only $60k of the $100k that actually left.
    await track(pool, clock, WHALE, 1)
    await observe(
        pool,
        gateway,
        clock,
        equity="200000",
        positions=[position(size_usd="100000", size_coin="10", unrealized_pnl="40000")],
    )

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(
        pool,
        gateway,
        clock,
        equity="100000",
        positions=[position(size_usd="200000", size_coin="20", unrealized_pnl="40000")],
    )

    (row,) = await alerts(pool)
    assert row["amount_usd"] == Decimal("100000")


async def test_an_account_worth_nothing_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # There is no share of an empty account, and bad debt is not a state to
    # reason about a percentage from — the gate exists so the arithmetic never
    # divides by that zero.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="0", positions=[])

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="-5000", positions=[])

    assert await alerts(pool) == []


async def test_a_muted_track_is_suppressed_at_queue_time(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # Consistent with Position Alerts (#10): a muted Track never becomes a row,
    # so unmuting cannot backfill the withdrawal it missed.
    await track(pool, clock, WHALE, 1, 2)
    await pool.execute("UPDATE tracks SET muted = true WHERE user_telegram_id = 2")
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")

    assert [row["user_telegram_id"] for row in await alerts(pool)] == [1]


async def test_a_drop_below_the_fraction_threshold_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # A tenth of the account moving out is housekeeping, not the signal.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="360000")

    assert Decimal("40000") / Decimal("400000") < WITHDRAWAL_FRACTION_THRESHOLD
    assert await alerts(pool) == []


async def test_a_drop_below_the_absolute_floor_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # A tiny account emptying itself clears the percentage gate and still says
    # nothing worth a push notification.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="1600")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="800")

    assert Decimal("800") < WITHDRAWAL_FLOOR_USD
    assert await alerts(pool) == []


async def test_a_traders_first_ever_pass_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # There is no previous observation to have dropped from — the baseline rule
    # every lane in this system obeys, applied to equity.
    await track(pool, clock, WHALE, 1)

    await observe(pool, gateway, clock, equity="100")

    assert await alerts(pool) == []


async def test_a_drop_across_a_gap_in_watching_alerts_nobody(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # The stream was down for ten minutes. Positions could have opened and
    # closed entirely inside that gap, so the PnL this pass can account for is
    # not the PnL that happened: the drop is unattributable, not a withdrawal.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(MAX_OBSERVATION_GAP_INTERVALS * POLL_INTERVAL_SECONDS + 1)
    await observe(pool, gateway, clock, equity="100000")

    assert await alerts(pool) == []


async def test_a_withdrawal_is_detected_once_and_never_re_detected(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # At-most-once at the detection end: the pass that alerted also replaced the
    # equity it compared against, so the shrunken account is simply the new
    # normal and every later pass sees no delta at all.
    await track(pool, clock, WHALE, 1)
    await observe(pool, gateway, clock, equity="400000")
    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")
    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")

    assert len(await alerts(pool)) == 1


async def test_a_wallet_nobody_tracks_queues_nothing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    # A wallet in the poll set only because a User linked it as their own
    # (#121) is snapshotted and priced, but the fan-out reads `tracks` — so
    # nobody is told about the owner's own transfers.
    await pool.execute("INSERT INTO users (telegram_id, linked_wallet) VALUES (7, $1)", WHALE)
    await pool.execute(
        "INSERT INTO traders (address, first_seen_at, last_seen_at) VALUES ($1, $2, $2)",
        WHALE,
        clock.now(),
    )
    await observe(pool, gateway, clock, equity="400000")

    clock.advance(POLL_INTERVAL_SECONDS)
    await observe(pool, gateway, clock, equity="100000")

    assert await alerts(pool) == []
