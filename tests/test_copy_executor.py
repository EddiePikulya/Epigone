"""The operator copy executor (issue #136, ADR-0007).

These pin the DECISIONS, not the plumbing: tracking is not copying, sizing is
fixed and relative, entries are guarded and one-shot, exits are ungated and
retried, a claim means handled, and nothing signs while halted.

Seam per the house convention: fake read gateway, fake execution gateway, fake
clock, real Postgres.
"""

import logging
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest

from epigone.execute import episodes as ep
from epigone.execute import subs as subs_store
from epigone.execute.config import ExecutorConfig
from epigone.execute.executor import EXECUTOR_CONSUMER
from epigone.execute.notices import PAGER, SKIP_DIGEST_THRESHOLD
from epigone.execute.policy import ENTRY_STALENESS_GUARD, LEADER_EQUITY_FLOOR, RiskPolicy
from epigone.gateway import GatewayError, MarketStats, Position, Side
from epigone.gateway.execution import (
    ActionRejectedError,
    Grouping,
    OrderFilled,
    OrderRejected,
    OrderResting,
    RejectReason,
    Tif,
)
from epigone.gateway.execution_http import MAINNET_EXCHANGE_URL, TESTNET_EXCHANGE_URL
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.gateway.http import INFO_URL, TESTNET_INFO_URL
from epigone.monitor.checks import COPY_PAGER_ACTIONS
from epigone.position_events import ClaimableEvent, PositionEvent, outstanding_events
from epigone.safety.audit import ExecutionAudit
from epigone.safety.config import WatchdogConfig
from epigone.safety.halt import KILL_SOURCE, is_halted, request_halt, resume
from tests.support.clock import FakeClock
from tests.support.copy import (
    LEADER,
    MASTER,
    OPERATOR,
    SUB,
    build_harness,
    copy_sub,
    emit,
    position,
    seed_trader,
    set_limits,
)
from tests.support.orders import open_order


def opened(
    coin: str = "ETH", side: str = "long", size_coin: str = "5", leverage: str = "1"
) -> PositionEvent:
    """A Leader's open, as the poller recorded it.

    `leverage` is theirs, and under amendment D-4 it is a SIZING input: the
    copy runs at min(theirs, the backstop, the asset max). It defaults to 1x
    so every test that is not ABOUT leverage reads its sizes at face value —
    stake in, stake-worth of position out — and the tests that are about it
    say so by passing a number."""
    return PositionEvent(
        kind="open",
        coin=coin,
        side=side,
        size_usd=Decimal("10000"),
        size_coin=Decimal(size_coin),
        entry_price=Decimal("2000"),
        leverage=Decimal(leverage),
    )


def scaled(kind: str, prev: str, new: str, coin: str = "ETH") -> PositionEvent:
    return PositionEvent(
        kind=kind,
        coin=coin,
        side="long",
        size_usd=Decimal(new) * Decimal("2000"),
        size_coin=Decimal(new),
        prev_size_usd=Decimal(prev) * Decimal("2000"),
        prev_size_coin=Decimal(prev),
    )


def closed(coin: str = "ETH") -> PositionEvent:
    return PositionEvent(
        kind="close",
        coin=coin,
        prev_side="long",
        size_usd=Decimal("10000"),
        size_coin=Decimal("5"),
        realized_pnl=Decimal("120"),
    )


async def outstanding(pool: asyncpg.Pool) -> list[int]:
    async with pool.acquire() as conn:
        return [event.id for event in await outstanding_events(conn, EXECUTOR_CONSUMER)]


# --- tracking is not copying --------------------------------------------------


async def test_only_copy_enabled_leaders_are_mirrored(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The product's central boundary: the operator tracks many wallets to
    observe and copies a deliberately enabled subset. An event from a tracked
    but uncopied wallet must produce no order — and must NOT be claimed, or a
    later /copy would silently inherit a drained backlog."""
    h = await build_harness(pool, clock, gateway)
    other = "0xother0000000000000000000000000000000000dd"
    await seed_trader(pool, clock, other)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now(), trader=other)
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) != []  # never claimed: it never qualified


async def test_disabling_a_leader_takes_effect_without_a_restart(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # The acceptance criterion. `enabled` is read every loop, so /uncopy stops
    # the next event with no process bounce and no cache to invalidate.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()
    assert len(h.placed()) == 1

    await subs_store.disable_sub(pool, operator_id=OPERATOR, leader_address=LEADER)
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    await emit(pool, scaled("scale_in", "5", "7"), clock.now())
    await h.executor.run_cycle()
    assert len(h.placed()) == 1  # unchanged: the mapping is off


# --- sizing (decision 2) ------------------------------------------------------


async def test_an_open_is_copied_at_base_notional_not_the_leaders_size(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 2: the Leader opened $10,000 of ETH; we open OUR $200 of it.
    The order is an IOC inside the slippage cap — nothing entry-shaped rests."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, base_stake="200")
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, vault), = h.placed()
    (order,) = orders
    assert order.size == Decimal("0.1")  # $200 / $2000, not the leader's 5 ETH
    assert order.tif is Tif.IOC
    assert order.is_buy is True
    assert order.reduce_only is False
    assert order.limit_price == Decimal("2020")  # +1% cap
    assert vault == SUB  # placed on the Leader's own Copy Sub-account

    (episode,) = await h.episodes(sub.id)
    assert episode["entry_price"] == Decimal("2000")  # OUR fill, not the leader's
    assert episode["size_coin"] == Decimal("0.1")


async def test_a_scale_in_mirrors_the_leaders_fraction_of_what_we_actually_hold(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 10's self-damping principle. The Leader grows 5 → 7.5 (+50%);
    we hold 0.08 (a partial fill left us short of the 0.1 we asked for), so we
    buy 50% OF 0.08 — never 50% of the size we bookkept."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.08")])

    await emit(pool, scaled("scale_in", "5", "7.5"), clock.now())
    await h.executor.run_cycle()

    (orders, _g, _b, _v), = h.placed()
    assert orders[0].size == Decimal("0.04")  # 50% of the REAL 0.08


async def test_a_scale_without_coin_units_is_skipped_rather_than_guessed(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # ADR-0006: NULL coin units mean "not mirrorable", never a notional ratio
    # — the two notionals are marked at different observations.
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])

    await emit(
        pool,
        PositionEvent(
            kind="scale_in",
            coin="ETH",
            side="long",
            size_usd=Decimal("15000"),
            prev_size_usd=Decimal("10000"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []  # claimed: handled, not forgotten
    assert any("not mirrorable" in body for body in await h.notices())


# --- entries are guarded (decisions 7, 8) -------------------------------------


async def test_a_stale_entry_is_claimed_and_skipped_never_traded(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 8: an executor coming back from downtime must not fire a burst
    of stale opens. The event is still CLAIMED — handled, with its reason on
    the trail — or the backlog never drains."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1))
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []
    assert any("stale entry" in body for body in await h.notices())


async def test_a_leader_below_the_liveness_floor_is_not_copied_into(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 7: 38% of quality-screened wallets had emptied out while their
    stored metrics still looked alive. The gate reads LIVE equity at signal
    time, and it gates entries only.

    The Leader emptied out on the network their stats were earned on (#184),
    and no amount of money on the book we trade answers for that — so the
    trade-side fake is deliberately RICH here, and the entry still skips."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.signal.account_values[(LEADER, None)] = LEADER_EQUITY_FLOOR - Decimal("1")
    gateway.account_values[(LEADER, None)] = Decimal("250000")

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    observed = str(LEADER_EQUITY_FLOOR - Decimal("1"))
    assert h.placed() == []
    assert await outstanding(pool) == []  # claimed: a decision, not a deferral
    # The observed figure is the SIGNAL side's, in both places the operator
    # can look: the notice they read and the trail that answers for it later.
    assert any("liveness floor" in body and observed in body for body in await h.notices())
    assert any(
        action == "copy_skipped" and "liveness floor" in reason and observed in reason
        for action, reason in await h.audit_actions()
    )


async def test_a_leader_alive_on_the_signal_network_is_copied_though_the_book_reads_zero(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Issue #184, the A5 shakedown exactly: a MAINNET Leader mirrored onto the
    TESTNET book. Their testnet account holds $0 and always will — the account
    decision 7 asks about is the one the signal came from, so the gate reads
    the signal network and the entry copies."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.signal.account_values[(LEADER, None)] = LEADER_EQUITY_FLOOR + Decimal("1")
    gateway.account_values[(LEADER, None)] = Decimal("0")

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1
    assert not any("liveness floor" in body for body in await h.notices())
    # ONE liveness read, on the signal side only. Two would double the weight
    # this fetch has always spent; a trade-side one would be the bug itself.
    assert [c for c in h.signal.positions_calls if c[0] == LEADER] == [(LEADER, None)]
    assert [c for c in gateway.positions_calls if c[0] == LEADER] == []


async def test_an_unreadable_leader_equity_defers_rather_than_claims(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # A network blip is not a decision about the event. Claiming here would
    # silently drop a copy; leaving it outstanding asks again next cycle. The
    # blip that matters is on the SIGNAL-side gateway (#184) — the trade side
    # answers for the Leader nowhere.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.signal.positions_errors[LEADER] = RuntimeError("info down")

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) != []


async def test_the_operators_own_position_makes_the_coin_occupied(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # Decision 10's table: a position with no episode is the operator's own
    # and is never touched — so a leader open on that coin skips, loudly.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="3")])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any("coin occupied" in body for body in await h.notices())


# --- exits are ungated (decisions 5, 8) ---------------------------------------


async def test_a_close_executes_at_any_age_and_ends_the_episode(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 8's exemption: closing late is strictly safer than never
    closing. A close skipped as stale would leave a position no future event
    can ever close, because the Leader is already flat."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=2, total_size=Decimal("0.1"), avg_price=Decimal("2100"))]
    )

    await emit(pool, closed(), clock.now() - timedelta(hours=2))  # far past the guard
    await h.executor.run_cycle()

    (orders, _g, _b, _v), = h.placed()
    assert orders[0].reduce_only is True
    assert orders[0].is_buy is False  # closing a long sells
    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_reason"] == ep.ENDED_LEADER_CLOSE


async def test_a_close_that_will_not_fill_retries_then_pages(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 5's PAGER CASE, kept: a reduce-only IOC the book will not
    absorb within the slippage cap is pathological, gets its own audit reason,
    and leaves the position alone rather than escalating."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    for _ in range(3):
        h.exec_fake.place_results.append(
            [OrderRejected(reason=RejectReason.NO_IMMEDIATE_MATCH, message="no liquidity")]
        )

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 3  # bounded retry of the reduce-only remainder
    assert any("CLOSE UNFILLED" in body for body in await h.notices())
    assert any(action == "copy_close_unfilled" for action, _ in await h.audit_actions())


async def test_an_event_for_a_position_we_never_opened_is_claimed_and_skipped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # Decision 8's residual: a skipped stale entry leaves later leader events
    # referring to a position that does not exist. Claim, skip, say so.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []
    assert any("no local ETH position" in body for body in await h.notices())


# --- flips (decision 3) -------------------------------------------------------


async def test_a_flip_closes_reduce_only_then_opens_through_the_full_pipeline(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 3: one event row, TWO orders. The close leg is reduce-only and
    structurally cannot reverse; the open leg then runs the fresh-open
    pipeline, so there is exactly one code path that opens positions."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=3, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )
    h.exec_fake.place_results.append(
        [OrderFilled(oid=4, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
            realized_pnl=Decimal("50"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    close_leg, open_leg = h.placed()
    assert close_leg[0][0].reduce_only is True and close_leg[0][0].is_buy is False
    assert open_leg[0][0].reduce_only is False and open_leg[0][0].is_buy is False  # short
    assert await outstanding(pool) == []  # ONE event, claimed once


async def test_a_flip_whose_claim_was_lost_never_opens_the_second_leg(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A flip is one event and two orders, so only the CLOSE leg claims — and
    the open leg must therefore refuse to run when the close leg reports that
    another instance won that claim. Otherwise the loser would place an
    unclaimed open on top of the winner's, which is the doubled position the
    whole write-ahead scheme exists to prevent."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
        ),
        clock.now(),
    )
    # Another instance got there first: the claim row already exists.
    event_id = await pool.fetchval("SELECT id FROM position_events")
    await pool.execute(
        "INSERT INTO position_event_claims (event_id, consumer, claimed_at) VALUES ($1, $2, $3)",
        event_id,
        EXECUTOR_CONSUMER,
        clock.now(),
    )
    # …and it is still in this executor's backlog only because the query ran
    # before that claim landed, which is exactly the race being modelled.
    async with pool.acquire() as conn:
        events = await outstanding_events(conn, EXECUTOR_CONSUMER)
    assert events == []  # the backlog no longer offers it…
    await h.executor._handle(  # …so drive the handler directly, mid-race
        _claimable(pool, event_id, clock),
        sub,
        await h.executor._sub_state(sub),
    )

    assert h.placed() == []  # neither leg: not ours to act on


def _claimable(pool: asyncpg.Pool, event_id: int, clock: FakeClock) -> ClaimableEvent:
    return ClaimableEvent(
        id=event_id,
        trader_address=LEADER,
        observed_at=clock.now(),
        source="poll",
        event=PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
        ),
    )


async def test_a_flips_open_leg_is_never_dropped_without_an_audit_row(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """An unreadable leader equity normally DEFERS — the event stays
    outstanding and the next cycle asks again. That is a lie for a flip's open
    leg, whose close leg already claimed the single event both legs come from:
    the event will never be offered again. Decision 3 requires that outcome to
    end FLAT with an audit row, and decision 11 requires the operator to hear
    about it, so an already-claimed event reports instead of deferring."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=9, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )
    # The close leg reads our sub fine; only the LEADER's equity is unreadable,
    # and it is the SIGNAL-side gateway that answers for the Leader (#184).
    h.signal.positions_errors[LEADER] = RuntimeError("info down")

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # the close leg only: we end FLAT
    assert await outstanding(pool) == []  # claimed, so it never comes back
    assert any("leader equity unreadable" in body for body in await h.notices())
    assert any(action == "copy_skipped" for action, _ in await h.audit_actions())


async def test_a_disabled_subs_episodes_are_still_reconciled(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """/uncopy stops event consumption, not the money (decision 12) — the sub
    still holds positions, its bracket can still fire and it can still be
    liquidated. Decision 10's "each loop the executor compares every sub's
    live state" therefore means EVERY sub. It also keeps a later /copy honest:
    a stranded live episode would make the next leader open skip as "already
    in a copy episode"."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await subs_store.disable_sub(pool, operator_id=OPERATOR, leader_address=LEADER)
    gateway.set_positions(SUB, [])  # it closed while we were not copying

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_reason"] == ep.ENDED_OPERATOR
    assert h.placed() == []  # reconciliation still never trades


async def test_a_kill_between_exit_retries_stops_the_remaining_attempts(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The retries span ~30s, which is exactly where a /kill lands. The
    inherited obligation is to re-check halt state as late as possible before
    signing, so once before the first attempt is not enough."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    # Every attempt finds no liquidity, so all three retries would run.
    for _ in range(3):
        h.exec_fake.place_results.append(
            [OrderRejected(reason=RejectReason.NO_IMMEDIATE_MATCH, message="no liquidity")]
        )
    attempts = 0

    async def halt_after_first(*args: object, **kwargs: object) -> list[object]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            await request_halt(
                pool,
                clock,
                ExecutionAudit(pool, clock),
                source=KILL_SOURCE,
                reason="operator /kill mid-exit",
                requested_by=OPERATOR,
            )
        return [OrderRejected(reason=RejectReason.NO_IMMEDIATE_MATCH, message="no liquidity")]

    h.exec_fake.place_orders = halt_after_first  # type: ignore[method-assign]

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    assert attempts == 1  # the halt stopped attempts two and three


async def test_a_bracket_anchors_to_the_exchanges_blended_entry(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 9's (r1a) says the anchor is the position's entry price FROM
    clearinghouseState — which after a scale-in is the BLENDED entry, not the
    price of our first fill. A bracket computed off the opening fill would sit
    at percentages of a price we no longer average."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
    )
    await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),  # our FIRST fill
        size_coin=Decimal("0.2"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    # The exchange reports a blended entry after the scale-in.
    gateway.set_positions(
        SUB, [position(coin="ETH", size_coin="0.2", entry_price="2200")]
    )
    gateway.set_open_orders(SUB, [])

    await h.executor.run_cycle()

    (legs, _g, _b, _v), = h.placed()
    assert {leg.trigger.trigger_price for leg in legs} == {
        Decimal("2420"),  # +10% of 2200, not of 2000
        Decimal("2090"),  # −5% of 2200
    }


async def test_a_skip_after_a_bracket_exit_says_the_bracket_took_us_out(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # "Our bracket already took us out" and "we never opened this one" are
    # different sentences, and only one is something the operator might want
    # to change — so rule g1's skips quote the ended episode's reason.
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await ep.end_episode(pool, episode.id, reason=ep.ENDED_BRACKET, ended_at=clock.now())
    gateway.set_positions(SUB, [])

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    notices = await h.notices()
    assert any("the last one ended: bracket" in body for body in notices)
    assert any("until the leader closes and re-opens" in body for body in notices)


async def test_a_halt_landing_mid_flip_stops_the_open_leg_and_leaves_us_flat(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 3's chosen failure direction, and the #143 halt-recheck
    contract: the gate is re-read immediately before signing, so a halt
    arriving between the legs ends the flip FLAT with an audit row rather than
    half-reversed."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])

    halted = False

    async def halt_after_close(*args: object, **kwargs: object) -> list[object]:
        nonlocal halted
        if not halted:
            halted = True
            await request_halt(
                pool,
                clock,
                ExecutionAudit(pool, clock),
                source=KILL_SOURCE,
                reason="operator /kill mid-flip",
                requested_by=OPERATOR,
            )
        return [OrderFilled(oid=5, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]

    h.exec_fake.place_orders = halt_after_close  # type: ignore[method-assign]

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    assert any("NOT sent — execution is halted" in body for body in await h.notices())
    assert any(action == "copy_halted" for action, _ in await h.audit_actions())


# --- the kill switch ----------------------------------------------------------


async def test_nothing_is_signed_while_halted_and_the_backlog_is_left_intact(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """/kill halts within one loop. The backlog is NOT claimed during a halt —
    decision 9 is explicit that /resume drains it under the already-locked
    rules, which a claim would make impossible."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason="operator /kill",
        requested_by=OPERATOR,
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) != []


# --- idempotency (the ticket's acceptance criterion) --------------------------


async def test_a_replayed_cycle_does_not_double_copy_the_same_event(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Write-ahead claiming (ADR-0006): the claim commits with the attempt row
    before the wire, so a second cycle over the same backlog sees nothing to
    do — no matter how many times the process restarts."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=6, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    await h.executor.run_cycle()

    assert len(h.placed()) == 1


async def test_the_backlog_ignores_whichever_lane_is_only_shadowing(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 4's forced filter, as ADR-0009 restates it. Both lanes write
    every (trader, coin) they observe — that is what keeps the two-producer
    comparison alive — so an unfiltered executor would copy every trade TWICE,
    forever. The filter is `authoritative`, not `source`: here the websocket
    owns production and the poller is the one shadowing."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=7, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now(), source="ws", authoritative=True)
    await emit(pool, opened(), clock.now(), source="poll", authoritative=False)
    await h.executor.run_cycle()

    assert len(h.placed()) == 1


async def test_the_backlog_follows_production_across_a_failover(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Why the filter cannot be on `source`. Pinned to one transport the
    executor goes blind at exactly the moment the other takes over — the
    websocket's entry here, the escalated poller's exit next — and a copy that
    opens and never closes is the worst outcome this system can produce."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=7, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )
    await emit(pool, opened(), clock.now(), source="ws", authoritative=True)
    await h.executor.run_cycle()
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=8, total_size=Decimal("0.1"), avg_price=Decimal("2100"))]
    )

    # The websocket goes bad; the poller takes production back and produces the
    # Leader's exit.
    clock.advance(1)
    await emit(pool, closed(), clock.now(), source="poll", authoritative=True)
    await h.executor.run_cycle()

    assert [orders[0].reduce_only for orders, *_rest in h.placed()] == [False, True]


# --- the risk policy (issue #137) ---------------------------------------------


async def test_a_declined_entry_is_audited_and_claimed_not_silently_dropped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # A decline is a decision — it goes on the trail and into the chat, and it
    # claims. Here the sub's aggregate stake cap is already spent by a position
    # on ANOTHER coin, which is what the per-sub cap is for.
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, sub_stake="100")
    await copy_sub(pool, clock, base_stake="200")
    gateway.set_positions(SUB, [position(coin="BTC", size_usd="150", size_coin="0.002")])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []
    decisions = [decision for _, decision in await h.audit_actions()]
    assert any("no stake headroom left" in decision for decision in decisions)
    # The denial wording rule: a denial never claims something did not EXIT.
    assert any("did not enter" in decision for decision in decisions)
    assert all("did not exit" not in decision for decision in decisions)


async def test_an_entry_over_a_stake_cap_is_clamped_to_the_headroom_not_refused(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§5: an order over a cap is CLAMPED, audited `allowed-clamped` with both
    the asked and the given size. A copy at reduced size still follows the
    Leader; refusing outright would leave us half-mirrored."""
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, coin_stake="50")
    await copy_sub(pool, clock, base_stake="200")
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.025"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()
    # $50 of the $200 asked, at 1x, on a $2000 mark.
    assert orders[0].size == Decimal("0.025")
    decision = next(
        decision for _, decision in await h.audit_actions() if "allowed-clamped" in decision
    )
    assert "asked $200" in decision and "given $50.00" in decision


async def test_a_scale_in_over_a_stake_cap_is_clamped_on_its_derived_stake(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A scale-in decides nothing about leverage — the position already runs at
    one the exchange enforces — so its stake is DERIVED: the margin the added
    notional will consume at that leverage. The caps bound it through that
    derivation, and the clamp scales the coin-unit size by the same ratio.

    Held: 0.1 ETH at 1x on a $2,000 mark = $200 of margin against a $250
    per-coin cap, so $50 of room. The Leader grows 50%, which asks for 0.05 ETH
    = $100 of margin: twice what is left."""
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, coin_stake="250")
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])

    await emit(pool, scaled("scale_in", "5", "7.5"), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()
    # Half the asked size, because half the asked stake was granted.
    assert orders[0].size == Decimal("0.025")
    decision = next(
        decision for _, decision in await h.audit_actions() if "allowed-clamped" in decision
    )
    assert "asked $100" in decision and "given $50.00" in decision
    # A scale-in never re-decides leverage, so nothing was signed to change it.
    assert not [m for m, _ in h.exec_fake.actions if m == "update_leverage"]


async def test_an_unreadable_read_on_a_flips_open_leg_does_not_promise_a_retry(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A flip's open leg runs already-claimed (its close leg claimed the single
    event both come from), so "the next cycle asks again" is a sentence that
    cannot come true. The trail has to say what actually happened instead:
    no retry, and — since the close filled — we are FLAT."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.market_stats_errors[None] = GatewayError("metaAndAssetCtxs down")
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            leverage=Decimal("1"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # the close leg only: we end FLAT
    decision = next(
        decision for _, decision in await h.audit_actions() if "unreadable" in decision
    )
    assert "no retry" in decision and "FLAT" in decision
    assert "asks again" not in decision
    # The event is gone from the backlog — claimed by the close leg — which is
    # exactly why the sentence above must not promise another look.
    assert await outstanding(pool) == []


async def test_an_order_that_rounds_to_dust_is_not_sent_even_when_the_stake_cleared(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The policy judges the $10 minimum in DOLLARS, on the stake it granted.
    The exchange judges the ORDER — which has been rounded DOWN to the asset's
    precision since. On a coarse asset those disagree, and the last word
    belongs to the size that will actually be signed: sending it would buy a
    guaranteed MIN_NOTIONAL reject and the alarming audit row the constant
    exists to avoid."""
    h = await build_harness(pool, clock, gateway)
    # A $6 coin quoted in WHOLE units: $11 of stake at 1x buys 1.83 of them,
    # which rounds DOWN to 1 — an $11 grant the policy allows, and a $6 order
    # the exchange would refuse.
    gateway.perp_universes[None] = ["BTC", "ETH", "CHUNK"]
    gateway.sz_decimals["CHUNK"] = 0
    gateway.mid_prices[None]["CHUNK"] = Decimal("6")
    await copy_sub(pool, clock, base_stake="11")
    gateway.set_positions(SUB, [])

    await emit(pool, opened(coin="CHUNK"), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []
    assert any(
        "under the exchange's $10 minimum order value" in decision
        for _, decision in await h.audit_actions()
    )


async def test_a_clamp_below_the_exchange_minimum_becomes_a_denial(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§5's one exception to clamping: the exchange refuses anything under $10
    of order value, so a clamp that lands there is a denial with that said —
    not a doomed order and an alarming reject on the trail."""
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, coin_stake="4")
    await copy_sub(pool, clock, base_stake="200")
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any(
        "minimum order value" in decision for _, decision in await h.audit_actions()
    )


# --- the Liquidity Floor (§1, §2) ---------------------------------------------


def thin(coin: str, *, volume: str = "1000", open_interest: str = "1") -> MarketStats:
    """A market nobody is trading — the shape the floor exists to keep us out
    of, where a copied trade's counterparty can be the Leader themselves."""
    return MarketStats(
        coin=coin,
        day_notional_volume=Decimal(volume),
        open_interest=Decimal(open_interest),
        mark_price=Decimal("2000"),
        max_leverage=20,
    )


async def test_a_sub_floor_coin_is_not_entered(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.market_stats["ETH"] = thin("ETH")
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []  # claimed: a denial is a decision
    assert any(
        "below the Liquidity Floor" in decision for _, decision in await h.audit_actions()
    )


async def test_a_live_episode_is_grandfathered_when_its_coin_goes_thin(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§2's lifecycle: the floor speaks ONCE, at the open that starts the
    episode. A scale-in on a coin that has since gone sub-floor still copies —
    a live episode is never interrupted."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.market_stats["ETH"] = thin("ETH")
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])

    await emit(pool, scaled("scale_in", "5", "7.5"), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()
    assert orders[0].size == Decimal("0.05")  # 50% of what we hold


async def test_an_exit_is_never_blocked_by_the_floor(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§3: exits are unconditionally exempt from every limit. A copy trapped in
    a coin that went thin is exactly the position we most need out of."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.market_stats["ETH"] = thin("ETH")
    await set_limits(pool, clock, coin_stake="1", sub_stake="1")  # every cap spent
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()
    assert orders[0].reduce_only is True


async def test_an_exit_is_never_blocked_by_either_networks_answer_on_liveness(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 7's first bullet, across both gateways (#184): a Leader dead on
    the signal network and unreadable there, holding nothing on the book we
    trade, still gets their close copied. Gating an exit on a liveness answer
    would leave us holding a position the Leader has left."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    h.signal.account_values[(LEADER, None)] = Decimal("0")
    h.signal.positions_errors[LEADER] = RuntimeError("info down")
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, closed(), clock.now())
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()
    assert orders[0].reduce_only is True
    assert [c for c in h.signal.positions_calls if c[0] == LEADER] == []


async def test_a_flip_into_a_sub_floor_coin_ends_flat(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§2: a flip ENDS the episode, so its opening leg is a fresh entry and the
    floor speaks again. The close leg signs unconditionally; the open leg is
    denied; we end flat, both halves audited."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.market_stats["ETH"] = thin("ETH")
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            leverage=Decimal("1"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    (orders, _grouping, _builder, _vault), = h.placed()  # ONE order: the close
    assert orders[0].reduce_only is True
    assert any(
        "below the Liquidity Floor" in decision for _, decision in await h.audit_actions()
    )


async def test_unreadable_liquidity_defers_the_entry_rather_than_denying_it(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """"I cannot tell" is not "denied": a network blip must not silently stop
    copying. The event stays OUTSTANDING for the next cycle to ask again."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.market_stats_errors[None] = GatewayError("metaAndAssetCtxs down")
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) != []


async def test_the_floor_can_be_turned_off(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Operator-tunable down to 0 = off (§1). The floor is a default stance,
    not a cage."""
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, floor_volume="0", floor_oi="0")
    await copy_sub(pool, clock)
    gateway.market_stats["ETH"] = thin("ETH")
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1


# --- Base Stake x Mirrored Leverage (amendment D-4) ---------------------------


async def test_an_open_is_stake_times_the_leaders_leverage_on_isolated_margin(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The amendment, end to end: $100 of the operator's margin behind a 10x
    Leader is a $1,000 position — and the leverage is SET on the sub, isolated,
    before the order, or the stake would not be the worst case."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, base_stake="100")
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.5"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(leverage="10"), clock.now())
    await h.executor.run_cycle()

    leverage_calls = [
        payload for method, payload in h.exec_fake.actions if method == "update_leverage"
    ]
    assert leverage_calls == [(1, 10, False, SUB)]  # ETH's asset id, 10x, ISOLATED, on the sub
    (orders, _grouping, _builder, _vault), = h.placed()
    assert orders[0].size == Decimal("0.5")  # $100 x 10 / $2000
    # And the leverage is signed BEFORE the order it sizes.
    methods = [method for method, _ in h.exec_fake.actions]
    assert methods.index("update_leverage") < methods.index("place_orders")

    (episode,) = await h.episodes(sub.id)
    assert episode["leverage"] == Decimal("10")


async def test_the_backstop_caps_a_leaders_leverage(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The Leader's leverage dial is an attack surface without a cap: notional,
    and everything that scales with it, is stake x leverage."""
    h = await build_harness(pool, clock, gateway)
    await set_limits(pool, clock, max_leverage="5")
    await copy_sub(pool, clock, base_stake="100")
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.25"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(leverage="40"), clock.now())
    await h.executor.run_cycle()

    leverage_calls = [
        payload for method, payload in h.exec_fake.actions if method == "update_leverage"
    ]
    assert leverage_calls == [(1, 5, False, SUB)]
    (orders, _grouping, _builder, _vault), = h.placed()
    assert orders[0].size == Decimal("0.25")  # $100 x 5 / $2000
    assert any(
        "operator's backstop" in decision for _, decision in await h.audit_actions()
    )


async def test_a_fixed_leverage_sub_ignores_the_leaders_own(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, base_stake="100", leverage_mode="fixed", fixed_leverage=3)
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.15"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(leverage="40"), clock.now())
    await h.executor.run_cycle()

    leverage_calls = [
        payload for method, payload in h.exec_fake.actions if method == "update_leverage"
    ]
    assert leverage_calls == [(1, 3, False, SUB)]


async def test_a_mirror_open_with_no_leader_leverage_is_skipped_not_guessed(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Every plausible default is a decision about position size that nobody
    made — 1x silently shrinks the copy, the backstop silently maximises it."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    event = opened()
    await emit(pool, replace(event, leverage=None), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any("not mirrorable" in decision for _, decision in await h.audit_actions())


async def test_a_halt_between_the_leverage_and_the_order_stops_the_order(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """updateLeverage is a signature with its own round trip, so the halt gate
    before the ORDER is not redundant with the one before it. A leverage
    setting on an asset we hold nothing in changes nothing about the account."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    async def halt_then_answer(*_args: object, **_kwargs: object) -> None:
        await request_halt(
            pool,
            clock,
            h.audit,
            source=KILL_SOURCE,
            reason="operator /kill mid-entry",
            requested_by=OPERATOR,
        )

    h.exec_fake.update_leverage = halt_then_answer  # type: ignore[method-assign]
    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any(action == "copy_halted" for action, _ in await h.audit_actions())


async def test_a_refused_leverage_stops_the_entry_rather_than_opening_at_another(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A position opened at whatever leverage the account carried is not the
    position the policy judged."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.exec_fake.errors.append(ActionRejectedError("Leverage too high"))

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any("refused to set" in body for body in await h.notices())


# --- the equity history the deferred daily-loss pause will be built on --------


async def test_every_cycle_records_each_subs_equity(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """§6: the pause is deferred; the enabler ships. The executor already reads
    this figure to reconcile and used to discard it."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    gateway.set_account_value(SUB, Decimal("980"))
    gateway.set_positions(SUB, [])

    await h.executor.run_cycle()
    clock.advance(seconds=5)
    gateway.set_account_value(SUB, Decimal("940"))
    await h.executor.run_cycle()

    rows = await pool.fetch(
        "SELECT account_value FROM copy_sub_equity WHERE sub_id = $1 ORDER BY id", sub.id
    )
    assert [row["account_value"] for row in rows] == [Decimal("980"), Decimal("940")]


# --- the Loss Budget (issue #181, amendment D-9) ------------------------------


async def test_a_breached_sub_refuses_an_open_and_copies_an_exit_in_the_same_cycle(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """THE HEADLINE. The budget is spent, so the book may only shrink: new
    risk is refused and the copy still follows the leader OUT of what is
    already open — and both answers are given in the one cycle that noticed
    the breach, not the one after it.

    A wind-down that stopped exits too would be the worst of both: no new
    positions and no way out of the old ones."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("400"))  # $600 down on a $500 budget
    h.exec_fake.place_results.append(
        [OrderFilled(oid=2, total_size=Decimal("0.1"), avg_price=Decimal("1800"))]
    )

    await emit(pool, opened(coin="BTC"), clock.now())  # new risk: refused
    await emit(pool, closed(coin="ETH"), clock.now())  # leaving risk: copied
    await h.executor.run_cycle()

    (orders, _g, _b, _v), = h.placed()
    (order,) = orders
    assert order.reduce_only is True  # the exit, and nothing else, was signed

    notices = await h.notices()
    assert any("loss budget" in body.lower() and "BTC" in body for body in notices)
    assert any("Copied close" in body for body in notices)
    actions = await h.audit_actions()
    assert any(
        action == "copy_skipped" and "loss budget for this leader is spent" in reason
        for action, reason in actions
    )
    # Both events are off the backlog: a refusal is a decision, not a deferral.
    assert await outstanding(pool) == []


async def test_a_flip_during_wind_down_closes_and_refuses_its_open_leg(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The same shape the Liquidity Floor's flip semantics have: the close leg
    runs, the open leg is refused, and the sub sits out that coin FLAT — which
    is decision 3's chosen failure direction, not a half-mirrored position."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("380"))
    h.exec_fake.place_results.append(
        [OrderFilled(oid=3, total_size=Decimal("0.1"), avg_price=Decimal("1800"))]
    )

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
            realized_pnl=Decimal("-50"),
        ),
        clock.now(),
    )
    await h.executor.run_cycle()

    (close_leg,) = h.placed()  # one order, not two
    assert close_leg[0][0].reduce_only is True
    assert any(
        "loss budget" in body.lower() and "ETH" in body for body in await h.notices()
    )
    assert await outstanding(pool) == []


async def test_brackets_are_still_maintained_while_a_sub_winds_down(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The protective side of execution never lapses. A wind-down refuses NEW
    risk; a position left open without its stop would be new risk of the worst
    kind — the kind nobody chose."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool,
        clock,
        mode="bracket",
        take_profit_pct="10",
        stop_loss_pct="5",
        loss_budget="500",
        baseline="1000",
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("300"))  # far past the budget
    h.exec_fake.place_results.append(
        [OrderResting(oid=7), OrderResting(oid=8)]
    )

    await h.executor.run_cycle()

    (legs, grouping, _b, _v), = h.placed()
    assert [leg.reduce_only for leg in legs] == [True, True]
    assert grouping is Grouping.POSITION_TPSL
    assert (await h.sub(sub.id)).winding_down is True


async def test_a_wound_down_sub_is_disabled_once_flat_and_only_once(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The terminal state, and it is the same one /uncopy produces. Flat is
    both halves — no exchange position AND no live episode — and the disable
    happens once: a later cycle over the same rows must not re-announce the end
    of a copy relationship that already ended."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("400"))

    await h.executor.run_cycle()  # breached, but still holding: not disabled
    assert (await h.sub(sub.id)).enabled is True

    # The leader is out and reconciliation adopts it: no position, no episode.
    gateway.set_positions(SUB, [])
    clock.advance(seconds=5)
    await h.executor.run_cycle()
    clock.advance(seconds=5)
    await h.executor.run_cycle()

    assert (await h.sub(sub.id)).enabled is False
    actions = [action for action, _ in await h.audit_actions()]
    assert actions.count("copy_budget_disabled") == 1
    assert actions.count("copy_budget_breached") == 1
    disables = [body for body in await h.notices() if "is DISABLED" in body]
    assert len(disables) == 1
    assert "$600.00" in disables[0] and "$500" in disables[0]


async def test_a_stray_bracket_leg_is_cancelled_before_the_disable(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A disabled mapping leaves the bracket-maintenance invariant's scope for
    good, so a leg that outlived its position would rest there until a human or
    a /kill sweep found it. It comes off the book BEFORE the flag flips — and
    only OUR legs do: an order the operator placed by hand in that sub is
    theirs, and a spent budget is not a mandate to clear someone else's book."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool,
        clock,
        mode="bracket",
        take_profit_pct="10",
        stop_loss_pct="5",
        loss_budget="500",
        baseline="1000",
    )
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await ep.record_bracket(
        pool, episode_id=episode.id, order_id=555, tpsl="sl", placed_at=clock.now()
    )
    await ep.end_episode(pool, episode.id, reason=ep.ENDED_LEADER_CLOSE, ended_at=clock.now())
    # Flat by both halves — and yet a stop is still on the book, which is the
    # exact case the periodic bracket verification exists for.
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("400"))
    gateway.set_open_orders(SUB, [open_order("ETH", 555), open_order("BTC", 999)])

    await h.executor.run_cycle()

    assert h.cancelled() == [555]  # ours only: 999 is the operator's own
    assert (await h.sub(sub.id)).enabled is False
    notices = await h.notices()
    assert any("leftover bracket order" in body for body in notices)
    assert any("is DISABLED" in body for body in notices)


async def test_a_disable_waits_rather_than_leaving_a_bracket_behind(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The order of the two acts is the safety property: cancel, then disable.
    If the book cannot be read, the mapping stays enabled and wound down —
    still refusing every entry — and the next cycle tries again, because a
    disabled mapping is one nothing looks at."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool,
        clock,
        mode="bracket",
        take_profit_pct="10",
        stop_loss_pct="5",
        loss_budget="500",
        baseline="1000",
    )
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await ep.record_bracket(
        pool, episode_id=episode.id, order_id=555, tpsl="sl", placed_at=clock.now()
    )
    await ep.end_episode(pool, episode.id, reason=ep.ENDED_LEADER_CLOSE, ended_at=clock.now())
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("400"))
    gateway.open_orders_errors[SUB] = GatewayError("order book unreachable")

    await h.executor.run_cycle()

    judged = await h.sub(sub.id)
    assert judged.enabled is True  # not disabled around an order it cannot see
    assert judged.winding_down is True  # and still refusing new risk meanwhile
    assert h.cancelled() == []

    gateway.open_orders_errors.pop(SUB)
    gateway.set_open_orders(SUB, [open_order("ETH", 555)])
    clock.advance(seconds=5)
    await h.executor.run_cycle()

    assert h.cancelled() == [555]
    assert (await h.sub(sub.id)).enabled is False


async def test_a_halt_defers_the_disable_rather_than_signing_a_cancel(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The terminal step signs, so it lives below the halt gate: halted still
    means Epigone signs nothing, and the sub simply stays wound down until
    /resume. Nothing is lost by waiting — a halt's own sweep enumerates and
    cancels per sub anyway."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    gateway.set_positions(SUB, [])
    gateway.set_open_orders(SUB, [])
    gateway.set_account_value(SUB, Decimal("400"))
    await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason="operator /kill",
        requested_by=OPERATOR,
    )

    await h.executor.run_cycle()

    judged = await h.sub(sub.id)
    assert judged.winding_down is True  # measured while halted
    assert judged.enabled is True  # but not acted on
    assert h.cancelled() == [] and h.placed() == []


async def test_the_eighty_percent_warning_fires_once_not_every_cycle(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A signal repeated every cycle it stays true is not a signal. The mark is
    persisted, so the "once" is a property of the row rather than of a process
    that happens to still be running."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("580"))  # $420 of $500

    for _ in range(3):
        await h.executor.run_cycle()
        clock.advance(seconds=5)

    warnings = [body for body in await h.notices() if "80% spent" in body]
    assert len(warnings) == 1
    assert "$420.00" in warnings[0]
    # and it is a warning, not a wind-down: nothing is refused yet
    assert [action for action, _ in await h.audit_actions()].count(
        "copy_budget_breached"
    ) == 0


async def test_a_restart_mid_wind_down_neither_re_arms_nor_forgets(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A deploy or a crash must not silently re-arm a breached sub back into
    copying opens, nor announce the breach a second time. Every mark is on the
    row, so a brand-new executor over the same database inherits the wind-down
    exactly as it stood."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("400"))
    await h.executor.run_cycle()
    assert (await h.sub(sub.id)).winding_down is True

    clock.advance(seconds=5)
    fresh = await build_harness(pool, clock, gateway)  # a new process, same DB
    await emit(pool, opened(coin="BTC"), clock.now())
    await fresh.executor.run_cycle()

    assert fresh.placed() == []  # still refusing new risk
    assert (await fresh.sub(sub.id)).budget_baseline_usd == Decimal("1000")
    assert [action for action, _ in await fresh.audit_actions()].count(
        "copy_budget_breached"
    ) == 1


async def test_raising_the_budget_resumes_copying_on_the_next_cycle(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The override this feature promises, seen from the executor's side: the
    operator raises their own threshold and opens are signed again — no
    restart, no second command, and the loss already booked still counts
    against the new number."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    # Still holding an ETH copy — so the wind-down stands rather than reaching
    # its flat-and-disabled terminus, which is the state an override is FOR.
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("400"))
    await h.executor.run_cycle()
    assert (await h.sub(sub.id)).winding_down is True

    clock.advance(seconds=5)
    await subs_store.reenable_sub(
        pool,
        operator_id=OPERATOR,
        leader_address=LEADER,
        allocation_usd=Decimal("1000"),
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        loss_budget_usd=Decimal("900"),
    )
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )
    await h.executor.run_cycle()  # re-funds the sub the re-issue re-opened
    gateway.set_account_value(SUB, Decimal("1000"))
    clock.advance(seconds=5)
    await emit(pool, opened(coin="BTC"), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # copying again
    judged = await h.sub(sub.id)
    assert judged.winding_down is False
    assert judged.budget_baseline_usd == Decimal("1000")  # the ledger survived
    assert judged.budget_spent_usd == Decimal("600")  # and so did the loss


async def test_a_top_up_between_cycles_is_not_mistaken_for_profit(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The btcgod lesson through the real provisioning path: the sub loses
    $600, Epigone funds it back up to its allocation, and the budget still
    knows the $600 is gone. Read as profit instead, the top-up would hand the
    leader back exactly the runway they just lost."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, allocation="1000", loss_budget="500")
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("1000"))
    await h.executor.run_cycle()  # arms the baseline at $1,000
    assert (await h.sub(sub.id)).budget_baseline_usd == Decimal("1000")

    # A re-issued /copy re-opens the funding leg, which is the only way money
    # moves INTO a live sub. The sub is down $600 by the time it runs.
    clock.advance(seconds=60)
    gateway.set_account_value(SUB, Decimal("400"))
    await subs_store.reenable_sub(
        pool,
        operator_id=OPERATOR,
        leader_address=LEADER,
        allocation_usd=Decimal("1000"),
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        loss_budget_usd=Decimal("500"),
    )
    await h.executor.run_cycle()  # tops the sub back up to $1,000
    gateway.set_account_value(SUB, Decimal("1000"))

    clock.advance(seconds=60)
    await h.executor.run_cycle()

    judged = await h.sub(sub.id)
    assert judged.budget_baseline_usd == Decimal("1000")  # kept: same budget
    assert judged.budget_spent_usd == Decimal("600")  # not $0
    assert judged.winding_down is True


async def test_a_sub_that_arrives_holding_money_is_judged_from_that_baseline(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """An adopted orphan or a re-copy comes back holding whatever last time
    left in it. The operator's number is about what happens NEXT, so the
    baseline is what the sub is worth when the budget is armed — never the
    allocation, and never zero."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, allocation="1000", loss_budget="500")
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("1400"))  # inherited more than it asked for

    await h.executor.run_cycle()
    assert (await h.sub(sub.id)).budget_baseline_usd == Decimal("1400")

    clock.advance(seconds=5)
    gateway.set_account_value(SUB, Decimal("1000"))  # down $400 from ITS baseline
    await h.executor.run_cycle()
    judged = await h.sub(sub.id)
    assert judged.budget_spent_usd == Decimal("400")
    assert judged.winding_down is False  # a naive read from $1,000 would have breached


async def test_the_budget_sees_collateral_on_every_venue_the_executor_trades(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A position's margin sitting on a builder DEX is not money lost. The
    core venue's accountValue alone would read this sub as $600 down and wind
    a perfectly healthy copy down on the strength of a venue it forgot to
    look at."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("400"))  # core
    gateway.set_account_value(SUB, Decimal("300"), dex="xyz")  # the builder DEX

    await h.executor.run_cycle()

    judged = await h.sub(sub.id)
    assert judged.budget_spent_usd == Decimal("300")
    assert judged.winding_down is False


async def test_a_sub_without_a_budget_is_untouched_by_any_of_this(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Opt-in, and the absence of the feature is the old behaviour exactly: a
    sub that has lost most of its allocation still copies, because nobody said
    otherwise."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("40"))  # 96% of the allocation gone
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # copied, as it always was
    judged = await h.sub(sub.id)
    assert judged.loss_budget_usd is None and judged.budget_armed_at is None
    assert not any("budget" in body.lower() for body in await h.notices())


async def test_the_halt_outranks_everything_a_budget_does(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """"Halted means Epigone signs nothing" keeps meaning exactly that. A
    budget adds a per-sub state the halt outranks everywhere: a halted cycle
    still MEASURES (reconciliation signs nothing and runs anyway) but nothing
    it decides can produce an order — not even the exits a wind-down would
    otherwise copy — and nothing about a budget can clear a halt."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, loss_budget="500", baseline="1000")
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_account_value(SUB, Decimal("400"))
    await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason="operator /kill",
        requested_by=OPERATOR,
    )

    await emit(pool, closed(coin="ETH"), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []  # nothing signed, in either direction
    assert await outstanding(pool) != []  # nothing claimed either
    judged = await h.sub(sub.id)
    assert judged.winding_down is True  # the breach was still measured and recorded
    assert await is_halted(pool) is True  # and the halt stands, untouched


async def test_budget_events_never_page(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Operator decision: a page means something is BROKEN, and a budget doing
    exactly what it was set to do is not that. Asserted by absence — the
    monitor's action tuple gains nothing, and no notice rides the pager kind."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, loss_budget="500", baseline="1000")
    gateway.set_positions(SUB, [])
    gateway.set_account_value(SUB, Decimal("300"))

    await h.executor.run_cycle()

    budget_actions = {
        action for action, _ in await h.audit_actions() if action.startswith("copy_budget")
    }
    assert budget_actions  # the cycle really did decide something
    assert budget_actions.isdisjoint(COPY_PAGER_ACTIONS)
    kinds = await pool.fetch("SELECT DISTINCT kind FROM copy_notices")
    assert PAGER not in {row["kind"] for row in kinds}


# --- reconciliation (decision 10) ---------------------------------------------


async def test_a_size_divergence_adopts_the_exchanges_answer(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # Exchange is truth. Adopt, audit, never place an order to close the gap.
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.07")])

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["size_coin"] == Decimal("0.07")
    assert h.placed() == []  # never auto-corrected
    assert any(action == "copy_size_adopted" for action, _ in await h.audit_actions())


async def test_a_position_gone_with_no_trigger_is_the_operator_winning(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [])

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_reason"] == ep.ENDED_OPERATOR
    assert h.placed() == []


async def test_a_liquidation_ends_the_episode_and_pages(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The fills say it outright. ADR-0007's table names this case "position
    gone + equity cratered", but no honest equity threshold separates a
    liquidated $200 position inside a $1000 allocation from a losing exit —
    the fill's own `dir` does."""
    from epigone.gateway import Fill

    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [])
    gateway.set_fills(
        SUB,
        [
            Fill(
                coin="ETH",
                price=Decimal("1800"),
                size=Decimal("0.1"),
                direction="Liquidated Isolated Long",
                closed_pnl=Decimal("-20"),
                start_position=Decimal("0.1"),
                crossed=True,
                order_id=99,
                time=clock.now(),
            )
        ],
    )

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_reason"] == ep.ENDED_LIQUIDATED
    kinds = await pool.fetch("SELECT kind FROM copy_notices ORDER BY id")
    assert any(k["kind"] == "pager" for k in kinds)


async def test_a_side_that_disagrees_with_the_episode_pages_and_adopts_nothing(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # Decision 10's "unclassifiable" row: adopt nothing, page, re-flag until
    # resolved. Nothing in the executor can produce this, so it is a bug or an
    # outside actor — and either way guessing would be worse.
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [position(coin="ETH", side=Side.SHORT, size_coin="0.1")])

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_at"] is None  # adopted nothing
    assert row["size_coin"] == Decimal("0.1")
    assert any(
        action == "copy_divergence_unclassifiable" for action, _ in await h.audit_actions()
    )


# --- brackets (decisions 6, 9) ------------------------------------------------


async def test_a_bracket_sub_wraps_its_fill_in_exchange_native_triggers(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
    )
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
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

    await h.executor.run_cycle()

    (orders, grouping, _b, vault), = h.placed()
    assert vault == SUB
    assert {o.trigger.tpsl.value for o in orders} == {"tp", "sl"}
    assert {o.trigger.trigger_price for o in orders} == {Decimal("2200"), Decimal("1900")}
    assert all(o.reduce_only for o in orders)
    assert grouping.value == "positionTpsl"


async def test_a_bracket_is_placed_at_our_fill_not_at_the_next_sweep(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 6 says the bracket is applied AT OUR FILL TIME. Leaving it to
    the periodic verification pass would leave a fresh position unstopped for
    up to a minute — exactly the window a stop exists to cover — so the entry
    and its bracket go out in the same cycle, anchored to the price we
    actually paid."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
    )
    gateway.set_positions(SUB, [])
    gateway.set_open_orders(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=8, total_size=Decimal("0.1"), avg_price=Decimal("1950"))]
    )

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    entry, bracket = h.placed()
    assert entry[0][0].reduce_only is False
    # Anchored to OUR 1950 fill, not the leader's 2000 entry.
    assert {leg.trigger.trigger_price for leg in bracket[0]} == {
        Decimal("2145"),  # +10%
        Decimal("1852.5"),  # −5%
    }
    (episode,) = await h.episodes(sub.id)
    stored = await ep.bracket_orders(pool, episode["id"])
    assert {o.tpsl for o in stored} == {"tp", "sl"}


async def test_brackets_are_restored_after_a_halt_sweep_cancelled_them(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 9's (r1a). A halt sweep cancels the triggers and hold-and-alert
    keeps the position, so a bracket sub's survivors come out unstopped.
    Expressed as an invariant to restore rather than a transition to detect,
    so a restart cannot lose it."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
    )
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await ep.record_bracket(
        pool, episode_id=episode.id, order_id=555, tpsl="sl", placed_at=clock.now()
    )
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_open_orders(SUB, [])  # the sweep cancelled them

    await h.executor.run_cycle()

    assert h.placed()  # re-placed
    assert any("Bracket re-placed" in body for body in await h.notices())
    assert any(
        action == "bracket_restored" for action, _ in await h.audit_actions()
    )


async def test_a_fired_bracket_ends_the_episode_and_later_events_are_skipped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The episode rule g1: a bracket exit is a local override of the Leader's
    exit timing, so re-entering would churn against it. Later events on that
    position are claimed and skipped."""
    h = await build_harness(pool, clock, gateway, policy=RiskPolicy())
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
    )
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    await ep.record_bracket(
        pool, episode_id=episode.id, order_id=555, tpsl="tp", placed_at=clock.now()
    )
    gateway.set_positions(SUB, [])  # the trigger fired and closed us
    gateway.set_open_orders(SUB, [])

    await emit(pool, scaled("scale_in", "5", "7"), clock.now())
    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_reason"] == ep.ENDED_BRACKET
    assert h.placed() == []  # the leader's scale is not followed
    assert await outstanding(pool) == []  # …but it IS claimed


async def test_a_halted_cycle_places_no_brackets_and_says_the_position_is_unstopped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Brackets are the ONE order shape this executor leaves RESTING, so they
    are the one that can outlive the halt sweep's enumeration — the #143
    residual race, for real. The gate is re-checked immediately before
    signing, and the operator is told the position is unstopped."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(
        pool, clock, mode="bracket", take_profit_pct="10", stop_loss_pct="5"
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    gateway.set_open_orders(SUB, [])

    # Halted AFTER the cycle-top check would have run — i.e. the reconcile
    # happens, then /kill lands, then the bracket path is reached.
    original = h.executor._maintain_brackets

    async def halt_then_maintain(*args: object, **kwargs: object) -> None:
        await request_halt(
            pool,
            clock,
            ExecutionAudit(pool, clock),
            source=KILL_SOURCE,
            reason="operator /kill",
            requested_by=OPERATOR,
        )
        await original(*args, **kwargs)  # type: ignore[arg-type]

    h.executor._maintain_brackets = halt_then_maintain  # type: ignore[method-assign]

    await h.executor.run_cycle()

    assert h.placed() == []
    assert any("UNSTOPPED until /resume" in body for body in await h.notices())


# --- provisioning (decision 12) -----------------------------------------------


async def test_a_pending_mapping_is_created_and_funded_by_the_executor(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """/copy runs in the bot process, which holds no signer (ADR-0005), so the
    sub is minted and funded here — and the address is written down BEFORE the
    transfer, because a sub-account cannot be deleted and the master holds
    only ten."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    h.exec_fake.sub_addresses.append(SUB)

    await h.executor.run_cycle()

    (created, funded) = [
        payload for method, payload in h.exec_fake.actions if method != "place_orders"
    ]
    assert created == sub.sub_name
    assert funded == (SUB, True, 1_000_000_000)  # micro-USD
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] == SUB and stored["provisioned_at"] is not None


async def test_a_halted_cycle_neither_mints_nor_funds_a_sub_account(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Unlike an IOC, a funding transfer cannot be un-sent and a sub-account
    cannot be un-minted — so both legs carry the same late halt re-check the
    order legs do."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, provisioned=False)
    h.exec_fake.sub_addresses.append(SUB)
    await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason="operator /kill",
        requested_by=OPERATOR,
    )
    # run_cycle stops at its own halt check, so drive provisioning directly —
    # this pins the LATE check, not the cycle-top one.
    await h.executor._provision(
        await subs_store.enabled_subs(pool, OPERATOR), clock.now()
    )

    assert h.exec_fake.actions == []
    assert any("NOT created — execution is halted" in body for body in await h.notices())


async def test_a_recopied_sub_is_topped_up_to_its_allocation(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """/uncopy never flattens, so a re-copied Leader's sub comes back holding
    whatever last time left in it. The allocation is a TARGET BALANCE — it is
    the exchange-enforced exposure cap — so only the difference moves: a
    drained sub must not trade on a cap the operator never agreed to, and a
    surviving balance must not be doubled."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, allocation="1000")
    await subs_store.disable_sub(pool, operator_id=OPERATOR, leader_address=LEADER)
    # It lost most of the allocation while we were copying it.
    gateway.account_values[(SUB, None)] = Decimal("150")
    reenabled = await subs_store.reenable_sub(
        pool,
        operator_id=OPERATOR,
        leader_address=LEADER,
        allocation_usd=Decimal("1000"),
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        loss_budget_usd=None,
    )
    assert reenabled is not None and reenabled.provisioned_at is None  # funding reopened

    await h.executor.run_cycle()

    transfers = [p for m, p in h.exec_fake.actions if m == "sub_account_transfer"]
    assert transfers == [(SUB, True, 850_000_000)]  # $850, not $1000
    assert [m for m, _ in h.exec_fake.actions if m == "create_sub_account"] == []
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["provisioned_at"] is not None


async def test_a_sub_already_holding_its_allocation_is_not_funded_again(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # It won while we were away. Topping up nothing is right; draining the
    # excess back is not provisioning's decision to make.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, allocation="1000")
    await subs_store.disable_sub(pool, operator_id=OPERATOR, leader_address=LEADER)
    gateway.account_values[(SUB, None)] = Decimal("1400")
    await subs_store.reenable_sub(
        pool,
        operator_id=OPERATOR,
        leader_address=LEADER,
        allocation_usd=Decimal("1000"),
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        loss_budget_usd=None,
    )

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "sub_account_transfer"] == []
    # The BALANCE, not the allocation: "already holds its $1000 allocation"
    # would understate a sub sitting on $1400 — and an adopted orphan is
    # exactly where the operator needs the real figure.
    assert any("already holds $1400 against its $1000 allocation" in b for b in await h.notices())


async def test_a_failed_funding_transfer_never_mints_a_second_sub_account(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # The irreversible half is persisted the instant it happens; only the
    # retryable half is retried.
    from epigone.gateway.execution import ExecutionError

    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False)
    h.exec_fake.sub_addresses.append(SUB)
    failed = False

    async def transfer_once(*args: object, **kwargs: object) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise ExecutionError("transfer failed")

    h.exec_fake.sub_account_transfer = transfer_once  # type: ignore[method-assign]

    await h.executor.run_cycle()
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] == SUB and stored["provisioned_at"] is None

    await h.executor.run_cycle()
    creates = [m for m, _ in h.exec_fake.actions if m == "create_sub_account"]
    assert len(creates) == 1  # never a second slot burned
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["provisioned_at"] is not None


async def test_provisioning_past_the_v0_ceilings_is_declined_before_money_moves(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock, provisioned=False, allocation="500000", base_stake="200")

    await h.executor.run_cycle()

    assert h.exec_fake.actions == []  # nothing signed, nothing funded
    assert any("was NOT set up" in body for body in await h.notices())
    assert await subs_store.enabled_subs(pool, OPERATOR) == []


# --- adoption at the sub-account cap (issue #178) -----------------------------
#
# The shape every test here uses is the live testnet master's own, because it
# is the one the A4 shakedown runs against: TEN sub-accounts, every slot spent
# (`epicopy`, `agsub`, capprobe_000-007), so `createSubAccount` is refused
# "Too many sub-accounts." and no Leader could be copied at all without
# adoption. The probe subs hold nothing, which is exactly what makes them
# adoptable.


def capped_master(gateway: FakeHyperliquidGateway) -> list[str]:
    """A master at the cap: ten subs, addresses only (what `subAccounts`
    gives). Returned in listing order — adoption takes the FIRST adoptable
    one, so order is part of what these tests pin."""
    held = [f"0x{index:040x}" for index in range(20, 30)]
    gateway.sub_accounts[MASTER] = held
    return held


def cap_refusal() -> ActionRejectedError:
    return ActionRejectedError("Too many sub-accounts.")


async def test_a_cap_refusal_adopts_an_empty_orphan_and_funds_it_to_the_allocation(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The cap is not a wall, it is a fallback: an orphaned sub of the master
    — unmapped and position-free — becomes this Leader's sub, and the ordinary
    funding leg tops it up to the allocation. The inherited balance is handled
    by the same target-balance logic a re-copy uses; it is not a special
    case."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    gateway.account_values[(held[0], None)] = Decimal("150")  # whatever it inherited
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "create_sub_account"] == [
        "create_sub_account"
    ]  # tried to mint FIRST; adoption is the fallback, never the first choice
    transfers = [p for m, p in h.exec_fake.actions if m == "sub_account_transfer"]
    assert transfers == [(held[0], True, 850_000_000)]  # $1000 target, $150 held
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] == held[0] and stored["provisioned_at"] is not None
    assert "copy_sub_adopted" in [action for action, _ in await h.audit_actions()]
    assert any("ADOPTED" in body for body in await h.notices())  # never reads as "created"


async def test_a_sub_mapped_to_any_leader_is_never_adopted(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Three mappings, three reasons the sub behind them is untouchable: an
    ENABLED one is obvious; a DISABLED one still belongs to its Leader
    (/uncopy stops event consumption, not ownership — a later /copy re-enables
    that mapping onto that same sub); and ANOTHER OPERATOR's is excluded by
    the same query, which is read unscoped for exactly this reason. Adopting
    any of them would hand one Leader's money and history to another."""
    h = await build_harness(pool, clock, gateway)
    held = capped_master(gateway)
    enabled_elsewhere = "0xleader11111111111111111111111111111111ff"
    disabled_elsewhere = "0xleader22222222222222222222222222222222ff"
    other_operator = "0xleader33333333333333333333333333333333ff"
    for leader in (enabled_elsewhere, disabled_elsewhere, other_operator):
        await seed_trader(pool, clock, leader)
    await copy_sub(pool, clock, leader=enabled_elsewhere, sub_address=held[0])
    await copy_sub(pool, clock, leader=disabled_elsewhere, sub_address=held[1])
    await subs_store.disable_sub(
        pool, operator_id=OPERATOR, leader_address=disabled_elsewhere
    )
    foreign = await subs_store.register_sub(
        pool,
        operator_id=OPERATOR + 1,
        leader_address=other_operator,
        sub_name="someone-elses",
        allocation_usd=Decimal("1000"),
        base_stake_usd=Decimal("200"),
        leverage_mode="mirror",
        fixed_leverage=None,
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        now=clock.now(),
        loss_budget_usd=None,
    )
    await subs_store.record_sub_address(pool, foreign.id, held[2])
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT sub_address FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["sub_address"] == held[3]


async def test_a_sub_holding_an_open_position_is_never_adopted(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 10's never-touch rule: a sub with a live position is operator
    territory, whoever opened it. The check walks every venue Epigone covers,
    so a builder-DEX-only position protects a sub exactly as a core one
    does."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    gateway.set_positions(held[0], [position(coin="xyz:META")], dex="xyz")
    gateway.set_positions(held[1], [position(coin="BTC")])
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT sub_address FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["sub_address"] == held[2]


async def test_a_cap_refusal_with_no_adoptable_orphan_provisions_nothing(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Nothing left to adopt is the operator's problem to solve, and it must
    read as one: a distinct notice, the mapping disabled, and not one dollar
    moved — the same shape the risk-declined path has."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    for address in held:
        gateway.set_positions(address, [position(coin="BTC")])
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "sub_account_transfer"] == []
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] is None and not stored["enabled"]
    assert any("all 10" in body and "was NOT set up" in body for body in await h.notices())


async def test_an_unreadable_candidate_defers_adoption_rather_than_failing_loudly(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """"I cannot tell" is not "there is none". A candidate whose positions
    will not read might be the empty one, so the pass defers and retries —
    disabling the mapping on a read failure would turn a blip into an
    operator ticket."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    for address in held[1:]:
        gateway.set_positions(address, [position(coin="BTC")])
    gateway.positions_errors[held[0]] = GatewayError("clearinghouseState unavailable")
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] is None and stored["enabled"]  # retried next cycle
    assert not any("was NOT set up" in body for body in await h.notices())

    # And when the read comes back, the same orphan is adopted.
    gateway.positions_errors.pop(held[0])
    h.exec_fake.errors.append(cap_refusal())
    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT sub_address FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["sub_address"] == held[0]


async def test_an_unreadable_sub_listing_defers_adoption(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    capped_master(gateway)
    gateway.sub_account_errors[MASTER] = GatewayError("subAccounts unavailable")
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] is None and stored["enabled"]


async def test_an_empty_listing_under_a_cap_refusal_defers_rather_than_disabling(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """"The cap is full" and "the master holds nothing" cannot both be true.
    One of the two reads is wrong and nothing here can say which, so this is
    an "I cannot tell" — and an empty candidate loop must not be mistaken for
    "I looked at ten and none was adoptable"."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    gateway.sub_accounts[MASTER] = []
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] is None and stored["enabled"]
    assert not any("was NOT set up" in body for body in await h.notices())


async def test_an_adopted_sub_is_renamed_for_its_leader(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Finding 11: `subAccountModify` renames a sub, so an adopted
    `capprobe_003` does not stay `capprobe_003` in the operator's exchange
    UI. Cosmetic, and ordered before the money so a rename that fails cannot
    strand a funded sub mid-pass."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    h.exec_fake.errors.append(cap_refusal())

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions] == [
        "create_sub_account",  # refused at the cap
        "rename_sub_account",
        "sub_account_transfer",
    ]
    renames = [p for m, p in h.exec_fake.actions if m == "rename_sub_account"]
    assert renames == [(held[0], sub.sub_name)]


async def test_a_refused_rename_never_blocks_an_adoption(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The ticket's own instruction: if the name cannot follow the Leader,
    note the cosmetic mismatch and move on. A sub that is funded and copying
    under the wrong label is strictly better than a Leader that is not copied
    at all."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    h.exec_fake.errors.extend([cap_refusal(), ActionRejectedError("Invalid sub-account name.")])

    await h.executor.run_cycle()

    transfers = [p for m, p in h.exec_fake.actions if m == "sub_account_transfer"]
    assert transfers == [(held[0], True, 1_000_000_000)]
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None and stored["provisioned_at"] is not None
    assert any("still named" in body for body in await h.notices())


async def test_a_refusal_that_is_not_the_cap_never_adopts(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Adoption is keyed to ONE refusal. Any other — a volume gate, an
    unauthorized signer — leaves the mapping pending for the next cycle,
    because taking over someone else's sub is not a reasonable answer to a
    problem we have not diagnosed."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    capped_master(gateway)
    h.exec_fake.errors.append(
        ActionRejectedError(
            "Cannot create sub-accounts until enough volume traded. Required: $100000"
        )
    )

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "sub_account_transfer"] == []
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] is None and stored["enabled"]


async def test_a_halt_landing_between_the_adoption_and_the_rename_signs_nothing(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """THE WINDOW THIS GATE EXISTS FOR. The "create" halt check is already
    seconds to tens of seconds old by the time a sub is adopted — the listing
    read and one position read per candidate happen in between — and the
    rename is a SIGNATURE. So the halt is requested here after the adoption
    commits and before the rename is called: no subAccountModify is signed, no
    transfer follows, and the adoption STAYS (a row in our own database is not
    something the exchange saw), so the next cycle resumes at the funding leg
    instead of taking over a second sub."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    h.exec_fake.errors.append(cap_refusal())
    audit = ExecutionAudit(pool, clock)
    adopt = h.executor._adopt_orphan_sub

    async def adopt_then_halt(*args: object, **kwargs: object) -> str | None:
        address = await adopt(*args, **kwargs)  # type: ignore[arg-type]
        await request_halt(
            pool,
            clock,
            audit,
            source=KILL_SOURCE,
            reason="operator /kill",
            requested_by=OPERATOR,
        )
        return address

    h.executor._adopt_orphan_sub = adopt_then_halt  # type: ignore[method-assign]

    await h.executor.run_cycle()

    signed = [m for m, _ in h.exec_fake.actions]
    assert "rename_sub_account" not in signed  # the gap this fix closed
    assert "sub_account_transfer" not in signed
    assert any("NOT renamed — execution is halted" in body for body in await h.notices())
    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] == held[0] and stored["provisioned_at"] is None


async def test_an_adoption_is_reported_before_the_funding_leg_can_fail(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The operator learns a sub was ADOPTED rather than minted at the moment
    it happens, in the same transaction as the address and the audit row —
    not at the end of a provisioning run that may not finish. A funding leg
    that defers must not cost them that sentence."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    h.exec_fake.errors.append(cap_refusal())

    # The candidate reads fine while it is being JUDGED and fails only on the
    # funding leg's equity read, which comes after the rename — so adoption
    # completes and funding defers.
    async def unreadable_once_adopted(address: str, dex: str | None = None) -> list[Position]:
        renamed = any(method == "rename_sub_account" for method, _ in h.exec_fake.actions)
        if renamed and address == held[0]:
            raise GatewayError("balance unreadable")
        return []

    gateway.get_open_positions = unreadable_once_adopted  # type: ignore[method-assign]

    await h.executor.run_cycle()

    stored = await pool.fetchrow("SELECT * FROM copy_subs WHERE id = $1", sub.id)
    assert stored is not None
    assert stored["sub_address"] == held[0] and stored["provisioned_at"] is None
    assert any("ADOPTED" in body for body in await h.notices())
    assert "copy_sub_adopted" in [action for action, _ in await h.audit_actions()]


async def test_an_exit_sliver_under_the_exchange_minimum_is_skipped_not_retried(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Below the exchange's $10 minimum order value, three reduce-only retries
    are three guaranteed rejects and a "0 of X closed" report that reads like
    a market problem. Skip it with its own reason; the residue stays ours and
    reconciliation keeps reporting it (decision 10's self-damping)."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.02"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    # 0.02 ETH at $2000 is $40 held; a 10% trim is $4 — under the minimum.
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.02", size_usd="40")])

    await emit(pool, scaled("scale_out", "10", "9"), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []  # not one guaranteed-reject order
    assert await outstanding(pool) == []  # claimed: handled
    assert any("exit sliver" in body for body in await h.notices())


async def test_unreadable_fills_page_as_unclassifiable_not_operator_closed(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """A vanished position whose fills cannot be read might have been
    liquidated. Labelling it "the operator closed it" would adopt the state
    and page nobody — decision 10 has a row for exactly this: adopt nothing,
    page, re-flag until resolved."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    episode = await ep.open_episode(
        pool,
        sub_id=sub.id,
        coin="ETH",
        side="long",
        entry_price=Decimal("2000"),
        size_coin=Decimal("0.1"),
        opened_at=clock.now(),
        opened_event_id=None,
    )
    gateway.set_positions(SUB, [])
    gateway.fills_errors[SUB] = RuntimeError("fills endpoint down")

    await h.executor.run_cycle()

    row = await pool.fetchrow("SELECT * FROM copy_episodes WHERE id = $1", episode.id)
    assert row is not None and row["ended_at"] is None  # adopted NOTHING
    assert any(
        action == "copy_divergence_unclassifiable" for action, _ in await h.audit_actions()
    )
    kinds = await pool.fetch("SELECT kind FROM copy_notices ORDER BY id")
    assert any(k["kind"] == "pager" for k in kinds)


async def test_a_scale_in_is_copied_without_a_leader_equity_fetch(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """ADR-0007 amendment D-2: the liveness floor is decision 7's letter —
    open and flip's open leg ONLY. A scale-in continues a position we already
    opened on a Leader we already judged, and refusing it would leave us
    half-mirrored rather than flat."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    # The leader is dead on the SIGNAL network and unreadable on it besides,
    # and holds nothing on the book we trade: no side's answer may matter to a
    # scale-in (#184 keeps amendment D-2's letter across both gateways).
    h.signal.account_values[(LEADER, None)] = Decimal("1")
    h.signal.positions_errors[LEADER] = RuntimeError("info down")
    gateway.account_values[(LEADER, None)] = Decimal("1")
    before = len([c for c in gateway.positions_calls if c[0] == LEADER])

    await emit(pool, scaled("scale_in", "10", "15"), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # copied
    after = len([c for c in gateway.positions_calls if c[0] == LEADER])
    assert after == before  # …and the leader was never read
    assert [c for c in h.signal.positions_calls if c[0] == LEADER] == []


async def test_a_stale_flip_half_executes_to_flat(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 8's stated consequence, pinned on its own: the close leg is
    risk-REDUCING so it fires at any age, the open leg is risk-increasing so
    the 5-minute guard skips it, and we end FLAT with both halves audited."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
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
    gateway.set_positions(SUB, [position(coin="ETH", size_coin="0.1")])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=12, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(
        pool,
        PositionEvent(
            kind="flip",
            coin="ETH",
            side="short",
            prev_side="long",
            size_usd=Decimal("10000"),
            size_coin=Decimal("5"),
            entry_price=Decimal("2000"),
            leverage=Decimal("1"),
        ),
        clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1),
    )
    await h.executor.run_cycle()

    (close_leg,) = h.placed()  # the close leg only
    assert close_leg[0][0].reduce_only is True
    notices = await h.notices()
    assert any("Copied flip close" in body for body in notices)
    assert any("stale entry" in body for body in notices)
    assert await outstanding(pool) == []


# --- the heartbeat ------------------------------------------------------------


async def test_every_cycle_beats_the_heartbeat_the_watchdog_watches(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # ADR-0002: the watchdog's ONLY view of this process. A stale row is what
    # trips the dead-man's switch, so the beat is the first thing a cycle does.
    from epigone.safety import heartbeat

    h = await build_harness(pool, clock, gateway)
    await h.executor.run_cycle()
    assert await heartbeat.last_beat(pool, heartbeat.EXECUTOR_PROCESS) == clock.now()


# --- the live gate (§8) -------------------------------------------------------
#
# A4's version of this section asserted that NOTHING in the execute package
# passed `allow_mainnet` at all — the gate was closed by there being no wiring.
# A5 ships the wiring, so the property being pinned changes shape: the gate is
# now closed by DEFAULT and takes two deliberate acts to open. That is the
# ticket's §8, and these are the tests that make "the default stays testnet" a
# fact rather than an intention.


def test_mainnet_takes_the_flag_and_the_url_and_defaults_to_neither(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXECUTOR_ALLOW_MAINNET", raising=False)
    monkeypatch.delenv("EXECUTOR_EXCHANGE_URL", raising=False)
    config = ExecutorConfig.from_env()
    assert config.exchange_url == TESTNET_EXCHANGE_URL
    assert config.allow_mainnet is False
    assert config.is_mainnet is False

    # The URL alone is not enough: the gateway still refuses at construction.
    monkeypatch.setenv("EXECUTOR_EXCHANGE_URL", MAINNET_EXCHANGE_URL)
    url_only = ExecutorConfig.from_env()
    assert url_only.allow_mainnet is False
    assert url_only.is_mainnet is False

    # And the flag alone is not enough either — it leaves the testnet URL.
    monkeypatch.delenv("EXECUTOR_EXCHANGE_URL")
    monkeypatch.setenv("EXECUTOR_ALLOW_MAINNET", "true")
    flag_only = ExecutorConfig.from_env()
    assert flag_only.allow_mainnet is True
    assert flag_only.is_mainnet is False

    monkeypatch.setenv("EXECUTOR_EXCHANGE_URL", MAINNET_EXCHANGE_URL)
    assert ExecutorConfig.from_env().is_mainnet is True


def test_a_flag_value_that_is_not_a_yes_leaves_mainnet_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "EXECUTOR_ALLOW_MAINNET=0 enabled mainnet" must never be a sentence
    # anyone can say about this system.
    for value in ("0", "false", "no", "maybe", " "):
        monkeypatch.setenv("EXECUTOR_ALLOW_MAINNET", value)
        assert ExecutorConfig.from_env().allow_mainnet is False


def test_the_watchdog_opens_on_the_executors_own_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sweep must reach whatever book the executor can trade on. One flag
    for the pair is what makes a live executor beside a testnet-refused
    watchdog — a halt that records and never sweeps — impossible to type."""
    monkeypatch.delenv("EXECUTOR_ALLOW_MAINNET", raising=False)
    assert WatchdogConfig.from_env().allow_mainnet is False
    monkeypatch.setenv("EXECUTOR_ALLOW_MAINNET", "1")
    assert WatchdogConfig.from_env().allow_mainnet is True


# --- the signal network (#184) ------------------------------------------------
#
# The one read that is NOT about the book this process trades: the Leader's
# liveness equity, which is a fact about the account the SIGNAL came from. On a
# mainnet deployment the two urls coincide and none of this is observable; on
# the testnet shakedown it is the difference between copying a real mainnet
# Leader and skipping every entry at $0.


def test_the_signal_info_url_defaults_to_the_mainnet_tracking_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default is the endpoint the POLLERS read — the network the signal
    was observed on — while the trade-side info url stays derived from the
    exchange url. The harness needs no special configuration."""
    monkeypatch.delenv("EXECUTOR_SIGNAL_INFO_URL", raising=False)
    monkeypatch.delenv("EXECUTOR_EXCHANGE_URL", raising=False)
    config = ExecutorConfig.from_env()
    assert config.signal_info_url == INFO_URL
    assert config.info_url == TESTNET_INFO_URL


def test_the_signal_info_url_takes_an_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A topology change — a Leader tracked on some other network — is a
    configuration act, not a code change. And it moves NOTHING else: the
    trade-side info url stays derived from the exchange url, which is what
    keeps orders and order-adjacent reads single-network by construction."""
    monkeypatch.setenv("EXECUTOR_SIGNAL_INFO_URL", TESTNET_INFO_URL)
    monkeypatch.setenv("EXECUTOR_EXCHANGE_URL", MAINNET_EXCHANGE_URL)
    config = ExecutorConfig.from_env()
    assert config.signal_info_url == TESTNET_INFO_URL
    assert config.info_url == INFO_URL  # derived from the exchange url, untouched


def test_a_malformed_signal_info_url_degrades_to_the_default_with_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The executor-config convention (the exchange-url precedent): a typo
    never wedges the copy loop, and it never passes silently either."""
    for bad in ("api.hyperliquid.xyz", "https://api.hyperliquid.xyz/exchange", " "):
        caplog.clear()
        monkeypatch.setenv("EXECUTOR_SIGNAL_INFO_URL", bad)
        with caplog.at_level(logging.WARNING):
            assert ExecutorConfig.from_env().signal_info_url == INFO_URL
        assert "EXECUTOR_SIGNAL_INFO_URL" in caplog.text


# --- backlog-drain notice coalescing (issue #190, amendment D-7) --------------


async def test_a_backlog_drain_reports_one_summary_instead_of_a_storm(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Observed live 2026-08-06: the first /copy of a Leader tracked for weeks
    flushed ~20 chat messages in one burst, every one of them the same
    sentence about a different coin. Decision 11's full verbosity is right for
    the trickle it was written for and useless for a flush — so past the
    threshold the cycle speaks once."""
    h = await build_harness(pool, clock, gateway)
    gateway.set_positions(SUB, [])

    # The events land while the Leader is merely TRACKED: nobody copies them,
    # so nothing claims them and they sit in the backlog accumulating.
    stale = clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1)
    count = SKIP_DIGEST_THRESHOLD + 1
    for i in range(count):
        await emit(pool, opened(coin=f"COIN{i}"), stale)
    assert len(await outstanding(pool)) == count

    await copy_sub(pool, clock)  # /copy — and the whole backlog qualifies at once
    await h.executor.run_cycle()

    notices = await h.notices()
    assert len(notices) == 1
    assert str(count) in notices[0] and "stale entry" in notices[0]
    assert h.placed() == []
    assert await outstanding(pool) == []  # every one of them still handled


async def test_a_handful_of_skips_still_speaks_one_sentence_each(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The other side of the same line, and the one decision 11 already had
    right: live operation is a trickle, and a trickle is exactly what full
    verbosity was written for. At the threshold itself, nothing coalesces."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])

    stale = clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1)
    for i in range(SKIP_DIGEST_THRESHOLD):
        await emit(pool, opened(coin=f"COIN{i}"), stale)

    await h.executor.run_cycle()

    notices = await h.notices()
    assert len(notices) == SKIP_DIGEST_THRESHOLD
    assert all(body.startswith("⏭ Skipped open on COIN") for body in notices)
    assert all("stale entry: observed" in body for body in notices)


async def test_the_summary_counts_by_reason_and_by_leader(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """What the operator needs out of a drain is not the list — it is "how
    many died on the guard, how many referred to positions we never had, whose
    were they". One message, per leader, per reason."""
    other = "0xsecond00000000000000000000000000000000ee"
    other_sub = "0x00000000000000000000000000000000000000002"
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    await seed_trader(pool, clock, other)
    await copy_sub(pool, clock, leader=other, sub_address=other_sub)
    gateway.set_positions(SUB, [])
    gateway.set_positions(other_sub, [])

    stale = clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1)
    for i in range(6):
        await emit(pool, opened(coin=f"COIN{i}"), stale)
    for i in range(2):
        await emit(pool, closed(coin=f"GONE{i}"), clock.now(), trader=other)

    await h.executor.run_cycle()

    notices = await h.notices()
    assert len(notices) == 1
    assert "8 leader events skipped, summarised" in notices[0]
    # Addresses as the operator reads them, written out rather than derived,
    # so a change to the shortening has to be a deliberate one.
    assert "• 0xlead…00aa — 6: 6 stale entry" in notices[0]
    assert "• 0xseco…00ee — 2: 2 no local position" in notices[0]
    # The pointer to where the detail actually lives, since the sentences went.
    assert "audit trail" in notices[0] and "copy_skipped" in notices[0]
    assert "`" not in notices[0]  # sent with no parse_mode (#185)


async def test_pagers_and_copied_actions_stay_individual_inside_a_flush(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The hard boundary on the coalescing: volume is a reason to summarise
    what did NOT happen, never what did. An order that reached the wire always
    reports on its own, and a 🚨 is a page — burying either one inside a count
    would make a busy cycle the best moment to miss the thing that mattered."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock)
    # Decision 10's unclassifiable divergence: the episode's position is gone
    # and the fills that would explain it are unreadable. Adopt nothing, page.
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
    gateway.set_positions(SUB, [])
    gateway.fills_errors[SUB] = RuntimeError("fills endpoint down")

    stale = clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1)
    for i in range(SKIP_DIGEST_THRESHOLD + 1):
        await emit(pool, opened(coin=f"COIN{i}"), stale)
    await emit(pool, opened(coin="BTC"), clock.now())  # fresh, and copyable
    h.exec_fake.place_results.append(
        [OrderFilled(oid=1, total_size=Decimal("0.003"), avg_price=Decimal("63500"))]
    )

    await h.executor.run_cycle()

    notices = await h.notices()
    assert len(h.placed()) == 1
    assert len(notices) == 3
    assert sum(body.startswith("🚨") for body in notices) == 1
    assert sum(body.startswith("📈 Copied open — BTC long") for body in notices) == 1
    assert sum("leader events skipped, summarised" in body for body in notices) == 1


async def test_a_resume_drains_its_halt_backlog_into_one_summary(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The second shape of the same burst. A halt claims nothing (decision 9
    keeps the backlog intact for /resume), so the events pile up for as long
    as the halt lasts and the first cycle after it disposes of all of them —
    every one past the staleness guard by then, which is decision 8 working,
    not a failure to report."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    halt, _ = await request_halt(
        pool,
        clock,
        ExecutionAudit(pool, clock),
        source=KILL_SOURCE,
        reason="operator /kill",
        requested_by=OPERATOR,
    )

    count = SKIP_DIGEST_THRESHOLD + 3
    for i in range(count):
        await emit(pool, opened(coin=f"COIN{i}"), clock.now())
    await h.executor.run_cycle()
    assert len(await outstanding(pool)) == count  # a halt drains nothing
    assert await h.notices() == []

    clock.advance(ENTRY_STALENESS_GUARD.total_seconds() + 1)
    await resume(pool, clock, ExecutionAudit(pool, clock), halt_id=halt.id, resumed_by=OPERATOR)
    await h.executor.run_cycle()

    notices = await h.notices()
    assert h.placed() == []
    assert len(notices) == 1
    assert f"⏭ {count} leader events skipped, summarised" in notices[0]
    assert f"{count} stale entry" in notices[0]


async def test_the_audit_trail_is_one_row_per_event_in_both_regimes(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The boundary the coalescing must not cross. Chat delivery is the only
    thing that changes with volume: the trail records the same row, with the
    same sentence, whether the cycle sent five messages or one.

    Both regimes run here against the same clock and the same event shape, so
    the sentences are comparable literally rather than by inspection — and a
    summary that quietly abbreviated the trail would show up as two."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    stale = clock.now() - ENTRY_STALENESS_GUARD - timedelta(seconds=1)

    for i in range(3):  # under the threshold: one sentence each
        await emit(pool, opened(coin=f"TRICKLE{i}"), stale)
    await h.executor.run_cycle()

    for i in range(SKIP_DIGEST_THRESHOLD + 1):  # over it: one summary
        await emit(pool, opened(coin=f"FLUSH{i}"), stale)
    await h.executor.run_cycle()

    assert len(await h.notices()) == 4  # 3 sentences + 1 summary
    skipped = [reason for action, reason in await h.audit_actions() if action == "copy_skipped"]
    assert len(skipped) == 3 + SKIP_DIGEST_THRESHOLD + 1
    assert len(set(skipped)) == 1  # verbatim, and the same sentence in both
    assert skipped[0].startswith("stale entry: observed")
