"""The operator copy executor (issue #136, ADR-0007).

These pin the DECISIONS, not the plumbing: tracking is not copying, sizing is
fixed and relative, entries are guarded and one-shot, exits are ungated and
retried, a claim means handled, and nothing signs while halted.

Seam per the house convention: fake read gateway, fake execution gateway, fake
clock, real Postgres.
"""

from datetime import timedelta
from decimal import Decimal

import asyncpg

from epigone.execute import episodes as ep
from epigone.execute import subs as subs_store
from epigone.execute.executor import EXECUTOR_CONSUMER
from epigone.execute.policy import ENTRY_STALENESS_GUARD, LEADER_EQUITY_FLOOR, RiskPolicyV0
from epigone.gateway import Side
from epigone.gateway.execution import OrderFilled, OrderRejected, RejectReason, Tif
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.position_events import ClaimableEvent, PositionEvent, outstanding_events
from epigone.safety.audit import ExecutionAudit
from epigone.safety.halt import KILL_SOURCE, request_halt
from tests.support.clock import FakeClock
from tests.support.copy import (
    LEADER,
    OPERATOR,
    SUB,
    build_harness,
    copy_sub,
    emit,
    position,
    seed_trader,
)


def opened(coin: str = "ETH", side: str = "long", size_coin: str = "5") -> PositionEvent:
    return PositionEvent(
        kind="open",
        coin=coin,
        side=side,
        size_usd=Decimal("10000"),
        size_coin=Decimal(size_coin),
        entry_price=Decimal("2000"),
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
    sub = await copy_sub(pool, clock, base_notional="200")
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
    time, and it gates entries only."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    gateway.account_values[(LEADER, None)] = LEADER_EQUITY_FLOOR - Decimal("1")

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert any("liveness floor" in body for body in await h.notices())


async def test_an_unreadable_leader_equity_defers_rather_than_claims(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # A network blip is not a decision about the event. Claiming here would
    # silently drop a copy; leaving it outstanding asks again next cycle.
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    gateway.positions_errors[LEADER] = RuntimeError("info down")

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
    # The close leg reads our sub fine; only the LEADER's equity is unreadable.
    gateway.positions_errors[LEADER] = RuntimeError("info down")

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


async def test_the_backlog_ignores_the_websocket_shadow_lanes_duplicate_rows(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Decision 4's forced filter. The WS shadow lane dual-writes every
    (trader, coin) it observes; an unfiltered executor would copy each trade
    TWICE for as long as the shadow phase runs."""
    h = await build_harness(pool, clock, gateway)
    await copy_sub(pool, clock)
    gateway.set_positions(SUB, [])
    h.exec_fake.place_results.append(
        [OrderFilled(oid=7, total_size=Decimal("0.1"), avg_price=Decimal("2000"))]
    )

    await emit(pool, opened(), clock.now(), source="poll")
    await emit(pool, opened(), clock.now(), source="ws")
    await h.executor.run_cycle()

    assert len(h.placed()) == 1


# --- the risk policy ----------------------------------------------------------


async def test_a_declined_entry_is_audited_and_claimed_not_silently_dropped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    # The ticket: "hardcoded v0 risk limits enforced pending A5". A decline is
    # a decision — it goes on the trail and into the chat, and it claims.
    h = await build_harness(
        pool, clock, gateway, policy=RiskPolicyV0(max_base_notional=Decimal("50"))
    )
    await copy_sub(pool, clock, base_notional="200")
    gateway.set_positions(SUB, [])

    await emit(pool, opened(), clock.now())
    await h.executor.run_cycle()

    assert h.placed() == []
    assert await outstanding(pool) == []
    assert any("v0 policy DECLINED" in decision for _, decision in await h.audit_actions())


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
        action == "bracket_replaced_after_resume" for action, _ in await h.audit_actions()
    )


async def test_a_fired_bracket_ends_the_episode_and_later_events_are_skipped(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """The episode rule g1: a bracket exit is a local override of the Leader's
    exit timing, so re-entering would churn against it. Later events on that
    position are claimed and skipped."""
    h = await build_harness(
        pool, clock, gateway, policy=RiskPolicyV0()
    )
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
    await copy_sub(pool, clock, provisioned=False, allocation="500000", base_notional="200")

    await h.executor.run_cycle()

    assert h.exec_fake.actions == []  # nothing signed, nothing funded
    assert any("was NOT set up" in body for body in await h.notices())
    assert await subs_store.enabled_subs(pool, OPERATOR) == []


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


def test_no_mainnet_path_is_reachable_from_the_executor() -> None:
    """The live gate, structurally (the ticket: "no mainnet code path
    reachable while the live gate is closed"). Nothing in the execute package
    CALLS HttpExecutionGateway with allow_mainnet, so a mainnet URL is refused
    at construction (MainnetNotEnabledError) until A5 wires it. Asserted over
    the parsed source rather than by string search, so a mention in a comment
    — there is one, explaining the gate — cannot pass or fail it."""
    import ast
    import inspect

    import epigone.execute.config as config_module
    import epigone.execute.executor as executor_module
    import epigone.execute.main as main_module

    for module in (executor_module, main_module, config_module):
        tree = ast.parse(inspect.getsource(module))
        keywords = [
            keyword.arg
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            for keyword in node.keywords
        ]
        assert "allow_mainnet" not in keywords
