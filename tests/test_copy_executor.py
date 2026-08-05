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
from epigone.gateway import GatewayError, Position, Side
from epigone.gateway.execution import (
    ActionRejectedError,
    OrderFilled,
    OrderRejected,
    RejectReason,
    Tif,
)
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.position_events import ClaimableEvent, PositionEvent, outstanding_events
from epigone.safety.audit import ExecutionAudit
from epigone.safety.halt import KILL_SOURCE, request_halt
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
        action == "bracket_restored" for action, _ in await h.audit_actions()
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
        base_notional_usd=Decimal("200"),
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
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
        base_notional_usd=Decimal("200"),
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
    )

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "sub_account_transfer"] == []
    assert any("already holds its $1000 allocation" in b for b in await h.notices())


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
        base_notional_usd=Decimal("200"),
        copy_mode="default",
        take_profit_pct=None,
        stop_loss_pct=None,
        now=clock.now(),
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


async def test_a_halt_landing_after_an_adoption_still_stops_the_funding(
    pool: asyncpg.Pool, clock: FakeClock, gateway: FakeHyperliquidGateway
) -> None:
    """Adoption ends in the same place creation does — money about to move —
    so it meets the same late halt re-check. The halt is requested DURING the
    pass, after the sub has been adopted, which is the window the check exists
    for: nothing is transferred, and the mapping resumes at the funding leg
    next cycle rather than adopting a second sub."""
    h = await build_harness(pool, clock, gateway)
    sub = await copy_sub(pool, clock, provisioned=False, allocation="1000")
    held = capped_master(gateway)
    h.exec_fake.errors.append(cap_refusal())
    audit = ExecutionAudit(pool, clock)

    async def halt_then_rename(*args: object, **kwargs: object) -> None:
        await request_halt(
            pool,
            clock,
            audit,
            source=KILL_SOURCE,
            reason="operator /kill",
            requested_by=OPERATOR,
        )

    h.exec_fake.rename_sub_account = halt_then_rename  # type: ignore[method-assign]

    await h.executor.run_cycle()

    assert [m for m, _ in h.exec_fake.actions if m == "sub_account_transfer"] == []
    assert any("NOT funded — execution is halted" in body for body in await h.notices())
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
    # The leader is BELOW the floor and their equity is unreadable besides:
    # neither may matter to a scale-in.
    gateway.account_values[(LEADER, None)] = Decimal("1")
    before = len([c for c in gateway.positions_calls if c[0] == LEADER])

    await emit(pool, scaled("scale_in", "10", "15"), clock.now())
    await h.executor.run_cycle()

    assert len(h.placed()) == 1  # copied
    after = len([c for c in gateway.positions_calls if c[0] == LEADER])
    assert after == before  # …and the leader was never read


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
