"""The websocket order lane (issue #168, ADR-0008).

The position lane's tests are built around four things that would make its
dataset a lie. This lane's are built around six, and the first is new and is
the reason the lane exists in this shape at all:

1. **Attribution.** An `orderUpdates` frame carries no `user` — verified live
   2026-08-03. The only thing that says whose order it is, is which connection
   delivered it. Every test here stages payloads WITHOUT a user field, because
   putting one in would test a transport that does not exist.
2. **Reconnect resync.** A websocket delivers from the moment you subscribe, so
   what happened during a gap must surface exactly once afterwards — and for
   orders the honest answer is often `gone`, because absolute state cannot say
   whether an order that left the book filled or was cancelled.
3. **Vocabulary.** The exchange's status set is open-ended; a status nobody has
   seen must be recorded, not guessed at and not dropped.
4. **The rate ceiling.** One measured market maker produced 1471 order updates
   in 60s. The seam has to refuse them — and the refusal has to leave honest
   state behind, because it will sometimes catch a human (ADR-0008 deviation 5).
5. **A healthy-looking lane that is recording nothing.** The failure mode this
   transport makes easy: `orderUpdates` reports transitions only, `allMids` keeps
   the socket alive indefinitely, so memory can be permanently wrong behind a
   connection that never fails. Two ways in — a change lost in the
   resync→subscribe window, and a subscription the server refused — and both
   have to end the connection rather than persist.
6. **Nothing consumes it.** Position Alerts, the position lane and REST polling
   behave exactly as before.

Seam per the house convention: fake gateway, fake clock, staged websocket, real
Postgres.
"""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import asyncpg
import pytest

from epigone.budget import WeightBudget
from epigone.gateway import OpenOrder
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.order_events import RESYNC_ORIGIN, STREAM_ORIGIN
from epigone.ws import LIVENESS_CHANNEL, ORDERS_CHANNEL, WebsocketClosed, order_lane
from epigone.ws.lane import (
    LIVENESS_TIMEOUT_SECONDS,
    RECEIVE_TICK_SECONDS,
    RECONNECT_MIN_SECONDS,
    LaneSilent,
)
from epigone.ws.order_lane import (
    CONNECTION_LIMIT,
    CONNECTION_RATE_LIMIT,
    MAX_ORDER_LANES,
    ORDER_CONNECTION_MAX_SECONDS,
    ORDER_RECONNECT_MIN_SECONDS,
    ORDER_UPDATE_RATE_LIMIT,
    REFUSED_COOLDOWN_SECONDS,
    SUBSCRIPTION_COOLDOWN_SECONDS,
    LaneRecycled,
    LaneRefused,
    SubscriptionRefused,
    reconcile_order_lanes,
    run_order_connection,
    run_order_lane,
)
from tests.support.clock import FakeClock
from tests.support.websocket import FakeWebsocket, connecting

WIDE_OPEN_BUDGET = 1_000_000

TRADER = "0xaaa"
OTHER = "0xbbb"
FOLLOWER = 42

PLACED_AT = datetime(2026, 8, 3, 11, 0, tzinfo=UTC)
TICKS_TO_SILENCE = int(LIVENESS_TIMEOUT_SECONDS / RECEIVE_TICK_SECONDS) + 1


def resting(
    oid: int = 1,
    coin: str = "BTC",
    size: str = "3",
    limit_price: str = "63000",
    is_trigger: bool = False,
) -> OpenOrder:
    """One resting order as `frontendOpenOrders` returns it — the resync's view,
    which unlike the stream's knows the order's type."""
    return OpenOrder(
        coin=coin,
        is_buy=True,
        limit_price=Decimal(limit_price),
        size=Decimal(size),
        order_id=oid,
        placed_at=PLACED_AT,
        order_type="Stop Market" if is_trigger else "Limit",
        is_trigger=is_trigger,
        trigger_price=Decimal("60000") if is_trigger else None,
        is_position_tpsl=False,
        reduce_only=False,
    )


def update(
    oid: int = 1,
    status: str = "open",
    coin: str = "BTC",
    size: str = "3",
    original_size: str | None = None,
    limit_price: str = "63000",
    cloid: str | None = None,
) -> dict[str, Any]:
    """One entry of an `orderUpdates` frame, in the shape recorded live on
    2026-08-03. Note what is absent and stays absent: no `user`, and none of the
    trigger fields."""
    order: dict[str, Any] = {
        "coin": coin,
        "side": "B",
        "limitPx": limit_price,
        "sz": size,
        "oid": oid,
        "timestamp": int(PLACED_AT.timestamp() * 1000),
        "origSz": original_size if original_size is not None else size,
    }
    if cloid is not None:
        order["cloid"] = cloid
    return {
        "order": order,
        "status": status,
        "statusTimestamp": int(PLACED_AT.timestamp() * 1000),
    }


def orders_message(*updates: dict[str, Any]) -> dict[str, Any]:
    return {"channel": ORDERS_CHANNEL, "data": list(updates)}


def liveness_message() -> dict[str, Any]:
    return {"channel": LIVENESS_CHANNEL, "data": {"mids": {"BTC": "100"}}}


def error_frame(data: str) -> dict[str, Any]:
    """A server `error` frame, in the shape the probe recorded on 2026-08-03 —
    a bare string that names no subscription."""
    return {"channel": "error", "data": data}


def alive_for(seconds: float) -> list[dict[str, Any] | None]:
    """A script that keeps a connection healthy for `seconds` of lane time.

    Only a quiet tick moves the fake clock (a message costs nothing), so the
    seconds are bought by the `None`s; the liveness messages spaced through them
    are what stop the silence deadline firing first, which is the failure this
    helper exists to NOT stage."""
    script: list[dict[str, Any] | None] = []
    for tick in range(int(seconds / RECEIVE_TICK_SECONDS) + 1):
        if tick % int(LIVENESS_TIMEOUT_SECONDS / 2) == 0:
            script.append(liveness_message())
        script.append(None)
    return script


async def track(pool: asyncpg.Pool, clock: FakeClock, address: str, *user_ids: int) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
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


async def run_once(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    socket: FakeWebsocket,
    address: str = TRADER,
) -> BaseException:
    """Drive one connection to its end and return why it ended. Every staged
    connection ends — that is what makes the test terminate — so the reason is
    an assertion rather than an accident."""
    with pytest.raises(
        (WebsocketClosed, LaneSilent, LaneRefused, SubscriptionRefused, LaneRecycled)
    ) as caught:
        await run_order_connection(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            connecting(socket),
            clock,
            address,
        )
    return caught.value


async def events(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    return await pool.fetch("SELECT * FROM order_events ORDER BY id")


# -- attribution --------------------------------------------------------------


async def test_an_update_is_attributed_to_the_connections_trader(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The whole shape of this lane in one test. The frame says nothing about
    whose order it is; the row still names the right Trader, because the
    connection it arrived on serves exactly one."""
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    gateway.set_open_orders(OTHER, [])

    # Baseline, then a real connection carrying one placement.
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    await run_once(
        pool,
        gateway,
        clock,
        FakeWebsocket(clock, [liveness_message(), orders_message(update(oid=7))]),
    )

    rows = await events(pool)
    assert [(row["trader_address"], row["order_id"], row["kind"]) for row in rows] == [
        (TRADER, 7, "placed")
    ]


async def test_the_frame_is_never_asked_who_it_belongs_to(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A regression guard on the finding itself: even if a frame DID carry a
    user — and a future API version might — the lane must keep attributing by
    connection, because that is the fact it verified. Here a frame claims to be
    someone else's and is still recorded as this connection's Trader."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    impostor = orders_message(update(oid=9))
    impostor["data"][0]["order"]["user"] = OTHER  # type: ignore[index]
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), impostor]))

    assert [row["trader_address"] for row in await events(pool)] == [TRADER]


# -- the lifecycle vocabulary -------------------------------------------------


async def test_the_order_lifecycle_is_persisted_with_its_raw_status(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Place, fill, cancel, reject — each classified, each keeping the word the
    exchange used. The two rejection statuses are the ones observed live."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            orders_message(update(oid=1, status="open")),
            orders_message(update(oid=1, status="filled", size="0")),
            orders_message(update(oid=2, status="open")),
            orders_message(update(oid=2, status="marginCanceled", size="3")),
            orders_message(update(oid=3, status="badAloPxRejected")),
            orders_message(update(oid=4, status="iocCancelRejected")),
        ],
    )
    await run_once(pool, gateway, clock, socket)

    rows = await events(pool)
    assert [(row["kind"], row["status"]) for row in rows] == [
        ("placed", "open"),
        ("filled", "filled"),
        ("placed", "open"),
        ("canceled", "marginCanceled"),
        ("rejected", "badAloPxRejected"),
        ("rejected", "iocCancelRejected"),
    ]
    assert {row["origin"] for row in rows} == {STREAM_ORIGIN}


async def test_a_modify_is_persisted_as_the_two_things_it_actually_is(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Hyperliquid's modify is an atomic cancel/replace minting a new oid, and
    it arrives as a cancel beside a placement in one frame. The seam records
    both rather than collapsing them into a 'modified' kind: collapsing needs
    the alert layer's size-tolerance heuristic, and ADR-0006 keeps that out of
    the execution seam. `status_at` is what lets a consumer pair them."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1)])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    requote = orders_message(
        update(oid=1, status="canceled", size="3"),
        update(oid=2, status="open", size="3", limit_price="63100"),
    )
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), requote]))

    rows = await events(pool)
    assert [(row["order_id"], row["kind"]) for row in rows] == [(1, "canceled"), (2, "placed")]
    assert rows[0]["status_at"] == rows[1]["status_at"]
    # And memory followed: the old oid is gone, the new one rests.
    resting_ids = await pool.fetch(
        "SELECT order_id FROM ws_order_state WHERE trader_address = $1 ORDER BY order_id", TRADER
    )
    assert [row["order_id"] for row in resting_ids] == [2]


async def test_a_partial_fill_leaves_the_order_resting_at_its_new_size(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A fill that did not finish the order must not forget it — the next
    reconnect would then report a phantom `placed` for an order that never
    left."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1, size="3")])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    partial = orders_message(update(oid=1, status="filled", size="1", original_size="3"))
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), partial]))

    assert [(row["kind"], row["size"]) for row in await events(pool)] == [
        ("filled", Decimal("1"))
    ]
    assert await pool.fetchval(
        "SELECT size FROM ws_order_state WHERE trader_address = $1 AND order_id = 1", TRADER
    ) == Decimal("1")


async def test_what_the_stream_cannot_see_stays_unknown_and_what_it_can_is_kept(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A resync reads the order's type over REST; the stream cannot. So a
    resync-derived event carries the trigger fields and a streamed one carries
    NULL — and a streamed update must never blank what the resync learned about
    an order it already knows."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    # A stop appears while the lane is down: the resync sees it, in full.
    gateway.set_open_orders(TRADER, [resting(oid=1, is_trigger=True)])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    placed = (await events(pool))[0]
    assert (placed["kind"], placed["origin"]) == ("placed", "resync")
    assert (placed["is_trigger"], placed["order_type"]) == (True, "Stop Market")

    # Then the stream cancels it. The EVENT cannot know it was a trigger...
    cancel = orders_message(update(oid=1, status="canceled", size="0"))
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), cancel]))
    assert (await events(pool))[-1]["is_trigger"] is None


# -- reconnect: no silent gaps ------------------------------------------------


async def test_a_traders_first_observation_emits_nothing(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A ladder that predates the lane's first look is not something anyone
    could have watched being placed."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1), resting(oid=2, coin="ETH")])

    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    assert await events(pool) == []
    assert await pool.fetchval("SELECT count(*) FROM ws_order_state") == 2


async def test_an_order_placed_during_a_disconnect_yields_exactly_one_event(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The reconnect criterion. The resync sees the new order; the stream's own
    later traffic about it must not double it."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    gateway.set_open_orders(TRADER, [resting(oid=1)])
    clock.advance(30)
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    # A third connection resyncs the same unchanged book: still nothing new.
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    assert [(row["order_id"], row["kind"], row["origin"]) for row in await events(pool)] == [
        (1, "placed", "resync")
    ]


async def test_an_order_that_left_during_a_disconnect_is_gone_not_silent(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Absolute state can say the order ended and never whether it filled or was
    cancelled. `gone` names that. Staying silent would be the silent gap the
    acceptance criterion forbids; picking one of the two would be a guess a copy
    consumer could act on."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1)])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    rows = await events(pool)
    assert [(row["order_id"], row["kind"], row["status"]) for row in rows] == [(1, "gone", None)]
    # It carries what memory knew, since there is nothing live left to read.
    assert rows[0]["limit_price"] == Decimal("63000")
    assert await pool.fetchval("SELECT count(*) FROM ws_order_state") == 0


async def test_a_partial_fill_during_a_disconnect_is_reported(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A resting order's remaining size can only shrink by filling — a modify
    would have minted a new oid — so this is the one thing a resync may infer."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1, size="3")])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    gateway.set_open_orders(TRADER, [resting(oid=1, size="1")])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    assert [(row["kind"], row["size"], row["origin"]) for row in await events(pool)] == [
        ("filled", Decimal("1"), "resync")
    ]


async def test_a_resync_never_forgets_what_an_order_started_as(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """`frontendOpenOrders` reports what REMAINS and never what the order started
    as, so a resync of a partially filled order can only guess original ==
    remaining. Memory holds the real figure and has to win on EVERY resync, not
    only the one that happens to observe the shrink.

    Otherwise a 3-lot order with 1 left quietly becomes a 1-lot order after one
    idle reconnect, and every later event about it — including the `gone` that
    ends it — understates what the Trader had actually committed."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1, size="3")])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    # It partially fills over the stream: 3 committed, 1 left.
    partial = orders_message(update(oid=1, status="filled", size="1", original_size="3"))
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), partial]))

    # Two quiet reconnects that observe nothing new at all.
    for _ in range(2):
        gateway.set_open_orders(TRADER, [resting(oid=1, size="1")])
        await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    assert await pool.fetchval(
        "SELECT original_size FROM ws_order_state WHERE order_id = 1"
    ) == Decimal("3")

    # And the event that ends it still says what the order was for.
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    ended = (await events(pool))[-1]
    assert (ended["kind"], ended["size"], ended["original_size"]) == (
        "gone",
        Decimal("1"),
        Decimal("3"),
    )


class JournallingGateway(FakeHyperliquidGateway):
    """Records its reads into a journal shared with the socket, so a test can
    assert what happened BEFORE what."""

    def __init__(self, journal: list[str]) -> None:
        super().__init__()
        self._journal = journal

    async def get_open_orders(self, address: str, dex: str | None = None) -> list[OpenOrder]:
        self._journal.append(f"resync:{address}")
        return await super().get_open_orders(address, dex=dex)


class JournallingWebsocket(FakeWebsocket):
    def __init__(
        self, clock: FakeClock, script: list[dict[str, Any] | None], journal: list[str]
    ) -> None:
        super().__init__(clock, script)
        self._journal = journal

    async def send(self, message: Any) -> None:
        await super().send(message)
        if message.get("method") == "subscribe":
            self._journal.append(f"subscribe:{message['subscription']['type']}")


async def test_the_resync_happens_before_the_order_subscription(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Order is the correctness argument, not a detail. Subscribing first would
    let a push land while the REST read was in flight; applying the staler
    answer afterwards would resurrect an order the stream had just seen
    cancelled."""
    journal: list[str] = []
    gateway = JournallingGateway(journal)
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1)])

    socket = JournallingWebsocket(clock, [liveness_message()], journal)
    await run_once(pool, gateway, clock, socket)

    assert journal.index(f"resync:{TRADER}") < journal.index(f"subscribe:{ORDERS_CHANNEL}")
    assert socket.subscriptions(ORDERS_CHANNEL) == [TRADER]
    assert await pool.fetchval(
        "SELECT resynced_at FROM ws_order_lane_state WHERE trader_address = $1", TRADER
    ) is not None


async def test_a_failed_resync_never_subscribes(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Subscribing without a resync is the one thing that must not happen: the
    lane would diff a stream against memory it never anchored. A REST failure
    ends the connection instead, and the supervisor backs off and retries."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.open_orders_errors[TRADER] = TimeoutError("hyperliquid is having a moment")

    socket = FakeWebsocket(clock, [liveness_message()])
    with pytest.raises(Exception):  # noqa: B017 — any failure, so long as it does not subscribe
        await run_order_connection(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            connecting(socket),
            clock,
            TRADER,
        )

    assert socket.subscriptions(ORDERS_CHANNEL) == []
    assert await events(pool) == []


# -- a healthy-looking lane that is recording nothing --------------------------


class WindowWebsocket(FakeWebsocket):
    """A socket that plants an order in the Trader's book at the exact instant
    the order subscription is sent.

    That instant IS the resync→subscribe window: the REST read has already
    returned without the order, and the subscription is only now taking effect,
    so the change falls between the two. `orderUpdates` reports transitions, so
    no frame will ever mention this order again — which is precisely why the
    lane cannot heal it by listening harder."""

    def __init__(
        self,
        clock: FakeClock,
        script: list[dict[str, Any] | None],
        gateway: FakeHyperliquidGateway,
        address: str,
        appears: list[OpenOrder],
    ) -> None:
        super().__init__(clock, script)
        self._gateway = gateway
        self._address = address
        self._appears = appears

    async def send(self, message: Any) -> None:
        await super().send(message)
        if (
            message.get("method") == "subscribe"
            and message["subscription"]["type"] == ORDERS_CHANNEL
        ):
            self._gateway.set_open_orders(self._address, self._appears)


async def test_a_change_lost_in_the_resync_subscribe_window_heals_on_a_recycle(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The gap resync-before-subscribe cannot close by itself, and the bound
    that closes it.

    The position lane never has to think about this: its feed re-states absolute
    state every ~5s, so a change lost in the window is corrected by the next
    push. This feed carries transitions only. An order placed in the window is
    mentioned once, to nobody, and never again — and `allMids` would keep the
    socket alive indefinitely over memory that is quietly wrong, emitting no
    `gone`, until a reconnect that nothing forces.

    So a healthy connection ends on purpose at ORDER_CONNECTION_MAX_SECONDS, and
    the replacement's resync — which happens before it subscribes, so no
    observation is ever applied out of order — is what finds the order."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    socket = WindowWebsocket(
        clock,
        alive_for(ORDER_CONNECTION_MAX_SECONDS),
        gateway,
        TRADER,
        [resting(oid=1)],
    )
    ended = await run_once(pool, gateway, clock, socket)

    # The connection was healthy throughout — liveness never lapsed, no frame
    # was unreadable, nothing closed — and it ended anyway.
    assert isinstance(ended, LaneRecycled)
    assert await events(pool) == []  # nothing on this connection could have known

    # The replacement resyncs, and the order the window swallowed surfaces once,
    # honestly labelled as something the lane inferred rather than watched.
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    assert [(row["order_id"], row["kind"], row["origin"]) for row in await events(pool)] == [
        (1, "placed", RESYNC_ORIGIN)
    ]


def stopping_after(*connections: FakeWebsocket) -> Any:
    """`connecting`, but the exhausted factory CANCELS instead of raising.

    `run_order_lane` is a forever-loop by design, so a test needs something the
    loop deliberately does not catch to get out of it — which is what makes the
    reconnect sleeps it took on the way an assertion."""
    remaining = list(connections)

    async def connect() -> FakeWebsocket:
        if not remaining:
            raise asyncio.CancelledError
        return remaining.pop(0)

    return connect


async def test_a_recycle_reconnects_at_once_rather_than_blacking_the_feed_out(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A recycle is not a failure and must not be paced like one. Paying the
    60-second reconnect floor every 15 minutes would black the feed out for 7%
    of its life to fix a sub-second window — and the pacing that floor protects
    is already satisfied, because the recycle period is itself the rate limit
    (MAX_ORDER_LANES lanes recycling every 15 minutes spend 0.53 new connections
    a minute of the 30 an IP has)."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    socket = FakeWebsocket(clock, alive_for(ORDER_CONNECTION_MAX_SECONDS))
    with pytest.raises(asyncio.CancelledError):
        await run_order_lane(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            stopping_after(socket),
            clock,
            TRADER,
        )

    assert clock.slept == []


async def test_a_refused_subscription_ends_the_connection_rather_than_looking_healthy(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The other way this lane can look healthy while recording nothing.

    The position lane logs an `error` frame and carries on, because its
    connection multiplexes many subscriptions and the error may concern a wallet
    that has just left. Here there is no such ambiguity: this connection serves
    one Trader and asked for exactly two things. If the order feed was refused —
    the concrete case being the per-IP allowance of 15 unique users already full
    when this lane asks — `allMids` keeps arriving, liveness stays satisfied, and
    the lane would sit there forever recording nothing and emitting no `gone`.

    The staged script says exactly that: the market feed goes on flowing after
    the refusal. The connection has to end anyway."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [resting(oid=1)])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            error_frame("Cannot track more than 15 total users."),
            *[liveness_message() for _ in range(5)],
        ],
    )
    ended = await run_once(pool, gateway, clock, socket)

    assert isinstance(ended, SubscriptionRefused)
    assert "15 total users" in str(ended)
    assert socket.closed


async def test_a_refused_subscription_stands_down_instead_of_re_asking_every_minute(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A refusal is usually structural — the per-IP user allowance is full — so
    the answer cannot change within a minute. Retrying on the reconnect floor
    would spend MAX_ORDER_LANES connections a minute re-asking it, out of the 30
    the whole box has."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    socket = FakeWebsocket(clock, [liveness_message(), error_frame("Cannot track more than 15")])
    with pytest.raises(asyncio.CancelledError):
        await run_order_lane(
            pool,
            gateway,
            WeightBudget(WIDE_OPEN_BUDGET, clock),
            stopping_after(socket),
            clock,
            TRADER,
        )

    assert clock.slept == [SUBSCRIPTION_COOLDOWN_SECONDS]


# -- what must not reach the seam ---------------------------------------------


async def test_spot_orders_are_dropped(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """The feed carries the wallet's resting SPOT orders too (an `@156` order
    was observed live) and Epigone is perp-only, so a spot row would put a
    meaningless ticker for an uncovered asset class into an execution seam."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    spot = orders_message(update(oid=1, coin="@156"), update(oid=2, coin="PURR/USDC"))
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), spot]))

    assert await events(pool) == []


async def test_a_venue_the_resync_cannot_see_is_not_recorded(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """`orderUpdates` is account-wide and covers every dex; the REST resync that
    anchors it reads POSITION_VENUES only. Recording an uncovered venue's order
    would report it `placed` and then `gone` on the next reconnect, forever."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    for _ in range(2):  # place it, then reconnect and resync again
        await run_once(
            pool,
            gateway,
            clock,
            FakeWebsocket(
                clock, [liveness_message(), orders_message(update(oid=1, coin="mkts:FOO"))]
            ),
        )
        assert await events(pool) == []


async def test_an_unreadable_frame_does_not_end_the_connection(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A shape surprise is logged and skipped: killing the connection over one
    odd frame would cost the seam the data it exists to collect, and half-reading
    one would record an order that was never placed."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    socket = FakeWebsocket(
        clock,
        [
            liveness_message(),
            {"channel": ORDERS_CHANNEL, "data": [{"order": {"coin": "BTC"}}]},
            orders_message(update(oid=1)),
        ],
    )
    ended = await run_once(pool, gateway, clock, socket)

    assert isinstance(ended, WebsocketClosed)  # ran to the end of the script
    assert [row["order_id"] for row in await events(pool)] == [1]


# -- the rate ceiling ---------------------------------------------------------


async def test_a_market_maker_is_refused_rather_than_recorded(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """One measured address produced 1471 order updates a minute. Above the
    ceiling the lane refuses that Trader — CONTEXT.md's Bot rule applied at the
    seam: an account requoting this fast has no plan to mirror, and recording it
    would bury the leaders this seam exists for."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    requotes = [
        orders_message(update(oid=oid, status="open"))
        for oid in range(ORDER_UPDATE_RATE_LIMIT + 50)
    ]
    ended = await run_once(
        pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), *requotes])
    )

    assert isinstance(ended, LaneRefused)
    recorded = await pool.fetchval("SELECT count(*) FROM order_events")
    assert recorded == ORDER_UPDATE_RATE_LIMIT


async def test_a_refusal_ends_the_connection_rather_than_muting_it(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Refusing has to DISCONNECT, and this is the test that says why. Muting the
    Trader on a live connection would freeze the lane's memory at whatever was
    resting when the ceiling hit while the exchange moved on — and `allMids`
    keeps the socket alive indefinitely, so nothing would ever force the resync
    that repairs it. The lane would sit on one of ten connections believing a
    stale book. Here the refusal ends the connection, and the NEXT one resyncs
    and reports the divergence honestly rather than as a phantom."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    requotes = [
        orders_message(update(oid=oid)) for oid in range(ORDER_UPDATE_RATE_LIMIT + 40)
    ]
    # Frames after the refusal are staged but must never be read: the connection
    # is finished at the ceiling, not muted through the rest of the script.
    socket = FakeWebsocket(clock, [liveness_message(), *requotes])
    ended = await run_once(pool, gateway, clock, socket)

    assert isinstance(ended, LaneRefused)
    assert socket.closed
    # The book the lane believed is now known to be stale, so the next
    # connection resyncs it: the orders it recorded are reported `gone` rather
    # than left sitting in memory forever.
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))
    assert await pool.fetchval("SELECT count(*) FROM ws_order_state") == 0
    assert await pool.fetchval(
        "SELECT count(*) FROM order_events WHERE kind = 'gone'"
    ) == ORDER_UPDATE_RATE_LIMIT


async def test_the_ceiling_is_a_rolling_minute_not_a_lifetime_budget(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A ceiling that never forgot would stop recording a perfectly ordinary
    Trader after their first busy minute, permanently."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    # Two connections, a minute apart: each may spend the full allowance. Only
    # the placements are counted — each reconnect also resyncs against an empty
    # book, so the previous burst's orders correctly report as `gone`, and that
    # is a different property (tested above) rather than the ceiling refilling.
    for run in range(2):
        clock.advance(120)
        burst = [
            orders_message(update(oid=run * 1000 + oid))
            for oid in range(ORDER_UPDATE_RATE_LIMIT)
        ]
        await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), *burst]))

    placed = await pool.fetchval("SELECT count(*) FROM order_events WHERE kind = 'placed'")
    assert placed == 2 * ORDER_UPDATE_RATE_LIMIT


async def test_a_refused_leader_is_left_in_honest_state_after_the_cooldown(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """ADR-0008 deviation 5, and the property that makes it survivable.

    CONTEXT.md excludes a Bot by statistical profile, once, at Universe vetting.
    This ceiling applies the same word to one minute of behaviour, at the seam,
    to an account vetting already let through — so it will sometimes catch a
    human. A Leader who changes their mind about a market and pulls a
    150-order ladder in one minute is refused mid-burst and blacked out.

    What must survive that is ACCURACY, not resolution. Resolution is genuinely
    lost: the cancels inside the blackout are never recorded as cancels. But
    every order must still be accounted for exactly once, the inferred rows must
    say they were inferred, orders that never moved must produce no event at
    all, and no phantom may be left resting in memory."""
    ladder = [resting(oid=oid) for oid in range(1, 151)]
    untouched = [resting(oid=oid, coin="ETH") for oid in range(200, 210)]
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [*ladder, *untouched])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    cancels = [
        orders_message(update(oid=oid, status="canceled", size="3")) for oid in range(1, 151)
    ]
    ended = await run_once(
        pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), *cancels])
    )
    assert isinstance(ended, LaneRefused)

    # The cooldown passes; the lane comes back and reads what is actually there.
    clock.advance(REFUSED_COOLDOWN_SECONDS)
    gateway.set_open_orders(TRADER, untouched)
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    rows = await events(pool)
    # Every order of the ladder, exactly once, and nothing else.
    assert sorted(row["order_id"] for row in rows) == list(range(1, 151))
    watched = [row for row in rows if row["kind"] == "canceled"]
    inferred = [row for row in rows if row["kind"] == "gone"]
    assert len(watched) == ORDER_UPDATE_RATE_LIMIT
    assert len(inferred) == 150 - ORDER_UPDATE_RATE_LIMIT
    # The rows the lane inferred rather than watched say so, which is what lets
    # a consumer discount them.
    assert {row["origin"] for row in inferred} == {RESYNC_ORIGIN}
    assert {row["origin"] for row in watched} == {STREAM_ORIGIN}
    # The orders the Leader never touched are still resting and produced nothing:
    # a refusal must not report a phantom `gone` for a ladder that never moved.
    assert await pool.fetchval(
        "SELECT array_agg(order_id ORDER BY order_id) FROM ws_order_state"
    ) == list(range(200, 210))


async def test_a_refusal_is_loud_enough_to_tell_a_leader_from_a_market_maker(
    pool: asyncpg.Pool,
    gateway: FakeHyperliquidGateway,
    clock: FakeClock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The refusal costs a tracked Trader their coverage for a quarter of an
    hour (deviation 5). If it read like an ordinary connection close, the
    operator could never tell the case the rule is FOR — a market maker — from
    the case it is a false positive on. So the closing line names the Trader,
    the ceiling and the blackout, at WARNING."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    requotes = [orders_message(update(oid=oid)) for oid in range(ORDER_UPDATE_RATE_LIMIT + 5)]
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="epigone.ws.order_lane"):
        await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message(), *requotes]))

    closing = [record for record in caplog.records if "connection closed" in record.message]
    assert [record.levelno for record in closing] == [logging.WARNING]
    said = closing[0].getMessage()
    assert TRADER in said
    assert str(ORDER_UPDATE_RATE_LIMIT) in said
    assert f"{REFUSED_COOLDOWN_SECONDS:.0f}s" in said


# -- liveness -----------------------------------------------------------------


async def test_a_quiet_trader_is_not_a_dead_connection(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """This matters more here than on the position lane: a positions feed pushes
    every ~5s even for an idle account, while a Trader resting nothing produces
    no order frames for hours. Only the market feed can tell the two apart."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    script: list[dict[str, Any] | None] = []
    for _ in range(TICKS_TO_SILENCE + 10):
        script.extend([liveness_message(), None])
    ended = await run_once(pool, gateway, clock, FakeWebsocket(clock, script))

    assert isinstance(ended, WebsocketClosed)  # the script ran out, not silence
    assert await events(pool) == []


async def test_a_dead_connection_is_detected_though_nothing_reports_it(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    silent: list[dict[str, Any] | None] = [None] * TICKS_TO_SILENCE
    ended = await run_once(pool, gateway, clock, FakeWebsocket(clock, silent))

    assert isinstance(ended, LaneSilent)


async def test_the_lanes_own_pongs_do_not_pass_for_liveness(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """A lane whose order subscription had died would otherwise keep itself
    looking healthy by talking to itself through the keepalive."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    script: list[dict[str, Any] | None] = []
    for _ in range(TICKS_TO_SILENCE + 5):
        script.extend([None, {"channel": "pong"}])
    ended = await run_once(pool, gateway, clock, FakeWebsocket(clock, script))

    assert isinstance(ended, LaneSilent)


# -- allowances ---------------------------------------------------------------


async def test_one_trader_costs_one_connection_and_two_subscriptions(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """What a Trader costs is load-bearing, because connections are the scarce
    resource: 10 per IP, and this lane spends one per Trader on purpose."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])

    socket = FakeWebsocket(clock, [liveness_message()])
    await run_once(pool, gateway, clock, socket)

    assert socket.subscriptions(ORDERS_CHANNEL) == [TRADER]
    assert socket.subscriptions(LIVENESS_CHANNEL) == [""]
    assert socket.subscription_count == 2


async def idle() -> None:
    """A stand-in for a running lane: it holds its slot until cancelled, which
    is the only thing the reconciler cares about."""
    await asyncio.Event().wait()


async def test_one_lane_runs_per_shadowed_trader(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    lanes: dict[str, asyncio.Task[None]] = {}

    await reconcile_order_lanes(pool, lanes, lambda _: asyncio.create_task(idle()))

    assert sorted(lanes) == sorted([TRADER, OTHER])
    for task in lanes.values():
        task.cancel()


async def test_a_second_reconcile_does_not_restart_a_running_lane(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """Restarting a healthy lane would resync it over REST and reconnect it for
    nothing — every 30 seconds, forever."""
    await track(pool, clock, TRADER, FOLLOWER)
    lanes: dict[str, asyncio.Task[None]] = {}
    starts: list[str] = []

    def start(address: str) -> asyncio.Task[None]:
        starts.append(address)
        return asyncio.create_task(idle())

    await reconcile_order_lanes(pool, lanes, start)
    await reconcile_order_lanes(pool, lanes, start)

    assert starts == [TRADER]
    assert not lanes[TRADER].done()
    lanes[TRADER].cancel()


async def test_an_unfollowed_traders_lane_is_stopped_and_forgotten(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """A lane left running would hold one of the ten connections this IP has,
    for a Trader nobody watches. And the memory has to go with it, so a
    re-follow re-baselines silently instead of reporting a months-old ladder as
    freshly `gone`."""
    await track(pool, clock, TRADER, FOLLOWER)
    await pool.execute(
        """
        INSERT INTO ws_order_lane_state (trader_address, baselined_at, resynced_at)
        VALUES ($1, $2, $2)
        """,
        TRADER,
        clock.now(),
    )
    lanes: dict[str, asyncio.Task[None]] = {}
    await reconcile_order_lanes(pool, lanes, lambda _: asyncio.create_task(idle()))
    lane = lanes[TRADER]

    await pool.execute("DELETE FROM tracks WHERE trader_address = $1", TRADER)
    await reconcile_order_lanes(pool, lanes, lambda _: asyncio.create_task(idle()))

    assert lanes == {}
    assert lane.cancelled()
    assert await pool.fetchval("SELECT count(*) FROM ws_order_lane_state") == 0


async def test_a_poll_set_past_the_connection_cap_is_truncated_not_overrun(
    pool: asyncpg.Pool, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Past the cap the lane shadows what it can and says so, rather than
    opening connections until the server refuses — at which point it would be
    silently partial with nothing in the logs to say which Traders it dropped."""
    monkeypatch.setattr(order_lane, "MAX_ORDER_LANES", 1)
    await track(pool, clock, TRADER, FOLLOWER)
    await track(pool, clock, OTHER, FOLLOWER)
    lanes: dict[str, asyncio.Task[None]] = {}

    await reconcile_order_lanes(pool, lanes, lambda _: asyncio.create_task(idle()))

    assert list(lanes) == [TRADER]  # the poll set is sorted, so this is deterministic
    lanes[TRADER].cancel()


def test_reconnects_are_paced_against_the_per_ip_connection_rate() -> None:
    """30 new connections a minute is a PER-IP allowance shared by every lane on
    the box. The position lane's 2-second floor was sized when this process held
    one connection; MAX_ORDER_LANES lanes reconnecting at that floor would spend
    ~240/min between them. Stated as the arithmetic so it cannot drift."""
    new_connections_per_minute = MAX_ORDER_LANES * (60 / ORDER_RECONNECT_MIN_SECONDS)
    assert new_connections_per_minute <= CONNECTION_RATE_LIMIT / 3
    # And the floor is genuinely slower than the position lane's, which is the
    # whole point — that one was sized for a single connection.
    assert ORDER_RECONNECT_MIN_SECONDS > RECONNECT_MIN_SECONDS


def test_recycling_every_connection_still_fits_the_per_ip_connection_rate() -> None:
    """The bounded connection lifetime buys a healed window by spending new
    connections, so what it spends is worth stating rather than trusting. Every
    lane recycling on this period, all the time, is a rounding error against the
    per-IP allowance — which is why a recycle can reconnect immediately where a
    failure has to pay the floor."""
    recycles_per_minute = MAX_ORDER_LANES * (60 / ORDER_CONNECTION_MAX_SECONDS)
    assert recycles_per_minute < 1
    # And the lifetime is long enough that a recycle is rarer than the reconnect
    # floor it skips, which is what makes skipping it safe.
    assert ORDER_CONNECTION_MAX_SECONDS > ORDER_RECONNECT_MIN_SECONDS * 10


def test_the_connection_allowance_bounds_the_shadowed_set() -> None:
    """Stated as arithmetic so the day it binds is a logged refusal rather than
    the server quietly refusing connections. One is the position lane's, one is
    reconnect overlap."""
    assert MAX_ORDER_LANES == 8
    assert MAX_ORDER_LANES < CONNECTION_LIMIT


# -- the promise the ticket rests on ------------------------------------------


async def test_the_lane_writes_no_alerts_and_leaves_every_other_lane_alone(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> None:
    """Nothing consumes these rows, and a User sees nothing. The order POLLER's
    own diff memory is untouched too — it is a different table on purpose, since
    a second writer advancing `order_snapshots` would make the poller diff
    against a book it never observed and miss the alert it was about to send."""
    await track(pool, clock, TRADER, FOLLOWER)
    gateway.set_open_orders(TRADER, [])
    await run_once(pool, gateway, clock, FakeWebsocket(clock, [liveness_message()]))

    await run_once(
        pool,
        gateway,
        clock,
        FakeWebsocket(clock, [liveness_message(), orders_message(update(oid=1))]),
    )

    assert await events(pool) != []  # the lane did produce a signal
    assert await pool.fetchval("SELECT count(*) FROM order_alerts") == 0
    assert await pool.fetchval("SELECT count(*) FROM position_alerts") == 0
    assert await pool.fetchval("SELECT count(*) FROM order_snapshots") == 0
    assert await pool.fetchval("SELECT count(*) FROM order_poll_state") == 0
    assert await pool.fetchval("SELECT count(*) FROM position_events") == 0
