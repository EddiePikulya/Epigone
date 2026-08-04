"""The websocket order lane: the durable seam for resting-order changes
(issue #168, ADR-0008).

The position lane multiplexes every shadowed Trader onto one connection,
because `allDexsClearinghouseState` messages name the user they are about.
**`orderUpdates` messages do not** — measured 2026-08-03, the frame is

    {"channel": "orderUpdates", "data": [{"order": {coin, side, limitPx, sz,
      oid, timestamp, origSz, cloid}, "status", "statusTimestamp"}, …]}

with no `user` field at any level. Subscribe two Traders to one connection and
their frames arrive interleaved and anonymous. That single fact shapes this
whole module: **attribution is the connection.** One connection, one Trader, one
`orderUpdates` subscription, and an update's owner is not read from the payload
but known from which socket delivered it.

Everything else follows the position lane's rules, deliberately, because they
were right for the same reasons:

- **Resync before trusting the stream.** A websocket delivers only what happens
  after you subscribe, so a connection that streamed first would lose the gap in
  silence. Each connection begins with a point-in-time `frontendOpenOrders` read
  across the covered venues, diffs it against the lane's memory, and only then
  subscribes. What changed while the lane was away surfaces once, as `placed` /
  `filled` / `gone` with `origin = 'resync'`.
- **A Trader's first observation emits nothing.** A ladder that predates the
  first look is not something anyone could have watched being placed.
- **Silence is made unambiguous** by subscribing the always-emitting `allMids`
  feed and measuring liveness on that alone. A Trader resting quietly and a
  subscription that died look identical otherwise — and here that matters more
  than on the position lane, because a position feed pushes every ~5s even when
  nothing happens, while an order feed for a quiet Trader is genuinely silent
  for hours.
- **Nothing consumes the rows.** Same shadow discipline as #157: the lane runs
  in the ws process (ADR-0002), keeps its memory in its own tables (migration
  0031), and writes to a table no other query names. Alerting cannot be reached
  from here.

## The two allowances this lane is bounded by

Connections are the scarce resource: **10 per IP**. The position lane holds one,
one is held back so a reconnecting lane never races its predecessor's slot, and
the rest is this lane's ceiling — `MAX_ORDER_LANES`. Past it, the refusal is
logged rather than left to the server.

Unique users are NOT additionally spent. The per-IP allowance of 15 counts
distinct addresses, and an order lane subscribes an address the position lane
already tracks; the same address on a second connection was measured free.

Reconnects are paced against the per-IP allowance too — 30 new connections a
minute is shared by every lane on the box, so an order lane's floor is
`ORDER_RECONNECT_MIN_SECONDS` rather than the position lane's 2 seconds, which
was sized when this process held one connection.

## The rate ceiling is a Bot rule, not a performance knob

One active market-making address alone produced **1471 order updates in 60s**.
Position events are rare; order events are not. Above
`ORDER_UPDATE_RATE_LIMIT` on a rolling minute the lane REFUSES the Trader:
CONTEXT.md excludes such an account from the Universe as a Bot — automated
market-making rather than copyable skill — and a Trader whose plan changes twice
a second has no plan to mirror. Recording them anyway would bury the leaders
this seam exists for.

A refusal **ends the connection** and earns a cooldown, and that is a
correctness property rather than tidiness. Muting a Trader on a live connection
would freeze `ws_order_state` at whatever was resting when the ceiling hit while
the exchange moved on — and since `allMids` keeps the socket alive indefinitely,
nothing would ever force the resync that repairs it. The lane would sit on a
connection believing a stale book, which is the silent gap the resync discipline
exists to prevent. Disconnecting means the next connection re-establishes
absolute state and reports the divergence honestly, as `origin = 'resync'`.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, OpenOrder, on_covered_venue
from epigone.order_diff import OrderChange, RestingOrder, diff_open_orders, stream_change
from epigone.order_events import record_order_events
from epigone.stream.orders import fetch_orders_paced
from epigone.stream.poller import fetch_poll_set
from epigone.ws import (
    LIVENESS_CHANNEL,
    ORDERS_CHANNEL,
    Connect,
    OrderUpdate,
    RollingMinute,
    liveness_subscription,
    orders_subscription,
    parse_orders_message,
    ping,
    subscribe,
)
from epigone.ws.lane import (
    LIVENESS_TIMEOUT_SECONDS,
    PING_INTERVAL_SECONDS,
    RECEIVE_TICK_SECONDS,
    RECONNECT_MAX_SECONDS,
    TRACKED_REFRESH_SECONDS,
    LaneSilent,
    forget_unwatched,
    outbound_allowance,
    send_if_allowed,
)

log = logging.getLogger(__name__)

# The per-IP connection allowance, and what this lane may take of it: everything
# except the position lane's one connection and one held back as reconnect
# overlap — a replacement connection is opened before its predecessor's slot is
# certainly released, and running that race at the cap would turn every
# reconnect into a refused connection.
CONNECTION_LIMIT = 10
RESERVED_CONNECTIONS = 2
# New connections per minute, also per IP and also shared with the position
# lane — the allowance ORDER_RECONNECT_MIN_SECONDS is sized against.
CONNECTION_RATE_LIMIT = 30
MAX_ORDER_LANES = CONNECTION_LIMIT - RESERVED_CONNECTIONS

# The Bot rule at the seam (module docstring). Two updates a second sustained is
# a market maker requoting, not a Trader with a plan; the measured worst case is
# 12× this. Generous enough that no human leader trips it, small enough that a
# maker cannot fill the seam.
ORDER_UPDATE_RATE_LIMIT = 120

# What happens after the ceiling is hit: the connection ENDS, and this lane
# stands down for a while before resyncing and trying again.
#
# Ending it is the part that matters. Simply muting a Trader on a live
# connection would leave `ws_order_state` frozen at whatever was resting when
# the ceiling hit, while the exchange moved on — and because `allMids` keeps the
# socket alive indefinitely, nothing would ever force the resync that repairs
# it. The lane would hold a connection, believe a stale book, and report the
# whole divergence as a burst of `gone`s whenever it eventually reconnected.
# That is exactly the silent gap the resync discipline exists to prevent, so a
# refusal is a disconnect and the next connection re-establishes absolute state.
#
# The cooldown is what keeps the refusal meaningful. Reconnecting on the usual
# backoff would let a market maker write ORDER_UPDATE_RATE_LIMIT events per
# cycle indefinitely; standing down for 15 minutes bounds them to ~11.5k a day
# where the measured maker would have written 2.1M, and costs one paced resync
# (2 REST calls) per lane per cooldown.
REFUSED_COOLDOWN_SECONDS = 900.0

# Reconnect pacing for an ORDER lane, which cannot use the position lane's
# 2-second floor: that floor was sized when this process held ONE connection,
# and the 30-new-connections/min allowance is PER IP. MAX_ORDER_LANES lanes
# reconnecting independently at a 2s floor would spend ~240/min between them —
# eight times the whole cap — and the position lane still has to fit.
#
# At a minute each, all eight together spend at most 8/min, roughly a quarter of
# the allowance, leaving the position lane the room its own floor may ask for.
# Order lanes take the smaller share deliberately: position events are what
# Position Alerts and the cutover comparison are built on, and these rows are
# read by nobody. The cost is that a transient failure delays one Trader's order
# feed by a minute, which for a shadow lane is the right side to err on.
ORDER_RECONNECT_MIN_SECONDS = 60.0


class LaneRefused(Exception):
    """This Trader is producing order updates faster than the seam will record
    (ORDER_UPDATE_RATE_LIMIT) — a Bot by CONTEXT.md's definition rather than a
    Trader with a plan worth mirroring. Ends the connection so memory cannot go
    stale behind a socket that stays open, and earns a cooldown."""


@dataclass
class OrderLaneStats:
    """What one Trader's connection did, for the log line when it ends."""

    events: int = 0
    order_messages: int = 0
    liveness_messages: int = 0


async def run_order_lanes(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connect: Connect,
    clock: Clock,
) -> None:
    """Keep one order lane running per shadowed Trader, reconciled against the
    poll set forever.

    Each lane is an independent task with its own connection, its own resync and
    its own reconnect backoff, so one Trader's feed misbehaving cannot stop
    another's. A wallet that leaves the poll set has its lane cancelled and its
    memory forgotten, so a re-follow re-baselines silently rather than diffing
    against a ladder from months ago — the poller's re-follow rule, owed here for
    the same reason."""
    lanes: dict[str, asyncio.Task[None]] = {}

    def start(address: str) -> asyncio.Task[None]:
        return asyncio.create_task(
            run_order_lane(pool, gateway, budget, connect, clock, address),
            name=f"ws-order-lane-{address}",
        )

    try:
        while True:
            try:
                await reconcile_order_lanes(pool, lanes, start)
            except Exception:
                # A refresh that fails — Postgres blipped, the poll-set query
                # raised — must not end this loop. It runs beside the position
                # lane in one process, and a supervisor that died here would
                # take that lane's dataset down with it while looking healthy.
                # The lanes already running keep running; the next tick retries.
                log.exception("ws order lane: refresh failed; retrying next tick")
            await clock.sleep(TRACKED_REFRESH_SECONDS)
    finally:
        # Whatever ends this loop — process shutdown, a cancelled supervisor —
        # takes every lane's connection down with it. A leaked lane would hold
        # one of the ten connections this IP has, and nothing would reclaim it.
        for task in lanes.values():
            await _stop(task)


async def reconcile_order_lanes(
    pool: asyncpg.Pool,
    lanes: dict[str, asyncio.Task[None]],
    start: Callable[[str], asyncio.Task[None]],
) -> None:
    """Bring the running lanes in line with the poll set, once.

    Separated from the forever-loop so the decision can be tested without an
    event loop racing a fake clock: what is interesting here is WHICH lanes run,
    and that a departing Trader's lane is actually stopped rather than left
    holding a connection.

    A wallet that leaves is stopped and forgotten, so a re-follow re-baselines
    silently instead of diffing against a ladder from months ago — the poller's
    re-follow rule, owed here for the same reason."""
    await forget_unwatched(pool, "ws_order_state", "ws_order_lane_state")
    wanted = await _shadowed_traders(pool)
    for address in list(lanes):
        if address not in wanted:
            await _stop(lanes.pop(address))
    for address in wanted:
        if address not in lanes:
            lanes[address] = start(address)


async def _stop(task: asyncio.Task[None]) -> None:
    """Cancel a lane and wait for it to actually let go of its connection."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _shadowed_traders(pool: asyncpg.Pool) -> list[str]:
    """The poll set, truncated to what the connection allowance permits.

    Truncation is loud on purpose. The alternative — opening connections until
    the server refuses — leaves the lane silently partial with nothing in the
    logs to say which Traders it gave up on, and this seam's whole value is that
    a consumer can trust what is missing to be missing for a stated reason."""
    wanted = await fetch_poll_set(pool)
    if len(wanted) > MAX_ORDER_LANES:
        log.error(
            "ws order lane: %d wallets exceeds the %d order connections one IP allows "
            "(%d total, %d reserved for the position lane and reconnect overlap); "
            "shadowing orders for the first %d only",
            len(wanted),
            MAX_ORDER_LANES,
            CONNECTION_LIMIT,
            RESERVED_CONNECTIONS,
            MAX_ORDER_LANES,
        )
    return wanted[:MAX_ORDER_LANES]


async def run_order_lane(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connect: Connect,
    clock: Clock,
    address: str,
) -> None:
    """One Trader's lane, reconnecting through anything that goes wrong.

    Nothing escapes. A shadow lane that could crash its own process would still
    be harmless to alerting — that is what the process split buys — but it would
    silently stop collecting the seam it exists for."""
    delay = ORDER_RECONNECT_MIN_SECONDS
    while True:
        try:
            await run_order_connection(pool, gateway, budget, connect, clock, address)
        except LaneRefused as exc:
            # Not a failure: the Trader is a Bot by the seam's own rule. Stand
            # down long enough that they cannot write a cycle's worth of events
            # every backoff, then resync and re-judge — the rate is a property of
            # what they are doing now, not a verdict on the account forever.
            log.error(
                "ws order lane %s: %s; standing down for %.0fs",
                address,
                exc,
                REFUSED_COOLDOWN_SECONDS,
            )
            await clock.sleep(REFUSED_COOLDOWN_SECONDS)
            delay = ORDER_RECONNECT_MIN_SECONDS
            continue
        except Exception as exc:
            log.warning(
                "ws order lane %s: connection ended (%s); reconnecting in %.0fs",
                address,
                exc,
                delay,
            )
        else:
            delay = ORDER_RECONNECT_MIN_SECONDS
        await clock.sleep(delay)
        delay = min(RECONNECT_MAX_SECONDS, max(ORDER_RECONNECT_MIN_SECONDS, delay * 2))


async def run_order_connection(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connect: Connect,
    clock: Clock,
    address: str,
) -> None:
    """One connection's life, serving exactly one Trader.

    Liveness first, then the resync, then — and only then — the order
    subscription. That order is the correctness argument, not a detail:
    subscribing before the resync would let a push land while the REST read was
    in flight, and applying the staler answer afterwards would resurrect an
    order the stream had just seen cancelled.

    Returns only by raising; the caller reconnects."""
    connection = await connect()
    allowance = outbound_allowance(clock)
    stats = OrderLaneStats()
    ceiling = RollingMinute(clock, ORDER_UPDATE_RATE_LIMIT)
    try:
        if not await send_if_allowed(connection, allowance, subscribe(liveness_subscription())):
            raise LaneSilent("could not subscribe to the liveness feed")
        await _resync(pool, gateway, budget, address, clock, stats)
        # The resync is REST time, not socket time, so it cannot be evidence the
        # feed went quiet — charging it to the liveness deadline would declare a
        # healthy connection dead before it had read a message, then do it again
        # on every reconnect.
        last_liveness = clock.now()
        last_ping = last_liveness
        feed = subscribe(orders_subscription(address))
        if not await send_if_allowed(connection, allowance, feed):
            raise LaneSilent(f"could not subscribe to {address}'s order feed")
        while True:
            now = clock.now()
            if (now - last_ping).total_seconds() >= PING_INTERVAL_SECONDS:
                await send_if_allowed(connection, allowance, ping())
                last_ping = now
            if (now - last_liveness).total_seconds() >= LIVENESS_TIMEOUT_SECONDS:
                raise LaneSilent(
                    f"no {LIVENESS_CHANNEL} message in {LIVENESS_TIMEOUT_SECONDS:.0f}s"
                )
            message = await connection.receive(timeout=RECEIVE_TICK_SECONDS)
            if message is None:
                continue
            channel = message.get("channel")
            if channel == LIVENESS_CHANNEL:
                # Market data is flowing, so silence from this Trader's order
                # feed is that Trader resting quietly.
                stats.liveness_messages += 1
                last_liveness = clock.now()
            elif channel == ORDERS_CHANNEL:
                stats.order_messages += 1
                await _apply_orders_message(pool, address, message, ceiling, clock.now(), stats)
            elif channel == "error":
                log.warning(
                    "ws order lane %s: server error frame: %s", address, message.get("data")
                )
    finally:
        await connection.close()
        log.info(
            "ws order lane %s: connection closed after %d events from %d order messages "
            "(%d liveness)",
            address,
            stats.events,
            stats.order_messages,
            stats.liveness_messages,
        )


async def _resync(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    address: str,
    clock: Clock,
    stats: OrderLaneStats,
) -> None:
    """Re-establish this Trader's resting book from a point-in-time REST read,
    before anything is streamed for them.

    Paced by the same shared budget everything else uses, carrying the lane
    process's ingest-style reserve, so a reconnect storm can slow this lane and
    can never draw the bucket below the floor that guarantees position polling —
    and therefore Position Alerts — its instant claim.

    A failure here leaves the Trader unsubscribed and raises, which the
    reconnect supervisor turns into a backoff and another attempt. Subscribing
    without a resync is the one thing that must not happen."""
    orders = await fetch_orders_paced(gateway, budget, address)
    stats.events += await _apply_orders(pool, address, orders, clock.now())


async def _apply_orders(
    pool: asyncpg.Pool, address: str, orders: list[OpenOrder], now: datetime
) -> int:
    """Diff a point-in-time resting book against memory, and commit the events
    with the memory advance in ONE transaction (ADR-0006's atomicity, inherited
    rather than reinvented). Returns the event count."""
    async with pool.acquire() as conn, conn.transaction():
        baselined = await conn.fetchval(
            "SELECT 1 FROM ws_order_lane_state WHERE trader_address = $1", address
        )
        if not baselined:
            # First observation: remember the ladder, emit nothing.
            for order in orders:
                await _remember(conn, address, RestingOrder.from_open_order(order))
            await conn.execute(
                """
                INSERT INTO ws_order_lane_state
                    (trader_address, baselined_at, resynced_at, last_message_at)
                VALUES ($1, $2, $2, $2)
                """,
                address,
                now,
            )
            return 0

        previous = await _read_memory(conn, address)
        changes = diff_open_orders(previous, orders)
        events = await _commit(conn, address, changes, now)
        await conn.execute(
            """
            UPDATE ws_order_lane_state SET resynced_at = $2, last_message_at = $2
            WHERE trader_address = $1
            """,
            address,
            now,
        )
        return events


async def _apply_orders_message(
    pool: asyncpg.Pool,
    address: str,
    message: dict[str, Any],
    ceiling: RollingMinute,
    now: datetime,
    stats: OrderLaneStats,
) -> None:
    """Turn one streamed frame into events, or refuse the Trader.

    The frame is attributed to `address` because it arrived on `address`'s own
    connection; there is nothing in it to read an owner from.

    Two reductions before anything is diffed, both of them the position lane's
    for the position lane's reasons. Spot orders are dropped in the parser
    (Epigone is perp-only). Venues the REST resync cannot see are dropped here:
    `orderUpdates` is account-wide and covers every dex, while the resync that
    anchors it reads POSITION_VENUES, so an uncovered-venue order would be
    recorded as `placed` and then reported `gone` on the next reconnect,
    forever."""
    try:
        updates = parse_orders_message(message["data"])
    except (GatewayError, KeyError, TypeError) as exc:
        log.warning("ws order lane %s: unreadable %s message: %s", address, ORDERS_CHANNEL, exc)
        return
    updates = [update for update in updates if on_covered_venue(update.coin)]
    if not updates:
        return
    if not all(ceiling.take() for _ in updates):
        raise LaneRefused(
            f"over {ORDER_UPDATE_RATE_LIMIT} order updates/minute — an account requoting "
            "this fast is automated market-making (CONTEXT.md's Bot), not a plan worth "
            "mirroring"
        )
    stats.events += await _apply_updates(pool, address, updates, now)


async def _apply_updates(
    pool: asyncpg.Pool, address: str, updates: list[OrderUpdate], now: datetime
) -> int:
    """Apply one frame's updates to memory and record them, atomically.

    A Trader whose baseline has not been recorded is not streamed at all: memory
    the resync never anchored would diff into nonsense. In practice that only
    catches a push racing the very first resync."""
    async with pool.acquire() as conn, conn.transaction():
        baselined = await conn.fetchval(
            "SELECT 1 FROM ws_order_lane_state WHERE trader_address = $1", address
        )
        if not baselined:
            log.debug("ws order lane %s: frame arrived before the baseline; dropped", address)
            return 0
        memory = await _read_memory(conn, address)
        changes = [stream_change(memory.get(update.order_id), update) for update in updates]
        events = await _commit(conn, address, changes, now)
        await conn.execute(
            "UPDATE ws_order_lane_state SET last_message_at = $2 WHERE trader_address = $1",
            address,
            now,
        )
        return events


async def _commit(
    conn: asyncpg.Connection, address: str, changes: list[OrderChange], now: datetime
) -> int:
    """Write one batch of decisions: memory first, then the events that explain
    it, inside the caller's transaction so the two can never disagree."""
    for change in changes:
        if change.resting is None:
            await conn.execute(
                "DELETE FROM ws_order_state WHERE trader_address = $1 AND order_id = $2",
                address,
                change.order_id,
            )
        else:
            await _remember(conn, address, change.resting)
    events = [change.event for change in changes if change.event is not None]
    if events:
        await record_order_events(conn, address, events, now)
    return len(events)


async def _read_memory(
    conn: asyncpg.Connection, address: str
) -> dict[int, RestingOrder]:
    rows = await conn.fetch("SELECT * FROM ws_order_state WHERE trader_address = $1", address)
    return {row["order_id"]: RestingOrder.from_row(row) for row in rows}


async def _remember(conn: asyncpg.Connection, address: str, order: RestingOrder) -> None:
    await conn.execute(
        """
        INSERT INTO ws_order_state
            (trader_address, order_id, coin, is_buy, limit_price, size, original_size,
             cloid, order_type, is_trigger, trigger_price, reduce_only, is_position_tpsl,
             placed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (trader_address, order_id) DO UPDATE SET
            coin = EXCLUDED.coin,
            is_buy = EXCLUDED.is_buy,
            limit_price = EXCLUDED.limit_price,
            size = EXCLUDED.size,
            original_size = EXCLUDED.original_size,
            cloid = coalesce(EXCLUDED.cloid, ws_order_state.cloid),
            -- The stream cannot observe the trigger fields, so a streamed
            -- update must never blank what a resync learned about the order.
            order_type = coalesce(EXCLUDED.order_type, ws_order_state.order_type),
            is_trigger = coalesce(EXCLUDED.is_trigger, ws_order_state.is_trigger),
            trigger_price = coalesce(EXCLUDED.trigger_price, ws_order_state.trigger_price),
            reduce_only = coalesce(EXCLUDED.reduce_only, ws_order_state.reduce_only),
            is_position_tpsl = coalesce(
                EXCLUDED.is_position_tpsl, ws_order_state.is_position_tpsl
            ),
            placed_at = EXCLUDED.placed_at
        """,
        address,
        order.order_id,
        order.coin,
        order.is_buy,
        order.limit_price,
        order.size,
        order.original_size,
        order.cloid,
        order.order_type,
        order.is_trigger,
        order.trigger_price,
        order.reduce_only,
        order.is_position_tpsl,
        order.placed_at,
    )


