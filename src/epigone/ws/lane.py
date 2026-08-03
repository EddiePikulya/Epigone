"""The websocket shadow lane: a second producer of position events (issue #157).

The lane subscribes to every wallet the poll pass watches and writes what it
sees into `position_events` with `source = 'ws'`, beside the poller's `'poll'`
rows. **Nothing reads those rows.** REST polling stays fully authoritative,
Position Alerts are untouched, and a User sees no difference whatsoever. What
this produces is two independent descriptions of the same reality in one table,
distinguishable by `source` — the dataset the cutover ticket (#158) needs to
decide whether the websocket can be trusted.

That "nothing consumes it" property is also the safety argument, and it is
structural rather than a promise: the lane runs in its OWN process (ADR-0002 —
processes meet only in Postgres), keeps its diff memory in its OWN tables
(migration 0030), and the only row it writes that anyone else can see is an
event nobody selects. If the lane is wrong, flaky or dead, alerting continues
from the poller exactly as before, because there is no code path from here to
there.

Why websocket at all: subscriptions run on a budget entirely separate from the
REST weight cap, take a `user` parameter for any address, and cover every venue
in one subscription — where REST position polling saturates near 18 wallets. It
is also the only transport on which mirroring a leader's resting orders is
achievable at all, which issue #168 collects on in `epigone.ws.order_lane`.

**Correction, 2026-08-03 (#168).** This module used to claim 499 Traders on one
connection, from the 1000-subscription cap. That is wrong by ~33×: there is an
undocumented allowance of 15 unique users PER IP across all user-scoped
subscriptions, and it is what actually binds (see MAX_SUBSCRIBED_TRADERS). The
websocket's advantage over REST polling is real but far smaller than ADR-0006
argued, and a tracked set past 15 wallets is now a logged refusal here rather
than the server silently rejecting subscriptions.

On latency, one honest correction to the premise. The positions feed was
measured pushing on a ~5s cadence even for an account doing nothing at all,
carrying absolute state each time; whether a change ALSO triggers an immediate
push could not be settled without an account trading during the probe. So the
sub-second figure ADR-0006 argues for is what the transport ALLOWS, not
something observed. Measuring the real change-to-event latency is exactly what
this lane's dataset is for (#158), and it is now measurable because both lanes
stamp `observed_at` on the same change.

## The three things this lane has to get right

**1. Resync before trusting the stream.** A websocket delivers what happens
from the moment you subscribe. A reconnect that resumed streaming without first
re-establishing absolute state would lose everything that happened during the
gap, silently — no error, no gap marker, just a missing event. So every
connection begins by re-reading each Trader's positions point-in-time over
REST, diffing that against the lane's memory, and only then subscribing. A
change that happened while the lane was down surfaces as exactly one event on
the resync, and the subscription's own opening push (the server sends current
state on subscribe, verified live) diffs to nothing because the resync already
recorded it. Neither lost nor doubled.

The order matters and is the reason resync comes first: every position message
is ABSOLUTE state, so the only way to corrupt the memory is to apply an older
observation after a newer one. Subscribing first and resyncing after would race
exactly that way — a push could land while the REST read was in flight, and the
staler REST answer would then overwrite it and manufacture a phantom event.

**2. Silence is ambiguous, so make it unambiguous.** A Trader who isn't trading
produces no messages. So does a subscription that silently died. The two are
indistinguishable from inside the lane, which is why it also subscribes to
`allMids` — a market feed that always emits — and measures liveness on THAT
alone. Not on any message: the lane's own keepalive pings come back as pongs,
and counting those would let a lane with dead subscriptions look healthy while
it talked to itself.

The heartbeat follows from the same rule. `process_heartbeats` is beaten only
when market data actually arrives, so a stale heartbeat means "this lane is not
receiving" and never "this lane's Traders are quiet". Another process reads it
with the existing machinery (`epigone.safety.heartbeat.last_beat`).

**3. Allowances are per-IP and shared with nothing.** 10 connections, 30 new
connections/min, 1000 subscriptions, 2000 outbound messages/min, and — the one
that actually binds — 15 unique users. This lane holds ONE connection, spends 1
subscription on liveness and 1 per Trader, refuses to subscribe past the user
cap (loudly, rather than letting the server start rejecting), paces its own
outbound messages under a rolling-minute ceiling, and never reconnects faster
than the connection-rate cap allows. The order lanes take a connection each out
of the same per-IP pool, and no extra user allowance: the same address on a
second connection is free (measured).

## Measured against testnet/mainnet, 2026-08-02 (issue #157's two unknowns)

- The 2000 messages/min allowance is **outbound-only**: a connection sustaining
  far more than that in inbound pushes, sending nothing but a keepalive, is
  never throttled or cut. So the Trader ceiling is not set by message volume —
  though it is not set by subscriptions either, as this section originally
  concluded; the 15-unique-users-per-IP allowance found on 2026-08-03 binds
  first.
- There is **a ping/pong** (`{"method": "ping"}` → `{"channel": "pong"}`) and
  **a 60-second idle timeout**, undocumented and real: a connection with no
  traffic in EITHER direction is closed at ~60s. Inbound traffic resets it, so
  the `allMids` subscription happens to keep the socket open too — but the lane
  pings anyway, because depending on someone else's feed for your own liveness
  is exactly the coupling this module is trying to avoid.

Both findings are recorded in docs/research/ecosystem-survey.md.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway import (
    GatewayError,
    HyperliquidGateway,
    Position,
    RateLimitedError,
    on_covered_venue,
)
from epigone.position_diff import diff_positions, events_of
from epigone.position_events import WS_SOURCE, record_events
from epigone.position_snapshots import WS_SNAPSHOTS, apply_changes, read_snapshots, remember
from epigone.safety.heartbeat import beat, record_start
from epigone.stream.poller import fetch_poll_set, fetch_positions_paced
from epigone.ws import (
    LIVENESS_CHANNEL,
    POSITIONS_CHANNEL,
    Connect,
    RollingMinute,
    WebsocketClosed,
    WebsocketConnection,
    liveness_subscription,
    parse_positions_message,
    ping,
    positions_subscription,
    subscribe,
    unsubscribe,
)

log = logging.getLogger(__name__)

# The heartbeat name other processes read the lane's health under. Its own,
# never shared: a heartbeat row IS the process (epigone.safety.heartbeat), and
# retiring the lane means deleting this row.
WS_LANE_PROCESS = "ws_shadow"

# Per-IP websocket allowances (Hyperliquid docs; the message figure verified
# outbound-only on 2026-08-02 — see the module docstring). These are a budget
# entirely separate from the 1200 weight/min REST cap that epigone.budget
# paces against, which is the whole reason this transport is worth having.
SUBSCRIPTION_LIMIT = 1000

# What one Trader costs this connection: the all-dex positions feed, and only
# that. The per-dex forms would cost one subscription per venue for the same
# coverage. The account-wide order feed used to be subscribed here too — it is
# not any more, because its frames carry no `user` and are therefore
# unattributable on a connection serving many Traders; the order lane takes one
# connection per Trader instead (ADR-0007, issue #168).
SUBSCRIPTIONS_PER_TRADER = 1

# ⚠️ The real ceiling, and it is NOT the subscription cap. Measured 2026-08-03
# (scripts/testnet_ws_probe.py users): there is an undocumented allowance of 15
# UNIQUE USERS PER IP across all user-scoped subscriptions — a 16th address is
# refused with "Cannot track more than 15 total users." It is per IP rather than
# per connection (a brand-new address on a freshly opened second connection is
# refused while the first holds its 15), it counts distinct addresses rather
# than subscriptions, and closing a connection frees its slots within ~2s.
#
# This retires the 499 that ADR-0006's arithmetic produced from the subscription
# cap, and it is ~33× smaller — small enough that today's tracked set can exceed
# it (15 wallets per User × 2–3 Users). The lane's promise has always been that
# passing the cap is a LOGGED REFUSAL rather than the server quietly rejecting
# subscriptions; against the true number it has been doing the opposite, and
# this is what makes the promise true. Raising it means more IPs, which is a
# real decision and not one this constant can make.
MAX_SUBSCRIBED_TRADERS = 15

# Outbound pacing, kept a margin under the real ceiling: the lane's steady-state
# outbound is ~2 pings/min, and only a churning tracked set sends more, so this
# ceiling exists to make a pathological churn degrade into deferred
# subscriptions rather than a rejected connection.
OUTBOUND_BUDGET_PER_MINUTE = 1800

# The server closes a connection idle in both directions at ~60s (measured).
# Ping at less than half that, so one dropped ping is not a disconnect.
PING_INTERVAL_SECONDS = 25.0

# No market-data message for this long means the connection is dead, whatever
# it claims: `allMids` emits continuously, so this cannot be triggered by
# Traders being quiet. Generous next to the observed cadence — the cost of
# being wrong is a reconnect, but reconnecting in a loop would burn the
# connection-rate allowance.
LIVENESS_TIMEOUT_SECONDS = 60.0

# How long the read loop waits for a message before looking at the clock. Small
# enough that pings, liveness checks and tracked-set refreshes stay punctual;
# large enough that an idle lane is not a spin loop.
RECEIVE_TICK_SECONDS = 1.0

# How often the lane re-reads the poll set to pick up follows and unfollows.
TRACKED_REFRESH_SECONDS = 30.0

# How often an affirmative liveness signal is written through to Postgres. The
# feed arrives far faster than this; the throttle keeps the lane from turning a
# market data firehose into a write-per-message.
HEARTBEAT_INTERVAL_SECONDS = 10.0

# Reconnect pacing. The floor keeps the lane inside the 30-new-connections/min
# allowance even when the server is refusing instantly, and the backoff keeps a
# sustained outage from hammering it.
RECONNECT_MIN_SECONDS = 2.0
RECONNECT_MAX_SECONDS = 60.0


@dataclass
class LaneStats:
    """What one connection did, for the log line when it ends."""

    events: int = 0
    position_messages: int = 0
    liveness_messages: int = 0


class LaneSilent(Exception):
    """No market data for LIVENESS_TIMEOUT_SECONDS — the connection is dead
    even though nothing reported it closed. The lane's whole reason for
    subscribing to an always-emitting feed."""


def outbound_allowance(clock: Clock, limit: int | None = None) -> RollingMinute:
    """The per-IP outbound message ledger. The ceiling is read here rather than
    bound as a default argument, so it stays one editable constant instead of a
    value frozen into a signature at import time."""
    return RollingMinute(clock, OUTBOUND_BUDGET_PER_MINUTE if limit is None else limit)


async def run_lane(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connect: Connect,
    clock: Clock,
) -> None:
    """Run the lane forever, reconnecting through anything that goes wrong.

    `connect` is an awaitable factory returning a WebsocketConnection, so tests
    stage disconnects by handing back a connection that ends.

    Nothing escapes this loop. A shadow lane that could crash its own process
    would still be harmless to alerting — that is what the process split buys —
    but it would silently stop collecting the dataset it exists for, so every
    failure is logged, backed off, and retried."""
    await record_start(pool, WS_LANE_PROCESS, clock.now())
    delay = RECONNECT_MIN_SECONDS
    while True:
        try:
            await run_connection(pool, gateway, budget, connect, clock)
        except (WebsocketClosed, LaneSilent) as exc:
            log.warning("ws lane: connection ended (%s); reconnecting in %.0fs", exc, delay)
        except Exception:
            log.exception("ws lane: unexpected failure; reconnecting in %.0fs", delay)
        else:
            delay = RECONNECT_MIN_SECONDS
        await clock.sleep(delay)
        # Back off a persistent outage, never below the connection-rate floor.
        delay = min(RECONNECT_MAX_SECONDS, max(RECONNECT_MIN_SECONDS, delay * 2))


async def run_connection(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connect: Connect,
    clock: Clock,
) -> None:
    """One connection's life: subscribe to liveness, then loop — refreshing the
    tracked set, pinging, watching for silence, and turning position messages
    into events. Returns only by raising; the caller reconnects."""
    connection = await connect()
    allowance = outbound_allowance(clock)
    stats = LaneStats()
    # A fresh connection has subscribed to nothing and, crucially, resynced
    # nothing: `subscribed` starts empty on every reconnect, so the refresh
    # below re-establishes absolute state for every Trader before any of them
    # is streamed again.
    subscribed: set[str] = set()
    try:
        if not await send_if_allowed(connection, allowance, subscribe(liveness_subscription())):
            raise LaneSilent("could not subscribe to the liveness feed")
        started = clock.now()
        last_liveness = started
        last_ping = started
        last_beat = started
        last_refresh: datetime | None = None
        while True:
            now = clock.now()
            if last_refresh is None or (now - last_refresh).total_seconds() >= (
                TRACKED_REFRESH_SECONDS
            ):
                await _refresh_subscriptions(
                    pool, gateway, budget, connection, allowance, clock, subscribed, stats
                )
                last_refresh = clock.now()
                # A refresh is time spent resyncing over REST, not reading the
                # socket — so it cannot be evidence that the feed went quiet.
                # Charging it to the liveness deadline would be self-defeating
                # at exactly the worst moment: the first refresh of a
                # connection resyncs EVERY tracked wallet, which at a large
                # poll set and budget pacing can run into tens of seconds, and
                # the lane would declare a perfectly healthy connection dead
                # before it had read a single message — then reconnect, resync
                # again, and never get started.
                last_liveness += last_refresh - now
                now = last_refresh
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
                # The affirmative signal: market data is flowing, so silence
                # from a Trader's feed is that Trader being quiet.
                stats.liveness_messages += 1
                last_liveness = clock.now()
                if (last_liveness - last_beat).total_seconds() >= HEARTBEAT_INTERVAL_SECONDS:
                    await beat(pool, WS_LANE_PROCESS, last_liveness)
                    last_beat = last_liveness
            elif channel == POSITIONS_CHANNEL:
                stats.position_messages += 1
                stats.events += await _apply_positions_message(
                    pool, message, subscribed, clock.now()
                )
            elif channel == "error":
                log.warning("ws lane: server error frame: %s", message.get("data"))
    finally:
        await connection.close()
        log.info(
            "ws lane: connection closed after %d events from %d position messages "
            "(%d liveness, %d traders subscribed)",
            stats.events,
            stats.position_messages,
            stats.liveness_messages,
            len(subscribed),
        )


async def send_if_allowed(
    connection: WebsocketConnection, allowance: RollingMinute, message: dict[str, Any]
) -> bool:
    """Send if the outbound allowance permits. False means it did not go — the
    caller decides whether that is a deferral or a failure.

    Public because the order lanes (issue #168) send on their own connections
    under the same per-IP outbound ceiling, and one implementation of "may I
    speak" is the point: two would drift and the cap is shared."""
    if not allowance.take():
        log.warning(
            "ws lane: outbound allowance exhausted (%d/min); deferring %s",
            OUTBOUND_BUDGET_PER_MINUTE,
            message.get("method"),
        )
        return False
    await connection.send(message)
    return True


async def _refresh_subscriptions(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    connection: WebsocketConnection,
    allowance: RollingMinute,
    clock: Clock,
    subscribed: set[str],
    stats: LaneStats,
) -> None:
    """Bring the subscription set in line with the poll set.

    A wallet joining gets the treatment a reconnect gives everyone: its
    absolute state is re-established over REST FIRST, then it is subscribed. A
    wallet leaving is unsubscribed and forgotten, so a re-follow re-baselines
    silently instead of diffing against months-old memory — the poller's
    re-follow rule (`_prune_untracked`), which this lane owes for the same
    reason."""
    await _prune_unwatched(pool)
    wanted = await fetch_poll_set(pool)
    if len(wanted) > MAX_SUBSCRIBED_TRADERS:
        log.error(
            "ws lane: %d wallets exceeds the %d unique users one IP may track "
            "(measured 2026-08-03, ADR-0007); shadowing the first %d only — "
            "raising this needs another IP, not another constant",
            len(wanted),
            MAX_SUBSCRIBED_TRADERS,
            MAX_SUBSCRIBED_TRADERS,
        )
        wanted = wanted[:MAX_SUBSCRIBED_TRADERS]
    for address in sorted(subscribed - set(wanted)):
        # A deferred unsubscribe leaves the wallet subscribed and retries on the
        # next refresh, rather than leaving the lane's idea of what it holds
        # wrong.
        gone = unsubscribe(positions_subscription(address))
        if await send_if_allowed(connection, allowance, gone):
            subscribed.discard(address)
    for address in wanted:
        if address in subscribed:
            continue
        try:
            await _resync(pool, gateway, budget, address, clock, stats)
        except RateLimitedError:
            # Pacing, not an outage (issue #28): leave the wallet unsubscribed
            # and try again on the next refresh. Subscribing without a resync
            # is the one thing that must not happen.
            log.warning("ws lane: rate limited resyncing %s; retrying next refresh", address)
            continue
        except GatewayError:
            log.warning("ws lane: resync failed for %s; retrying next refresh", address)
            continue
        wanted_feed = subscribe(positions_subscription(address))
        if not await send_if_allowed(connection, allowance, wanted_feed):
            continue
        subscribed.add(address)


async def _resync(
    pool: asyncpg.Pool,
    gateway: HyperliquidGateway,
    budget: Budget,
    address: str,
    clock: Clock,
    stats: LaneStats,
) -> None:
    """Re-establish one Trader's absolute state from a point-in-time REST read,
    before anything is streamed for them.

    This is the lane's most important behaviour and the reason it costs any
    REST weight at all. The spend is paced by the same shared budget everything
    else uses, carrying the ingest-style reserve (see epigone.ws.main), so a
    reconnect storm can slow ingest but can never draw the bucket below the
    floor that guarantees position polling — and therefore Position Alerts —
    its instant claim."""
    positions = await fetch_positions_paced(gateway, budget, address)
    stats.events += await _apply_positions(pool, address, positions, clock.now(), resync=True)


async def _apply_positions_message(
    pool: asyncpg.Pool, message: dict[str, Any], subscribed: set[str], now: datetime
) -> int:
    """Turn one streamed position message into events, or ignore it.

    A message for an address this connection has not resynced is dropped: it
    would be diffed against memory the stream is not yet trusted against, which
    is precisely the gap-losing behaviour the resync exists to prevent. In
    practice that only catches a push racing an unsubscribe."""
    try:
        address, streamed = parse_positions_message(message["data"])
    except (GatewayError, KeyError, TypeError) as exc:
        log.warning("ws lane: unreadable %s message: %s", POSITIONS_CHANNEL, exc)
        return 0
    if address not in subscribed:
        log.debug("ws lane: dropping %s message for unsubscribed %s", POSITIONS_CHANNEL, address)
        return 0
    # The all-dex subscription carries EVERY venue; the resync that anchors it
    # is a REST read of POSITION_VENUES only. Diffing the wider observation
    # against memory built from the narrower one would read every uncovered
    # coin as an OPEN it never saw close — and then, on each reconnect, as a
    # CLOSE and an OPEN again, since the resync would drop it once more. So the
    # stream is reduced to the venues the anchor can cover.
    #
    # That also keeps the comparison this lane exists for honest: the poller
    # covers POSITION_VENUES, so a websocket event on a venue it never looks at
    # would show up in #158 as a websocket-only signal and mean nothing about
    # the transports. Widening coverage stays a REST-side decision (the mkts
    # drop, epigone.gateway.POSITION_VENUES), taken once, for both lanes.
    positions = [position for position in streamed if on_covered_venue(position.coin)]
    return await _apply_positions(pool, address, positions, now, resync=False)


async def _apply_positions(
    pool: asyncpg.Pool,
    address: str,
    positions: list[Position],
    now: datetime,
    *,
    resync: bool,
) -> int:
    """Diff an observation against the lane's memory and commit events plus
    memory in ONE transaction. Returns the event count.

    The atomicity is ADR-0006's, inherited rather than reinvented: the events
    land with the memory advance they were diffed from, so an interrupted lane
    leaves both or neither and the next observation re-diffs the same change
    exactly once. `record_events` is the same seam the poller writes through —
    only `source` differs — which is what makes the two lanes' rows comparable
    at all."""
    async with pool.acquire() as conn, conn.transaction():
        baselined = await conn.fetchval(
            "SELECT 1 FROM ws_lane_state WHERE trader_address = $1", address
        )
        if not baselined:
            # First observation of this Trader: record it and emit nothing.
            # Positions that predate the lane's first look are not events —
            # the poller's baseline rule, which this lane must match or every
            # newly tracked wallet would produce a burst of phantom opens that
            # the comparison would read as a websocket-only signal.
            for position in positions:
                await remember(
                    conn, WS_SNAPSHOTS, address, position, opened_at=now, updated_at=now
                )
            await conn.execute(
                """
                INSERT INTO ws_lane_state (trader_address, baselined_at, resynced_at,
                                           last_message_at)
                VALUES ($1, $2, $2, $2)
                """,
                address,
                now,
            )
            return 0

        previous = await read_snapshots(conn, WS_SNAPSHOTS, address)
        changes = diff_positions(previous, positions, now)
        await apply_changes(conn, WS_SNAPSHOTS, address, changes, now)
        events = events_of(changes)
        if events:
            await record_events(conn, address, events, now, source=WS_SOURCE)
        if resync:
            await conn.execute(
                """
                UPDATE ws_lane_state SET resynced_at = $2, last_message_at = $2
                WHERE trader_address = $1
                """,
                address,
                now,
            )
        else:
            await conn.execute(
                "UPDATE ws_lane_state SET last_message_at = $2 WHERE trader_address = $1",
                address,
                now,
            )
        return len(events)


async def _prune_unwatched(pool: asyncpg.Pool) -> None:
    """Forget wallets that left the poll set, so a re-follow re-baselines
    silently rather than diffing against stale memory (the poller's re-follow
    rule). Events already written stay — they were true when observed."""
    async with pool.acquire() as conn, conn.transaction():
        for table in ("ws_position_snapshots", "ws_lane_state"):
            await conn.execute(
                f"""
                DELETE FROM {table}
                WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
                  AND trader_address NOT IN
                      (SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL)
                """
            )
