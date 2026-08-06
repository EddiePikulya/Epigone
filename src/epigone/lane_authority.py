"""Who produces position events right now (issue #158, ADR-0009).

Two lanes observe the same Traders. Since the cutover the WEBSOCKET produces
the events everything downstream acts on, and the REST poll pass drops to a low
cadence where it keeps reading, keeps comparing, and writes nothing anyone
consumes — until the websocket goes bad, at which point it takes production
back on its own. This module is the one fact both lanes read to know which of
those worlds they are in.

**Ownership follows the heartbeat, and the poller decides for itself.** There
is no supervisor process and no second piece of state: the poller reads the ws
lane's `process_heartbeats` row — the same row, the same staleness machinery
that has run since the safety layer shipped — and concludes. That is deliberate.
A separate arbiter would introduce exactly the window this design cannot
tolerate: one component believing the lane is healthy while another has already
given up, with both writing.

**Escalate fast, de-escalate slow.** A stale heartbeat transfers production
immediately. Returning it takes sustained health (`WS_RECOVERY_SECONDS` of
uninterrupted freshness, restarted by any staleness) AND evidence that the lane
re-established absolute state after the degradation began — its own per-Trader
`resynced_at` stamps, later than the moment the poller took over. A websocket
delivers what happens from the moment you subscribe, so a lane that streamed
through a degraded window may be diffing against memory the world moved past;
its resync is the only thing that says otherwise.

**One writer, enforced by the database rather than by timing.** `owns()` reads
the row `FOR SHARE` inside the producer's own event-writing transaction;
`take_ownership()` takes it `FOR UPDATE`. A transfer therefore cannot commit
while a producer's write is in flight, and a producer starting after the
transfer blocks on the lock, then sees the new owner and writes nothing
authoritative. Both lanes still WRITE everything they observe — the loser's
rows land with `authoritative = FALSE`, which is what keeps the shadow
comparison alive after the cutover rather than ending it.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg

from epigone.clock import Clock
from epigone.poll_set import fetch_poll_set
from epigone.position_events import POLL_SOURCE, WS_SOURCE
from epigone.safety.heartbeat import last_beat
from epigone.ws import MAX_SUBSCRIBED_TRADERS, WS_LANE_PROCESS

log = logging.getLogger(__name__)

# The owners are the event sources, deliberately the same two strings: a row's
# `source` says which transport saw it, and the owner says which transport was
# allowed to be believed at that instant. Two vocabularies for one distinction
# would need a mapping, and a mapping is a thing that can be wrong.
POLL_OWNER = POLL_SOURCE
WS_OWNER = WS_SOURCE

# How stale the websocket lane's heartbeat may get before the poller takes
# production back. The lane beats every ~10s off market data that always flows
# (epigone.ws.lane), so this is four missed beats — long enough that a GC pause
# or a slow resync is not an incident, short enough that the bounded failover
# time stays inside a minute end to end: staleness (45s) + the standby poll
# tick that notices it (10s) + the escalated pass itself.
WS_HEARTBEAT_STALE_SECONDS = 45.0

# How long the websocket must look continuously healthy before production goes
# back to it. Deliberately ~7× the escalation threshold: a lane that flaps
# costs one escalation and then stays escalated for five minutes, rather than
# thrashing ownership — and thrash is worse than staying degraded, because
# every transfer is a moment where the two lanes' memories are re-anchored
# against each other.
WS_RECOVERY_SECONDS = 300.0

# How far BEFORE its previous look the poller searches for the websocket's
# production of a change, before calling it a miss.
#
# Three things have to fit inside it, and the margin is deliberately generous
# because the cost of being too small is a false incident (a needless
# escalation) while the cost of being too large is a real miss noticed one
# standby cycle later:
#
#   - the websocket legitimately sees a change BEFORE the poll that first
#     reveals it (it led the poller by a median 4.2s on the shadow dataset);
#   - the entry-coalescing window holds a scale-in for up to
#     WS_COALESCE_WINDOW_SECONDS before emitting it as one event (ADR-0009);
#   - the positions feed pushes on a ~5s cadence, so a deferred entry emits on
#     the next push after the window rather than the instant it closes.
#
# tests/test_position_cutover.py pins the relation rather than the number.
RECONCILE_GRACE_SECONDS = 30.0
RECONCILE_GRACE = timedelta(seconds=RECONCILE_GRACE_SECONDS)

# What the operator's kill switch says when it is the reason.
DISABLED_REASON = "websocket authority disabled by configuration"


@dataclass(frozen=True)
class Authority:
    """Who owns production, since when, and why — the row, as a value.

    `healthy_since` is bookkeeping for the de-escalation probation: the first
    moment the poller saw a fresh websocket heartbeat in the current attempt at
    recovery, or None whenever the lane is not currently looking healthy. It is
    meaningless while the websocket already owns production."""

    owner: str
    since: datetime
    reason: str
    healthy_since: datetime | None

    @property
    def degraded(self) -> bool:
        """Whether the poller is carrying production because something went
        wrong — as opposed to the operator having switched the cutover off, or
        the pre-cutover world. The health monitor's alerting question."""
        return self.owner == POLL_OWNER and self.reason != DISABLED_REASON


# What an absent row means, and it is not "nobody owns production": the poll
# pass does, since before anything was recorded. The risk_limits precedent
# (migration 0036, epigone.execute.limits) — absence is never permission, and
# the permission this one would grant is the dangerous direction, a websocket
# believing itself authoritative because a row went missing. In practice the
# row is seeded by migration 0037 and re-established by the first evaluation;
# this is what holds in the instant between.
_UNRECORDED = Authority(
    owner=POLL_OWNER,
    since=datetime(1970, 1, 1, tzinfo=UTC),
    reason="no cutover recorded; the poll pass owns production",
    healthy_since=None,
)


def _authority(row: asyncpg.Record | None) -> Authority:
    if row is None:
        return _UNRECORDED
    return Authority(
        owner=row["owner"],
        since=row["since"],
        reason=row["reason"],
        healthy_since=row["healthy_since"],
    )


async def read_authority(conn: asyncpg.Pool | asyncpg.Connection) -> Authority:
    """Who owns production, unlocked — for readers that only want to know."""
    return _authority(await conn.fetchrow("SELECT * FROM lane_authority"))


async def owns(conn: asyncpg.Connection, source: str) -> bool:
    """May `source` write authoritative events in THIS transaction?

    Reads the row `FOR SHARE`, so the answer cannot go stale before the
    transaction commits: a concurrent transfer needs `FOR UPDATE` and waits.
    Producers hold this only for the length of one Trader's write, and two
    producers never block each other — share locks are compatible; it is the
    transfer that queues behind them, which is the direction that matters."""
    owner = await conn.fetchval("SELECT owner FROM lane_authority FOR SHARE")
    return bool((owner or _UNRECORDED.owner) == source)


async def take_ownership(
    conn: asyncpg.Connection, owner: str, reason: str, now: datetime
) -> Authority:
    """Move production to `owner` — `FOR UPDATE`, so it serializes against
    every producer's in-flight write (see `owns`).

    Probation (`healthy_since`) is cleared by every transfer: a lane that has
    just lost production starts its recovery from scratch, and a lane that has
    just gained it has no probation to serve."""
    await conn.execute("SELECT 1 FROM lane_authority FOR UPDATE")
    row = await conn.fetchrow(
        """
        INSERT INTO lane_authority (owner, since, reason, healthy_since)
        VALUES ($1, $2, $3, NULL)
        ON CONFLICT (id) DO UPDATE
        SET owner = EXCLUDED.owner,
            since = EXCLUDED.since,
            reason = EXCLUDED.reason,
            healthy_since = NULL
        RETURNING *
        """,
        owner,
        now,
        reason,
    )
    log.warning("lane authority: production moves to %s — %s", owner, reason)
    return _authority(row)


async def evaluate_authority(
    pool: asyncpg.Pool, clock: Clock, *, enabled: bool = True
) -> Authority:
    """Decide who owns production, transfer it if the decision changed, and
    return the standing answer. Called at the top of every position cycle.

    `enabled` is the operator's kill switch (WS_AUTHORITATIVE): False pins
    production to the poller at once — undoing a cutover must never be another
    probation — and re-enabling serves the full recovery window like any other
    return of ownership."""
    now = clock.now()
    async with pool.acquire() as conn, conn.transaction():
        authority = _authority(await conn.fetchrow("SELECT * FROM lane_authority FOR UPDATE"))
        beaten_at = await last_beat(conn, WS_LANE_PROCESS)
        age = None if beaten_at is None else (now - beaten_at).total_seconds()
        fresh = age is not None and age <= WS_HEARTBEAT_STALE_SECONDS

        if not enabled:
            if authority.owner == WS_OWNER:
                return await take_ownership(conn, POLL_OWNER, DISABLED_REASON, now)
            return await _probation(conn, authority, None)

        if authority.owner == WS_OWNER:
            if not fresh:
                return await take_ownership(conn, POLL_OWNER, _stale_reason(age), now)
            # Coverage is re-checked while the websocket HOLDS production, not
            # only when it asks for it: the poll set grows under the operator's
            # hand, at a moment nobody is looking at ownership, and a lane
            # authoritative for wallets it cannot see is a silent hole in
            # alerting and in the copy path — worse than a degraded lane.
            outgrown = await _outgrown(conn)
            if outgrown is not None:
                return await take_ownership(conn, POLL_OWNER, outgrown, now)
            return authority

        # The poller owns. Everything below is the handback, which is allowed to
        # fail for several separate reasons and must say which.
        if not fresh:
            return await _probation(conn, authority, None)
        authority = await _probation(conn, authority, authority.healthy_since or now)
        assert authority.healthy_since is not None
        if (now - authority.healthy_since).total_seconds() < WS_RECOVERY_SECONDS:
            return authority
        blocker = await _handback_blocker(conn, authority.since)
        if blocker is not None:
            log.info("lane authority: websocket healthy but not yet trusted — %s", blocker)
            return authority
        return await take_ownership(
            conn, WS_OWNER, "websocket healthy, resynced and covering the poll set", now
        )


def _stale_reason(age: float | None) -> str:
    if age is None:
        return "websocket lane has never beaten its heartbeat"
    return f"websocket heartbeat stale ({age:.0f}s > {WS_HEARTBEAT_STALE_SECONDS:.0f}s)"


async def _probation(
    conn: asyncpg.Connection, authority: Authority, healthy_since: datetime | None
) -> Authority:
    """Record where the current run of websocket health started, or that there
    is no such run. Written even though it is only bookkeeping, because the
    poller is not a single long-lived decision — it re-decides every cycle, and
    a restart mid-probation must not shorten it."""
    if authority.healthy_since == healthy_since:
        return authority
    row = await conn.fetchrow(
        """
        INSERT INTO lane_authority (owner, since, reason, healthy_since)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE SET healthy_since = EXCLUDED.healthy_since
        RETURNING *
        """,
        authority.owner,
        authority.since,
        authority.reason,
        healthy_since,
    )
    return _authority(row)


async def _outgrown(conn: asyncpg.Connection) -> str | None:
    """Whether the poll set has outgrown what one IP can stream, as a sentence.

    One IP may subscribe MAX_SUBSCRIBED_TRADERS unique users (ADR-0008,
    measured); past that the lane covers a prefix of the poll set and the rest
    is watched by nobody. Per the 2026-08-06 amendment on #158, v1 keeps
    ownership GLOBAL and caps the tracked set rather than splitting ownership
    per wallet — so the whole lane stands down, and the way past the ceiling is
    more source IPs (#29), not more machinery here."""
    tracked = await fetch_poll_set(conn)
    if len(tracked) <= MAX_SUBSCRIBED_TRADERS:
        return None
    return (
        f"the poll set is {len(tracked)} wallets and one IP may stream "
        f"{MAX_SUBSCRIBED_TRADERS}"
    )


async def _handback_blocker(conn: asyncpg.Connection, since: datetime) -> str | None:
    """Why production may NOT go back to the websocket yet, or None.

    Two conditions, both about coverage rather than liveness:

    **The lane must cover the whole poll set.** One IP may subscribe 15 unique
    users (MAX_SUBSCRIBED_TRADERS); a poll set past that leaves wallets the
    websocket never watches, and a lane that is authoritative for wallets it
    cannot see is a silent hole in alerting and in the copy path. Per the
    2026-08-06 amendment on #158, v1 keeps ownership GLOBAL and caps the
    tracked set instead — per-wallet ownership doubles the machinery for a
    scale this deployment does not have.

    **The lane must have re-established absolute state since the degradation.**
    Streaming resumes from the moment you subscribe, so a lane that kept
    streaming through a degraded window may be diffing against memory that no
    longer matches reality; its per-Trader `resynced_at` (stamped by its own
    REST re-read, `epigone.ws.lane`) is the evidence, and nothing else is."""
    outgrown = await _outgrown(conn)
    if outgrown is not None:
        return outgrown
    tracked = await fetch_poll_set(conn)
    unresynced = await conn.fetchval(
        """
        SELECT count(*)
        FROM unnest($1::text[]) AS tracked (trader_address)
        LEFT JOIN ws_lane_state s ON s.trader_address = tracked.trader_address
        WHERE s.resynced_at IS NULL OR s.resynced_at < $2
        """,
        tracked,
        since,
    )
    if unresynced:
        return f"{unresynced} of {len(tracked)} wallets have not resynced since {since:%H:%M:%S}"
    return None
