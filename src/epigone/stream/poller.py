"""The tracked-wallet poll pass: position diffing for Position Alerts (issue #4).

Each pass polls every distinct wallet in the poll set (deduped across Users) via
clearinghouseState, diffs against the persisted snapshots, and queues alerts. The
poll set is tracked Traders UNION Users' own linked wallets (issue #121): a linked
wallet is snapshotted as its owner's holdings reference but never queues tracking
alerts (only `tracks` followers are fanned out to). Every wallet is polled on each
POSITION_VENUE per pass — the core perps and the xyz HIP-3 builder DEX (issue #21;
the mkts index DEX was dropped 2026-07-29, see epigone.gateway.POSITION_VENUES) —
because most non-core activity (equity/"stock" perps like `xyz:META`) lives off
core. The venues' position lists merge before diffing; their coins are namespaced
(`xyz:META`) so the (trader, coin) snapshot key tracks the venues independently,
with no schema change and no false OPEN/CLOSE from mixing them.

Diff semantics live in `epigone.position_diff` (tested in
tests/test_position_poller.py) — baseline silence, OPEN, CLOSE, FLIP as one
event, SCALE-IN/SCALE-OUT above SCALE_SIGNIFICANCE_THRESHOLD, and the silent
sub-threshold update. They were lifted out of this module unchanged when the
websocket shadow lane (issue #157) became a second producer of the same
events: the lanes keep separate memory but must not keep separate opinions
about what a scale-in is, or the comparison that lane exists to feed would be
measuring two drifting copies of the rule instead of two transports.

This pass owns the parts that are its own: the poll set, the budget, the
per-Trader transaction, and the baseline flag in `position_poll_state`.

**Since the cutover (issue #158, ADR-0009) it is usually not the pass anyone is
listening to.** The websocket lane owns event production while it is healthy;
this pass drops to a low cadence where it still reads every wallet, still
diffs, and still records what it saw — with `authoritative = FALSE`, which
nothing consumes — and does two jobs no heartbeat can do:

- it is the failover path, exercised every single day rather than only during
  incidents, so escalation changes a cadence instead of waking dormant code;
- it RECONCILES. A websocket that is connected, delivering and silently missing
  changes looks healthy from every angle except this one: comparing what it
  produced against periodically re-read truth. A change the websocket never
  produced is an incident (`_reconcile`), and the pass takes production back
  rather than quietly writing the event beside another writer's.

Event production and the alert fan-out therefore live in
`epigone.position_publish`, called by both lanes, so a Position Alert has one
shape whichever transport observed it. What stays here is everything only a
REST read can do: account equity, withdrawal detection, and the poll set.

Every queued event is filtered per follower against that Track's alert
controls (issue #10): a muted Track receives nothing, and an effective minimum
position size (per-Track override, else the User's global floor) drops alerts
for positions notionally smaller than the floor. Suppression happens at queue
time, never at delivery, so unmuting or raising a floor never dumps a backlog.

**Re-follow.** When a Trader loses their last follower, the pass prunes their
snapshots and poll state; following them again re-baselines silently instead of
diffing against a stale snapshot and alerting on ancient changes.

Snapshot updates, event rows and alert-row inserts share one transaction per
Trader, so an event is detected exactly once: after a stream restart the
snapshots already reflect everything recorded, and anything not yet committed
diffs again. One alert row is queued per event per follower; the bot process
owns delivery (ADR-0002: processes meet only in Postgres).

Each event is also written down durably, once, in `position_events` (issue
#156, ADR-0006) — the seam the copy executor (#136) reads. That table is a
record of what this pass decided, never a second opinion about it: both writes
iterate the same in-memory list. Alerts are untouched by it. They keep their
per-follower shape and their mute/min-size suppression, because those are
notification preferences and an executor must see a leader's trade whether or
not any follower's floor admits it — so the event write is unfiltered, and a
wallet in the poll set only because a User linked it (#121) records events too,
though nobody is alerted about it. See epigone.position_events.

The pass also writes down what each Trader's account is WORTH (issue #170).
`clearinghouseState` has always answered with the account value beside the
positions, and the parser dropped it; now the same call yields both and the
latest observation lands in `trader_equity` inside the same per-Trader
transaction — so equity and positions are never separately true. It costs no
request and no weight. Unlike an event, an observation is not something the
Trader did, so only the newest is kept; the observation it replaces is
`record_equity`'s return value. See epigone.trader_equity.

That delta is what Withdrawal Alerts read (issue #171, ADR-0007 "Out of A4
scope"): an equity drop this pass's own PnL cannot account for is money that
left the wallet, and every follower is told. The judgement is
`epigone.withdrawals`, made from the same two looks at the book the position
diff was made from and queued into `withdrawal_alerts` in this same per-Trader
transaction — so a detection and the equity that can no longer produce it
commit together, and the alert fires once.
"""

import logging
from dataclasses import dataclass
from datetime import datetime

import asyncpg

from epigone.budget import Budget, record_rate_limit
from epigone.clock import Clock
from epigone.gateway import (
    POSITION_VENUES,
    AccountState,
    GatewayError,
    HyperliquidGateway,
    Position,
    RateLimitedError,
    fetch_account_state,
    fetch_open_positions,
)
from epigone.lane_authority import (
    POLL_OWNER,
    RECONCILE_GRACE,
    owns,
    read_authority,
    take_ownership,
)
from epigone.poll_set import OFF_THE_POLL_SET, fetch_poll_set, leaders_first
from epigone.position_diff import diff_positions, events_of
from epigone.position_events import POLL_SOURCE, WS_SOURCE, PositionEvent, record_events
from epigone.position_publish import publish
from epigone.position_snapshots import (
    POLL_SNAPSHOTS,
    WS_SNAPSHOTS,
    apply_changes,
    read_snapshots,
    remember,
)
from epigone.trader_equity import record_equity
from epigone.withdrawals import MAX_OBSERVATION_GAP_INTERVALS, Withdrawal, detect_withdrawal

log = logging.getLogger(__name__)

# The stream spends against the shared 900/min budget (epigone.budget, issue
# #28) with priority over ingest. Each wallet in the poll set costs one
# clearinghouseState call per POSITION_VENUE — core and the xyz builder DEX
# (#21) — so weight 4 per wallet per 10s poll. The poll set is tracked wallets
# UNION Users' own linked wallets (#121), so a linked wallet adds the same
# weight-4 as a tracked one; being one-per-User and only the followers' own,
# they add a handful of wallets, not a multiplier, to that distinct count.
POSITIONS_WEIGHT = 2  # clearinghouseState, per call — a wallet spends this once per DEX

POLL_INTERVAL_SECONDS = 10

# How often the pass runs while the websocket lane owns event production
# (issue #158, ADR-0009): slow enough that reconciliation costs a small
# fraction of the budget the escalated cadence needs, fast enough that a lane
# silently missing changes is caught within a minute of the change. The
# failover it feeds is bounded by the LOOP TICK, not by this — see
# epigone.stream.main.
STANDBY_POLL_INTERVAL_SECONDS = 60

# Same reasoning as the ingest passes: a sustained streak means Hyperliquid is
# down, not that wallets are odd — stop burning budget and resume next cycle.
MAX_CONSECUTIVE_FAILURES = 5

# Everything this pass remembers about one wallet, keyed by its address: the
# diff's memory, the baseline flag, and the latest equity observation (#170).
# _prune_untracked drops a wallet from all three at once — see its docstring.
# Module constants, never user input, so they are safe to inline into the SQL
# (position_snapshots' table-name convention).
POLL_MEMORY_TABLES = ("trader_equity", "position_snapshots", "position_poll_state")


@dataclass(frozen=True)
class _Applied:
    """What one Trader's transaction did: how many changes it saw, how many it
    PRODUCED (authoritatively, for consumers), whether it found drift, and
    which coins it withheld judgement on.

    A `held` coin is one whose change the websocket has not produced yet: its
    snapshot is deliberately NOT advanced, so the next pass re-diffs the same
    change and judges it with more evidence. `deferred` says this pass did that
    — the loop reads it and re-polls on the next tick instead of waiting out
    the standby interval, so a real miss costs ~10s rather than ~60s."""

    events: int = 0
    produced: int = 0
    drifted: bool = False
    held: frozenset[str] | set[str] = frozenset()
    deferred: bool = False


@dataclass(frozen=True)
class PollResult:
    polled: int
    failed: int
    events: int
    aborted: bool
    # Events this pass produced AUTHORITATIVELY (issue #158). While the
    # websocket owns production this is 0 and `events` is not: the pass is
    # still diffing and still recording everything it observed, it is simply
    # not the lane anyone is listening to. The gap between the two is the
    # cutover working.
    produced: int = 0
    # Wallets whose diff found a change the websocket never produced. Non-zero
    # means this pass raised an incident and took production back.
    drifted: int = 0
    # Wallets carrying a change the websocket has not produced YET, whose
    # verdict this pass withheld. Non-zero means the loop should look again on
    # the next tick rather than wait out the standby interval.
    deferred: int = 0


async def run_poll_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> PollResult:
    """One clearinghouseState call per POSITION_VENUE for each distinct wallet in
    the poll set — tracked Traders plus Users' own linked wallets (issue #121) —
    merged before diffing, paced by the budget. Those same responses carry each
    wallet's account equity, recorded with the diff (issue #170). A purely-linked
    wallet is snapshotted but queues no alerts: the fan-out reaches only `tracks`
    — its equity is still recorded, exactly as its events are.

    Since the cutover (#158) the pass runs identically whether or not it owns
    event production; what changes is the CADENCE it is called at
    (epigone.stream.main) and what `_apply_poll` does with the diff. Running the
    same code path every day rather than only during incidents is the point of
    a warm standby: a fallback exercised only when it is needed is least
    trustworthy exactly when it is."""
    await _prune_untracked(pool)
    # The cadence this pass is running at, which is the poller's own answer to
    # "how long is an ordinary gap between two looks at a wallet" — what the
    # withdrawal staleness gate is measured in (#171, #158).
    interval = (
        POLL_INTERVAL_SECONDS
        if (await read_authority(pool)).owner == POLL_OWNER
        else STANDBY_POLL_INTERVAL_SECONDS
    )
    addresses = await leaders_first(pool, await fetch_poll_set(pool))
    polled = failed = events = produced = drifted = deferred = consecutive_failures = 0
    for address in addresses:
        try:
            state = await fetch_account_state_paced(gateway, budget, address)
        except RateLimitedError:
            # Pacing, not an outage (issue #28): the gateway already backed off
            # and retried; the wallet just polls again next pass. Never counts
            # toward the abort streak.
            log.warning("poll pass: rate limited polling %s; retrying next pass", address)
            # Feed the health monitor's sustained-limiting signal (#54): a streak
            # that outlasted the gateway's backoff is real limiting, not pacing.
            await record_rate_limit(pool, clock.now())
            failed += 1
            continue
        except GatewayError:
            log.warning("poll pass: positions fetch failed for %s", address, exc_info=True)
            failed += 1
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error(
                    "poll pass aborted after %d consecutive failures; "
                    "%d polled so far, resuming next cycle",
                    consecutive_failures,
                    polled,
                )
                return PollResult(
                    polled=polled,
                    failed=failed,
                    events=events,
                    aborted=True,
                    produced=produced,
                    drifted=drifted,
                    deferred=deferred,
                )
            continue
        consecutive_failures = 0
        applied = await _apply_poll(pool, address, state, clock.now(), interval)
        events += applied.events
        produced += applied.produced
        drifted += int(applied.drifted)
        deferred += int(applied.deferred)
        polled += 1
    if events or failed:
        log.info(
            "poll pass done: %d polled, %d events (%d produced), %d failed",
            polled,
            events,
            produced,
            failed,
        )
    return PollResult(
        polled=polled,
        failed=failed,
        events=events,
        aborted=False,
        produced=produced,
        drifted=drifted,
        deferred=deferred,
    )


async def fetch_account_state_paced(
    gateway: HyperliquidGateway, budget: Budget, address: str
) -> AccountState:
    """A Trader's open positions AND covered-venue equity across the venues we
    cover (POSITION_VENUES: core plus the xyz builder DEX), paced by the budget:
    each clearinghouseState call costs POSITIONS_WEIGHT, so a wallet reserves
    every venue's weight before polling. Billing one spend per venue off
    POSITION_VENUES keeps the accounting in lockstep with the calls the shared
    fetch actually makes.

    The equity rides the calls this already made (issue #170) — the account
    value has always been in the clearinghouseState response and was discarded
    at parse time — so the loop below is unchanged and the exchange sees exactly
    the traffic it saw before. That is the whole cost story: zero extra
    requests, zero extra weight.

    The shared fetch (epigone.gateway.fetch_account_state) merges the venues and
    raises on a partial fetch; here that means the whole wallet is retried next
    pass with its snapshots untouched, never diffed against a half-empty list
    into false CLOSE alerts (issue #21) — and with its recorded equity
    untouched, never a sum missing a venue's collateral, which is the same
    figure a Trader emptying that venue would produce."""
    await _spend_venue_weight(budget)
    return await fetch_account_state(gateway, address)


async def fetch_positions_paced(
    gateway: HyperliquidGateway, budget: Budget, address: str
) -> list[Position]:
    """A Trader's open positions across the venues we cover, paced identically —
    the same calls, the same weight, parsed for the positions alone.

    Shared with the websocket lane's reconnect resync (issue #157), which needs
    exactly this — a paced, all-or-raise, point-in-time read — and must bill it
    the same way, off the same venue tuple, or the two lanes' spends would drift
    from the calls they make.

    Deliberately NOT the positions half of fetch_account_state_paced (issue
    #170). That read requires a marginSummary and raises without one, which is
    right where a missing equity would be acted on and pointless where nothing
    reads it: routing the resync through it would give the websocket lane a new
    way to fail for a field it never uses. The two share their pacing, which is
    the part that must not drift, and nothing else."""
    await _spend_venue_weight(budget)
    return await fetch_open_positions(gateway, address)


async def _spend_venue_weight(budget: Budget) -> None:
    """Reserve one clearinghouseState call's weight per covered venue, before
    the calls. Billing off POSITION_VENUES here — in one place both paced reads
    go through — keeps the accounting in lockstep with the calls the shared
    fetches actually make (issue #31)."""
    for _venue in POSITION_VENUES:
        await budget.spend(POSITIONS_WEIGHT)


async def _prune_untracked(pool: asyncpg.Pool) -> None:
    """Drop poll bookkeeping for wallets in no one's poll set — neither tracked
    nor linked (#121). The re-follow rule in the module docstring, widened to the
    union: a wallet only linked as a User's own drops the instant that link is
    cleared, exactly as a Trader drops when their last follower leaves, while a
    wallet still tracked (or still linked) by anyone keeps its snapshot. Queued
    alerts are untouched — they were real when detected and still owe delivery.

    The equity observation (issue #170) drops with the snapshots, for the same
    reason: it describes a moment Epigone was watching, and after a gap nobody
    watched it is not a baseline anything may be compared against. Left behind,
    it would hand withdrawal alerts (#171) a months-old figure and an alert for
    money that moved while the wallet was off the poll set.

    The three tables are pruned by ONE predicate, written once: they are pruned
    because they are all this pass's memory of a wallet, so a fourth table
    joining them — or the poll set widening again, as #121 widened it — must
    move them together or leave a table remembering a wallet the others have
    forgotten. Queued withdrawal_alerts and position_alerts are deliberately not
    in the list, for the reason above: an alert is not memory."""
    async with pool.acquire() as conn, conn.transaction():
        for table in POLL_MEMORY_TABLES:
            await conn.execute(
                f"DELETE FROM {table} WHERE {OFF_THE_POLL_SET}"  # noqa: S608 — module constants
            )


async def _apply_poll(
    pool: asyncpg.Pool, address: str, state: AccountState, now: datetime, interval: float
) -> "_Applied":
    """Diff one Trader's freshly fetched positions against the snapshots and
    commit snapshots + event rows + alert rows + this pass's equity observation
    atomically."""
    positions = state.positions
    async with pool.acquire() as conn, conn.transaction():
        # The equity observation lands first and unconditionally (issue #170):
        # it is what the account was worth when Epigone looked, which is as true
        # on a Trader's baselining pass as on any other, and as true of a pass
        # that diffed no events as of one that did. The observation it replaces
        # is its return value, and the only place both figures exist at once —
        # withdrawal detection (#171) takes its delta from it below, inside this
        # same transaction.
        previous_equity = await record_equity(conn, address, state.account_value, now)
        # `last_polled_at` is read, not just written, since the cutover (#158):
        # it is the start of the window reconciliation compares the websocket's
        # production against — everything this diff reports happened between
        # that instant and this one.
        state_row = await conn.fetchrow(
            """
            SELECT last_polled_at, reconcile_pending FROM position_poll_state
            WHERE trader_address = $1
            """,
            address,
        )
        polled_before = state_row["last_polled_at"] if state_row is not None else None
        if polled_before is None:
            for pos in positions:
                await remember(
                    conn, POLL_SNAPSHOTS, address, pos, opened_at=now, updated_at=now
                )
            await conn.execute(
                """
                INSERT INTO position_poll_state (trader_address, baselined_at, last_polled_at)
                VALUES ($1, $2, $2)
                """,
                address,
                now,
            )
            return _Applied()

        previous = await read_snapshots(conn, POLL_SNAPSHOTS, address)
        changes = diff_positions(previous, positions, now)
        events: list[PositionEvent] = events_of(changes)
        # WHETHER these are events anyone acts on — and whether this pass is
        # even ready to judge them — is decided in `_reconcile` (#158). While
        # the websocket owns production this pass records what it observed and
        # produces nothing; a change the websocket has not produced YET is held
        # back rather than escalated on sight.
        assert state_row is not None  # polled_before came off it
        applied = _Applied(events=len(events))
        if events:
            applied = await _reconcile(
                conn,
                address,
                events,
                positions,
                now,
                since=polled_before,
                pending=set(state_row["reconcile_pending"] or ()),
            )
        # The snapshot advance lands with the events that were diffed from it
        # (ADR-0006's atomicity, inherited rather than reinvented) — except for
        # the coins whose verdict was withheld, whose anchor stays put so the
        # next pass re-diffs the identical change and decides for real.
        await apply_changes(
            conn,
            POLL_SNAPSHOTS,
            address,
            [change for change in changes if change.coin not in applied.held],
            now,
        )
        # Every completed pass states the whole doubt, including that there is
        # none. Written HERE rather than inside `_reconcile` because a pass with
        # nothing to report never reaches it — and a doubt about a change that
        # has since evaporated would otherwise sit in this row for days and then
        # escalate the first unrelated change that ever raced the websocket,
        # with no look of patience at all.
        await conn.execute(
            """
            UPDATE position_poll_state SET last_polled_at = $2, reconcile_pending = $3
            WHERE trader_address = $1
            """,
            address,
            now,
            sorted(applied.held) or None,
        )
        # Money leaving the account (issue #171), judged from the same two looks
        # at the book the diff above just judged positions from — `previous` is
        # still what the last pass saw, since apply_changes advanced the table
        # and not this mapping. Reading one observation twice is the point: a
        # second fetch could disagree with the first, and a withdrawal is
        # precisely a disagreement between two figures.
        withdrawal = detect_withdrawal(
            previous_equity,
            state.account_value,
            previous,
            positions,
            now,
            # The staleness gate follows the cadence in force (#158): at the
            # standby interval a gate fixed at the escalated one would reject
            # every ordinary gap and turn withdrawal alerts off silently.
            max_gap_seconds=MAX_OBSERVATION_GAP_INTERVALS * interval,
        )
        if withdrawal is not None:
            await _queue_withdrawal_alerts(conn, address, withdrawal, now)
        return applied


async def _reconcile(
    conn: asyncpg.Connection,
    address: str,
    events: list[PositionEvent],
    positions: list[Position],
    now: datetime,
    *,
    since: datetime,
    pending: set[str],
) -> _Applied:
    """Record what this pass observed, and produce whatever the websocket did
    not (issue #158, ADR-0009).

    Continuous reconciliation is the half of the warm standby that a heartbeat
    cannot replace. A websocket that is connected, delivering, and silently
    missing changes looks perfectly healthy from the outside; the only thing
    that catches it is periodically re-reading truth and comparing. That is
    what this pass has been doing all along — it simply now compares before it
    writes.

    Per change, the question is **did the websocket produce an event moving the
    same direction on this (Trader, coin) since the poller's previous look?**
    If it did, this lane's own diff of the same change is recorded as a shadow
    row and nothing else happens — the change is already told, already alerted,
    already copied. If it did not, this pass has to tell three very different
    situations apart, because only one of them is a fault:

    - **The poller already owns production** (websocket degraded, or
      pre-cutover). Nothing to reconcile; it simply publishes.
    - **The websocket owes nothing.** Its rules are not this lane's rules: the
      significance threshold measures against the LAST OBSERVATION, and the
      lanes observe at different cadences, so a position creeping up ~8% per
      push crosses 25% against a 60s-old anchor and never against any single
      push. The lane emitted nothing and is completely current. `_still_owed`
      asks the lane's own memory, and a lane that agrees with reality missed
      nothing — this is the benign divergence class #158's 2026-08-02 comment
      names, and it must be classified as expected rather than as a lane error.
      The change is recorded as a shadow row; nobody produces it, which is what
      the websocket's rules already decided.
    - **The websocket owes it and has not delivered.** Now it matters, and this
      pass still gives it ONE look of patience: the lane is routinely a few
      seconds behind (it holds entry bursts, it re-sends state on its own
      cadence), so the coin is `held` — its anchor is not advanced — and the
      next pass re-diffs the identical change. A change still owed and still
      undelivered on that second look is drift: ownership transfers, loudly,
      and only then is the event produced. Quietly writing it instead would put
      two writers on one Trader and risk a doubled copy.

    Two classes of divergence, two mechanisms, and they must not be confused: a
    LATENCY difference resolves itself on the next look, a THRESHOLD difference
    never resolves and never should.

    **Direction, not kind or size.** The lanes legitimately describe the same
    reality with different kinds — a burst of fills the poller sees as one
    scale-in, a flip one lane decomposes and the other does not (both measured
    on the shadow dataset) — so comparing kinds would escalate on lanes that
    agree. Comparing the COIN alone goes wrong the other way: an entry the
    websocket did produce would vouch for an exit it did not, and an exit
    nobody produces is a copy position that never closes.

    The lookback reaches `RECONCILE_GRACE_SECONDS` before the previous poll,
    because the websocket can also see a change slightly BEFORE the poll that
    first reveals it (it led the poller by a median 4.2s)."""
    ws_rows = await _websocket_rows(conn, address, since - RECONCILE_GRACE)
    unvouched = [event for event in events if not _vouched_for(event, ws_rows.produced)]
    held: set[str] = set()
    drifted = False
    if unvouched and not await owns(conn, POLL_SOURCE):
        owed = await _still_owed(conn, address, positions, now)
        # Two ways a change can still be somebody's to produce, and the anchor
        # alone only sees the first:
        #
        #   MISSED   — the lane's own memory still owes an event on this coin;
        #   STRANDED — its memory does NOT, but it wrote an unconsumed row
        #              moving this way inside the window. That row is the
        #              fingerprint of a change observed while the OTHER lane
        #              owned production: the anchor advanced past it and the
        #              event went nowhere, so it will never be produced.
        #
        # Anything else is the benign class: no row at all, because the lane's
        # rules made the change a non-event, and its memory agrees with reality.
        missed = {event.coin for event in unvouched if event.coin in owed}
        stranded = {
            event.coin
            for event in unvouched
            if event.coin not in owed and _vouched_for(event, ws_rows.observed)
        }
        unvouched = [event for event in unvouched if event.coin in missed | stranded]
        doubted = missed | stranded
        confirmed = doubted & pending
        if doubted and not confirmed:
            # Every doubt is new. Withhold the verdict AND the memory advance,
            # so the next pass asks the same question about the same change
            # rather than about a change it has already forgotten.
            log.info(
                "reconciliation: %s %s not yet produced by the websocket lane; "
                "holding for one more look",
                address,
                ", ".join(sorted(doubted)),
            )
            held, unvouched = doubted, []
        elif confirmed:
            drifted = True
            reason = _drift_reason(address, missed & confirmed, stranded & confirmed)
            log.error("%s; escalating and taking production back", reason)
            # Named by the coins that actually served their look of patience: a
            # doubt raised for the first time this pass is not evidence, and
            # putting it in the reason would blame it for the escalation.
            await take_ownership(conn, POLL_OWNER, reason, now)
            # Re-read under the exclusive lock. A websocket write that committed
            # between the query above and the transfer is now visible, and no
            # further one can commit until this transaction ends — so this
            # answer is final rather than merely recent, which is what makes
            # "two writers never produce for one (Trader, coin)" a property of
            # the database.
            ws_rows = await _websocket_rows(conn, address, since - RECONCILE_GRACE)
            unvouched = [
                event for event in events if not _vouched_for(event, ws_rows.produced)
            ]
    published: list[PositionEvent] = []
    if unvouched and await publish(conn, address, unvouched, now, source=POLL_SOURCE):
        published = unvouched
    # Everything this pass saw that it did not produce, written down for the
    # comparison — except a held coin, whose change has not been judged yet and
    # which the next pass will re-diff and record identically.
    produced = {id(event) for event in published}
    shadow = [
        event for event in events if event.coin not in held and id(event) not in produced
    ]
    if shadow:
        await record_events(conn, address, shadow, now, source=POLL_SOURCE, authoritative=False)
    return _Applied(
        events=len(events),
        produced=len(published),
        drifted=drifted,
        held=held,
        deferred=bool(held),
    )


async def _still_owed(
    conn: asyncpg.Connection, address: str, positions: list[Position], now: datetime
) -> set[str]:
    """The coins on which the websocket lane's OWN memory says it still owes an
    event — reality diffed against the lane's anchor, by the lane's own rules.

    This is the discriminator between the two divergence classes, and it is
    already in the database: the lane's diff memory
    (`ws_position_snapshots`) IS its claim about what it has seen. Where that
    claim matches what the poller just read, the lane is current and owes
    nothing — whatever the poller's own longer-baseline diff decided. Where it
    does not, the lane is behind reality by its own account.

    Read-only, always: the poller never writes another lane's memory (ADR-0002
    — the lanes meet in Postgres, and each lane's memory has exactly one
    writer, or the writer's next diff would be against state it never saw).

    A Trader the lane has never baselined vouches for nothing: an empty memory
    agrees with a flat wallet the way an empty room agrees with an empty room,
    and absence of memory is absence of watching."""
    watching = await conn.fetchval(
        "SELECT 1 FROM ws_lane_state WHERE trader_address = $1", address
    )
    if not watching:
        return {position.coin for position in positions} | {
            row["coin"]
            for row in await conn.fetch(
                "SELECT coin FROM position_snapshots WHERE trader_address = $1", address
            )
        }
    anchor = await read_snapshots(conn, WS_SNAPSHOTS, address)
    return {
        change.coin
        for change in diff_positions(anchor, positions, now)
        if change.event is not None
    }


# Which way a change moves the position. `flip` is both legs at once (ADR-0006
# keeps it one event), so it vouches for — and is vouched for by — either
# direction; that is what absorbs the two lanes' different flip decompositions
# without letting an entry stand in for a missing exit.
ENTRY = "entry"
EXIT = "exit"
_DIRECTIONS = {
    "open": frozenset({ENTRY}),
    "scale_in": frozenset({ENTRY}),
    "close": frozenset({EXIT}),
    "scale_out": frozenset({EXIT}),
    "flip": frozenset({ENTRY, EXIT}),
}


def _vouched_for(event: PositionEvent, covered: set[tuple[str, str]]) -> bool:
    """Whether the websocket already produced something on this coin moving the
    same way — see `_reconcile` for why direction is the comparison."""
    return any((event.coin, direction) in covered for direction in _DIRECTIONS[event.kind])


async def _remember_doubt(conn: asyncpg.Connection, address: str, coins: set[str]) -> None:
    """Record (or clear) the coins whose verdict this pass withheld."""
    await conn.execute(
        "UPDATE position_poll_state SET reconcile_pending = $2 WHERE trader_address = $1",
        address,
        sorted(coins) or None,
    )


@dataclass(frozen=True)
class _WsRows:
    """What the websocket lane wrote about a Trader inside the reconciliation
    window, as (coin, direction) pairs.

    `produced` is what it wrote AUTHORITATIVELY — the rows consumers acted on,
    and the only ones that vouch for a change being told. `observed` is every
    row it wrote, authoritative or not.

    The difference between them is the whole point. A lane that is not the
    owner still writes everything it sees, unconsumed; those rows must never
    vouch for a change being produced (that would let the silent lane mute the
    live one) and must equally never be mistaken for silence — a change the
    lane observed while the other lane owned production is a change nobody
    produced and nobody now will."""

    produced: set[tuple[str, str]]
    observed: set[tuple[str, str]]


async def _websocket_rows(
    conn: asyncpg.Connection, address: str, since: datetime
) -> _WsRows:
    """The websocket lane's rows for this Trader since `since`, split by
    whether anyone consumed them."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT kind, coin, authoritative FROM position_events
        WHERE trader_address = $1 AND source = $2 AND observed_at >= $3
        """,
        address,
        WS_SOURCE,
        since,
    )
    keys = [
        ((row["coin"], direction), row["authoritative"])
        for row in rows
        for direction in _DIRECTIONS[row["kind"]]
    ]
    return _WsRows(
        produced={key for key, authoritative in keys if authoritative},
        observed={key for key, _authoritative in keys},
    )


def _drift_reason(address: str, missed: set[str], stranded: set[str]) -> str:
    """The sentence an operator reads in the health alert.

    It names the wallet and the coins, because "the websocket missed something"
    is not actionable and "0x037e… CASHCAT" is — and it says which of the two
    diagnoses this is, because they send a human to different places. A lane
    that dropped a change is a lane to investigate; a change that fell between
    two owners is a transfer doing what transfers do, and reading websocket
    logs for it wastes the incident."""
    parts = []
    if missed:
        parts.append(f"{', '.join(sorted(missed))} never arrived on the websocket")
    if stranded:
        parts.append(
            f"{', '.join(sorted(stranded))} was seen by the websocket while the poller "
            "owned production, so an ownership transfer left it produced by neither lane"
        )
    return f"reconciliation drift: {address} " + "; ".join(parts)


async def _queue_withdrawal_alerts(
    conn: asyncpg.Connection, address: str, withdrawal: Withdrawal, now: datetime
) -> None:
    """Tell every follower of this Trader that money left the account (issue
    #171) — one row each, in the poll pass's own transaction, so a withdrawal
    is queued exactly once for exactly the reason the equity it was computed
    from is stored exactly once.

    Muted Tracks are suppressed HERE, at queue time, for the reason every other
    suppression in this pass is: a row that never existed cannot be backfilled
    by unmuting (#10). The min-size floor is deliberately not applied — it
    judges a position's notional and a withdrawal has none, the same reading
    `_below_floor` gives an event that carries no size.

    Fan-out reads `tracks`, so a wallet in the poll set only because a User
    linked it as their own (#121) is priced and judged like any other and tells
    nobody: an owner's transfers between their own accounts are not an alert."""
    await conn.execute(
        """
        INSERT INTO withdrawal_alerts
            (user_telegram_id, trader_address, amount_usd, prior_equity, equity_usd,
             observed_at, created_at)
        SELECT t.user_telegram_id, $1, $2, $3, $4, $5, $6
        FROM tracks t
        WHERE t.trader_address = $1 AND NOT t.muted
        """,
        address,
        withdrawal.amount_usd,
        withdrawal.prior_equity,
        withdrawal.equity_usd,
        withdrawal.observed_at,
        now,
    )
