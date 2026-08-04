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
per-Trader transaction, the baseline flag in `position_poll_state`, and the
alert fan-out.

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
`record_equity`'s return value, which is where the withdrawal follow-up (#171)
will take its delta. See epigone.trader_equity.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

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
from epigone.ingest.fine import mark_due_now
from epigone.position_diff import diff_positions, events_of
from epigone.position_events import PositionEvent, record_events
from epigone.position_snapshots import POLL_SNAPSHOTS, apply_changes, read_snapshots, remember
from epigone.trader_equity import record_equity

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

# Same reasoning as the ingest passes: a sustained streak means Hyperliquid is
# down, not that wallets are odd — stop burning budget and resume next cycle.
MAX_CONSECUTIVE_FAILURES = 5


@dataclass(frozen=True)
class PollResult:
    polled: int
    failed: int
    events: int
    aborted: bool


async def run_poll_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> PollResult:
    """One clearinghouseState call per POSITION_VENUE for each distinct wallet in
    the poll set — tracked Traders plus Users' own linked wallets (issue #121) —
    merged before diffing, paced by the budget. Those same responses carry each
    wallet's account equity, recorded with the diff (issue #170). A purely-linked
    wallet is snapshotted but queues no alerts: _queue_alerts fans out only to
    `tracks` — its equity is still recorded, exactly as its events are."""
    await _prune_untracked(pool)
    addresses = await fetch_poll_set(pool)
    polled = failed = events = consecutive_failures = 0
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
                return PollResult(polled=polled, failed=failed, events=events, aborted=True)
            continue
        consecutive_failures = 0
        events += await _apply_poll(pool, address, state, clock.now())
        polled += 1
    if events or failed:
        log.info("poll pass done: %d polled, %d events, %d failed", polled, events, failed)
    return PollResult(polled=polled, failed=failed, events=events, aborted=False)


async def fetch_poll_set(pool: asyncpg.Pool) -> list[str]:
    """Every distinct wallet whose positions Epigone watches, sorted.

    Tracked wallets UNION Users' own linked wallets (#121). A linked wallet is
    polled purely so its positions are snapshotted as the User's holdings
    reference — the diff still runs, but _queue_alerts fans out only to `tracks`
    followers, so a wallet nobody tracks produces zero alerts. UNION dedups the
    both-roles case, so it costs one poll either way.

    Budget delta: each distinct wallet costs POSITIONS_WEIGHT per POSITION_VENUE
    — 2 venues × weight 2 = 4/pass — whether it is tracked, linked, or both.
    Linked wallets are one-per-User and only the followers' own, so in practice
    they add a handful of wallets, not a multiplier.

    The websocket shadow lane (issue #157) subscribes to exactly this set, from
    this one definition: the lanes must watch the same subjects or the
    poll-vs-websocket comparison would be reading a difference in coverage as a
    difference in transport."""
    rows = await pool.fetch(
        """
        SELECT trader_address FROM tracks
        UNION
        SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL
        ORDER BY 1
        """
    )
    return [row["trader_address"] for row in rows]


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
    it would hand the withdrawal follow-up (#171) a months-old figure and an
    alert for money that moved while the wallet was off the poll set."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            DELETE FROM trader_equity
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
              AND trader_address NOT IN
                  (SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL)
            """
        )
        await conn.execute(
            """
            DELETE FROM position_snapshots
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
              AND trader_address NOT IN
                  (SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL)
            """
        )
        await conn.execute(
            """
            DELETE FROM position_poll_state
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
              AND trader_address NOT IN
                  (SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL)
            """
        )


async def _apply_poll(
    pool: asyncpg.Pool, address: str, state: AccountState, now: datetime
) -> int:
    """Diff one Trader's freshly fetched positions against the snapshots and
    commit snapshots + event rows + alert rows + this pass's equity observation
    atomically. Returns the event count."""
    positions = state.positions
    async with pool.acquire() as conn, conn.transaction():
        # The equity observation lands first and unconditionally (issue #170):
        # it is what the account was worth when Epigone looked, which is as true
        # on a Trader's baselining pass as on any other, and as true of a pass
        # that diffed no events as of one that did. Nothing here reads its
        # return value yet; the withdrawal follow-up (#171) computes its delta
        # from that previous observation, in this transaction, before this line
        # replaces it.
        await record_equity(conn, address, state.account_value, now)
        baselined = await conn.fetchval(
            "SELECT 1 FROM position_poll_state WHERE trader_address = $1", address
        )
        if not baselined:
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
            return 0

        previous = await read_snapshots(conn, POLL_SNAPSHOTS, address)
        changes = diff_positions(previous, positions, now)
        await apply_changes(conn, POLL_SNAPSHOTS, address, changes, now)
        events: list[PositionEvent] = events_of(changes)
        await conn.execute(
            "UPDATE position_poll_state SET last_polled_at = $2 WHERE trader_address = $1",
            address,
            now,
        )
        if events:
            # The durable record first (issue #156, ADR-0006), in this same
            # already-open transaction as the snapshot advances above and the
            # alert fan-out below — never a second transaction and never a
            # write outside one. That is what makes an interrupted pass leave
            # both or neither: the exactly-once property the alerts have always
            # had, inherited rather than reinvented. Both writes iterate the
            # same in-memory list, so the table and the alerts cannot diverge.
            await record_events(conn, address, events, now)
            await _queue_alerts(conn, address, events, now)
            # A close or flip mints a round-trip; bump the wallet due-now so the
            # fine pass folds it in within minutes and Recent trades / track
            # record match the alert by the time the user taps through (issue
            # #129). Opens/scales don't — nothing lands in fine_trades that
            # matters at alert-read time (the open shows via live positions).
            # One bump per pass regardless of how many coins closed; the
            # freshness guard makes any repeat a harmless no-op. Postgres-only
            # (ADR-0002): mark_due_now only rewrites the `traders` row, and the
            # fine pass ignores a wallet that is neither tracked nor coarse-
            # profitable (DUE_ELIGIBILITY), so a purely-linked wallet's close is
            # a no-op there — no wasted refresh for a wallet no one follows.
            if any(event.kind in ("close", "flip") for event in events):
                await mark_due_now(conn, address, now)
        return len(events)


async def _queue_alerts(
    conn: asyncpg.Connection, address: str, events: list[PositionEvent], now: datetime
) -> None:
    """Fan out each event to this Trader's followers, honouring each Track's
    alert controls (issue #10): a muted Track gets nothing, and an effective
    min-size floor (per-Track override, else the User's global floor) drops
    events for positions smaller than it. Filtering here — at queue time —
    means a suppressed event is never stored, so unmuting never backfills."""
    followers = await conn.fetch(
        """
        SELECT t.user_telegram_id, t.muted,
               coalesce(t.min_size_usd, u.min_size_usd) AS min_size
        FROM tracks t
        JOIN users u ON u.telegram_id = t.user_telegram_id
        WHERE t.trader_address = $1
        """,
        address,
    )
    rows = [
        (
            follower["user_telegram_id"],
            address,
            event.kind,
            event.coin,
            event.side,
            event.size_usd,
            event.prev_size_usd,
            event.leverage,
            event.entry_price,
            event.prev_side,
            event.realized_pnl,
            event.pct_return,
            event.opened_at,
            now,
        )
        for follower in followers
        if not follower["muted"]
        for event in events
        if not _below_floor(event, follower["min_size"])
    ]
    await conn.executemany(
        """
        INSERT INTO position_alerts
            (user_telegram_id, trader_address, kind, coin, side, size_usd, prev_size_usd,
             leverage, entry_price, prev_side, realized_pnl, pct_return, opened_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """,
        rows,
    )


def _below_floor(event: PositionEvent, floor: Decimal | None) -> bool:
    """Whether a min-size floor suppresses this event. A floor judges every
    alert kind by the position notional it carries (event.size_usd); an event
    with no notional (should not happen) is never suppressed."""
    return floor is not None and event.size_usd is not None and event.size_usd < floor
