"""The tracked-wallet poll pass: position diffing for Position Alerts (issue #4).

Each pass polls every distinct tracked Trader (deduped across Users) via
clearinghouseState, diffs against the persisted snapshots, and queues alerts.
Every Trader is polled on two venues per pass — the core perps and the xyz
HIP-3 builder DEX (issue #21) — because most non-core activity (equity/"stock"
perps like `xyz:META`) lives on xyz. The two position lists merge before
diffing; xyz coins are namespaced (`xyz:META`) so the (trader, coin) snapshot
key tracks the venues independently, with no schema change and no false
OPEN/CLOSE from mixing them.

Diff semantics (tested in tests/test_position_poller.py):

- **Baseline.** A Trader's first-ever poll records snapshots and emits nothing:
  positions that existed before anyone could have seen an open are not news.
- **OPEN** — a coin appears that the last snapshot didn't have.
- **CLOSE** — a snapshot coin disappears. Realized PnL is approximated by the
  last observed unrealized PnL (up to one poll stale): the exact figure would
  cost a weight-20 userFills call against clearinghouseState's weight 2.
  pct_return is return on margin (PnL over positionValue/leverage at the last
  observation); holding time runs from when the poller first saw the position.
- **FLIP** — the same coin changes side: one alert carrying the closed leg
  (as CLOSE) and the new leg (as OPEN). The snapshot restarts at flip time.
- **SCALE-IN / SCALE-OUT** (issue #10) — same coin, same side, notional size
  changed by at least SCALE_SIGNIFICANCE_THRESHOLD of the last snapshot: a
  whale adding to (scale-in) or trimming (scale-out) an existing position.
  One alert carrying the resized leg and the size it grew/shrank from.
- **Silent update** — same coin, same side, size change *below* the threshold:
  ordinary drift, partial closes, and entry/leverage changes update the
  snapshot (preserving opened_at) without alerting, exactly as before #10.

Every queued event is filtered per follower against that Track's alert
controls (issue #10): a muted Track receives nothing, and an effective minimum
position size (per-Track override, else the User's global floor) drops alerts
for positions notionally smaller than the floor. Suppression happens at queue
time, never at delivery, so unmuting or raising a floor never dumps a backlog.
- **Re-follow.** When a Trader loses their last follower, the pass prunes
  their snapshots and poll state; following them again re-baselines silently
  instead of diffing against a stale snapshot and alerting on ancient changes.

Snapshot updates and alert-row inserts share one transaction per Trader, so an
event is detected exactly once: after a stream restart the snapshots already
reflect everything alerted, and anything not yet committed diffs again.
One alert row is queued per event per follower; the bot process owns delivery
(ADR-0002: processes meet only in Postgres).
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
    GatewayError,
    HyperliquidGateway,
    Position,
    RateLimitedError,
    fetch_open_positions,
)

log = logging.getLogger(__name__)

# The stream spends against the shared 900/min budget (epigone.budget, issue
# #28) with priority over ingest. Each tracked wallet costs two
# clearinghouseState calls per poll — core plus the xyz builder DEX (issue #21)
# — so weight 4 per wallet per 30s poll, giving ~110 distinct tracked wallets
# before pacing stretches the interval even with ingest fully idle.
POSITIONS_WEIGHT = 2  # clearinghouseState, per call — a wallet spends this once per DEX

# A same-side size change alerts as SCALE-IN/SCALE-OUT (issue #10) only once it
# reaches this fraction of the last snapshot's notional size; anything smaller
# stays a silent update. Conservative by design — a 25% swing is a deliberate
# add or trim, not the incidental notional drift of a mark-price move (over a
# 30s poll a real coin never moves 25%). Tune here to retune the signal.
SCALE_SIGNIFICANCE_THRESHOLD = Decimal("0.25")

POLL_INTERVAL_SECONDS = 30

# Same reasoning as the ingest passes: a sustained streak means Hyperliquid is
# down, not that wallets are odd — stop burning budget and resume next cycle.
MAX_CONSECUTIVE_FAILURES = 5


@dataclass(frozen=True)
class PollResult:
    polled: int
    failed: int
    events: int
    aborted: bool


@dataclass(frozen=True)
class _Event:
    kind: str  # 'open' | 'close' | 'flip' | 'scale_in' | 'scale_out'
    coin: str
    side: str | None = None  # new leg
    size_usd: Decimal | None = None  # the position notional the alert is about
    prev_size_usd: Decimal | None = None  # size before a scale
    leverage: Decimal | None = None
    entry_price: Decimal | None = None
    prev_side: str | None = None  # closed leg
    realized_pnl: Decimal | None = None
    pct_return: Decimal | None = None
    opened_at: datetime | None = None


async def run_poll_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> PollResult:
    """Two clearinghouseState calls per distinct tracked Trader — core and the
    xyz builder DEX (issue #21) — merged before diffing, paced by the budget."""
    await _prune_untracked(pool)
    rows = await pool.fetch("SELECT DISTINCT trader_address FROM tracks ORDER BY trader_address")
    polled = failed = events = consecutive_failures = 0
    for row in rows:
        address: str = row["trader_address"]
        try:
            positions = await _fetch_positions(gateway, budget, address)
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
        events += await _apply_poll(pool, address, positions, clock.now())
        polled += 1
    if events or failed:
        log.info("poll pass done: %d polled, %d events, %d failed", polled, events, failed)
    return PollResult(polled=polled, failed=failed, events=events, aborted=False)


async def _fetch_positions(
    gateway: HyperliquidGateway, budget: Budget, address: str
) -> list[Position]:
    """A Trader's open positions across the venues we cover (POSITION_VENUES:
    core plus the xyz builder DEX), paced by the budget: each clearinghouseState
    call costs POSITIONS_WEIGHT, so a wallet reserves every venue's weight before
    polling. Billing one spend per venue off POSITION_VENUES keeps the accounting
    in lockstep with the calls the shared fetch actually makes.

    The shared fetch (epigone.gateway.fetch_open_positions) merges the venues
    and raises on a partial fetch; here that means the whole wallet is retried
    next pass with its snapshots untouched, never diffed against a half-empty
    list into false CLOSE alerts (issue #21)."""
    for _venue in POSITION_VENUES:
        await budget.spend(POSITIONS_WEIGHT)
    return await fetch_open_positions(gateway, address)


async def _prune_untracked(pool: asyncpg.Pool) -> None:
    """Drop poll bookkeeping for Traders nobody follows (the re-follow rule in
    the module docstring). Queued alerts are untouched — they were real when
    detected and still owe delivery."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            DELETE FROM position_snapshots
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
            """
        )
        await conn.execute(
            """
            DELETE FROM position_poll_state
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
            """
        )


async def _apply_poll(
    pool: asyncpg.Pool, address: str, positions: list[Position], now: datetime
) -> int:
    """Diff one Trader's freshly fetched positions against the snapshots and
    commit snapshots + alert rows atomically. Returns the event count."""
    current = {p.coin: p for p in positions}
    async with pool.acquire() as conn, conn.transaction():
        baselined = await conn.fetchval(
            "SELECT 1 FROM position_poll_state WHERE trader_address = $1", address
        )
        if not baselined:
            for pos in positions:
                await _insert_snapshot(conn, address, pos, now)
            await conn.execute(
                """
                INSERT INTO position_poll_state (trader_address, baselined_at, last_polled_at)
                VALUES ($1, $2, $2)
                """,
                address,
                now,
            )
            return 0

        previous = {
            r["coin"]: r
            for r in await conn.fetch(
                "SELECT * FROM position_snapshots WHERE trader_address = $1", address
            )
        }
        events: list[_Event] = []
        for coin, snapshot in previous.items():
            if coin not in current:
                events.append(_close_event(snapshot))
                await conn.execute(
                    "DELETE FROM position_snapshots WHERE trader_address = $1 AND coin = $2",
                    address,
                    coin,
                )
        for coin, pos in current.items():
            snapshot = previous.get(coin)
            if snapshot is None:
                events.append(_open_event(pos))
                await _insert_snapshot(conn, address, pos, now)
            elif snapshot["side"] != pos.side.value:
                events.append(_flip_event(snapshot, pos))
                await _replace_snapshot(conn, address, pos, opened_at=now, updated_at=now)
            else:
                # Same coin, same side: a significant size change scales in/out
                # (issue #10); smaller drift stays a silent snapshot update.
                scale = _scale_event(snapshot, pos)
                if scale is not None:
                    events.append(scale)
                await _replace_snapshot(
                    conn, address, pos, opened_at=snapshot["opened_at"], updated_at=now
                )
        await conn.execute(
            "UPDATE position_poll_state SET last_polled_at = $2 WHERE trader_address = $1",
            address,
            now,
        )
        if events:
            await _queue_alerts(conn, address, events, now)
        return len(events)


def _open_event(pos: Position) -> _Event:
    return _Event(
        kind="open",
        coin=pos.coin,
        side=pos.side.value,
        size_usd=pos.size_usd,
        leverage=pos.leverage,
        entry_price=pos.entry_price,
    )


def _close_event(snapshot: asyncpg.Record) -> _Event:
    return _Event(
        kind="close",
        coin=snapshot["coin"],
        # The closed position's last notional, so a min-size floor (issue #10)
        # judges a close by the position it closed, not a null.
        size_usd=snapshot["size_usd"],
        prev_side=snapshot["side"],
        realized_pnl=snapshot["unrealized_pnl"],
        pct_return=_return_on_margin(snapshot),
        opened_at=snapshot["opened_at"],
    )


def _scale_event(snapshot: asyncpg.Record, pos: Position) -> _Event | None:
    """A same-coin/same-side size change worth an alert, or None if it is below
    SCALE_SIGNIFICANCE_THRESHOLD (ordinary drift — keep today's silent update).

    Change is measured against the last snapshot's notional, so gradual drift
    that never clears the threshold in one poll stays quiet by design."""
    old: Decimal = snapshot["size_usd"]
    new = pos.size_usd
    if old <= 0:
        return None
    if abs(new - old) / old < SCALE_SIGNIFICANCE_THRESHOLD:
        return None
    return _Event(
        kind="scale_in" if new > old else "scale_out",
        coin=pos.coin,
        side=pos.side.value,
        size_usd=new,
        prev_size_usd=old,
        leverage=pos.leverage,
        entry_price=pos.entry_price,
        # The position's live return on margin (issue #35), so the alert can say
        # whether the trade is winning — more useful than the size-growth %.
        pct_return=pos.return_on_margin,
        opened_at=snapshot["opened_at"],
    )


def _flip_event(snapshot: asyncpg.Record, pos: Position) -> _Event:
    closed = _close_event(snapshot)
    opened = _open_event(pos)
    return _Event(
        kind="flip",
        coin=pos.coin,
        side=opened.side,
        size_usd=opened.size_usd,
        leverage=opened.leverage,
        entry_price=opened.entry_price,
        prev_side=closed.prev_side,
        realized_pnl=closed.realized_pnl,
        pct_return=closed.pct_return,
        opened_at=closed.opened_at,
    )


def _return_on_margin(snapshot: asyncpg.Record) -> Decimal | None:
    margin: Decimal = snapshot["size_usd"] / snapshot["leverage"]
    if margin == 0:
        return None
    pnl: Decimal = snapshot["unrealized_pnl"]
    return pnl / margin


async def _insert_snapshot(
    conn: asyncpg.Connection, address: str, pos: Position, now: datetime
) -> None:
    await _replace_snapshot(conn, address, pos, opened_at=now, updated_at=now)


async def _replace_snapshot(
    conn: asyncpg.Connection,
    address: str,
    pos: Position,
    *,
    opened_at: datetime,
    updated_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO position_snapshots
            (trader_address, coin, side, size_usd, leverage, entry_price,
             unrealized_pnl, opened_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (trader_address, coin) DO UPDATE
            SET side = EXCLUDED.side,
                size_usd = EXCLUDED.size_usd,
                leverage = EXCLUDED.leverage,
                entry_price = EXCLUDED.entry_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                opened_at = EXCLUDED.opened_at,
                updated_at = EXCLUDED.updated_at
        """,
        address,
        pos.coin,
        pos.side.value,
        pos.size_usd,
        pos.leverage,
        pos.entry_price,
        pos.unrealized_pnl,
        opened_at,
        updated_at,
    )


async def _queue_alerts(
    conn: asyncpg.Connection, address: str, events: list[_Event], now: datetime
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


def _below_floor(event: _Event, floor: Decimal | None) -> bool:
    """Whether a min-size floor suppresses this event. A floor judges every
    alert kind by the position notional it carries (event.size_usd); an event
    with no notional (should not happen) is never suppressed."""
    return floor is not None and event.size_usd is not None and event.size_usd < floor
