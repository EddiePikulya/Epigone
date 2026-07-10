"""The tracked-wallet poll pass: position diffing for Position Alerts (issue #4).

Each pass polls every distinct tracked Trader once (deduped across Users) via
clearinghouseState, diffs against the persisted snapshots, and queues alerts.

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
- **Silent update** — same coin, same side: partial closes, adds, and
  entry/leverage drift update the snapshot (preserving opened_at) without
  alerting.
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

from epigone.budget import WeightBudget
from epigone.clock import Clock
from epigone.gateway import GatewayError, HyperliquidGateway, Position

log = logging.getLogger(__name__)

# The stream owns what ingest's 400/min share leaves of the 1200/min per-IP
# budget. At weight 2 per wallet per 30s poll that is ~200 distinct tracked
# wallets before pacing stretches the interval (spec-defaults).
STREAM_WEIGHT_PER_MINUTE = 800
POSITIONS_WEIGHT = 2  # clearinghouseState

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
    kind: str  # 'open' | 'close' | 'flip'
    coin: str
    side: str | None = None  # new leg
    size_usd: Decimal | None = None
    leverage: Decimal | None = None
    entry_price: Decimal | None = None
    prev_side: str | None = None  # closed leg
    realized_pnl: Decimal | None = None
    pct_return: Decimal | None = None
    opened_at: datetime | None = None


async def run_poll_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: WeightBudget, clock: Clock
) -> PollResult:
    """One clearinghouseState call per distinct tracked Trader, paced by the budget."""
    await _prune_untracked(pool)
    rows = await pool.fetch("SELECT DISTINCT trader_address FROM tracks ORDER BY trader_address")
    polled = failed = events = consecutive_failures = 0
    for row in rows:
        address: str = row["trader_address"]
        await budget.spend(POSITIONS_WEIGHT)
        try:
            positions = await gateway.get_open_positions(address)
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
        prev_side=snapshot["side"],
        realized_pnl=snapshot["unrealized_pnl"],
        pct_return=_return_on_margin(snapshot),
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
    followers = await conn.fetch(
        "SELECT user_telegram_id FROM tracks WHERE trader_address = $1", address
    )
    await conn.executemany(
        """
        INSERT INTO position_alerts
            (user_telegram_id, trader_address, kind, coin, side, size_usd, leverage,
             entry_price, prev_side, realized_pnl, pct_return, opened_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """,
        [
            (
                follower["user_telegram_id"],
                address,
                event.kind,
                event.coin,
                event.side,
                event.size_usd,
                event.leverage,
                event.entry_price,
                event.prev_side,
                event.realized_pnl,
                event.pct_return,
                event.opened_at,
                now,
            )
            for event in events
            for follower in followers
        ],
    )
