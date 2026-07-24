"""The tracked-wallet order-poll pass: resting-order diffing for Order Alerts
(issue #115).

A resting ladder is a trader's *plan* before it executes, so the stream polls
every distinct tracked Trader's open orders (frontendOpenOrders across
POSITION_VENUES — per-dex exactly like clearinghouseState, verified live
2026-07-24) and alerts followers when NEW orders appear. It runs beside the
position poller in the stream process, on its own much slower cadence
(config.DEFAULT_ORDER_POLL_INTERVAL_SECONDS — resting orders live
minutes-to-days) and spends against a budget carrying the ingest-style stream
reserve, so order polling can never starve position polling (see
epigone.stream.main).

Diff semantics (tested in tests/test_order_poller.py):

- **Baseline.** A Trader's first-ever order poll records ids and emits
  nothing: a ladder that predates observation is not news. Same rule after a
  refollow — losing the last follower prunes the id set, so following again
  re-baselines instead of re-alerting the standing ladder.
- **New order** — an id the snapshot set doesn't have. All of a wallet's new
  orders in one cycle batch into ONE alert row per follower (#115's noise
  rule: active makers place constantly, never one message per order), in
  placement order.
- **Cancel / fill** — a known id disappears: pruned silently. Fills already
  alert as position events; cancels are not news.

Mute and min-size floors suppress at queue time, never at delivery (the #10
rule): a muted Track gets no row, and each order is judged by its own
notional — a whole-position TP/SL has none (OpenOrder.notional_usd is None)
and is never floor-suppressed. The alert row carries the batch as rendered
JSONB (numbers as strings, the criteria.filters Decimal round-trip
precedent): the id set is the only diff state, so the delivery side just
formats what the poll saw.

Snapshot updates and alert-row inserts share one transaction per Trader, so a
new order is alerted exactly once across stream restarts (the position
poller's transactional pattern)."""

import json
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
    OpenOrder,
    RateLimitedError,
    fetch_open_orders,
)

log = logging.getLogger(__name__)

# frontendOpenOrders' nominal weight, billed once per venue per wallet (the
# POSITION_VENUES lockstep rule, #31). 20 is the documented weight class for
# openOrders-style info requests; measured live 2026-07-24 the real figure is
# lower — bursting a full per-IP bucket allowed 168 calls before the first 429
# (⇒ ~8 weight/call, with a 900-call clearinghouseState control matching its
# documented weight 2, validating the method) — so billing 20 deliberately
# over-pays. Pacing built on it can only be gentler than the cap; revisit
# toward the measured figure if order-poll capacity ever gets tight.
ORDERS_WEIGHT = 20

# Same reasoning as the position poller: a sustained streak means Hyperliquid
# is down, not that wallets are odd — stop burning budget and resume next cycle.
MAX_CONSECUTIVE_FAILURES = 5


@dataclass(frozen=True)
class OrderPollResult:
    polled: int
    failed: int
    new_orders: int
    aborted: bool


async def run_order_poll_pass(
    pool: asyncpg.Pool, gateway: HyperliquidGateway, budget: Budget, clock: Clock
) -> OrderPollResult:
    """One frontendOpenOrders call per covered venue per distinct tracked
    Trader (per-dex, verified live 2026-07-24), diffed against the persisted
    known-order-id set, paced by the budget."""
    await _prune_untracked(pool)
    rows = await pool.fetch("SELECT DISTINCT trader_address FROM tracks ORDER BY trader_address")
    polled = failed = new_orders = consecutive_failures = 0
    for row in rows:
        address: str = row["trader_address"]
        try:
            orders = await _fetch_orders(gateway, budget, address)
        except RateLimitedError:
            # Pacing, not an outage (issue #28): the gateway already backed off
            # and retried; the wallet just polls again next pass. Never counts
            # toward the abort streak.
            log.warning("order poll: rate limited polling %s; retrying next pass", address)
            await record_rate_limit(pool, clock.now())
            failed += 1
            continue
        except GatewayError:
            log.warning("order poll: orders fetch failed for %s", address, exc_info=True)
            failed += 1
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                log.error(
                    "order poll pass aborted after %d consecutive failures; "
                    "%d polled so far, resuming next cycle",
                    consecutive_failures,
                    polled,
                )
                return OrderPollResult(
                    polled=polled, failed=failed, new_orders=new_orders, aborted=True
                )
            continue
        consecutive_failures = 0
        new_orders += await _apply_order_poll(pool, address, orders, clock.now())
        polled += 1
    if new_orders or failed:
        log.info(
            "order poll pass done: %d polled, %d new orders, %d failed",
            polled,
            new_orders,
            failed,
        )
    return OrderPollResult(polled=polled, failed=failed, new_orders=new_orders, aborted=False)


async def _fetch_orders(
    gateway: HyperliquidGateway, budget: Budget, address: str
) -> list[OpenOrder]:
    """A Trader's resting orders across POSITION_VENUES, paced by the budget:
    one ORDERS_WEIGHT spend per venue, billed off the same tuple the shared
    fetch iterates so the accounting never drifts from the calls made (#31).

    fetch_open_orders raises on any venue failure; the wallet then retries
    next pass with its id set untouched, never diffed against a half-empty
    book into a silent prune and a later ladder-wide re-alert."""
    for _venue in POSITION_VENUES:
        await budget.spend(ORDERS_WEIGHT)
    return await fetch_open_orders(gateway, address)


async def _prune_untracked(pool: asyncpg.Pool) -> None:
    """Drop order bookkeeping for Traders nobody follows, so a refollow
    re-baselines (module docstring). Queued alerts are untouched — they were
    real when detected and still owe delivery."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            """
            DELETE FROM order_snapshots
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
            """
        )
        await conn.execute(
            """
            DELETE FROM order_poll_state
            WHERE trader_address NOT IN (SELECT trader_address FROM tracks)
            """
        )


async def _apply_order_poll(
    pool: asyncpg.Pool, address: str, orders: list[OpenOrder], now: datetime
) -> int:
    """Diff one Trader's fresh resting-order set against the known ids and
    commit id changes + alert rows atomically. Returns the new-order count."""
    async with pool.acquire() as conn, conn.transaction():
        baselined = await conn.fetchval(
            "SELECT 1 FROM order_poll_state WHERE trader_address = $1", address
        )
        if not baselined:
            await _insert_ids(conn, address, orders, now)
            await conn.execute(
                """
                INSERT INTO order_poll_state (trader_address, baselined_at, last_polled_at)
                VALUES ($1, $2, $2)
                """,
                address,
                now,
            )
            return 0

        known = {
            r["order_id"]
            for r in await conn.fetch(
                "SELECT order_id FROM order_snapshots WHERE trader_address = $1", address
            )
        }
        current_ids = {o.order_id for o in orders}
        gone = known - current_ids
        if gone:
            # Cancels and fills: silent by design (fills already alert as
            # position events, a cancel is not news).
            await conn.execute(
                """
                DELETE FROM order_snapshots
                WHERE trader_address = $1 AND order_id = ANY($2::bigint[])
                """,
                address,
                list(gone),
            )
        new = sorted(
            (o for o in orders if o.order_id not in known),
            key=lambda o: (o.placed_at, o.order_id),
        )
        await _insert_ids(conn, address, new, now)
        await conn.execute(
            "UPDATE order_poll_state SET last_polled_at = $2 WHERE trader_address = $1",
            address,
            now,
        )
        if new:
            await _queue_alerts(conn, address, new, now)
        return len(new)


async def _insert_ids(
    conn: asyncpg.Connection, address: str, orders: list[OpenOrder], now: datetime
) -> None:
    await conn.executemany(
        """
        INSERT INTO order_snapshots (trader_address, order_id, first_seen_at)
        VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
        """,
        [(address, o.order_id, now) for o in orders],
    )


async def _queue_alerts(
    conn: asyncpg.Connection, address: str, new_orders: list[OpenOrder], now: datetime
) -> None:
    """One batched alert row per unmuted follower (#115's noise rule),
    honouring each Track's alert controls at queue time exactly like the
    position poller (#10): the effective min-size floor (per-Track override,
    else the User's global floor) drops each order below it, judged by the
    order's own notional; a follower whose whole batch fell below the floor
    gets no row at all, and a suppressed order is never stored — unmuting or
    lowering a floor never backfills."""
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
    rows = []
    for follower in followers:
        if follower["muted"]:
            continue
        visible = [o for o in new_orders if not _below_floor(o, follower["min_size"])]
        if not visible:
            continue
        rows.append(
            (
                follower["user_telegram_id"],
                address,
                json.dumps([o.to_wire() for o in visible]),
                now,
            )
        )
    await conn.executemany(
        """
        INSERT INTO order_alerts (user_telegram_id, trader_address, orders, created_at)
        VALUES ($1, $2, $3::jsonb, $4)
        """,
        rows,
    )


def _below_floor(order: OpenOrder, floor: Decimal | None) -> bool:
    """Whether a min-size floor suppresses this order. Judged by the order's
    notional; a whole-position TP/SL has none (notional_usd is None — its size
    is the position's at trigger time) and is never suppressed, matching the
    position poller's no-notional rule."""
    notional = order.notional_usd
    return floor is not None and notional is not None and notional < floor
