"""What a Trader did to their resting orders, from what the lane last believed.

The order lane learns about orders two ways, and they are not equivalent — which
is the whole reason this module exists rather than one diff function:

- **A stream update** is the exchange's own account of a transition. It says
  what happened (`status`), when (`statusTimestamp`), and to which order
  (`oid`). Nothing has to be inferred.
- **A resync** is a point-in-time read of the whole resting book, diffed against
  memory. It can see THAT something changed while the lane was disconnected, and
  usually not what: an order that is simply gone might have filled or been
  cancelled, and absolute state cannot tell the two apart.

So the resync's vocabulary is deliberately weaker than the stream's, and
`origin` on every event records which one produced it. A consumer deciding how
far to trust a row reads that column.

Kept pure — observations in, decisions out, no database — for `position_diff`'s
reason: the semantics are worth testing without Postgres, and the caller owns
the memory these decisions are applied to.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.gateway import OpenOrder
from epigone.order_events import (
    CANCELED,
    FILLED,
    GONE,
    OTHER,
    PLACED,
    REJECTED,
    RESYNC_ORIGIN,
    STREAM_ORIGIN,
    OrderEvent,
    classify,
)
from epigone.ws import OrderUpdate

# Kinds after which an order is no longer on the book. A partial fill is the
# exception that shapes the rule: `filled` with size left did not finish the
# order, so it stays remembered at its new remaining size — which is also the
# only way the next resync can tell a further partial fill from no change at
# all. Cancels and rejections carry a leftover size too (observed live), but it
# means the opposite: that is what was NOT filled when the order ended.
_TERMINAL_KINDS = frozenset({FILLED, CANCELED, REJECTED})


@dataclass(frozen=True)
class RestingOrder:
    """One order the lane believes is on the book right now — its memory.

    Richer than `order_snapshots` (the REST poller's id set) because a resync
    must be able to emit a complete event for an order that has ALREADY left the
    book: by then there is nothing live left to read its coin, side, price or
    size from. Memory is the only source, exactly as a position CLOSE event
    carries the closed position's last known notional.

    The trigger fields are None for an order the lane only ever saw over the
    stream, which does not carry them."""

    order_id: int
    coin: str
    is_buy: bool
    limit_price: Decimal
    size: Decimal
    original_size: Decimal
    placed_at: datetime
    cloid: str | None = None
    order_type: str | None = None
    is_trigger: bool | None = None
    trigger_price: Decimal | None = None
    reduce_only: bool | None = None
    is_position_tpsl: bool | None = None

    @classmethod
    def from_row(cls, row: asyncpg.Record) -> "RestingOrder":
        return cls(
            order_id=row["order_id"],
            coin=row["coin"],
            is_buy=row["is_buy"],
            limit_price=row["limit_price"],
            size=row["size"],
            original_size=row["original_size"],
            placed_at=row["placed_at"],
            cloid=row["cloid"],
            order_type=row["order_type"],
            is_trigger=row["is_trigger"],
            trigger_price=row["trigger_price"],
            reduce_only=row["reduce_only"],
            is_position_tpsl=row["is_position_tpsl"],
        )

    @classmethod
    def from_open_order(cls, order: OpenOrder) -> "RestingOrder":
        """A REST observation, which knows everything about the order — this is
        the only path that can populate the trigger fields."""
        return cls(
            order_id=order.order_id,
            coin=order.coin,
            is_buy=order.is_buy,
            limit_price=order.limit_price,
            size=order.size,
            # frontendOpenOrders reports what REMAINS; it does not report what
            # the order started as. Seeding original_size from the remaining
            # size would claim a partial fill had not happened, so the two are
            # equal here by construction and a later shrink is still visible.
            original_size=order.size,
            placed_at=order.placed_at,
            order_type=order.order_type,
            is_trigger=order.is_trigger,
            trigger_price=order.trigger_price,
            reduce_only=order.reduce_only,
            is_position_tpsl=order.is_position_tpsl,
        )

    @classmethod
    def from_update(cls, update: OrderUpdate) -> "RestingOrder":
        return cls(
            order_id=update.order_id,
            coin=update.coin,
            is_buy=update.is_buy,
            limit_price=update.limit_price,
            size=update.size,
            original_size=update.original_size,
            placed_at=update.placed_at,
            cloid=update.cloid,
        )


@dataclass(frozen=True)
class OrderChange:
    """The decision about one order: what to emit, and what to remember.

    `event` is None when nothing newsworthy happened (an order still resting,
    unchanged). `resting` is None when the order should be forgotten — it has
    left the book. The two are independent: a partial fill both emits an event
    and keeps the order remembered."""

    order_id: int
    event: OrderEvent | None
    resting: RestingOrder | None


def diff_open_orders(
    previous: Mapping[int, RestingOrder], orders: Sequence[OpenOrder]
) -> list[OrderChange]:
    """A point-in-time resting book against what the lane remembered.

    Departures come first, then the orders present now, so the events a resync
    writes land in a stable order — a cancel before the replacement placement
    reads correctly, and the reverse would not.

    The three cases, and the reasoning each rests on:

    - **Resting now, not remembered** → `placed`. It appeared during the gap.
      The moment is unknown, which is what `origin = resync` says.
    - **Remembered, not resting now** → `gone`. Filled or cancelled, and
      absolute state cannot say which. Naming the ambiguity beats guessing, and
      beats the silence a reconnect must never produce.
    - **Remembered, still resting, SMALLER** → `filled`, carrying the new
      remaining size. This is the one inference safe to make: a resting order's
      remaining size can only shrink by filling, because Hyperliquid's modify is
      an atomic cancel/replace that mints a new oid (verified live, #115). A
      size that GREW cannot happen; if it ever does it is left as a silent
      memory update rather than reported as a fill that did not occur.

    The caller passes a book it trusts to be COMPLETE for the venues it covers.
    `fetch_open_orders` raises on any venue's failure for exactly that reason: a
    partial read would report a whole venue's ladder as `gone`."""
    current = {order.order_id: order for order in orders}
    changes: list[OrderChange] = []
    for order_id, remembered in previous.items():
        if order_id not in current:
            changes.append(
                OrderChange(order_id=order_id, event=_resync_event(GONE, remembered), resting=None)
            )
    for order_id, order in current.items():
        known = previous.get(order_id)
        fresh = RestingOrder.from_open_order(order)
        if known is not None:
            # `frontendOpenOrders` reports what REMAINS and never what the order
            # started as, so `from_open_order` can only guess original == remaining.
            # For an order we already know, memory holds the real figure — and it
            # must survive EVERY resync, not just the one that observes a shrink.
            # Letting the guess win would quietly rewrite a 10-lot order that has
            # 7 left as a 7-lot order, and every later event about it, including
            # its `gone`, would then understate what the Trader had actually
            # committed.
            fresh = replace(fresh, original_size=known.original_size)
        if known is None:
            changes.append(
                OrderChange(order_id=order_id, event=_resync_event(PLACED, fresh), resting=fresh)
            )
        elif order.size < known.size:
            changes.append(
                OrderChange(order_id=order_id, event=_resync_event(FILLED, fresh), resting=fresh)
            )
        else:
            changes.append(OrderChange(order_id=order_id, event=None, resting=fresh))
    return changes


def stream_change(remembered: RestingOrder | None, update: OrderUpdate) -> OrderChange:
    """One streamed update, classified and applied to memory.

    Every update is an event: unlike a position push, which is absolute state
    that often says nothing changed, an `orderUpdates` entry IS a transition and
    the exchange only sends it because something happened.

    What memory does depends on the kind. A placement or a trigger leaves the
    order on the book; a cancel, a rejection or a complete fill takes it off; a
    PARTIAL fill leaves it on at its new remaining size. An unrecognised status
    (`other`) deliberately leaves memory ALONE — the lane does not know what
    happened, so guessing that the order left would make the next resync report
    a phantom `placed`, and guessing that it stayed costs nothing worse than one
    `gone` later.

    An update for an order the lane never knew still produces an event — it
    happened, and dropping it would be a silent gap — but does not resurrect
    memory for an order that is already finished."""
    kind = classify(update.status)
    fresh = RestingOrder.from_update(update)
    event = OrderEvent(
        kind=kind,
        order_id=update.order_id,
        coin=update.coin,
        is_buy=update.is_buy,
        origin=STREAM_ORIGIN,
        status=update.status,
        limit_price=update.limit_price,
        size=update.size,
        original_size=update.original_size,
        cloid=update.cloid,
        placed_at=update.placed_at,
        status_at=update.status_at,
    )
    if kind == FILLED and update.size > 0:
        # A fill that left something behind: still on the book, smaller.
        # Forgetting it here would make the next resync report the order as a
        # phantom `placed`, since it is still resting as far as the exchange is
        # concerned.
        resting = fresh
    elif kind in _TERMINAL_KINDS:
        # Off the book. A cancel or rejection reports the size that was still
        # UNFILLED when it ended — observed live, `canceled` carries a non-zero
        # `sz` — so a leftover size is the normal shape here and must not be
        # read as "partially done, still resting" the way it is for a fill.
        resting = None
    elif kind == OTHER:
        resting = remembered
    else:
        resting = fresh
    return OrderChange(order_id=update.order_id, event=event, resting=resting)


def events_of(changes: Sequence[OrderChange]) -> list[OrderEvent]:
    """Just the news, in decision order."""
    return [change.event for change in changes if change.event is not None]


def _resync_event(kind: str, order: RestingOrder) -> OrderEvent:
    """A resync-derived event carries no `status` — no status ever described it,
    and inventing one would let a consumer believe the exchange had spoken. It
    carries no `status_at` either: the moment the change happened is precisely
    what a gap does not record."""
    return OrderEvent(
        kind=kind,
        order_id=order.order_id,
        coin=order.coin,
        is_buy=order.is_buy,
        origin=RESYNC_ORIGIN,
        limit_price=order.limit_price,
        size=order.size,
        original_size=order.original_size,
        cloid=order.cloid,
        order_type=order.order_type,
        is_trigger=order.is_trigger,
        trigger_price=order.trigger_price,
        reduce_only=order.reduce_only,
        is_position_tpsl=order.is_position_tpsl,
        placed_at=order.placed_at,
    )
