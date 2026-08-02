"""What a Trader did, derived from two observations of their positions.

This is the poll pass's diff (issue #4, #10), lifted out of it verbatim so a
SECOND producer can reach it (issue #157). The websocket shadow lane writes
`position_events` beside the poller's, and the whole point of that dataset is
comparing the two; if each lane carried its own copy of "what counts as a
scale-in", the comparison would measure the copies drifting rather than the
transports differing. So the semantics live here, once, and both lanes call in.

The rules, unchanged and now stated in one place:

- **Baseline.** A Trader's first observation emits nothing. That rule is the
  CALLER's — it owns the persisted "have I ever seen this Trader" flag — but it
  is the same rule on both lanes: positions that existed before anyone could
  have watched them open are not news.
- **OPEN** — a coin the last observation didn't have.
- **CLOSE** — a coin the last observation had and this one doesn't. Realized
  PnL is approximated by the last observed unrealized PnL, and the closed
  position's last notional and coin size ride along, because by the time a lane
  sees the close there is nothing live left to read them from.
- **FLIP** — same coin, opposite side: ONE event carrying both legs, never a
  close followed by an open. The position's clock restarts at flip time.
- **SCALE-IN / SCALE-OUT** — same coin, same side, notional changed by at least
  SCALE_SIGNIFICANCE_THRESHOLD of the last observation.
- **Silent update** — same coin, same side, a smaller size change: drift, a
  small partial close, an entry/leverage change. Memory advances, nothing is
  emitted.

`diff_positions` is pure: two observations in, a decision per coin out. It
never touches a database, because the two lanes keep their memory in DIFFERENT
tables — the poller in `position_snapshots`, the shadow lane in
`ws_position_snapshots` — and must, since one producer advancing the other's
memory would destroy the other's diff. What they share is the reasoning, not
the storage.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.gateway import Position
from epigone.position_events import PositionEvent

# A same-side size change alerts as SCALE-IN/SCALE-OUT (issue #10) only once it
# reaches this fraction of the last observation's notional size; anything
# smaller stays a silent update. Conservative by design — a 25% swing is a
# deliberate add or trim, not the incidental notional drift of a mark-price move
# (over a 10s poll a real coin never moves 25%). Tune here to retune the signal
# on BOTH lanes at once, which is the reason it lives here.
SCALE_SIGNIFICANCE_THRESHOLD = Decimal("0.25")


@dataclass(frozen=True)
class SnapshotState:
    """One coin's last observed position — a lane's memory of it.

    The columns `position_snapshots` and `ws_position_snapshots` share, as a
    value rather than an `asyncpg.Record`, so the diff can be exercised without
    a database and neither lane's table shape leaks into the semantics.

    `size_coin` is nullable for the same reason it is on the row: a snapshot
    written before migration 0028 never observed coin units, and a CLOSE built
    from it carries None rather than a notional-derived guess (issue #155)."""

    coin: str
    side: str
    size_usd: Decimal
    leverage: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal
    opened_at: datetime
    size_coin: Decimal | None = None

    @classmethod
    def from_row(cls, row: asyncpg.Record) -> "SnapshotState":
        """Read one snapshot row. Both lanes' tables carry these columns under
        these names deliberately — the shadow table is a copy of the poller's
        shape so this one reader serves both."""
        return cls(
            coin=row["coin"],
            side=row["side"],
            size_usd=row["size_usd"],
            leverage=row["leverage"],
            entry_price=row["entry_price"],
            unrealized_pnl=row["unrealized_pnl"],
            opened_at=row["opened_at"],
            size_coin=row["size_coin"],
        )


@dataclass(frozen=True)
class CoinChange:
    """The decision about one coin: what to emit, and what to remember.

    `event` is None for a silent update — memory still advances. `position` is
    None for a CLOSE — the coin's memory is dropped entirely. `opened_at` is
    the timestamp the remembered position carries: `now` for an open or a flip
    (the position's clock restarts), the previously remembered one for a scale
    or a silent update (holding time is continuous through a resize)."""

    coin: str
    event: PositionEvent | None
    position: Position | None
    opened_at: datetime | None


def diff_positions(
    previous: Mapping[str, SnapshotState],
    positions: Sequence[Position],
    now: datetime,
) -> list[CoinChange]:
    """What changed between a lane's remembered positions and a fresh
    observation of them, one decision per affected coin.

    Closes come first, then the coins present now, which fixes the order events
    are written in and therefore their `id` order. That ordering is part of the
    contract both lanes meet, not an accident of iteration: ADR-0006 guarantees
    a total order per (Trader, coin), and a comparison of the two lanes reads
    much better when a shared change produces a matching sequence.

    Callers pass an observation they trust to be COMPLETE for the venues they
    cover — a partial fetch read as "everything else closed" is the false-CLOSE
    hazard both lanes guard at their own edge (the REST fetch raises on a
    partial venue; the websocket's all-dex message carries every venue at once).
    """
    current = {position.coin: position for position in positions}
    changes: list[CoinChange] = []
    for coin, snapshot in previous.items():
        if coin not in current:
            changes.append(
                CoinChange(coin=coin, event=close_event(snapshot), position=None, opened_at=None)
            )
    for coin, position in current.items():
        remembered = previous.get(coin)
        if remembered is None:
            changes.append(
                CoinChange(
                    coin=coin, event=open_event(position), position=position, opened_at=now
                )
            )
        elif remembered.side != position.side.value:
            changes.append(
                CoinChange(
                    coin=coin,
                    event=flip_event(remembered, position),
                    position=position,
                    opened_at=now,
                )
            )
        else:
            # Same coin, same side: a significant size change scales in/out
            # (issue #10); smaller drift stays a silent update, which still
            # advances memory — and keeps the position's original opened_at,
            # so holding time survives a resize.
            changes.append(
                CoinChange(
                    coin=coin,
                    event=scale_event(remembered, position),
                    position=position,
                    opened_at=remembered.opened_at,
                )
            )
    return changes


def events_of(changes: Sequence[CoinChange]) -> list[PositionEvent]:
    """Just the news, in decision order — silent updates dropped."""
    return [change.event for change in changes if change.event is not None]


def open_event(position: Position) -> PositionEvent:
    return PositionEvent(
        kind="open",
        coin=position.coin,
        side=position.side.value,
        size_usd=position.size_usd,
        size_coin=position.size_coin,
        leverage=position.leverage,
        entry_price=position.entry_price,
    )


def close_event(snapshot: SnapshotState) -> PositionEvent:
    return PositionEvent(
        kind="close",
        coin=snapshot.coin,
        # The closed position's last notional, so a min-size floor (issue #10)
        # judges a close by the position it closed, not a null. Its coin units
        # travel the same way (#155, ADR-0006) and can only come from here — by
        # the time a lane sees the close, a live read shows nothing at all.
        size_usd=snapshot.size_usd,
        size_coin=snapshot.size_coin,
        prev_side=snapshot.side,
        realized_pnl=snapshot.unrealized_pnl,
        pct_return=_return_on_margin(snapshot),
        opened_at=snapshot.opened_at,
    )


def scale_event(snapshot: SnapshotState, position: Position) -> PositionEvent | None:
    """A same-coin/same-side size change worth an event, or None if it is below
    SCALE_SIGNIFICANCE_THRESHOLD (ordinary drift — a silent update).

    Change is measured against the last observation's notional, so gradual
    drift that never clears the threshold in one step stays quiet by design."""
    old = snapshot.size_usd
    new = position.size_usd
    if old <= 0:
        return None
    if abs(new - old) / old < SCALE_SIGNIFICANCE_THRESHOLD:
        return None
    return PositionEvent(
        kind="scale_in" if new > old else "scale_out",
        coin=position.coin,
        side=position.side.value,
        size_usd=new,
        size_coin=position.size_coin,
        prev_size_usd=old,
        prev_size_coin=snapshot.size_coin,
        leverage=position.leverage,
        entry_price=position.entry_price,
        # The position's live return on margin (issue #35), so the alert can say
        # whether the trade is winning — more useful than the size-growth %.
        pct_return=position.return_on_margin,
        opened_at=snapshot.opened_at,
    )


def flip_event(snapshot: SnapshotState, position: Position) -> PositionEvent:
    """One event carrying both legs (ADR-0006): never a close then an open."""
    closed = close_event(snapshot)
    opened = open_event(position)
    return PositionEvent(
        kind="flip",
        coin=position.coin,
        side=opened.side,
        size_usd=opened.size_usd,
        size_coin=opened.size_coin,
        leverage=opened.leverage,
        entry_price=opened.entry_price,
        prev_side=closed.prev_side,
        realized_pnl=closed.realized_pnl,
        pct_return=closed.pct_return,
        opened_at=closed.opened_at,
    )


def _return_on_margin(snapshot: SnapshotState) -> Decimal | None:
    margin = snapshot.size_usd / snapshot.leverage
    if margin == 0:
        return None
    return snapshot.unrealized_pnl / margin
