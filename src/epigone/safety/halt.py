"""Execution-halt state: the kill switch's memory (issue #135, ADR-0005).

At most one ACTIVE halt exists (DB-enforced, migration 0024); requesting one
while halted returns the standing halt instead of stacking a second. A halt
means: the executor must not sign new orders (its loop checks is_halted —
the A4+ contract), and the watchdog owes the book a sweep — it cancels all
resting orders and stamps `swept_at` only once a fresh enumeration confirms
the book is empty (never on a cancel call's word alone: an ambiguous result
may have left live orders, the AmbiguousExecutionError contract).

Positions are NOT closed by a halt — the sweep applies the documented
unwind policy (docs/runbooks/halt-and-unwind.md) and records the position
snapshot on the halt row. Resuming requires the operator's explicit
confirmation (/resume in the bot) and does not re-place anything.

Every state change here writes its audit event in the SAME transaction as
the state row, so the trail and the state can never disagree.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

from epigone.clock import Clock
from epigone.safety.audit import OPERATOR_ACTOR, WATCHDOG_ACTOR, ExecutionAudit

KILL_SOURCE = "kill"
WATCHDOG_SOURCE = "watchdog"

# The v0 unwind policy (docs/runbooks/halt-and-unwind.md): hold positions,
# alert the operator with the snapshot. Recorded per halt so the trail says
# which policy version governed each incident.
HOLD_POLICY = "hold-and-alert"


class HaltContentionError(RuntimeError):
    """request_halt's bounded-retry backstop: halts kept appearing and
    resuming concurrently for every attempt. Pathological by construction —
    but /kill catches it and answers with a retry prompt, because a kill
    switch that crashes without a reply is the one unacceptable outcome."""


@dataclass(frozen=True)
class Halt:
    id: int
    halted_at: datetime
    source: str  # KILL_SOURCE | WATCHDOG_SOURCE
    reason: str
    requested_by: int | None  # Telegram id for /kill; None for the watchdog
    swept_at: datetime | None
    positions: list[dict[str, Any]] | None  # snapshot recorded at sweep time
    unwind_policy: str | None
    resumed_at: datetime | None
    resumed_by: int | None


async def request_halt(
    pool: asyncpg.Pool,
    clock: Clock,
    audit: ExecutionAudit,
    *,
    source: str,
    reason: str,
    requested_by: int | None = None,
) -> tuple[Halt, bool]:
    """Open a halt, or join the one already active. Returns (halt, created);
    created=False means a halt was already standing and NO new state or audit
    row was written — /kill during a watchdog halt is one incident, not two.

    Race-proof by retry (PR #143 review): a unique violation proves a halt
    was standing at insert time, but it can be RESUMED before the re-read —
    in that window neither "created" nor "joined" is true yet, so the loop
    simply tries again. A crashing kill switch is the one unacceptable
    outcome; the bound exists only to turn a pathological livelock into a
    loud, catchable error instead of a silent spin."""
    for _ in range(10):
        now = clock.now()  # per attempt: a retried halt starts when it lands
        async with pool.acquire() as conn:
            try:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        INSERT INTO execution_halts (halted_at, source, reason, requested_by)
                        VALUES ($1, $2, $3, $4)
                        RETURNING *
                        """,
                        now,
                        source,
                        reason,
                        requested_by,
                    )
                    assert row is not None
                    await audit.record_event(
                        actor=OPERATOR_ACTOR if source == KILL_SOURCE else WATCHDOG_ACTOR,
                        action="halt",
                        risk_decision=reason,
                        detail={"halt_id": row["id"], "source": source,
                                "requested_by": requested_by},
                        conn=conn,
                    )
                    return _halt(row), True
            except asyncpg.UniqueViolationError:
                pass  # a halt was standing at insert time; try to join it
        standing = await active_halt(pool)
        if standing is not None:
            return standing, False
        # The standing halt was resumed between the violation and the
        # re-read — loop and insert again.
    raise HaltContentionError(
        "request_halt could not settle: halts kept appearing and resuming "
        "concurrently across 10 attempts"
    )


async def active_halt(pool: asyncpg.Pool) -> Halt | None:
    row = await pool.fetchrow("SELECT * FROM execution_halts WHERE resumed_at IS NULL")
    return None if row is None else _halt(row)


async def is_halted(pool: asyncpg.Pool) -> bool:
    """The one-line gate the executor loop checks before signing anything
    (the A4+ contract; /kill halts within one loop through this check)."""
    halted: bool = await pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM execution_halts WHERE resumed_at IS NULL)"
    )
    return halted


async def mark_swept(
    pool: asyncpg.Pool,
    clock: Clock,
    audit: ExecutionAudit,
    *,
    halt: Halt,
    positions: list[dict[str, Any]],
    unwind_policy: str,
) -> None:
    """Stamp the sweep done: the book was ENUMERATED EMPTY after canceling —
    the caller's obligation, never inferred from cancel results — and the
    unwind policy was applied to the recorded position snapshot."""
    now = clock.now()
    async with pool.acquire() as conn:
        async with conn.transaction():
            updated = await conn.execute(
                """
                UPDATE execution_halts
                SET swept_at = $2, positions = $3::jsonb, unwind_policy = $4
                WHERE id = $1 AND swept_at IS NULL
                """,
                halt.id,
                now,
                json.dumps(positions, default=str),
                unwind_policy,
            )
            if updated == "UPDATE 0":
                return  # already stamped — a concurrent sweep won; nothing new to say
            await audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="halt_swept",
                risk_decision=f"halt #{halt.id}: book enumerated empty after cancel-all",
                detail={
                    "halt_id": halt.id,
                    "unwind_policy": unwind_policy,
                    "open_positions": len(positions),
                    "positions": positions,
                },
                conn=conn,
            )


async def resume(
    pool: asyncpg.Pool,
    clock: Clock,
    audit: ExecutionAudit,
    *,
    halt_id: int,
    resumed_by: int,
) -> Halt | None:
    """Close halt `halt_id` IF it is the standing one (the operator confirmed
    /resume for that specific halt). Returns the closed halt, or None when
    that halt is not the active one — a stale confirm tap must never lift a
    different, later halt (PR #143 review). Nothing is re-placed; if the
    executor heartbeat is still stale the watchdog will halt again within
    one cycle — resume is consent to trade, not an override of the switch."""
    now = clock.now()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE execution_halts SET resumed_at = $1, resumed_by = $2
                WHERE resumed_at IS NULL AND id = $3
                RETURNING *
                """,
                now,
                resumed_by,
                halt_id,
            )
            if row is None:
                return None
            await audit.record_event(
                actor=OPERATOR_ACTOR,
                action="resume",
                risk_decision=f"operator {resumed_by} confirmed /resume",
                detail={"halt_id": row["id"], "resumed_by": resumed_by},
                conn=conn,
            )
            return _halt(row)


def _halt(row: asyncpg.Record) -> Halt:
    positions = row["positions"]
    return Halt(
        id=row["id"],
        halted_at=row["halted_at"],
        source=row["source"],
        reason=row["reason"],
        requested_by=row["requested_by"],
        swept_at=row["swept_at"],
        positions=None if positions is None else json.loads(positions),
        unwind_policy=row["unwind_policy"],
        resumed_at=row["resumed_at"],
        resumed_by=row["resumed_by"],
    )
