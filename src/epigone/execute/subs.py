"""The Leader→Copy Sub-account mapping (issue #136, ADR-0007 decisions 1, 12).

Tracking is not copying. A Track says "tell me what this wallet does"; a row
here says "mirror it with money". There is no path from one to the other
except an explicit operator command, and the default for every tracked wallet
is off.

Two states worth naming, because the executor treats them differently:

- **pending** (`sub_address is None`): /copy wrote the intent, nothing exists
  on the exchange yet. The bot process holds no signer (ADR-0005), so the
  execute process — which holds the executor lane's agent key — creates and
  funds the sub on its next loop. Until then the mapping produces no orders.
- **active**: the sub exists and is funded, and its events are copied for as
  long as `enabled` stays true.

`enabled` is read EVERY loop, never cached at startup: that is what makes
/copy and /uncopy take effect without a restart, and it is why a disabled
mapping's events simply never enter the backlog rather than being claimed and
skipped — claim-means-handled applies to events that qualified, not to events
that never did.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.execute.policy import FIXED_LEVERAGE, LEVERAGE_MODES

DEFAULT_MODE = "default"
BRACKET_MODE = "bracket"
COPY_MODES = (DEFAULT_MODE, BRACKET_MODE)


@dataclass(frozen=True)
class CopySub:
    """One Leader's Copy Sub-account as the executor reads it each loop."""

    id: int
    operator_id: int
    leader_address: str
    sub_name: str
    sub_address: str | None
    allocation_usd: Decimal
    # The Operator's own MARGIN behind each copied open, isolated per position
    # (ADR-0007 amendment D-4). The position it buys is this times the mirrored
    # leverage; this figure alone is the worst case.
    base_stake_usd: Decimal
    # 'mirror' (the Leader's own leverage on the position) or 'fixed'. Either
    # is an ASK — the global backstop and the asset's own maximum still cap it.
    leverage_mode: str
    fixed_leverage: int | None
    copy_mode: str
    take_profit_pct: Decimal | None
    stop_loss_pct: Decimal | None
    enabled: bool
    created_at: datetime
    provisioned_at: datetime | None

    @property
    def is_provisioned(self) -> bool:
        """Ready to trade: the sub EXISTS and has been FUNDED.

        Both halves, because they are persisted separately on purpose
        (`record_sub_address` then `mark_funded`): creating a sub-account is
        irreversible and capped at ten per master, funding is retryable, so a
        crash between them must resume at the funding leg rather than mint a
        second sub. A sub with an address but no `provisioned_at` is exactly
        that in-between state, and it must not be copied into — orders on an
        unfunded sub reject on margin."""
        return self.sub_address is not None and self.provisioned_at is not None

    @property
    def brackets(self) -> bool:
        return self.copy_mode == BRACKET_MODE

    @property
    def leverage_summary(self) -> str:
        """How this sub picks its leverage, as one phrase for a chat message.

        Reads the MODE, not the presence of a number: the two agree by CHECK
        constraint today, and the mode is the column that decides — a reader
        keying off the number would quietly answer "mirroring" for any future
        mode that also happens to leave `fixed_leverage` NULL."""
        return (
            f"fixed {self.fixed_leverage}x"
            if self.leverage_mode == FIXED_LEVERAGE
            else "mirroring the leader"
        )

    def require_address(self) -> str:
        """The sub's address, for the paths that only run once provisioned.
        An assertion rather than a None-check at every call site: reaching the
        order path with an unprovisioned sub is a bug in the loop's ordering,
        not a runtime condition to degrade around."""
        assert self.sub_address is not None, f"copy sub {self.id} is not provisioned"
        return self.sub_address


class CopySubExistsError(ValueError):
    """This operator already has a mapping for this Leader. Re-copying an
    uncopied Leader must REUSE its row and its sub-account, because a second
    sub would burn one of the master's 10 slots (finding 10) forever — so the
    command path re-enables instead of inserting."""


async def register_sub(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    operator_id: int,
    leader_address: str,
    sub_name: str,
    allocation_usd: Decimal,
    base_stake_usd: Decimal,
    leverage_mode: str,
    fixed_leverage: int | None,
    copy_mode: str,
    take_profit_pct: Decimal | None,
    stop_loss_pct: Decimal | None,
    now: datetime,
) -> CopySub:
    """Record the operator's intent to copy `leader_address`. Writes a PENDING
    mapping — no sub-account exists yet — because this runs in the bot process,
    which has no key to create one with (module docstring)."""
    if copy_mode not in COPY_MODES:
        raise ValueError(f"copy_mode must be one of {COPY_MODES}, got {copy_mode!r}")
    if leverage_mode not in LEVERAGE_MODES:
        raise ValueError(f"leverage_mode must be one of {LEVERAGE_MODES}, got {leverage_mode!r}")
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO copy_subs
                (operator_id, leader_address, sub_name, allocation_usd, base_stake_usd,
                 leverage_mode, fixed_leverage,
                 copy_mode, take_profit_pct, stop_loss_pct, enabled, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, $11)
            RETURNING *
            """,
            operator_id,
            leader_address.lower(),
            sub_name,
            allocation_usd,
            base_stake_usd,
            leverage_mode,
            fixed_leverage,
            copy_mode,
            take_profit_pct,
            stop_loss_pct,
            now,
        )
    except asyncpg.UniqueViolationError as exc:
        raise CopySubExistsError(
            f"operator {operator_id} already has a copy mapping for {leader_address}"
        ) from exc
    assert row is not None
    return _sub(row)


async def reenable_sub(
    conn: asyncpg.Pool | asyncpg.Connection,
    *,
    operator_id: int,
    leader_address: str,
    allocation_usd: Decimal,
    base_stake_usd: Decimal,
    leverage_mode: str,
    fixed_leverage: int | None,
    copy_mode: str,
    take_profit_pct: Decimal | None,
    stop_loss_pct: Decimal | None,
) -> CopySub | None:
    """Turn an existing mapping back on with fresh terms, reusing its
    sub-account. Returns None when there is nothing to re-enable.

    `provisioned_at` is CLEARED, which is what re-opens the funding leg:
    /uncopy never flattens, so the sub comes back holding whatever last time
    left in it — possibly almost nothing. The executor then tops it up to the
    requested allocation on its next loop, because that balance IS the
    exchange-enforced exposure cap and a drained sub would otherwise trade on
    a cap the operator never agreed to. The sub-account itself is untouched:
    subs cannot be deleted and a master holds at most ten (finding 10), so
    re-copying MUST reuse it."""
    row = await conn.fetchrow(
        """
        UPDATE copy_subs
        SET enabled = TRUE,
            provisioned_at = NULL,
            allocation_usd = $3,
            base_stake_usd = $4,
            leverage_mode = $5,
            fixed_leverage = $6,
            copy_mode = $7,
            take_profit_pct = $8,
            stop_loss_pct = $9
        WHERE operator_id = $1 AND leader_address = $2
        RETURNING *
        """,
        operator_id,
        leader_address.lower(),
        allocation_usd,
        base_stake_usd,
        leverage_mode,
        fixed_leverage,
        copy_mode,
        take_profit_pct,
        stop_loss_pct,
    )
    return None if row is None else _sub(row)


async def disable_sub(
    conn: asyncpg.Pool | asyncpg.Connection, *, operator_id: int, leader_address: str
) -> CopySub | None:
    """/uncopy: stop consuming this Leader's events. Returns the mapping that
    was disabled, or None if there was none.

    DISABLE NEVER AUTO-FLATTENS (decision 12, consistent with decision 10's
    never-auto-fix): whatever the sub still holds is left for the operator to
    act on. Reporting what that is belongs to the caller, which has the chat
    to report it in."""
    row = await conn.fetchrow(
        """
        UPDATE copy_subs SET enabled = FALSE
        WHERE operator_id = $1 AND leader_address = $2 AND enabled
        RETURNING *
        """,
        operator_id,
        leader_address.lower(),
    )
    return None if row is None else _sub(row)


async def enabled_subs(
    conn: asyncpg.Pool | asyncpg.Connection, operator_id: int
) -> list[CopySub]:
    """Every enabled mapping this operator owns, provisioned or not.

    Scoped to `operator_id` in the QUERY, not by a caller's filter: decision
    12 puts the operator-only gate in two places, and this is the second one —
    the executor cannot read a mapping it does not own even if a row for
    another user somehow existed."""
    rows = await conn.fetch(
        "SELECT * FROM copy_subs WHERE operator_id = $1 AND enabled ORDER BY id",
        operator_id,
    )
    return [_sub(row) for row in rows]


async def all_subs(
    conn: asyncpg.Pool | asyncpg.Connection, operator_id: int
) -> list[CopySub]:
    """Every mapping, enabled or not — for the operator's own listing, and for
    the sweep, which must reach a DISABLED sub's book too: disabling stops
    copying, it does not remove the money or the orders."""
    rows = await conn.fetch(
        "SELECT * FROM copy_subs WHERE operator_id = $1 ORDER BY id", operator_id
    )
    return [_sub(row) for row in rows]


async def sub_addresses(conn: asyncpg.Pool | asyncpg.Connection) -> list[str]:
    """Every provisioned Copy Sub-account address, across every operator and
    regardless of `enabled`.

    The watchdog's sweep reads this one (ADR-0007 decision 1: /kill and the
    dead-man's switch must enumerate and cancel PER SUB). Deliberately NOT
    operator-scoped: a sweep that skipped a sub because its mapping was
    disabled, or because it belonged to a row the sweep's operator id did not
    match, would leave exactly the orders an emergency stop exists to cancel."""
    rows = await conn.fetch(
        "SELECT sub_address FROM copy_subs WHERE sub_address IS NOT NULL ORDER BY id"
    )
    return [row["sub_address"] for row in rows]


async def record_sub_address(
    conn: asyncpg.Pool | asyncpg.Connection, sub_id: int, address: str
) -> None:
    """Write down the sub-account the executor just created, IMMEDIATELY and
    before it is funded.

    Idempotent-safe by the WHERE clause: a second write can never point a
    mapping at a different sub, which would strand the first one's money —
    and since sub-accounts cannot be deleted and a master holds at most ten
    (finding 10), a stranded one is a slot lost for good."""
    await conn.execute(
        "UPDATE copy_subs SET sub_address = $2 WHERE id = $1 AND sub_address IS NULL",
        sub_id,
        address.lower(),
    )


async def mark_funded(
    conn: asyncpg.Pool | asyncpg.Connection, sub_id: int, now: datetime
) -> None:
    """The second half of provisioning: the allocation landed, so the mapping
    is ready to trade (`CopySub.is_provisioned`)."""
    await conn.execute(
        "UPDATE copy_subs SET provisioned_at = $2 WHERE id = $1 AND provisioned_at IS NULL",
        sub_id,
        now,
    )


async def record_sub_equity(
    conn: asyncpg.Pool | asyncpg.Connection, sub_id: int, account_value: Decimal, now: datetime
) -> None:
    """Write down what this sub was worth on this pass (issue #137 §6).

    HISTORY, not a latest-value row, which is the whole difference from
    `trader_equity` (0032): that table answers "what is this wallet worth now"
    and nothing needed more; this one exists to be a CURVE. The daily-loss
    pause (issue #181) is deferred precisely because nobody knows what
    threshold to set, and the only honest way to pick one is to look at what a
    sub's equity actually does across a day of copying.

    The equity is already in hand — the reconcile reads each sub's
    clearinghouseState every cycle and drops the account value — so this costs
    no request and no exchange weight. It is written outside the reconcile's
    per-episode work on purpose: a sub with no live episodes still has a
    curve, and a sub whose episode reconcile failed still had a readable
    equity."""
    await conn.execute(
        """
        INSERT INTO copy_sub_equity (sub_id, account_value, observed_at)
        VALUES ($1, $2, $3)
        """,
        sub_id,
        account_value,
        now,
    )


def _sub(row: asyncpg.Record) -> CopySub:
    return CopySub(
        id=row["id"],
        operator_id=row["operator_id"],
        leader_address=row["leader_address"],
        sub_name=row["sub_name"],
        sub_address=row["sub_address"],
        allocation_usd=row["allocation_usd"],
        base_stake_usd=row["base_stake_usd"],
        leverage_mode=row["leverage_mode"],
        fixed_leverage=row["fixed_leverage"],
        copy_mode=row["copy_mode"],
        take_profit_pct=row["take_profit_pct"],
        stop_loss_pct=row["stop_loss_pct"],
        enabled=row["enabled"],
        created_at=row["created_at"],
        provisioned_at=row["provisioned_at"],
    )
