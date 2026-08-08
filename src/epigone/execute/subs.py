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

# Micro-USD is subAccountTransfer's unit (finding 6, measured). It lives here
# rather than in the executor because two readers need it now and they must
# not disagree: the executor WRITES a top-up in these units, and
# `deposits_since` READS those same rows back out of the audit trail to adjust
# a Loss Budget for money Epigone itself put in (issue #181).
USD_MICRO = 1_000_000


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
    # The Loss Budget and its ledger (issue #181). All None on a sub without
    # one, which is every sub that predates the feature and every /copy that
    # names no budget — the feature is opt-in and its absence is the old
    # behaviour exactly.
    loss_budget_usd: Decimal | None = None
    # What the sub was worth when the executor armed the budget, summed across
    # every venue it trades, and when it read that. None while a budget is set
    # but not yet baselined — /copy writes the number, the executor's next
    # cycle writes these (migration 0039's header).
    budget_baseline_usd: Decimal | None = None
    budget_armed_at: datetime | None = None
    # The last loss the executor measured, for the operator's echoes. Negative
    # means the sub is in profit.
    budget_spent_usd: Decimal | None = None
    budget_warned_at: datetime | None = None
    budget_breached_at: datetime | None = None

    @property
    def winding_down(self) -> bool:
        """Breached: the book may only shrink. Risk-increasing orders are
        refused and exits keep copying, until the sub goes flat and is
        disabled — or the operator re-issues /copy and cancels it."""
        return self.budget_breached_at is not None

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
    loss_budget_usd: Decimal | None = None,
) -> CopySub:
    """Record the operator's intent to copy `leader_address`. Writes a PENDING
    mapping — no sub-account exists yet — because this runs in the bot process,
    which has no key to create one with (module docstring).

    `loss_budget_usd` is written WITHOUT a baseline: the equity that baselines
    it is an exchange read across every covered venue, and this process holds
    no gateway. The executor arms it on its next cycle (issue #181)."""
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
                 copy_mode, take_profit_pct, stop_loss_pct, enabled, created_at,
                 loss_budget_usd)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE, $11, $12)
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
            loss_budget_usd,
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
    loss_budget_usd: Decimal | None = None,
) -> CopySub | None:
    """Turn an existing mapping back on with fresh terms, reusing its
    sub-account. Returns None when there is nothing to re-enable.

    THE BUDGET'S RE-ARM SEMANTICS LIVE IN THE `CASE`s BELOW, and they are three
    rules (issue #181), all reading the row's OLD values because that is what
    an UPDATE's SET expressions see:

    - **no budget named** (`loss_budget_usd IS NULL`) — the budget and its whole
      ledger are cleared. /copy states a mapping's terms in full, exactly as an
      omitted TP/SL leaves a sub bracket-less, so an omitted budget leaves it
      budget-less. `loss off` is the same write said out loud.
    - **a budget named on a sub that is still ENABLED** — the amount changes and
      the BASELINE AND LEDGER SURVIVE. Raising the threshold is not an amnesty
      for losses already booked; lowering it below the current loss is allowed
      and breaches on the next cycle.
    - **a budget named on a DISABLED sub** — a fresh commitment after /uncopy or
      after a budget disable, so the baseline is cleared and the executor
      re-snapshots it. A new copy of the same Leader is not haunted by the old
      ledger.

    The two MARKS clear in every armed case: the operator has just restated
    their threshold, so an in-progress wind-down is cancelled (the override
    this feature promises — a breach of one's own number is never a ratchet)
    and the 80% notice may fire once against the new number. If the loss is
    still past the new budget, the next cycle breaches again and says so.

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
            stop_loss_pct = $9,
            loss_budget_usd = $10,
            budget_baseline_usd = CASE
                WHEN $10::numeric IS NULL OR NOT enabled THEN NULL
                ELSE budget_baseline_usd END,
            budget_armed_at = CASE
                WHEN $10::numeric IS NULL OR NOT enabled THEN NULL
                ELSE budget_armed_at END,
            budget_spent_usd = CASE
                WHEN $10::numeric IS NULL OR NOT enabled THEN NULL
                ELSE budget_spent_usd END,
            budget_warned_at = NULL,
            budget_breached_at = NULL
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
        loss_budget_usd,
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


async def find_sub(
    conn: asyncpg.Pool | asyncpg.Connection, *, operator_id: int, leader_address: str
) -> CopySub | None:
    """One operator's mapping for one Leader, enabled or not.

    /copy needs it BEFORE it writes: the confirmation echoes what an existing
    budget has already spent, and the audit row that records a budget change
    needs the old value to state it as old → new (issue #181)."""
    row = await conn.fetchrow(
        "SELECT * FROM copy_subs WHERE operator_id = $1 AND leader_address = $2",
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
    and nothing needed more; this one exists to be a CURVE.

    IT HAS NO CONSUMER TODAY, and that is a recorded gap rather than an
    oversight. The curve was written for the rolling daily-loss pause that
    issue #181 originally proposed; the Loss Budget that shipped instead
    measures from a STORED BASELINE on the sub's own row, so it never reads
    this table (ADR-0007 amendment D-9). What is written here is now the same
    covered-venue sum the budget measures — the reconcile's own read — so the
    curve and the budget can never disagree about what a sub was worth.

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


# --- the Loss Budget's ledger (issue #181) ------------------------------------


async def arm_budget(
    conn: asyncpg.Pool | asyncpg.Connection,
    sub_id: int,
    *,
    baseline_usd: Decimal,
    now: datetime,
) -> None:
    """Snapshot the baseline a budget is measured from, once.

    `budget_baseline_usd IS NULL` in the WHERE is the once: a second cycle can
    only ever re-arm a budget the operator re-issued (which cleared the
    baseline itself), never re-baseline a running one — that would forgive
    every loss booked so far, silently, on a restart."""
    await conn.execute(
        """
        UPDATE copy_subs SET budget_baseline_usd = $2, budget_armed_at = $3
        WHERE id = $1 AND loss_budget_usd IS NOT NULL AND budget_baseline_usd IS NULL
        """,
        sub_id,
        baseline_usd,
        now,
    )


async def record_budget_spend(
    conn: asyncpg.Pool | asyncpg.Connection,
    sub_id: int,
    *,
    spent_usd: Decimal,
    warned_at: datetime | None = None,
    breached_at: datetime | None = None,
    judged_budget_usd: Decimal | None = None,
    judged_armed_at: datetime | None = None,
) -> bool:
    """This cycle's measured loss, and the marks it earned. Returns whether the
    write landed.

    The marks are written with COALESCE so a mark, once set, is never moved by
    a later cycle: "warned at" means the first cycle that crossed 80%, and a
    breach keeps the instant the wind-down actually began. Clearing them is the
    re-issued /copy's job alone (`reenable_sub`).

    `judged_budget_usd` / `judged_armed_at` are a COMPARE-AND-SET on the terms
    the verdict was reached against, and they close a small but real race: an
    executor cycle reads a sub, measures it, and writes — and a /copy raising
    the budget in that window would have its wind-down cancellation silently
    undone by a verdict about the old number. Stating the terms makes such a
    write a no-op, and the next cycle judges the sub the operator actually has.
    Omitted (the tests' convenience form), the write is unconditional."""
    result = await conn.execute(
        """
        UPDATE copy_subs
        SET budget_spent_usd = $2,
            budget_warned_at = COALESCE(budget_warned_at, $3),
            budget_breached_at = COALESCE(budget_breached_at, $4)
        WHERE id = $1 AND budget_armed_at IS NOT NULL
          AND ($5::numeric IS NULL OR loss_budget_usd = $5)
          AND ($6::timestamptz IS NULL OR budget_armed_at = $6)
        """,
        sub_id,
        spent_usd,
        warned_at,
        breached_at,
        judged_budget_usd,
        judged_armed_at,
    )
    return str(result) != "UPDATE 0"


async def disable_for_spent_budget(
    conn: asyncpg.Pool | asyncpg.Connection, sub_id: int, *, breached_at: datetime
) -> bool:
    """End the copy relationship of a wound-down sub that has gone flat.
    Returns whether this call is the one that did it.

    Conditional on the sub still being ENABLED and still carrying the SAME
    breach, which makes the terminal step idempotent in both directions: a
    second cycle cannot re-announce a disable that already happened, and a
    /copy that cancelled the wind-down between this cycle's measurement and
    this write cannot be undone by it.

    Deliberately NOT `disable_sub`: that one is /uncopy's, keyed by leader
    address, and it would happily disable a mapping whose breach had just been
    cleared."""
    result = await conn.execute(
        """
        UPDATE copy_subs SET enabled = FALSE
        WHERE id = $1 AND enabled AND budget_breached_at = $2
        """,
        sub_id,
        breached_at,
    )
    return str(result) != "UPDATE 0"


async def deposits_since(
    conn: asyncpg.Pool | asyncpg.Connection, *, sub_address: str, since: datetime
) -> Decimal:
    """Dollars Epigone itself moved INTO this sub since `since`.

    The transfer-adjustment half of the loss measurement (issue #181): funding
    a sub must never read as trading profit — the btcgod lesson — so every
    top-up after the baseline was taken is added back to what the sub is
    expected to be worth.

    Read from the AUDIT TRAIL rather than from a bookkeeping column, because
    the trail is the only place a transfer is recorded write-ahead and the
    provisioning path already writes it there. Two things the join says
    exactly:

    - **successful outcomes only.** An attempt row with no `ok` outcome is a
      transfer that may never have landed; counting it would invent equity the
      sub does not have and slacken the budget by that much. An attempt-only
      row therefore contributes nothing, which is also the direction that
      cannot hide a loss.
    - **strictly after arming.** The transfer that FILLED the sub is already
      inside the baseline, so counting it again would show the whole allocation
      as lost on the first cycle. A transfer sharing the baseline's exact
      instant is excluded for the same reason — the two are indistinguishable
      by timestamp, and the safe reading of a tie is the one that never invents
      loss (it reads as profit instead, which the budget's own docs call out).

    Withdrawals are absent because no withdrawal path exists (issue #181's Out
    of Scope): the adjustment only ever subtracts inflows from an apparent
    profit, never adds to a loss."""
    micro = await conn.fetchval(
        """
        SELECT COALESCE(SUM((attempt.request ->> 'usd_micro')::numeric), 0)
        FROM execution_audit AS attempt
        JOIN execution_audit AS outcome ON outcome.attempt_of = attempt.id
        WHERE attempt.action = 'subAccountTransfer'
          AND attempt.request ->> 'sub_account_user' = $1
          AND (attempt.request ->> 'is_deposit')::boolean
          AND attempt.occurred_at > $2
          AND outcome.outcome = 'ok'
        """,
        sub_address.lower(),
        since,
    )
    return Decimal(micro) / USD_MICRO


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
        loss_budget_usd=row["loss_budget_usd"],
        budget_baseline_usd=row["budget_baseline_usd"],
        budget_armed_at=row["budget_armed_at"],
        budget_spent_usd=row["budget_spent_usd"],
        budget_warned_at=row["budget_warned_at"],
        budget_breached_at=row["budget_breached_at"],
    )
