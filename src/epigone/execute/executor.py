"""The copy executor's cycle (issues #136 and #137, ADR-0007).

One cycle, in the order the ADR forces:

0. **re-read the global risk limits** (issue #137 §7). Before anything is
   judged, because a `/limits` change must take effect without a restart and
   because everything below judges against them — and reset the cycle's market
   view, which is read lazily on the first entry that needs it;
1. **beat** the heartbeat — the watchdog's only view of us (ADR-0002), and a
   stale one is what trips the dead-man's switch;
2. **reconcile** every sub against the exchange (decision 10), recording each
   sub's equity as it goes (§6's enabler for the deferred daily-loss pause).
   First, because
   every relative operation downstream applies to the size the EXCHANGE
   reports, never to a bookkept expectation — that is the self-damping
   principle, and reading state before acting on it is how it holds. It runs
   even while halted: it places no orders, and an incident is exactly when
   knowing what we hold matters most;
3. **stop here if halted.** Everything below signs something;
4. **provision** any pending mapping (decision 12) — /copy is issued in the
   bot process, which holds no signer, so the sub is created and funded here;
5. **restore brackets** on live positions in bracket-mode subs (decision 9's
   r1a), expressed as a property to maintain rather than a /resume transition
   to detect — see `_maintain_brackets`;
6. **drain the backlog**: the copy-enabled Leaders' unclaimed AUTHORITATIVE
   events, oldest first — written by whichever lane owned production when it
   saw the change (ADR-0009), never by whichever transport.

THE WRITE-AHEAD RULE (ADR-0006, inherited): the `position_event_claims` row
and the `execution_audit` attempt row are written in ONE transaction, before
anything reaches the wire. A crash mid-flight therefore leaves a claimed event
with an attempt and no outcome — the "reconcile me" signal — which is a MISSED
copy. ADR-0006 chooses that over the alternative: an unclaimed but sent event
is a doubled position with real money behind it. A claim means HANDLED, not
traded: risk-declined, stale and coin-occupied events are all claimed, or the
backlog never drains.

THE HALT RE-CHECK (the #143 `skip_cancel` residual race, this ticket's
contract): halt state is re-read as late as possible — after the claim
commits, immediately before signing. What makes the remaining window
survivable here is decision 4: every copy action is an IOC, so nothing
entry-shaped can REST past the sweep's verify enumeration. The one thing this
executor leaves resting is a bracket trigger, and the watchdog's sweep now
enumerates and cancels per sub (decision 1) — so the residual race's "swept
halt with a live order" outcome has a cleaning path on both sides. A halt
observed after signing stays a reconciliation obligation, discharged by step 2
of the next cycle, never an assumption that the order did not happen.

DIRECTION ASYMMETRY, everywhere: entries are guarded (staleness, liveness,
Liquidity Floor, stake caps) and fire ONE SHOT; exits are gated by nothing,
execute at any age, and retry. Closing late is strictly safer than never
closing.

WHAT AN ENTRY SIGNS, in order (amendment D-4): the claim and attempt rows
commit, the halt is re-read, `updateLeverage(isolated)` sets the leverage the
policy judged, the halt is re-read AGAIN — a signature with its own round trip
sits between them — and only then does the order go. A halt in the middle
leaves a leverage setting on an asset the sub holds nothing in, which changes
nothing about the account; a halt after the order stays a reconciliation
obligation, as it always was.
"""

import logging
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.decimals import fixed_point
from epigone.execute import episodes as ep
from epigone.execute import limits as risk_limits
from epigone.execute import subs as subs_store
from epigone.execute.notices import (
    ACTION,
    PAGER,
    PROVISIONING,
    SKIP,
    SkipDigest,
    notify,
)
from epigone.execute.policy import (
    BRACKET_VERIFY_INTERVAL,
    BUDGET_BREACHED,
    BUDGET_WARNING,
    BUDGET_WARNING_FRACTION,
    BUDGET_WITHIN,
    ENTRY_STALENESS_GUARD,
    EXIT_RETRY_ATTEMPTS,
    EXIT_RETRY_DELAY_SECONDS,
    LEADER_EQUITY_FLOOR,
    MIN_ORDER_NOTIONAL,
    LeverageChoice,
    LeverageUnknownError,
    RiskPolicy,
    budget_loss,
    budget_spent_of,
    budget_stage,
    committed_stake,
    resolve_leverage,
)
from epigone.execute.pricing import (
    UnpriceableError,
    clamped_size,
    ioc_limit_price,
    open_size,
    relative_size,
    round_size,
    scale_fraction,
    trigger_price,
)
from epigone.execute.subs import USD_MICRO, CopySub
from epigone.gateway import (
    POSITION_VENUES,
    AssetSpec,
    GatewayError,
    HyperliquidGateway,
    MarketStats,
    OpenOrder,
    Position,
    Side,
    fetch_account_state,
    fetch_asset_specs,
    fetch_market_stats,
)
from epigone.gateway.execution import (
    ActionRejectedError,
    CancelSpec,
    ExecutionError,
    Grouping,
    OrderFilled,
    OrderRejected,
    OrderResting,
    OrderResult,
    OrderSpec,
    RejectReason,
    Tif,
    TpSl,
    Trigger,
)
from epigone.position_events import ClaimableEvent, claim_event, outstanding_events
from epigone.safety import heartbeat
from epigone.safety.audit import (
    EXECUTOR_ACTOR,
    OK,
    AuditedAttempt,
    AuditedExecutionGateway,
    AuditedProvisioning,
    ExecutionAudit,
)
from epigone.safety.halt import is_halted

log = logging.getLogger(__name__)

# This consumer's name in `position_event_claims`. Stable forever: changing it
# would present the entire retained backlog as unclaimed and re-copy a week of
# expired theses.
EXECUTOR_CONSUMER = "copy_executor"

# Read-side weights, billed per venue call like every other consumer bills its
# own (the watchdog's ORDERS_WEIGHT/POSITIONS_WEIGHT precedent).
POSITIONS_WEIGHT = 2  # clearinghouseState, per venue
ORDERS_WEIGHT = 20  # frontendOpenOrders, per venue
MIDS_WEIGHT = 2  # allMids, per venue
META_WEIGHT = 20  # meta / perpDexs
MARKET_STATS_WEIGHT = 20  # metaAndAssetCtxs, per venue
SUBS_WEIGHT = 20  # subAccounts — the rate the watchdog bills this listing at

# What the operator is told did not happen, per provisioning leg. Each leg is
# one signature (`_halted_before_provisioning`), and the sentence names the
# action rather than the leg because "NOT renamed" is what the chat needs to
# read — the leg name is for the audit row beside it.
_HALTED_LEG = {"create": "created", "rename": "renamed", "fund": "funded"}
FILLS_WEIGHT = 20  # userFills, per endpoint


@dataclass(frozen=True)
class SubState:
    """One sub's live truth for this cycle: its equity and its positions, both
    across every venue Epigone covers.

    THE EQUITY IS A SUM, not the core venue's figure (issue #181). Each venue
    collateralises itself — a HIP-3 builder DEX holds its own margin — so a
    copy holding xyz:META has real money on a venue the core `accountValue`
    knows nothing about. Reading the core alone was harmless while this figure
    only fed a history table; it stopped being harmless the moment a Loss
    Budget started measuring a sub against it, where the missing venue reads
    as money LOST and would wind a healthy copy down."""

    account_value: Decimal
    positions: dict[str, Position]


@dataclass(frozen=True)
class _Funding:
    """What one provisioning read learned about a sub's money: what it holds
    now, and how far that is below the allocation it is targeting. Both,
    because the transfer needs the gap and the operator's notice needs the
    balance — and re-reading equity to say the second would bill a second
    weight for a number already in hand."""

    held: Decimal
    topup: Decimal


@dataclass(frozen=True)
class _Ask:
    """What an entry WANTS, before the policy has judged it: the leverage it
    would run at, the stake it would put behind that, and the coin-unit size
    those two imply at the current mark.

    Computed before the verdict and clamped after it, in that order, because
    the policy grants STAKE and the wire takes COIN UNITS — keeping the full
    ask beside its size is what lets a clamp scale the order proportionally
    instead of re-deriving it by a second route that could disagree."""

    leverage: LeverageChoice
    stake_usd: Decimal
    size_coin: Decimal


# The coarse WHY behind a skip: the bucket a cycle's summary counts when the
# per-event sentences are too many to read (issue #190). Deliberately blunter
# than the sentences — a summary answers "what happened to most of them", and
# the exact coin, age and figure are one click away on the trail.
#
# The WORDS are not free choices: decision 11's list and the copy-execution
# runbook already name these reasons to the operator, and a summary that
# renamed them would be a second vocabulary for the same six things. Named
# REASON_* rather than SKIP_*, because `SKIP` in this module is already the
# notice KIND — one prefix for two concepts is how they get confused.
REASON_STALE = "stale entry"
REASON_NO_LOCAL_POSITION = "no local position"
REASON_COIN_OCCUPIED = "coin occupied"
REASON_UNORDERABLE = "coin not orderable"
REASON_NOT_MIRRORABLE = "not mirrorable"
REASON_RISK_DECLINED = "risk-declined"
REASON_LIQUIDITY_FLOOR = "below the liquidity floor"
REASON_LIVENESS_FLOOR = "leader below the liveness floor"
REASON_BELOW_MINIMUM = "under the exchange minimum"
REASON_UNREADABLE = "unreadable"
REASON_LOSS_BUDGET = "loss budget spent"


@dataclass(frozen=True)
class _Skip:
    """A decision not to act, carrying the sentence the operator reads and the
    reason the trail records. Every skip still CLAIMS the event.

    `category` is the same decision said coarsely, and it exists for one
    reader: the cycle summary a backlog drain sends instead of the sentences
    (issue #190). It never replaces `reason` — the trail records the sentence,
    verbatim, one row per event, in both regimes."""

    category: str
    reason: str
    detail: dict[str, object]


class CopyExecutor:
    """One cycle's logic, dependency-injected for tests; epigone.execute.main
    wires the real deps and loops it."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        clock: Clock,
        read_gateway: HyperliquidGateway,
        exec_gateway: AuditedExecutionGateway,
        provisioning: AuditedProvisioning,
        audit: ExecutionAudit,
        budget: Budget,
        policy: RiskPolicy,
        *,
        signal_gateway: HyperliquidGateway,
        operator_id: int,
        master_address: str,
        signer_address: str,
    ) -> None:
        self._pool = pool
        self._clock = clock
        self._read = read_gateway
        # The SIGNAL network's read-only gateway (issue #184). Used for exactly
        # one read — the Leader's liveness equity — because the Leader's
        # account is a fact about the network tracking observed them on, not
        # about the book this process trades. Every other read goes through
        # `self._read`, which stays pinned to the exchange url; on a mainnet
        # deployment the two gateways point at the same endpoint.
        self._signal_read = signal_gateway
        self._exec = exec_gateway
        self._provisioning = provisioning
        self._audit = audit
        self._budget = budget
        self._policy = policy
        self._operator_id = operator_id
        self._master = master_address.lower()
        self._signer = signer_address.lower()
        self._specs: dict[str, AssetSpec] = {}
        # The cycle's own copies of things that must not be stale WITHIN a
        # cycle and must not be cached ACROSS one. Limits change under the
        # operator's hand (/limits), market health changes under the market's;
        # both are re-read at the top of each cycle, and the market read is
        # LAZY — a cycle with no fresh entry to judge never spends its weight.
        self._limits = risk_limits.RiskLimits()
        self._stats: dict[str, MarketStats] | None = None
        self._stats_unreadable = False
        self._brackets_checked_at: datetime | None = None
        # Episodes already reported as unclassifiable. Decision 10 says
        # "re-flag until resolved", and the AUDIT TRAIL does exactly that —
        # a row every loop. The operator's CHAT does not: a divergence that
        # nothing can classify does not resolve itself, so a message every
        # cycle would bury every other notice within minutes. Re-paging is the
        # #52 monitor's job, on its own throttled cadence.
        self._flagged: set[int] = set()
        # This cycle's per-event skips, held until the drain knows how many
        # there were (issue #190). Owned by `_drain_backlog`, which fills it
        # and flushes it inside one cycle — nothing survives across cycles,
        # because "this many at once" is the only question it answers.
        self._skips = SkipDigest()
        # Wound-down subs whose disable could not complete, and the reason last
        # reported for each (issue #181). In memory, like `_flagged` and for the
        # same reason: it answers "have I already said this", and a restart
        # saying it once more is the safe direction.
        self._deferred_disables: dict[int, str] = {}

    # --- the cycle ------------------------------------------------------------

    async def run_cycle(self) -> None:
        now = self._clock.now()
        await heartbeat.beat(self._pool, heartbeat.EXECUTOR_PROCESS, now)
        # The global knobs, re-read every cycle so a /limits change takes
        # effect without a restart (the `enabled` precedent, decision 12), and
        # the market read reset so this cycle judges this cycle's liquidity.
        self._limits = await risk_limits.load(self._pool)
        self._stats = None
        self._stats_unreadable = False
        # Reconciliation covers EVERY mapping; everything below covers only the
        # enabled ones. /uncopy stops event consumption, not the money
        # (decision 12) — a disabled sub still holds positions, its bracket can
        # still fire, and it can still be liquidated, so decision 10's "each
        # loop the executor compares every sub's live state" means every sub.
        # Leaving a disabled sub unreconciled would also strand a live episode
        # that a later /copy would then read as "already in a copy episode".
        all_subs = await subs_store.all_subs(self._pool, self._operator_id)
        # `_reconcile` hands the mappings back AS THEY STAND AFTERWARDS, because
        # judging a Loss Budget is part of it and its verdicts land on the row:
        # a sub that BREACHED this cycle has to refuse this cycle's opens
        # (issue #181).
        states, all_subs = await self._reconcile(all_subs, now)
        if await is_halted(self._pool):
            # Halted: reconciliation has run (it signs nothing), and every
            # step below signs something. The backlog is NOT drained — an
            # event claimed during a halt would be an event /resume can never
            # replay, and decision 9 is explicit that resume drains the
            # backlog under the already-locked rules.
            log.info("copy executor: halted — reconciled only, nothing signed")
            return
        # The wind-down's terminal step, and it is BELOW the halt gate rather
        # than beside the measurement above because it CANCELS: a flat sub's
        # leftover triggers come off the book before the mapping is disabled,
        # and cancelling is signing. The enabled set is therefore taken after
        # it — a mapping the executor has just ended must not be provisioned,
        # bracketed or drained one more time in the same cycle.
        all_subs = await self._disable_wound_down_subs(all_subs, states, now)
        subs = [sub for sub in all_subs if sub.enabled]
        await self._provision(subs, now)
        await self._maintain_brackets(subs, states, now)
        await self._drain_backlog(subs, states)

    # --- provisioning (decision 12) -------------------------------------------

    async def _provision(self, subs: list[CopySub], now: datetime) -> None:
        """Create and fund any mapping /copy left pending. One sub per pass:
        provisioning moves money and mints a slot from the master's ten
        (finding 10), so a bad configuration costs one mistake per cycle, not
        a burst of them.

        TWO STEPS, PERSISTED SEPARATELY, and that split is the whole design:
        creating a sub-account is IRREVERSIBLE (subs cannot be deleted and the
        master holds at most ten), while funding is retryable. So the address
        is written down the instant it exists, before the transfer is
        attempted — a crash or a failed transfer then resumes at the funding
        leg instead of minting a second sub and stranding the first.

        FUNDING IS A TOP-UP, NOT A TRANSFER OF THE FULL ALLOCATION. `/uncopy`
        never flattens, so a re-copied Leader's sub comes back holding
        whatever last time left in it — which may be more than the allocation
        (it won) or almost nothing (it lost). The allocation is a TARGET
        BALANCE, because that balance IS the exchange-enforced exposure cap
        (decision 1); transferring the full figure again would stack a second
        allocation on top of the first, and transferring nothing would let a
        drained sub trade on a cap the operator never agreed to. So the sub's
        live equity is read and only the difference moves. An over-funded sub
        is left alone rather than drained back: taking money OUT is not
        something provisioning should decide.

        CREATION IS THE FIRST CHOICE, ADOPTION THE FALLBACK (issue #178). A
        master holds at most ten subs, flat, and a sub cannot be deleted
        (finding 10) — so a master whose slots are spent refuses
        `createSubAccount` and, without a second answer, could copy NO Leader
        at all. `_adopt_orphan_sub` is that answer: a sub of this master that
        no mapping claims and that holds no position becomes this Leader's,
        and the top-up above funds it from whatever it inherited. Minting is
        still attempted first every pass, because a free slot always beats
        re-using an account something else named."""
        for sub in subs:
            if sub.is_provisioned:
                continue
            verdict = self._policy.judge_provisioning(
                allocation_usd=sub.allocation_usd,
                base_stake_usd=sub.base_stake_usd,
                limits=self._limits,
            )
            if not verdict.allowed:
                await self._decline_provisioning(sub, verdict.decision, now)
                continue
            # On the PROVISIONING wrapper, not the order gateway: these two
            # actions ride their own audited seam (money moves, no order does),
            # and a verdict written to the wrong wrapper would leave the rows
            # that matter saying "unspecified".
            self._provisioning.decision = verdict.decision
            address = sub.sub_address
            adopted = False
            renamed = True
            try:
                if address is None:
                    # THE HALT GATE, as late as it can be before an
                    # IRREVERSIBLE action: a halted cycle must not mint a
                    # sub-account or move funding money. The cycle-top check
                    # is seconds-to-tens-of-seconds stale by now, because the
                    # per-sub reconciles ran in between. It covers adoption
                    # too — that path signs a rename and a transfer, and
                    # hands a live sub to a Leader.
                    if await self._halted_before_provisioning(sub, "create", now):
                        return
                    try:
                        address = await self._provisioning.create_sub_account(sub.sub_name)
                    except ActionRejectedError as exc:
                        if exc.reason is not RejectReason.SUB_ACCOUNT_CAP:
                            raise
                        # The master's ten slots are spent (finding 10). This
                        # is the ONE refusal with a recovery that does not
                        # need the operator. Adoption writes the address down
                        # WITH its own audit row and notice, in one
                        # transaction, so it persists the same way this branch
                        # does — see `_adopt_orphan_sub`.
                        address = await self._adopt_orphan_sub(sub, now)
                        if address is None:
                            # Either nothing is adoptable — reported and
                            # disabled in there — or we could not tell, in
                            # which case the next cycle asks again.
                            continue
                        adopted = True
                        # A SECOND LATE GATE, and the reason it is not
                        # redundant: the "create" check above is now SECONDS
                        # to tens of seconds old — between them sit a
                        # budget-throttled listing read and a position read
                        # per candidate — and the rename is a signature. The
                        # adoption itself is KEPT: a row in our own database
                        # is not something the exchange saw, so the next
                        # cycle resumes at the funding leg rather than
                        # adopting a second sub. Only the name is lost, which
                        # is the cosmetic half by construction.
                        if await self._halted_before_provisioning(sub, "rename", now):
                            return
                        renamed = await self._rename_adopted_sub(sub, address)
                    else:
                        await subs_store.record_sub_address(self._pool, sub.id, address)
                funding = await self._funding_gap(sub, address)
                if funding.topup > 0:
                    if await self._halted_before_provisioning(sub, "fund", now):
                        return
                    await self._provisioning.sub_account_transfer(
                        address, is_deposit=True, usd_micro=int(funding.topup * USD_MICRO)
                    )
            except ExecutionError:
                # The gateway already left its own attempt/outcome rows. The
                # mapping stays unfunded and the next cycle resumes from
                # whichever leg is still outstanding.
                log.exception(
                    "copy sub provisioning failed for %s; retrying next cycle", sub.sub_name
                )
                continue
            except GatewayError:
                # The equity read failed, so the top-up amount is unknown.
                # Funding a guess would be the one unrecoverable mistake here.
                log.warning(
                    "copy sub %s: balance unreadable, funding deferred", sub.sub_name,
                    exc_info=True,
                )
                continue
            async with self._pool.acquire() as conn, conn.transaction():
                await subs_store.mark_funded(conn, sub.id, now)
                await notify(
                    conn,
                    operator_id=self._operator_id,
                    kind=PROVISIONING,
                    body=(
                        f"✅ Copy sub ready for {_short(sub.leader_address)}\n"
                        f"sub {_short(address)} · "
                        # Adopted subs read differently on purpose: this one
                        # was NOT minted for this Leader, it was taken over,
                        # and the operator is the only one who can tell
                        # whether that is what they wanted.
                        + (
                            "ADOPTED — the master's 10 slots are full, so an "
                            "empty unmapped sub was taken over · "
                            if adopted
                            else ""
                        )
                        + (
                            f"topped up ${funding.topup} to a ${sub.allocation_usd} balance"
                            if funding.topup > 0
                            # The BALANCE, not the allocation: a sub that
                            # arrived with more than the allocation is not
                            # "holding its allocation", and an adopted orphan
                            # is exactly where that happens.
                            else f"already holds ${funding.held} against its "
                            f"${sub.allocation_usd} allocation"
                        )
                        + f" · stake ${sub.base_stake_usd} per open, "
                        + f"{sub.leverage_summary} · mode {sub.copy_mode}"
                        + (
                            ""
                            if renamed
                            else f" · still named whatever minted it, not {sub.sub_name}"
                        )
                    ),
                    now=now,
                )
            log.info("copy sub %s provisioned at %s", sub.sub_name, address)
            return

    async def _adopt_orphan_sub(self, sub: CopySub, now: datetime) -> str | None:
        """Find a sub of the master this mapping can take over, or say why it
        cannot (issue #178). Returns the adopted address, or None — and the
        two Nones mean different things, which is the whole care in here.

        ADOPTABLE means BOTH of:
        - **unmapped**: no `copy_subs` row anywhere points at it, enabled or
          not. A disabled mapping's sub still belongs to its Leader — /uncopy
          stops event consumption, not ownership, and a later /copy re-enables
          that row onto that same sub. The exclusion set is deliberately
          read across EVERY operator, not just this one;
        - **position-free**: decision 10's never-touch rule. A sub holding a
          live position is operator territory whoever opened it, and copying
          a Leader into it would mix two books that must not mix. Checked
          across every venue Epigone covers, so a builder-DEX-only position
          protects a sub exactly as a core one does.

        Resting ORDERS are not part of the test, and that is the ticket's rule
        rather than an oversight: the disqualifier is a live position, which
        is what "never touch" is about. The residue is small and visible — an
        adopted sub's inherited resting order belongs to no Copy Episode, so
        reconciliation never acts on it, and the next /kill sweep cancels it
        with every other order on that book.

        Equity does NOT disqualify: an orphan holding money is the good case
        — the funding leg treats the allocation as a target balance and moves
        only the difference (`_funding_gap`), exactly as it does for a
        re-copied sub.

        THE TWO NONES. "Nothing is adoptable" is a decision: it is reported
        loudly and the mapping is disabled, because no later cycle will change
        it without the operator. "I could not tell" — the listing would not
        read, came back EMPTY while the exchange was refusing at the cap (a
        contradiction, so not evidence of anything), or a candidate's
        positions would not load — is NOT: it defers, leaves the mapping
        pending and retries next cycle, because disabling a Leader over a
        transient read failure is the wrong direction of mistake.

        The adopted address is written down WITH the audit row and the
        operator's notice, in ONE transaction, before this returns. Same rule
        the create leg follows for the same reason — a trail that says a sub
        was adopted for a mapping that never recorded it is worse than no
        trail — and it is also what makes the report durable: if the funding
        leg then defers, the operator has already been told this sub was
        adopted rather than minted."""
        await self._budget.spend(SUBS_WEIGHT)
        try:
            held = await self._read.get_sub_accounts(self._master)
        except GatewayError:
            log.warning(
                "copy sub %s: subAccounts unreadable, adoption deferred",
                sub.sub_name,
                exc_info=True,
            )
            return None
        if not held:
            # The exchange refused at the cap and the listing says the master
            # holds nothing. One of the two is wrong and we cannot tell which,
            # so this is an "I could not tell", never a "there is none".
            log.warning(
                "copy sub %s: createSubAccount refused at the cap but subAccounts "
                "lists none — adoption deferred",
                sub.sub_name,
            )
            return None
        mapped = set(await subs_store.sub_addresses(self._pool))
        undecided = False
        for candidate in held:
            if candidate in mapped:
                continue
            try:
                if await self._holds_positions(candidate):
                    continue
            except GatewayError:
                # Might be the empty one; we cannot say. Not adoptable now,
                # and not evidence that nothing is.
                undecided = True
                log.warning(
                    "copy sub %s: candidate %s unreadable, skipped for adoption",
                    sub.sub_name,
                    candidate,
                    exc_info=True,
                )
                continue
            async with self._pool.acquire() as conn, conn.transaction():
                await subs_store.record_sub_address(conn, sub.id, candidate)
                await self._audit.record_event(
                    actor=EXECUTOR_ACTOR,
                    action="copy_sub_adopted",
                    risk_decision=(
                        f"createSubAccount refused at the cap of 10 (finding 10); adopted "
                        f"unmapped position-free sub {candidate} for {sub.leader_address}"
                    ),
                    detail={
                        "sub_id": sub.id,
                        "leader": sub.leader_address,
                        "sub_address": candidate,
                        "sub_name": sub.sub_name,
                    },
                    master_address=self._master,
                    conn=conn,
                )
                await notify(
                    conn,
                    operator_id=self._operator_id,
                    kind=PROVISIONING,
                    body=(
                        f"♻️ Copy sub ADOPTED for {_short(sub.leader_address)}\n"
                        f"the master's 10 slots are full, so sub {_short(candidate)} — "
                        f"unmapped and holding no position — was taken over rather than "
                        f"minted. Funding it to its ${sub.allocation_usd} allocation next."
                    ),
                    now=now,
                )
            return candidate
        if undecided:
            return None
        await self._report_cap_exhausted(sub, held_count=len(held), now=now)
        return None

    async def _holds_positions(self, address: str) -> bool:
        """Does this account hold ANY open position, on any covered venue?
        Stops at the first one — the question is adoptability, not inventory,
        and a sub with one position is as untouchable as a sub with ten."""
        for dex in POSITION_VENUES:
            await self._budget.spend(POSITIONS_WEIGHT)
            if await self._read.get_open_positions(address, dex=dex):
                return True
        return False

    async def _report_cap_exhausted(
        self, sub: CopySub, *, held_count: int, now: datetime
    ) -> None:
        """The cap is full AND nothing is adoptable: fail loudly, provision
        nothing, and disable the mapping — the same shape (and the same "was
        NOT set up" sentence) the risk-declined path has, because from the
        operator's chair both are "this Leader is not being copied and I have
        to do something about it". Its own audit action and its own sentence,
        though: the fix is different (free a sub, or retire a Leader) and a
        notice that read like a risk decline would send the operator to the
        wrong place."""
        async with self._pool.acquire() as conn, conn.transaction():
            await subs_store.disable_sub(
                conn, operator_id=self._operator_id, leader_address=sub.leader_address
            )
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_provisioning_cap_exhausted",
                risk_decision=(
                    f"createSubAccount refused at the cap and no sub of the master is "
                    f"adoptable — all {held_count} are mapped to a Leader or hold a position"
                ),
                detail={
                    "leader": sub.leader_address,
                    "sub_name": sub.sub_name,
                    "held": held_count,
                },
                master_address=self._master,
                conn=conn,
            )
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=SKIP,
                body=(
                    f"🚫 Copy of {_short(sub.leader_address)} was NOT set up — the master "
                    f"holds all {held_count} of its sub-accounts and none can be adopted: "
                    f"every one is already mapped to a Leader or holds an open "
                    f"position. /uncopy does "
                    f"NOT free one (a disabled mapping keeps its sub, so re-copying can "
                    f"reuse it) and subs cannot be deleted — retire a Leader's mapping "
                    f"for good, or copy from another master. The mapping is disabled; "
                    f"nothing was funded."
                ),
                now=now,
            )

    async def _rename_adopted_sub(self, sub: CopySub, address: str) -> bool:
        """Give the adopted sub this Leader's name (finding 11's
        `subAccountModify`, probed for this ticket). Cosmetic and best-effort:
        it changes a label in the operator's exchange UI and nothing else, so
        a refusal is reported in the ready notice and the pass carries on —
        a Leader copied under an inherited name beats a Leader not copied."""
        try:
            await self._provisioning.rename_sub_account(address, sub.sub_name)
        except ExecutionError:
            log.warning(
                "adopted sub %s could not be renamed to %s; cosmetic only",
                address,
                sub.sub_name,
                exc_info=True,
            )
            return False
        return True

    async def _funding_gap(self, sub: CopySub, address: str) -> _Funding:
        """How much this sub is short of its target balance. Zero when it
        already holds the allocation or more (module docstring: an over-funded
        sub is left alone, never drained back).

        The balance comes back beside the gap because the operator's notice
        needs it: "already holds its allocation" is a lie about an ADOPTED
        orphan that arrived holding more than the allocation, and the number
        that tells them what they actually have is one this read already
        fetched."""
        await self._budget.spend(POSITIONS_WEIGHT)
        held = (await self._read.get_account_state(address)).account_value
        return _Funding(held=held, topup=max(sub.allocation_usd - held, Decimal(0)))

    async def _halted_before_provisioning(
        self, sub: CopySub, leg: str, now: datetime
    ) -> bool:
        """The same discipline the order legs have, applied to the actions that
        move money and mint accounts rather than place orders. A halt means
        Epigone SIGNS nothing — and unlike an IOC, a funding transfer cannot
        be un-sent.

        Every leg that reaches the wire carries its OWN check rather than
        sharing one, because the gaps between them are real time: adoption's
        listing read and its per-candidate position reads sit between `create`
        and `rename`, and the equity read sits between `rename` and `fund`. A
        gate that fired once at the top would be tens of seconds stale by the
        last signature."""
        if not await is_halted(self._pool):
            return False
        async with self._pool.acquire() as conn, conn.transaction():
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_halted",
                risk_decision=(
                    f"provisioning {leg} for {sub.sub_name} NOT sent — execution is halted"
                ),
                detail={"sub_id": sub.id, "leg": leg},
                master_address=self._master,
                conn=conn,
            )
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=SKIP,
                body=(
                    f"🛑 Copy sub for {_short(sub.leader_address)} was NOT "
                    f"{_HALTED_LEG[leg]} — execution is halted."
                ),
                now=now,
            )
        return True

    async def _decline_provisioning(self, sub: CopySub, decision: str, now: datetime) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await subs_store.disable_sub(
                conn, operator_id=self._operator_id, leader_address=sub.leader_address
            )
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_provisioning_declined",
                risk_decision=decision,
                detail={"leader": sub.leader_address, "sub_name": sub.sub_name},
                master_address=self._master,
                conn=conn,
            )
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=SKIP,
                body=(
                    f"🚫 Copy of {_short(sub.leader_address)} was NOT set up — "
                    f"{decision}. The mapping is disabled; nothing was funded."
                ),
                now=now,
            )

    # --- reconciliation (decision 10) -----------------------------------------

    async def _reconcile(
        self, subs: list[CopySub], now: datetime
    ) -> tuple[dict[int, SubState], list[CopySub]]:
        """Compare every sub's live state against its episodes, classify each
        divergence, adopt the actual state as the new baseline, and page only
        what warrants it. It NEVER places an order to close a gap: auto-
        correcting would fight the operator (they close, we re-open), fight
        liquidations, and turn bookkeeping bugs into live orders.

        Hands back the mappings AS THEY STAND AFTERWARDS beside the states,
        because judging a Loss Budget is part of this step and its verdicts
        land on the mapping row: a sub that breached or was disabled here must
        be the sub the rest of the cycle acts on, not the snapshot taken before
        anything was measured."""
        states: dict[int, SubState] = {}
        judged: list[CopySub] = []
        for sub in subs:
            state = await self._readable_state(sub)
            if state is None:
                # Unprovisioned, or unreadable this cycle. Either way the sub
                # goes back UNJUDGED — carrying whatever the row already said,
                # so a breach recorded on an earlier cycle still binds.
                judged.append(sub)
                continue
            states[sub.id] = state
            # The equity this read already carried, written down before any
            # episode work: a sub with no live episodes still has a curve, and
            # a sub whose reconcile then fails still had a readable equity
            # (issue #137 §6).
            await subs_store.record_sub_equity(
                self._pool, sub.id, state.account_value, now
            )
            for episode in await ep.live_episodes(self._pool, sub.id):
                await self._reconcile_episode(sub, episode, state, now)
            # LAST, so "flat" counts the episodes this cycle just ended: a
            # bracket that fired ends its episode above, and the sub it leaves
            # behind is exactly the flat, wound-down sub the budget disables.
            judged.append(await self._judge_loss_budget(sub, state, now))
        return states, judged

    async def _readable_state(self, sub: CopySub) -> SubState | None:
        """This cycle's live state for one sub, or None when there is not one
        to be had: the mapping is not provisioned yet, or the read failed.

        A read failure is not a divergence. Skipping this sub for one cycle
        costs latency; guessing would cost money."""
        if not sub.is_provisioned:
            return None
        try:
            return await self._sub_state(sub)
        except Exception:
            log.warning(
                "copy executor: could not read sub %s this cycle", sub.sub_name, exc_info=True
            )
            return None

    # --- the Loss Budget (issue #181, amendment D-9) --------------------------

    async def _judge_loss_budget(
        self, sub: CopySub, state: SubState, now: datetime
    ) -> CopySub:
        """One sub's Loss Budget, judged once per cycle from the state this
        cycle already read, and the state machine it drives:

            armed → warned (first cycle ≥80%) → breached (first cycle ≥100%,
            wind-down begins) → disabled (first cycle breached AND flat)

        NO EXTRA READ. The equity is the reconcile's own, and the deposit
        adjustment is a query against the audit trail — so a budget costs the
        exchange nothing, which is what lets it be judged every cycle instead
        of on a timer nobody would trust.

        EVERY MARK IS PERSISTED, which is what makes the machine survive a
        restart: a fresh executor over the same database re-reads a warned sub
        as warned (it does not warn twice) and a breached sub as breached (it
        does not re-arm into copying opens). Nothing here is held in memory
        between cycles.

        SKIPPED ENTIRELY FOR A DISABLED MAPPING. A budget governs COPYING, and
        a mapping that is not copying has nothing to govern — measuring one
        would fire a breach notice about a Leader the operator already stopped.
        A later /copy re-arms it with a fresh baseline anyway (`reenable_sub`).

        The halt is nowhere in here, deliberately: a halt outranks everything
        this decides, and it does so structurally — `run_cycle` returns before
        anything below this point signs — so there is no branch to write.
        Reconciliation runs while halted, which means a halted executor still
        MEASURES budgets and still records a breach; what it cannot do is act
        on one, because it cannot act at all."""
        if sub.loss_budget_usd is None or not sub.enabled:
            return sub
        if sub.budget_baseline_usd is None:
            return await self._arm_loss_budget(sub, state, now)
        assert sub.budget_armed_at is not None  # paired by the schema's CHECK
        loss = budget_loss(
            baseline_usd=sub.budget_baseline_usd,
            deposits_usd=await subs_store.deposits_since(
                self._pool, sub_address=sub.require_address(), since=sub.budget_armed_at
            ),
            equity_usd=state.account_value,
        )
        stage = budget_stage(loss_usd=loss, budget_usd=sub.loss_budget_usd)
        # The warning mark is set by a BREACH too, not only by the warning
        # stage. Without that, a sub that jumped straight past its budget and
        # then recovered to 90% would announce "80% spent" AFTER the operator
        # had already been told the budget was gone.
        warned_at = (
            now if stage != BUDGET_WITHIN and sub.budget_warned_at is None else None
        )
        breached_at = (
            now if stage == BUDGET_BREACHED and sub.budget_breached_at is None else None
        )
        async with self._pool.acquire() as conn, conn.transaction():
            # Compare-and-set on the terms this verdict was reached against: a
            # /copy that raised the budget while this cycle was measuring wins,
            # and its wind-down cancellation is not undone by a verdict about
            # the number it replaced. A lost write means no marks and no
            # notices — the next cycle judges the sub the operator now has.
            if not await subs_store.record_budget_spend(
                conn,
                sub.id,
                spent_usd=loss,
                warned_at=warned_at,
                breached_at=breached_at,
                judged_budget_usd=sub.loss_budget_usd,
                judged_armed_at=sub.budget_armed_at,
            ):
                log.info(
                    "copy executor: sub %s changed under its budget verdict; "
                    "re-judging next cycle",
                    sub.sub_name,
                )
                return sub
            if breached_at is not None:
                await self._loss_budget_event(
                    conn,
                    sub,
                    action="copy_budget_breached",
                    reason=(
                        f"loss budget spent: {budget_spent_of(loss, sub.loss_budget_usd)} since "
                        f"the budget was set — wind-down begins"
                    ),
                    body=(
                        f"🛑 Loss budget SPENT on {_short(sub.leader_address)} — "
                        f"{budget_spent_of(loss, sub.loss_budget_usd)}.\n"
                        f"Winding down: opens, scale-ins and flip open-legs are refused "
                        f"from now on. Exits keep copying and brackets stay maintained, so "
                        f"the copy still follows them out. The sub is disabled once it is "
                        f"flat. /copy with a higher budget resumes it."
                    ),
                    now=now,
                )
            elif warned_at is not None and stage == BUDGET_WARNING:
                await self._loss_budget_event(
                    conn,
                    sub,
                    action="copy_budget_warned",
                    reason=(
                        f"loss budget {int(BUDGET_WARNING_FRACTION * 100)}% spent: "
                        f"{budget_spent_of(loss, sub.loss_budget_usd)}"
                    ),
                    body=(
                        f"⚠️ Loss budget {int(BUDGET_WARNING_FRACTION * 100)}% spent on "
                        f"{_short(sub.leader_address)} — "
                        f"{budget_spent_of(loss, sub.loss_budget_usd)} since the budget was "
                        f"set.\n"
                        f"At the full budget this copy winds down and the sub is disabled "
                        f"once flat. /copy changes the number; /uncopy stops now."
                    ),
                    now=now,
                )
        return replace(
            sub,
            budget_spent_usd=loss,
            budget_warned_at=sub.budget_warned_at or warned_at,
            budget_breached_at=sub.budget_breached_at or breached_at,
        )

    async def _arm_loss_budget(self, sub: CopySub, state: SubState, now: datetime) -> CopySub:
        """Snapshot the equity a new budget is measured from.

        HERE AND NOT IN /copy, because the baseline is a covered-venue equity
        read and the bot process holds no gateway (migration 0039's header).
        The instant recorded is THIS one, not the instant the operator typed
        the command, and that is what keeps the arithmetic honest across
        provisioning: the transfer that funded the sub landed before this read
        and is therefore already inside the baseline, while every top-up after
        it is added back by the deposit adjustment. Baselining at the command
        would count that funding twice and show the whole allocation as lost on
        the first cycle.

        MEASURED FROM WHAT IS THERE, whatever put it there. An adopted orphan
        or a re-copied sub arrives holding money; the operator's number is
        about what happens NEXT, so a sub that starts at $1,400 is judged from
        $1,400 and not from its allocation."""
        assert sub.loss_budget_usd is not None  # only a budgeted sub is baselined
        async with self._pool.acquire() as conn, conn.transaction():
            # Announced only if the snapshot LANDED: `arm_budget` refuses to
            # re-baseline a budget that already has one, and a notice naming a
            # baseline the budget is not measured from would be worse than
            # silence.
            if not await subs_store.arm_budget(
                conn, sub.id, baseline_usd=state.account_value, now=now
            ):
                return sub
            await self._loss_budget_event(
                conn,
                sub,
                action="copy_budget_armed",
                reason=(
                    f"loss budget ${fixed_point(sub.loss_budget_usd)} armed from a "
                    f"${_money(state.account_value)} baseline across every covered venue"
                ),
                body=(
                    f"🎯 Loss budget armed for {_short(sub.leader_address)}: "
                    f"${fixed_point(sub.loss_budget_usd)}, measured from what the sub is "
                    f"worth right now (${_money(state.account_value)}). It is a trigger, "
                    f"not a floor — at the number the copy winds down, and the last open "
                    f"position rides until the leader exits it."
                ),
                now=now,
            )
        return replace(sub, budget_baseline_usd=state.account_value, budget_armed_at=now)

    async def _disable_wound_down_subs(
        self, subs: list[CopySub], states: dict[int, SubState], now: datetime
    ) -> list[CopySub]:
        """Every breached sub that has gone flat, disabled — the wind-down's
        terminal step, run once per cycle after the halt gate because it
        CANCELS resting orders and cancelling is signing.

        A sub this cycle could not read is left alone: flatness is a claim
        about the exchange's state, and the honest answer to an unreadable one
        is next cycle."""
        return [
            await self._disable_if_flat(sub, states[sub.id], now)
            if sub.winding_down and sub.enabled and sub.id in states
            else sub
            for sub in subs
        ]

    async def _disable_if_flat(
        self, sub: CopySub, state: SubState, now: datetime
    ) -> CopySub:
        """A breached sub with nothing left open stops being a copy
        relationship.

        FLAT IS BOTH HALVES — no position the exchange reports in the sub, and
        no live Copy Episode. The exchange's answer alone would disable a sub
        whose close filled but whose episode this cycle has not ended yet; the
        episode's alone would disable one holding a position the operator
        opened by hand in the same sub.

        THE BOOK IS EMPTIED OF OUR TRIGGERS FIRST. Ordinarily there is nothing
        to empty: every bracket is placed POSITION_TPSL, so the venue takes the
        pair down with the position it was sized against. But a STRAY is
        exactly what `BRACKET_VERIFY_INTERVAL` exists to catch — a leg that
        outlived its position, an episode that ended while one rested — and a
        disabled mapping leaves `_maintain_brackets`' scope for good, so a
        stray that survived this moment would rest until the operator or a
        /kill sweep found it. Cancelled BEFORE the flag flips, in that order,
        so a failure anywhere leaves the sub enabled and the next cycle tries
        again rather than leaving an order behind a mapping nothing looks at.

        THE SAME TERMINAL STATE /uncopy PRODUCES, through the same flag and the
        same never-auto-flatten rule — there is nothing left to flatten by
        construction. Copying this Leader again is a fresh, explicit /copy,
        which starts a clean budget on a fresh baseline."""
        assert sub.loss_budget_usd is not None  # only a breached budget winds down
        assert sub.budget_breached_at is not None  # `winding_down` is exactly that
        if state.positions or await ep.live_episodes(self._pool, sub.id):
            return sub
        deferred = await self._cancel_stray_brackets(sub, now)
        if deferred is not None:
            await self._report_deferred_disable(sub, deferred, now)
            return sub
        # Cleared: the next time this sub wedges, the operator hears about it
        # afresh rather than being silenced by a reason that no longer holds.
        self._deferred_disables.pop(sub.id, None)
        loss = sub.budget_spent_usd or Decimal(0)
        async with self._pool.acquire() as conn, conn.transaction():
            # Conditional on the sub still being enabled and still carrying THIS
            # breach, so the announcement happens exactly once and a wind-down
            # the operator cancelled a moment ago is not disabled anyway.
            if not await subs_store.disable_for_spent_budget(
                conn, sub.id, breached_at=sub.budget_breached_at
            ):
                return sub
            await self._loss_budget_event(
                conn,
                sub,
                action="copy_budget_disabled",
                reason=(
                    f"wound down and flat: {budget_spent_of(loss, sub.loss_budget_usd)} — "
                    f"mapping disabled"
                ),
                body=(
                    f"⏹ Copy of {_short(sub.leader_address)} is DISABLED — its loss budget "
                    f"is spent and the sub is now flat. Final "
                    f"{budget_spent_of(loss, sub.loss_budget_usd)}. Nothing further is "
                    f"copied; copying this leader again is a fresh /copy."
                ),
                now=now,
            )
        return replace(sub, enabled=False)

    async def _cancel_stray_brackets(self, sub: CopySub, now: datetime) -> str | None:
        """Take this sub's own resting triggers off the book. Returns None when
        the book is now known to be clear of them, or the SENTENCE saying why
        it could not be — which the caller reports and then defers the disable.

        OURS ONLY, intersected against the bracket ids Epigone recorded for
        this sub. Cancelling everything resting would be simpler and would
        break decision 10's never-touch rule: a limit order the operator placed
        by hand in that sub is theirs, and a Loss Budget is not a mandate to
        clear someone else's book.

        THE HALT GATE, as late as it can be, and the same one `_place_brackets`
        carries: a cancel is a signature. A halt here defers the disable —
        which costs nothing, since a halt's own sweep enumerates and cancels
        per sub anyway (decision 1) and the mapping stays in wind-down
        meanwhile, refusing every entry."""
        known = await ep.sub_bracket_orders(self._pool, sub.id)
        if not known:
            return None
        try:
            resting = [order for order in await self._open_orders(sub) if order.order_id in known]
        except Exception:
            log.warning(
                "copy executor: order book unreadable for %s; deferring its budget disable",
                sub.sub_name,
                exc_info=True,
            )
            return "this sub's order book could not be read"
        if not resting:
            return None
        specs: list[CancelSpec] = []
        for order in resting:
            spec = await self._spec(order.coin)
            if spec is None:
                # The one order shape that cannot be cancelled by oid is one
                # whose coin has no asset id. Defer rather than disable around
                # it: this is the stray the whole step exists for — and it is
                # the one deferral that does NOT resolve itself, which is why
                # the caller has to say so out loud.
                log.warning(
                    "copy executor: %s has no asset id; deferring %s's budget disable",
                    order.coin,
                    sub.sub_name,
                )
                return (
                    f"{order.coin} has no asset id in the universe — a delisted coin's "
                    f"resting bracket cannot be cancelled by this executor"
                )
            specs.append(CancelSpec(asset=spec.asset_id, oid=order.order_id))
        if await is_halted(self._pool):
            log.warning(
                "copy executor: halted before clearing %s's brackets — disable deferred",
                sub.sub_name,
            )
            return "execution is halted, and a cancel is a signature"
        self._exec.decision = (
            f"loss budget spent and the sub is flat: cancelling {len(specs)} resting "
            f"bracket leg(s) before disabling the mapping"
        )
        try:
            await self._exec.cancel_orders(specs, vault_address=sub.require_address())
        except ExecutionError:
            # Already on the trail via the audited gateway. The mapping stays
            # enabled and wound down; the next cycle tries again.
            log.exception("copy executor: could not clear %s's brackets", sub.sub_name)
            return "the cancel did not reach the exchange"
        await self._notify(
            kind=ACTION,
            body=(
                f"🧹 Cancelled {len(specs)} leftover bracket order(s) in "
                f"{_short(sub.leader_address)}'s sub before disabling it."
            ),
            now=now,
        )
        return None

    async def _report_deferred_disable(
        self, sub: CopySub, reason: str, now: datetime
    ) -> None:
        """A wound-down sub that should have been disabled and could not be.

        SAID ONCE PER REASON, not once per cycle: the executor loops in
        seconds, and a sub wedged on a delisted coin's stray trigger would
        otherwise write a row and a chat line every few seconds for as long as
        it took anyone to notice. Once per reason still fires again when the
        reason CHANGES — an unreadable book that becomes a missing asset id is
        a different problem — and a restart re-says it once, which is the right
        side of that trade for a state whose whole failing is being quiet.

        IT NEEDS A TRACE AT ALL because the deferral has no natural end. Most
        of these resolve themselves on the next cycle (a blipped read, a halt
        the operator lifts) and would never be worth a word; a delisted coin's
        resting trigger does not, and leaves the mapping enabled-but-wound-down
        with a live order behind it indefinitely. `/copies` shows the sub as
        winding down either way — what only this can say is that it was
        SUPPOSED to be finished and is stuck.

        NOT A PAGER CASE, like everything else this feature does: the sub is
        flat, it refuses every entry, and the stray is a reduce-only trigger
        with no position under it. It wants a human eventually, not now."""
        if self._deferred_disables.get(sub.id) == reason:
            return
        self._deferred_disables[sub.id] = reason
        async with self._pool.acquire() as conn, conn.transaction():
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_budget_disable_deferred",
                risk_decision=(
                    f"loss budget spent and the sub is flat, but the mapping was NOT "
                    f"disabled: {reason}"
                ),
                detail={"sub_id": sub.id, "leader": sub.leader_address, "reason": reason},
                master_address=self._master,
                conn=conn,
            )
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=SKIP,
                body=(
                    f"⏳ {_short(sub.leader_address)}'s copy is spent and flat but could "
                    f"NOT be closed out: {reason}.\n"
                    f"It stays wound down — no new positions, exits still copied — and the "
                    f"executor retries every cycle. If this does not clear, /uncopy it and "
                    f"cancel what is resting in that sub from the master wallet."
                ),
                now=now,
            )

    async def _loss_budget_event(
        self,
        conn: asyncpg.Connection,
        sub: CopySub,
        *,
        action: str,
        reason: str,
        body: str,
        now: datetime,
    ) -> None:
        """One budget state change, recorded and announced in the CALLER'S
        transaction.

        The audit row is written with the state change it describes and is
        independent of chat delivery: a Telegram outage can delay the
        operator's notice but must never erase the record of a wind-down.

        NOT A PAGER CASE, and that is an operator decision rather than an
        oversight (issue #181): a page means something is BROKEN, and a budget
        doing exactly what it was set to do is not that. The monitor's action
        tuple is deliberately unchanged."""
        await self._audit.record_event(
            actor=EXECUTOR_ACTOR,
            action=action,
            risk_decision=reason,
            detail={
                "sub_id": sub.id,
                "leader": sub.leader_address,
                # Through `fixed_point`, not `str`: a NUMERIC read back from
                # Postgres can carry an exponent, and `5E+2` on the trail is
                # the #185 lesson said in a place nobody would look twice at.
                "budget_usd": _figure(sub.loss_budget_usd),
                "baseline_usd": _figure(sub.budget_baseline_usd),
            },
            master_address=self._master,
            conn=conn,
        )
        await notify(conn, operator_id=self._operator_id, kind=ACTION, body=body, now=now)

    async def _reconcile_episode(
        self, sub: CopySub, episode: ep.CopyEpisode, state: SubState, now: datetime
    ) -> None:
        position = state.positions.get(episode.coin)
        if position is None:
            await self._classify_vanished(sub, episode, state, now)
            return
        if position.side.value != episode.side:
            # The exchange says we hold the OPPOSITE side of what the episode
            # records. Nothing in this executor can produce that — a flip ends
            # one episode and opens another — so it is a bug or an outside
            # actor, and decision 10 says adopt nothing and page.
            await self._page(
                sub,
                action="copy_divergence_unclassifiable",
                reason=(
                    f"{episode.coin}: episode says {episode.side}, exchange says "
                    f"{position.side.value} — adopting nothing"
                ),
                detail={"episode_id": episode.id, "coin": episode.coin},
                now=now,
                notify_once=episode.id,
            )
            return
        held = position.size_coin
        if held is not None and held != episode.size_coin:
            await ep.adopt_size(self._pool, episode.id, held)
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_size_adopted",
                risk_decision=(
                    f"{episode.coin}: bookkept {episode.size_coin} → exchange {held} "
                    f"(partial fill, residue or manual resize; exchange is truth)"
                ),
                detail={"episode_id": episode.id, "coin": episode.coin, "size_coin": str(held)},
                master_address=self._master,
            )

    async def _classify_vanished(
        self, sub: CopySub, episode: ep.CopyEpisode, state: SubState, now: datetime
    ) -> None:
        """The position is gone. WHY decides what happens next, and the three
        answers behave differently: a fired bracket ends the Copy Episode
        under rule g1 (no re-entry until the Leader closes and freshly
        re-opens), a liquidation pages, and anything else is the operator
        winning — we adopt it silently and never re-open."""
        brackets = await ep.bracket_orders(self._pool, episode.id)
        if brackets and not await self._brackets_still_resting(sub, brackets):
            await self._end_episode(
                sub,
                episode,
                reason=ep.ENDED_BRACKET,
                headline=(
                    f"🎯 Bracket exit — {episode.coin} in {_short(sub.leader_address)}'s sub is "
                    f"closed by our own TP/SL. This copy episode is OVER: further events on "
                    f"this position are skipped until the leader closes and re-opens."
                ),
                now=now,
            )
            return
        liquidated = await self._was_liquidated(sub, episode)
        if liquidated is None:
            # Unreadable, so unclassifiable (decision 10's last row): adopt
            # nothing, page, and re-flag every cycle until the read succeeds
            # and one of the confident branches above takes it.
            await self._page(
                sub,
                action="copy_divergence_unclassifiable",
                reason=(
                    f"{episode.coin}: the position is gone and the fills are "
                    f"unreadable, so nothing can say whether it was liquidated — "
                    f"adopting nothing, the episode stays open"
                ),
                detail={"episode_id": episode.id, "coin": episode.coin},
                now=now,
                notify_once=episode.id,
            )
            return
        if liquidated:
            await self._end_episode(
                sub,
                episode,
                reason=ep.ENDED_LIQUIDATED,
                headline=(
                    f"🚨 LIQUIDATED — {episode.coin} in {_short(sub.leader_address)}'s sub "
                    f"(equity now ${state.account_value}). The episode is closed; the "
                    f"allocation is what it cost."
                ),
                now=now,
                pager=True,
            )
            return
        await self._end_episode(
            sub,
            episode,
            reason=ep.ENDED_OPERATOR,
            headline=(
                f"↩️ {episode.coin} in {_short(sub.leader_address)}'s sub is gone with no "
                f"trigger and no liquidation — closed outside the copy loop. Adopted; "
                f"nothing re-opened."
            ),
            now=now,
        )

    async def _brackets_still_resting(
        self, sub: CopySub, brackets: list[ep.BracketOrder]
    ) -> bool:
        """Whether any of this episode's trigger legs is still on the book.
        Enumerating orders costs ten times a position read, so it is asked
        only here — when a position has already vanished and the answer
        decides between "our bracket did it" and "someone else did"."""
        resting = {order.order_id for order in await self._open_orders(sub)}
        return any(bracket.order_id in resting for bracket in brackets)

    async def _was_liquidated(self, sub: CopySub, episode: ep.CopyEpisode) -> bool | None:
        """Ask the FILLS, not the equity.

        ADR-0007's table names this case "position gone + equity cratered",
        which is a heuristic that needs a threshold nobody has calibrated — a
        $200 position liquidating inside a $2000 allocation moves equity by
        10%, which no honest threshold separates from a losing exit. The fill
        stream states it outright: Hyperliquid tags a liquidation in the
        fill's own `dir`. Strictly more precise, at the cost of one read that
        only happens on an unexplained disappearance.

        THREE ANSWERS, not two. `None` means the fills could not be read, and
        it must not collapse into False: that would label a possible
        liquidation "the operator closed it", adopt the state, and page
        nobody. Decision 10 has a row for exactly this — unclassifiable: adopt
        nothing, page, re-flag until resolved."""
        for _ in range(2):  # userFills + userTwapSliceFills (FILL_ENDPOINTS)
            await self._budget.spend(FILLS_WEIGHT)
        try:
            fills = await self._read.get_fills_since(sub.require_address(), episode.opened_at)
        except Exception:
            log.warning("copy executor: fills unreadable for %s", sub.sub_name, exc_info=True)
            return None
        return any(
            fill.coin == episode.coin and "Liquidat" in fill.direction for fill in fills
        )

    async def _end_episode(
        self,
        sub: CopySub,
        episode: ep.CopyEpisode,
        *,
        reason: str,
        headline: str,
        now: datetime,
        pager: bool = False,
    ) -> None:
        async with self._pool.acquire() as conn, conn.transaction():
            await ep.end_episode(conn, episode.id, reason=reason, ended_at=now)
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                # A pager case gets its OWN action name (decision 11 puts
                # liquidations on the 🚨 monitor path): the monitor keys on
                # actions, and one shared `copy_episode_ended` would make it
                # page on every ordinary bracket exit too.
                action="copy_episode_liquidated" if pager else "copy_episode_ended",
                risk_decision=f"{reason}: {headline}",
                detail={"episode_id": episode.id, "coin": episode.coin, "reason": reason},
                master_address=self._master,
                conn=conn,
            )
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=PAGER if pager else ACTION,
                body=headline,
                now=now,
            )

    async def _page(
        self,
        sub: CopySub,
        *,
        action: str,
        reason: str,
        detail: dict[str, object],
        now: datetime,
        notify_once: int | None = None,
    ) -> None:
        """Record the incident and tell the operator. `notify_once` is for the
        findings that RE-FLAG every loop (decision 10's unclassifiable row):
        the audit row still lands each cycle, which is what "until resolved"
        means, but the chat hears it once — the #52 monitor owns re-paging,
        with a throttle this queue does not have."""
        chat = notify_once is None or notify_once not in self._flagged
        if notify_once is not None:
            self._flagged.add(notify_once)
        async with self._pool.acquire() as conn, conn.transaction():
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action=action,
                risk_decision=reason,
                detail=detail,
                master_address=self._master,
                conn=conn,
            )
            if chat:
                await notify(
                    conn,
                    operator_id=self._operator_id,
                    kind=PAGER,
                    body=f"🚨 {_short(sub.leader_address)}'s copy sub: {reason}",
                    now=now,
                )

    # --- brackets (decision 6, and decision 9's r1a) ---------------------------

    async def _maintain_brackets(
        self, subs: list[CopySub], states: dict[int, SubState], now: datetime
    ) -> None:
        """Every live position in a bracket-mode sub HAS its triggers resting.

        Expressed as a property to restore rather than a /resume transition to
        detect, deliberately. Decision 9's r1a is about one specific way the
        triggers go missing — the halt sweep cancels them and hold-and-alert
        keeps the positions, so a bracket sub's survivors come out of a halt
        unstopped — but it is not the only way, and transition detection would
        be lost across a restart. Restoring an invariant every minute covers
        the halt case, the restart case, and the operator's own cancel with
        one mechanism.

        Slower than the loop because the check costs a weight-20 enumeration
        per bracket sub against a weight-2 position read."""
        if self._brackets_checked_at is not None:
            if now - self._brackets_checked_at < BRACKET_VERIFY_INTERVAL:
                return
        self._brackets_checked_at = now
        for sub in subs:
            if not sub.brackets or not sub.is_provisioned or sub.id not in states:
                continue
            live = await ep.live_episodes(self._pool, sub.id)
            if not live:
                continue
            try:
                resting = {order.order_id for order in await self._open_orders(sub)}
            except Exception:
                log.warning(
                    "copy executor: order book unreadable for %s", sub.sub_name, exc_info=True
                )
                continue
            for episode in live:
                position = states[sub.id].positions.get(episode.coin)
                if position is None:
                    continue  # the reconcile above already classified it
                known = await ep.bracket_orders(self._pool, episode.id)
                if any(order.order_id in resting for order in known):
                    continue
                await ep.forget_brackets(self._pool, episode.id)
                await self._place_brackets(
                    sub, episode, position, now, replaced=bool(known)
                )

    async def _place_brackets(
        self,
        sub: CopySub,
        episode: ep.CopyEpisode,
        position: Position,
        now: datetime,
        *,
        replaced: bool,
    ) -> None:
        """Place the sub's configured TP/SL as exchange-native triggers.

        ANCHORED TO `clearinghouseState`'s entry price, which is what decision
        9's (r1a) asks for and is not the same number as the episode's opening
        fill: after a scale-in the exchange reports the BLENDED entry, and a
        bracket computed off the first fill would sit at percentages of a
        price we no longer average. The episode's `entry_price` stays what it
        is — the price we first paid — and is not re-used for this.

        POSITION_TPSL grouping, so the exchange sizes each leg against the
        position as it stands: a later scale-in cannot leave the bracket
        covering only part of what we hold."""
        spec = await self._spec(episode.coin)
        if spec is None:
            return
        size = position.size_coin
        if size is None:
            return
        is_long = episode.side == Side.LONG.value
        legs: list[OrderSpec] = []
        tags: list[str] = []
        for pct, tpsl, take_profit in (
            (sub.take_profit_pct, TpSl.TAKE_PROFIT, True),
            (sub.stop_loss_pct, TpSl.STOP_LOSS, False),
        ):
            if pct is None:
                continue
            try:
                trigger_at = trigger_price(
                    position.entry_price,
                    pct=pct,
                    is_long=is_long,
                    take_profit=take_profit,
                    sz_decimals=spec.sz_decimals,
                )
            except UnpriceableError:
                log.warning("copy executor: unpriceable %s bracket on %s", tpsl.value, episode.coin)
                continue
            legs.append(
                OrderSpec(
                    asset=spec.asset_id,
                    is_buy=not is_long,  # a bracket exits, so it trades the other way
                    size=size,
                    limit_price=trigger_at,
                    reduce_only=True,
                    trigger=Trigger(trigger_price=trigger_at, is_market=True, tpsl=tpsl),
                )
            )
            tags.append(f"{tpsl.value.upper()} {pct}% @ {trigger_at}")
        if not legs:
            return
        # THE HALT GATE, as late as it can be. Every other copy order is an
        # IOC and cannot outlive the sweep's enumeration; a bracket trigger
        # RESTS, so a bracket landing after a halt is exactly the #143
        # residual race, for the one order type that can reach it. A halt seen
        # after signing stays a reconciliation obligation — the next cycle's
        # bracket check re-reads the book, and the watchdog's per-sub sweep
        # cancels what it finds.
        if await is_halted(self._pool):
            log.warning(
                "copy executor: halted before placing brackets on %s — not sent",
                episode.coin,
            )
            await self._notify(
                kind=SKIP,
                body=(
                    f"🛑 Bracket for {episode.coin} ({_short(sub.leader_address)}) NOT "
                    f"placed — execution is halted. That position is UNSTOPPED until "
                    f"/resume."
                ),
                now=now,
            )
            return
        self._exec.decision = (
            f"bracket {'re-placed' if replaced else 'placed'} for episode {episode.id} "
            f"({sub.copy_mode} mode, anchored to the exchange's entry "
            f"{position.entry_price})"
        )
        try:
            results = await self._exec.place_orders(
                legs, grouping=Grouping.POSITION_TPSL, vault_address=sub.require_address()
            )
        except ExecutionError:
            log.exception("copy executor: bracket placement failed for episode %d", episode.id)
            return
        for leg, result in zip(legs, results, strict=False):
            if isinstance(result, OrderResting):
                assert leg.trigger is not None
                await ep.record_bracket(
                    self._pool,
                    episode_id=episode.id,
                    order_id=result.oid,
                    tpsl=leg.trigger.tpsl.value,
                    placed_at=now,
                )
        await self._notify(
            kind=ACTION,
            body=(
                f"{'🔁 Bracket re-placed' if replaced else '🛡 Bracket placed'} on "
                f"{episode.coin} ({_short(sub.leader_address)}): {' · '.join(tags)}"
            ),
            now=now,
        )
        if replaced:
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                # NOT `bracket_replaced_after_resume` (ADR-0007 decision 9's
                # original name): the operator settled the per-cycle invariant
                # over r1a's resume-only rule (amendment D-1), and a resume is
                # now the MINORITY of this event's firings — a restart, a
                # partial fill, or the operator's own cancel all reach it. A
                # name that says "after resume" would be a lie in the trail
                # most of the time.
                action="bracket_restored",
                risk_decision=self._exec.decision,
                detail={"episode_id": episode.id, "coin": episode.coin, "legs": tags},
                master_address=self._master,
            )

    # --- the backlog ----------------------------------------------------------

    async def _drain_backlog(self, subs: list[CopySub], states: dict[int, SubState]) -> None:
        by_leader = {sub.leader_address: sub for sub in subs if sub.is_provisioned}
        if not by_leader:
            return
        async with self._pool.acquire() as conn:
            events = await outstanding_events(
                conn,
                EXECUTOR_CONSUMER,
                # ADR-0007 decision 4's mandatory filter, as ADR-0009
                # restates it: whichever LANE owns production, never whichever
                # transport. A source filter cannot survive a failover in
                # either position — see epigone.position_events.
                authoritative=True,
                traders=list(by_leader),
            )
        try:
            for event in events:
                sub = by_leader[event.trader_address]
                state = states.get(sub.id)
                if state is None:
                    continue  # this sub was unreadable this cycle; try again next
                try:
                    await self._handle(event, sub, state)
                except ExecutionError:
                    # Already on the trail via the audited gateway. The event is
                    # claimed, so it will not be retried: a missed copy, which is
                    # the direction ADR-0006 chose.
                    log.exception("copy executor: event %d failed on the wire", event.id)
        finally:
            # In a `finally`, because a cycle that dies partway through still
            # claimed and audited everything it got to, and the operator should
            # hear about those rather than about nothing.
            await self._skips.flush(
                self._pool, operator_id=self._operator_id, now=self._clock.now()
            )

    async def _handle(self, event: ClaimableEvent, sub: CopySub, state: SubState) -> None:
        kind = event.event.kind
        if kind == "open":
            await self._entry(event, sub, state, leg="open")
        elif kind == "scale_in":
            await self._entry(event, sub, state, leg="scale_in")
        elif kind == "close":
            await self._exit(event, sub, state, leg="close")
        elif kind == "scale_out":
            await self._exit(event, sub, state, leg="scale_out")
        elif kind == "flip":
            # Decision 3: one event, TWO orders. The close leg is reduce-only
            # and structurally cannot over-close or reverse; the open leg then
            # runs the FULL fresh-open pipeline — halt re-check, staleness
            # guard, risk policy — as if it were a new open. There is exactly
            # one code path that opens positions, and this is it calling it.
            # The CLOSE leg claims the single event both legs come from; the
            # open leg then runs without claiming again — which is exactly why
            # the open leg only runs if the close leg reports that THIS
            # instance owns the event. Without that guard, an instance that
            # LOST the claim would return from the close leg and then place an
            # unclaimed open, doubling the position the winner already opened.
            if await self._exit(event, sub, state, leg="flip_close"):
                await self._entry(event, sub, state, leg="flip_open")
        else:  # pragma: no cover - the CHECK constraint enumerates the kinds
            log.error("copy executor: unknown event kind %r on event %d", kind, event.id)

    # --- entries (risk-increasing: guarded, one shot) -------------------------

    async def _entry(
        self, event: ClaimableEvent, sub: CopySub, state: SubState, *, leg: str
    ) -> None:
        now = self._clock.now()
        coin = event.event.coin
        # A flip is ONE event and two orders (decision 3), so its close leg
        # already claimed; the open leg audits and reports without claiming
        # again — otherwise the flip's own claim would swallow the open leg's
        # skip reason and the operator would never hear why we ended flat.
        claim = leg != "flip_open"
        skip = await self._entry_skip(event, sub, state, leg=leg, now=now)
        if skip is not None:
            await self._claim_and_skip(event, sub, skip, now, claim=claim)
            return
        spec = await self._spec(coin)
        assert spec is not None  # _entry_skip proved it resolves
        side = event.event.side
        assert side is not None  # the CHECK constraint guarantees it for open/flip
        is_long = side == Side.LONG.value
        try:
            mark = await self._mark(coin)
            ask = await self._entry_ask(event, sub, state, leg=leg, mark=mark, spec=spec)
            price = ioc_limit_price(mark, is_buy=is_long, sz_decimals=spec.sz_decimals)
        except (UnpriceableError, LeverageUnknownError) as exc:
            await self._claim_and_skip(
                event,
                sub,
                _Skip(REASON_NOT_MIRRORABLE, f"not mirrorable: {exc}", {"coin": coin, "leg": leg}),
                now,
                claim=claim,
            )
            return
        # The caps are judged against what the EXCHANGE says is committed, not
        # against bookkeeping — decision 10's self-damping principle applied to
        # margin. The sub's own equity is one pool, so a position the operator
        # opened by hand in it counts too.
        held = state.positions.get(coin)
        verdict = self._policy.judge_entry(
            coin=coin,
            requested_stake_usd=ask.stake_usd,
            leverage=ask.leverage,
            coin_stake_used=Decimal(0) if held is None else held.margin,
            sub_stake_used=committed_stake(state.positions.values()),
            limits=self._limits,
        )
        if not verdict.allowed:
            await self._claim_and_skip(
                event,
                sub,
                _Skip(REASON_RISK_DECLINED, verdict.decision, {"coin": coin, "leg": leg}),
                now,
                claim=claim,
            )
            return
        assert verdict.stake_usd is not None  # an allowed entry always grants a stake
        try:
            size = clamped_size(
                ask.size_coin,
                asked_stake=ask.stake_usd,
                granted_stake=verdict.stake_usd,
                sz_decimals=spec.sz_decimals,
            )
        except UnpriceableError as exc:
            # The grant survived the policy's $10 check in dollars but not the
            # asset's own precision — a real answer, not a bug, and one the
            # operator should read rather than see as a silent no-op.
            await self._claim_and_skip(
                event,
                sub,
                _Skip(
                    REASON_BELOW_MINIMUM,
                    f"did not enter: clamped below this asset's precision — {exc}",
                    {"coin": coin, "leg": leg},
                ),
                now,
                claim=claim,
            )
            return
        dust = self._rounded_below_minimum(size, mark, coin, leg)
        if dust is not None:
            await self._claim_and_skip(event, sub, dust, now, claim=claim)
            return
        order = OrderSpec(
            asset=spec.asset_id,
            is_buy=is_long,
            size=size,
            limit_price=price,
            tif=Tif.IOC,
        )
        attempt = await self._claim_and_attempt(
            event,
            action=f"copy_{leg}",
            risk_decision=verdict.decision,
            request={
                "coin": coin,
                "side": side,
                "size": str(size),
                "limit_price": str(price),
                "stake_usd": str(verdict.stake_usd),
                "leverage": ask.leverage.value,
                "clamped": verdict.clamped,
            },
            claim=claim,
        )
        if attempt is None:
            return
        if await self._halted_before_signing(event, sub, attempt, leg, now):
            return
        self._exec.decision = verdict.decision
        if leg != "scale_in" and not await self._set_leverage(
            sub, spec, ask.leverage, attempt, coin, leg, now
        ):
            return
        # THE HALT GATE AGAIN, and not redundantly: `_set_leverage` is a
        # SIGNATURE with its own HTTP round trip, so the check above is a
        # request old by the time the order is signed. Every leg that reaches
        # the wire carries its own check — the same rule provisioning's three
        # legs follow. A halt landing here leaves a leverage setting on an
        # asset we hold nothing in, which changes nothing about the account.
        if leg != "scale_in" and await self._halted_before_signing(
            event, sub, attempt, leg, now
        ):
            return
        results = await self._exec.place_orders(
            [order], vault_address=sub.require_address()
        )
        await self._settle_entry(
            event, sub, state, results[0], size, mark, coin, side, leg, attempt, ask.leverage
        )

    def _rounded_below_minimum(
        self, size: Decimal, mark: Decimal, coin: str, leg: str
    ) -> _Skip | None:
        """Whether the order we are about to send is dust once ROUNDED.

        The policy judges the exchange minimum in dollars, on the stake it
        granted — which is the right question for a clamp, and not quite the
        question the exchange asks. The venue judges the ORDER, and the order
        has been rounded DOWN to the asset's precision since: on a coarse
        asset a $10.40 grant can become one unit worth $6. Sending that buys a
        guaranteed MIN_NOTIONAL reject and an alarming audit row, which is
        exactly what the constant exists to avoid — so the last word belongs to
        the size that will actually be signed. The exit path has carried the
        same check since A4 (`_sub_minimum_skip`); this is its entry twin."""
        notional = size * mark
        if notional >= MIN_ORDER_NOTIONAL:
            return None
        return _Skip(
            REASON_BELOW_MINIMUM,
            f"did not enter: {size} {coin} rounds to about ${notional:.2f} at this asset's "
            f"precision, under the exchange's ${MIN_ORDER_NOTIONAL} minimum order value — "
            f"not sent, since it would only be rejected",
            {"coin": coin, "leg": leg, "size": str(size), "notional": str(notional)},
        )

    async def _entry_ask(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        state: SubState,
        *,
        leg: str,
        mark: Decimal,
        spec: AssetSpec,
    ) -> _Ask:
        """What this entry would do if nothing capped it: leverage, stake, and
        the coin-unit size those imply (amendment D-4).

        THE TWO LEGS ASK DIFFERENT QUESTIONS. A fresh open chooses a leverage —
        the sub's mode answers, the caps trim it — and then sizes Base Stake
        against it. A SCALE-IN chooses nothing: the position already runs at a
        leverage the exchange is enforcing, and its size is a fraction of what
        we ACTUALLY HOLD (decision 10's self-damping principle). Its stake is
        therefore DERIVED — the margin that added notional will consume at the
        position's own leverage — which is what lets the same stake caps bound
        a scale-in without re-deciding anything about it."""
        if leg == "scale_in":
            position = state.positions[event.event.coin]
            assert position.size_coin is not None  # _entry_skip proved it
            fraction = scale_fraction(event.event.prev_size_coin, event.event.size_coin)
            size = relative_size(position.size_coin, fraction, spec.sz_decimals)
            # The venue's own figure for the live position. Floored at 1x: a
            # leverage of zero is not a thing an open position has, and
            # dividing by it would be the one arithmetic error that silently
            # grants infinite stake.
            live = max(int(position.leverage), 1)
            leverage = LeverageChoice(
                value=live,
                asked=live,
                reason=f"the position's own {live}x, unchanged by a scale",
            )
            return _Ask(leverage=leverage, stake_usd=size * mark / live, size_coin=size)
        stats = await self._market_stats()
        assert stats is not None  # _entry_skip defers the whole entry when it is None
        market = stats.get(event.event.coin)
        assert market is not None  # _entry_skip denies an unlisted coin
        leverage = resolve_leverage(
            mode=sub.leverage_mode,
            fixed_leverage=sub.fixed_leverage,
            leader_leverage=event.event.leverage,
            asset_max_leverage=market.max_leverage,
            limits=self._limits,
        )
        return _Ask(
            leverage=leverage,
            stake_usd=sub.base_stake_usd,
            size_coin=open_size(
                sub.base_stake_usd, Decimal(leverage.value), mark, spec.sz_decimals
            ),
        )

    async def _set_leverage(
        self,
        sub: CopySub,
        spec: AssetSpec,
        leverage: LeverageChoice,
        attempt: AuditedAttempt,
        coin: str,
        leg: str,
        now: datetime,
    ) -> bool:
        """Put the sub on ISOLATED margin at this leverage for this asset,
        before the first order of the episode touches it (amendment D-4).
        Returns whether the order may proceed.

        ISOLATED IS THE POINT, not the leverage: it is what makes the Base
        Stake the worst case. Under cross margin a position's loss reaches the
        whole sub's balance, and "the stake is what you can lose" would be a
        sentence with no mechanism behind it.

        SET EVERY OPEN, not tracked and skipped when unchanged. Leverage is
        exchange-side state that /copy can change, the operator can change in
        the UI, and a re-adopted sub can arrive carrying; a cache of what we
        last set would be a second source of truth that is wrong exactly when
        it matters. Opens are rare, and this is one extra signature on each.

        A refusal ENDS the entry. The position would open at whatever leverage
        the account happened to carry — which is a different position from the
        one the policy judged — so the honest answer is not to open it."""
        try:
            await self._exec.update_leverage(
                spec.asset_id,
                leverage.value,
                is_cross=False,
                vault_address=sub.require_address(),
            )
        except ExecutionError as exc:
            log.warning("copy executor: could not set %s leverage on %s", coin, sub.sub_name)
            await self._finish(
                attempt,
                outcome_detail={
                    "status": "leverage_not_set",
                    "coin": coin,
                    "leverage": leverage.value,
                    "error": str(exc),
                },
                body=(
                    f"🚫 Copy {leg.replace('_', ' ')} on {coin} "
                    f"({_short(sub.leader_address)}) NOT sent — the exchange refused to set "
                    f"{leverage.value}x isolated margin, and opening at whatever leverage the "
                    f"sub carries is not the position the risk policy judged."
                ),
                now=now,
            )
            return False
        return True

    async def _entry_skip(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        state: SubState,
        *,
        leg: str,
        now: datetime,
    ) -> _Skip | None:
        """Every reason an entry does not happen, in the order that answers
        cheapest first. All of them CLAIM the event (claim-means-handled)."""
        coin = event.event.coin
        if sub.winding_down:
            # FIRST, because it is free — the verdict was reached during this
            # cycle's reconcile, from state already in hand — and because it is
            # the bluntest of the reasons: the operator has said they are done
            # losing money on this Leader, and no later question can change
            # that answer. Every leg that reaches `_entry` is risk-increasing
            # by construction (open, scale_in, flip_open), which is exactly the
            # set the wind-down refuses; exits never come through here, and the
            # exits-never-decline contract is untouched.
            assert sub.loss_budget_usd is not None  # a breach implies a budget
            verdict = self._policy.judge_wind_down(
                coin=coin,
                loss_usd=sub.budget_spent_usd or Decimal(0),
                budget_usd=sub.loss_budget_usd,
            )
            return _Skip(REASON_LOSS_BUDGET, verdict.decision, {"coin": coin, "leg": leg})
        age = now - event.observed_at
        if age > ENTRY_STALENESS_GUARD:
            # Decision 8: risk-increasing actions only. Five minutes never
            # trips in healthy operation and always trips across an outage —
            # firing a burst of stale opens on restart is the failure this
            # exists to prevent.
            return _Skip(
                REASON_STALE,
                f"stale entry: observed {int(age.total_seconds())}s ago, past the "
                f"{int(ENTRY_STALENESS_GUARD.total_seconds())}s guard",
                {"coin": coin, "leg": leg, "age_seconds": int(age.total_seconds())},
            )
        if await self._spec(coin) is None:
            return _Skip(
                REASON_UNORDERABLE,
                f"{coin} has no asset id in the universe — cannot be ordered",
                {"coin": coin, "leg": leg},
            )
        episode = await ep.live_episode(self._pool, sub_id=sub.id, coin=coin)
        if leg == "scale_in":
            if episode is None:
                return await self._no_local_position(sub, coin, leg)
            if state.positions.get(coin) is None or state.positions[coin].size_coin is None:
                return _Skip(
                    REASON_NO_LOCAL_POSITION,
                    f"no live {coin} position to scale — the exchange shows none",
                    {"coin": coin, "leg": leg},
                )
        else:
            if episode is not None:
                return _Skip(
                    REASON_COIN_OCCUPIED,
                    f"already in a {coin} copy episode — a fresh open on a coin we "
                    f"already hold would double the position",
                    {"coin": coin, "leg": leg},
                )
            if coin in state.positions:
                # Decision 10's table: a position with no episode is the
                # operator's own, and we never touch it.
                return _Skip(
                    REASON_COIN_OCCUPIED,
                    f"coin occupied: {coin} is held in this sub with no copy episode — "
                    f"the operator's own position, left alone",
                    {"coin": coin, "leg": leg},
                )
        if leg in ("open", "flip_open"):
            # Decision 7's letter, and the operator confirmed it (amendment
            # D-2): "on ENTRY events only (open, flip's open leg)". A scale-in
            # is not one of them — the position is already open on a Leader we
            # already judged, and the gate exists to ask "is this still the
            # trader whose stats earned the copy?", which is a question about
            # STARTING a position, not about following one we are already in.
            # Gating a scale-in would also spend a weight-2 fetch on the most
            # common event kind. Staleness (decision 8) is unchanged: a
            # scale-in stays guarded, because it increases risk.
            liveness = await self._liveness_skip(event, coin, leg)
            if liveness is not None:
                return liveness
            # The Liquidity Floor, and it speaks EXACTLY HERE: at the open that
            # starts a Copy Episode, and nowhere else. A live episode is never
            # interrupted by it (a scale-in never reaches this branch, so it
            # copies even after the coin has gone thin) and never trapped by it
            # (exits do not consult the policy at all). A flip ends an episode,
            # so a flip's open leg IS a fresh entry and is judged fresh —
            # which is how a sub-floor coin makes a flip end flat.
            floor = await self._floor_skip(coin, leg)
            if floor is not None:
                return floor
        return None

    async def _floor_skip(self, coin: str, leg: str) -> _Skip | None:
        """Is this coin a Copyable Coin — does its live market clear the
        Liquidity Floor (issue #137 §1)?

        THE READ FAILING IS NOT A VERDICT. An unreadable
        `metaAndAssetCtxs` leaves the event OUTSTANDING for the next cycle,
        exactly as an unreadable leader equity does: "I cannot tell" must never
        become "denied", or a network blip silently stops copying. A market the
        venue simply does not list IS a verdict, and the policy owns it.

        EXCEPT ON A FLIP'S OPEN LEG, where "the next cycle asks again" is a
        sentence that cannot come true and must not be written down. That leg
        runs with `claim=False` because its close leg already claimed the
        single event both legs come from (decision 3), so the event will never
        be offered again — `_claim_and_skip` knows this and reports the skip
        instead of deferring it, and the AUDIT PROSE has to say the same thing
        the mechanism does. What actually happens there is decision 3's chosen
        failure direction: the close filled, the open did not, and we are
        FLAT."""
        stats = await self._market_stats()
        if stats is None:
            return _Skip(
                REASON_UNREADABLE,
                _unreadable(f"market liquidity unreadable — {coin}", leg),
                {"coin": coin, "leg": leg, "retry": True},
            )
        verdict = self._policy.judge_coin(coin=coin, stats=stats.get(coin), limits=self._limits)
        if verdict.allowed:
            return None
        return _Skip(REASON_LIQUIDITY_FLOOR, verdict.decision, {"coin": coin, "leg": leg})

    async def _market_stats(self) -> dict[str, MarketStats] | None:
        """This cycle's market health for every covered venue, read at most
        ONCE and only when something needs it.

        One `metaAndAssetCtxs` per venue costs weight 20 — the same as `meta` —
        so judging every entry in a cycle against the floor costs what judging
        one does, and a cycle with no fresh entry costs nothing at all. NOT
        cached across cycles, unlike the asset specs: the universe changes at
        listing speed, but liquidity is the thing being judged, and a stale
        answer to "is this market healthy now" is not an answer."""
        if self._stats is None and not self._stats_unreadable:
            for _ in POSITION_VENUES:
                await self._budget.spend(MARKET_STATS_WEIGHT)
            try:
                self._stats = await fetch_market_stats(self._read)
            except Exception:
                log.warning("copy executor: market liquidity unreadable", exc_info=True)
                self._stats_unreadable = True
        return self._stats

    async def _liveness_skip(
        self, event: ClaimableEvent, coin: str, leg: str
    ) -> _Skip | None:
        """Decision 7: on ENTRY events only, the Leader must still have real
        money on the exchange. Not a sizing input — under Base Stake the money
        at risk is the operator's own constant, and the only thing the Leader
        contributes to sizing is their LEVERAGE — but a SIGNAL QUALITY gate:
        38% of quality-screened wallets had emptied their accounts while their
        stored metrics still looked alive (2026-07-29 research).

        THE ONE SIGNAL-NETWORK READ (issue #184, amendment D-6). The question
        is about the account whose stats earned the copy, which lives on the
        network tracking observed it on — not on the book this process trades.
        Same weight against the same shared budget as when it rode the trade
        gateway; only the endpoint moved."""
        await self._budget.spend(POSITIONS_WEIGHT)
        try:
            state = await self._signal_read.get_account_state(event.trader_address)
        except Exception:
            # Unreadable is not "below the floor". A gate that fails CLOSED on
            # a network blip would silently stop copying; the event stays
            # unclaimed and the next cycle asks again.
            log.warning(
                "copy executor: leader equity unreadable for %s",
                event.trader_address,
                exc_info=True,
            )
            return _Skip(
                REASON_UNREADABLE,
                # Same leg-aware wording as the floor's read failure, and for
                # the same reason: a flip's open leg is already claimed, so
                # "this cycle" would promise a retry that cannot happen.
                _unreadable(f"leader equity unreadable — {coin}", leg),
                {"coin": coin, "leg": leg, "retry": True},
            )
        if state.account_value < LEADER_EQUITY_FLOOR:
            return _Skip(
                REASON_LIVENESS_FLOOR,
                f"leader below the liveness floor: ${state.account_value} < "
                f"${LEADER_EQUITY_FLOOR} live equity — the signal is no longer the "
                f"trader whose stats earned the copy",
                {"coin": coin, "leg": leg, "equity": str(state.account_value)},
            )
        return None

    async def _settle_entry(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        state: SubState,
        result: OrderResult,
        requested: Decimal,
        mark: Decimal,
        coin: str,
        side: str,
        leg: str,
        attempt: AuditedAttempt,
        leverage: LeverageChoice,
    ) -> None:
        """Decision 5, the entry half: ONE SHOT, accept-and-audit. Requested
        versus filled is recorded and the under-copy corrects at the Leader's
        next event or at reconciliation. Missed-copy bias, consistent with
        ADR-0006."""
        now = self._clock.now()
        if isinstance(result, OrderRejected):
            await self._finish(
                attempt,
                outcome_detail={"status": "rejected", "reason": result.reason.value},
                body=(
                    f"❌ Copy {leg.replace('_', ' ')} REJECTED — {coin} "
                    f"({_short(sub.leader_address)}): {result.message}"
                ),
                now=now,
            )
            return
        if not isinstance(result, OrderFilled):
            # An IOC cannot rest (decision 4). If one ever does, say so rather
            # than book an episode against a fill that has not happened.
            await self._finish(
                attempt,
                outcome_detail={"status": "unexpected_resting"},
                body=(
                    f"⚠️ Copy {leg.replace('_', ' ')} on {coin} came back RESTING, which an "
                    f"IOC should never do — left alone, reconcile will classify it."
                ),
                now=now,
                kind=PAGER,
                action_override="copy_unexpected_resting",
            )
            return
        filled, price = result.total_size, result.avg_price
        episode = await ep.live_episode(self._pool, sub_id=sub.id, coin=coin)
        if episode is None:
            episode = await ep.open_episode(
                self._pool,
                sub_id=sub.id,
                coin=coin,
                side=side,
                entry_price=price,
                size_coin=filled,
                opened_at=now,
                opened_event_id=event.id,
                leverage=Decimal(leverage.value),
            )
        else:
            await ep.adopt_size(self._pool, episode.id, episode.size_coin + filled)
        self._apply_fill(
            state,
            coin,
            side=side,
            size_coin=_held(state, coin) + filled,
            price=price,
            leverage=Decimal(leverage.value),
        )
        if sub.brackets:
            # Decision 6: the bracket is applied at OUR FILL TIME. Waiting for
            # the periodic verification pass would leave a fresh position
            # unstopped for up to BRACKET_VERIFY_INTERVAL — which is exactly
            # the window a stop exists to cover.
            position = state.positions.get(coin)
            if position is not None:
                await ep.forget_brackets(self._pool, episode.id)
                await self._place_brackets(sub, episode, position, now, replaced=False)
        await self._finish(
            attempt,
            outcome_detail={
                "status": "filled",
                "requested": str(requested),
                "filled": str(filled),
                "avg_price": str(price),
                "leverage": leverage.value,
                "episode_id": episode.id,
            },
            body=(
                # The report leads with the POSITION, because that is what the
                # operator sees on the exchange, and names the STAKE behind it,
                # because that is the money that can be lost (amendment D-4).
                f"📈 Copied {leg.replace('_', ' ')} — {coin} {side} "
                f"({_short(sub.leader_address)}): requested ${_money(requested * mark)}, "
                f"filled ${_money(filled * price)} @ {price} · {leverage.value}x isolated, "
                f"${_money(filled * price / leverage.value)} of stake at risk"
            ),
            now=now,
        )

    # --- exits (risk-reducing: ungated, retried) ------------------------------

    async def _exit(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        state: SubState,
        *,
        leg: str,
        claim: bool = True,
    ) -> bool:
        """Exits are gated by NOTHING — not staleness, not liveness, not the
        risk policy. If we hold a copy and the Leader is out, we get out;
        closing late is strictly safer than never closing, and a close skipped
        as stale leaves a position no future event will ever close, because
        the Leader is already flat.

        Returns whether THIS instance owns the event — False only when another
        instance won the claim, which is the flip's signal not to run its open
        leg (see `_handle`)."""
        now = self._clock.now()
        coin = event.event.coin
        episode = await ep.live_episode(self._pool, sub_id=sub.id, coin=coin)
        position = state.positions.get(coin)
        if episode is None or position is None or position.size_coin is None:
            if claim:
                return await self._claim_and_skip(
                    event, sub, await self._no_local_position(sub, coin, leg), now
                )
            return True
        spec = await self._spec(coin)
        if spec is None:
            if claim:
                return await self._claim_and_skip(
                    event,
                    sub,
                    _Skip(
                        REASON_UNORDERABLE,
                        f"{coin} has no asset id — cannot be closed",
                        {"coin": coin},
                    ),
                    now,
                )
            return True
        held = position.size_coin
        try:
            if leg == "scale_out":
                fraction = scale_fraction(event.event.prev_size_coin, event.event.size_coin)
                target = relative_size(held, fraction, spec.sz_decimals)
            else:
                target = round_size(held, spec.sz_decimals)
        except UnpriceableError as exc:
            if claim:
                return await self._claim_and_skip(
                    event,
                    sub,
                    _Skip(
                        REASON_NOT_MIRRORABLE,
                        f"not mirrorable: {exc}",
                        {"coin": coin, "leg": leg},
                    ),
                    now,
                )
            return True
        sliver = await self._sub_minimum_skip(target, position, coin, leg)
        if sliver is not None:
            # Not a risk decline — exits are never declined (decision 5's
            # asymmetry stands). This order simply cannot be sent: the
            # exchange refuses anything under its minimum value, so the three
            # reduce-only retries below would be three guaranteed rejects and
            # a "0 of X closed" report that reads like a market problem. The
            # residue stays ours and reconciliation keeps reporting it, which
            # is decision 10's self-damping doing its job.
            if claim:
                return await self._claim_and_skip(event, sub, sliver, now)
            return True
        verdict = self._policy.judge_exit()
        attempt = await self._claim_and_attempt(
            event,
            action=f"copy_{leg}",
            risk_decision=verdict.decision,
            request={"coin": coin, "size": str(target), "reduce_only": True},
            claim=claim,
        )
        if attempt is None:
            return False  # another instance owns this event; do not act on it
        if await self._halted_before_signing(event, sub, attempt, leg, now):
            return False  # halted mid-flip: decision 3 ends us FLAT, not reversed
        self._exec.decision = verdict.decision
        filled, remaining = await self._reduce(sub, spec, episode, target, now)
        self._apply_fill(
            state,
            coin,
            side=episode.side,
            size_coin=held - filled,
            price=position.entry_price,
            # The exchange's own figure for what is left: an exit changes the
            # size, never the leverage the position runs at.
            leverage=position.leverage,
        )
        await self._settle_exit(sub, episode, leg, target, filled, remaining, attempt, coin)
        return True

    async def _sub_minimum_skip(
        self, target: Decimal, position: Position, coin: str, leg: str
    ) -> _Skip | None:
        """Whether this exit is too small for the exchange to accept.

        Priced off OUR OWN position (`size_usd / size_coin`) rather than a
        fresh mid: it is the same instant the size came from, it costs no
        call, and a sliver is a sliver at any nearby price."""
        if position.size_coin is None or position.size_coin <= 0:
            return None
        mark = position.size_usd / position.size_coin
        notional = target * mark
        if notional >= MIN_ORDER_NOTIONAL:
            return None
        return _Skip(
            REASON_BELOW_MINIMUM,
            f"exit sliver: {target} {coin} is about ${notional:.2f}, under the "
            f"exchange's ${MIN_ORDER_NOTIONAL} minimum order value — not sent, and "
            f"the residue stays until a larger move or reconciliation resolves it",
            {"coin": coin, "leg": leg, "size": str(target), "notional": str(notional)},
        )

    async def _reduce(
        self,
        sub: CopySub,
        spec: AssetSpec,
        episode: ep.CopyEpisode,
        target: Decimal,
        now: datetime,
    ) -> tuple[Decimal, Decimal]:
        """Decision 5, the exit half: a bounded retry of the REDUCE-ONLY
        remainder. Reduce-only is what makes retrying structurally safe — it
        cannot over-close or reverse — so every retry hazard (re-pricing a
        moved market, staleness, stale state) is entry-shaped and does not
        apply. Returns (filled, remaining)."""
        is_long = episode.side == Side.LONG.value
        filled = Decimal(0)
        remaining = target
        for attempt in range(EXIT_RETRY_ATTEMPTS):
            if attempt:
                await self._clock.sleep(EXIT_RETRY_DELAY_SECONDS)
                # The halt gate again, because the retries span ~30s and the
                # inherited obligation is to re-check AS LATE AS POSSIBLE
                # before signing — once, before the first attempt, does not
                # cover a /kill that lands during the wait. Reduce-only bounds
                # the damage either way; this stops us adding to it.
                if await is_halted(self._pool):
                    log.warning(
                        "copy executor: halted mid-exit on %s — stopping after %s of %s",
                        episode.coin,
                        filled,
                        target,
                    )
                    break
            try:
                mark = await self._mark(episode.coin)
                price = ioc_limit_price(mark, is_buy=not is_long, sz_decimals=spec.sz_decimals)
                size = round_size(remaining, spec.sz_decimals)
            except UnpriceableError:
                break
            results = await self._exec.place_orders(
                [
                    OrderSpec(
                        asset=spec.asset_id,
                        is_buy=not is_long,
                        size=size,
                        limit_price=price,
                        tif=Tif.IOC,
                        reduce_only=True,
                    )
                ],
                vault_address=sub.require_address(),
            )
            result = results[0]
            if isinstance(result, OrderFilled):
                filled += result.total_size
                remaining = target - filled
            if remaining <= 0:
                break
        return filled, max(remaining, Decimal(0))

    async def _settle_exit(
        self,
        sub: CopySub,
        episode: ep.CopyEpisode,
        leg: str,
        target: Decimal,
        filled: Decimal,
        remaining: Decimal,
        attempt: AuditedAttempt,
        coin: str,
    ) -> None:
        now = self._clock.now()
        full_exit = leg in ("close", "flip_close")
        if full_exit and remaining > 0:
            # THE PAGER CASE (decision 5, "do not lose"): the book could not
            # absorb a reduce-only IOC of this size within the slippage cap
            # across every retry. Pathological, and given its own audit reason
            # so the #52 monitor can page on it rather than drown in generic
            # partial-fill noise.
            await self._finish(
                attempt,
                outcome_detail={
                    "status": "close_unfilled",
                    "requested": str(target),
                    "filled": str(filled),
                    "remaining": str(remaining),
                },
                body=(
                    f"🚨 CLOSE UNFILLED after {EXIT_RETRY_ATTEMPTS} reduce-only attempts — "
                    f"{coin} ({_short(sub.leader_address)}): {remaining} still held. The "
                    f"book would not absorb it inside the slippage cap. The position is "
                    f"LEFT AS IS; act from the master wallet if it matters."
                ),
                now=now,
                kind=PAGER,
                action_override="copy_close_unfilled",
            )
            await ep.adopt_size(self._pool, episode.id, remaining)
            return
        if full_exit:
            await ep.end_episode(
                self._pool,
                episode.id,
                reason=ep.ENDED_LEADER_FLIP if leg == "flip_close" else ep.ENDED_LEADER_CLOSE,
                ended_at=now,
            )
        else:
            await ep.adopt_size(self._pool, episode.id, max(episode.size_coin - filled, Decimal(0)))
        await self._finish(
            attempt,
            outcome_detail={"status": "filled", "requested": str(target), "filled": str(filled)},
            body=(
                f"📉 Copied {leg.replace('_', ' ')} — {coin} ({_short(sub.leader_address)}): "
                f"{filled} of {target} closed"
            ),
            now=now,
        )

    # --- the write-ahead claim, the halt re-check, and the outcome ------------

    async def _claim_and_attempt(
        self,
        event: ClaimableEvent,
        *,
        action: str,
        risk_decision: str,
        request: dict[str, object],
        claim: bool = True,
    ) -> AuditedAttempt | None:
        """ADR-0006's write-ahead discipline, in one place: the claim row and
        the attempt row commit TOGETHER, before the wire. `claim=False` is the
        flip's open leg, whose close leg already claimed the single event both
        legs come from (decision 3: one row, two orders)."""
        now = self._clock.now()
        async with self._pool.acquire() as conn, conn.transaction():
            if claim and not await claim_event(conn, event.id, EXECUTOR_CONSUMER, now):
                # Another instance of this executor won the event. It has
                # already handled it; acting now would double the position.
                # None is the ONLY way this returns nothing — the attempt row
                # is write-ahead, never best-effort, so it either lands or the
                # transaction raises and no order is sent.
                return None
            return await self._audit.record_attempt(
                actor=EXECUTOR_ACTOR,
                action=action,
                request={**request, "event_id": event.id, "kind": event.event.kind},
                risk_decision=risk_decision,
                master_address=self._master,
                signer_address=self._signer,
                conn=conn,
            )

    async def _halted_before_signing(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        attempt: AuditedAttempt,
        leg: str,
        now: datetime,
    ) -> bool:
        """The halt gate, as late as it can be: after the claim commits,
        immediately before signing (the #143 `skip_cancel` contract).

        A halt landing between a flip's two legs stops the open leg cleanly
        and leaves us FLAT — decision 3's chosen failure direction — because
        the close leg has already run and this check refuses the open."""
        if not await is_halted(self._pool):
            return False
        await self._finish(
            attempt,
            outcome_detail={"status": "halted", "event_id": event.id},
            body=(
                f"🛑 Copy {leg.replace('_', ' ')} on {event.event.coin} "
                f"({_short(sub.leader_address)}) NOT sent — execution is halted."
            ),
            now=now,
            action_override="copy_halted",
        )
        return True

    async def _claim_and_skip(
        self,
        event: ClaimableEvent,
        sub: CopySub,
        skip: _Skip,
        now: datetime,
        *,
        claim: bool = True,
    ) -> bool:
        """Claim-means-handled: an event the executor declines is still taken
        off the backlog, with the reason on the trail and in the operator's
        chat, or the queue never drains.

        The one exception is a TRANSIENT reason (`retry` in the detail): an
        unreadable leader equity is not a decision about the event, so it
        stays outstanding for the next cycle to ask again.

        THAT EXCEPTION CANNOT APPLY WHEN THE EVENT IS ALREADY CLAIMED. A
        flip's open leg runs with `claim=False` because its close leg claimed
        the single event both legs come from — so "leave it outstanding" is a
        lie there: the event will never be offered again, and deferring would
        drop the open leg with nothing but a log line. Decision 3 requires
        that outcome to end FLAT *with an audit row*, and decision 11 requires
        every skip to reach the operator's chat, so a transient reason on an
        already-claimed event is reported like any other.

        Returns whether this instance owns the event, for the same reason
        `_exit` does: a flip whose close leg lost the claim must not open."""
        if skip.detail.get("retry") and claim:
            log.info("copy executor: deferring event %d — %s", event.id, skip.reason)
            return False
        async with self._pool.acquire() as conn, conn.transaction():
            if claim and not await claim_event(conn, event.id, EXECUTOR_CONSUMER, now):
                return False
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action="copy_skipped",
                risk_decision=skip.reason,
                detail={**skip.detail, "event_id": event.id, "kind": event.event.kind},
                master_address=self._master,
                conn=conn,
            )
        # The AUDIT ROW lands with the claim, in the transaction above, and it
        # is one row per event with the full sentence — that is the record, and
        # issue #190 does not touch it. The CHAT LINE is held instead: only the
        # end of the cycle knows whether this was one of three skips or one of
        # thirty, and that is the whole question the digest answers.
        leader = _short(sub.leader_address)
        self._skips.add(
            leader=leader,
            category=skip.category,
            body=(
                f"⏭ Skipped {event.event.kind} on {event.event.coin} "
                f"({leader}) — {skip.reason}"
            ),
            now=now,
        )
        return True

    async def _finish(
        self,
        attempt: AuditedAttempt,
        *,
        outcome_detail: dict[str, object],
        body: str,
        now: datetime,
        kind: str = ACTION,
        action_override: str | None = None,
    ) -> None:
        """Close the copy-level trail entry and tell the operator. The
        gateway's own attempt/outcome rows already record what hit the wire;
        this pair records what the COPY did, which is the thing a human reads
        an incident from. `action_override` adds a second, distinctly-named
        event row for the shapes the #52 monitor must be able to page on
        without reading every copy outcome (decision 5's PAGER CASE)."""
        await self._audit.record_outcome(attempt, outcome=OK, detail=outcome_detail)
        if action_override is not None:
            await self._audit.record_event(
                actor=EXECUTOR_ACTOR,
                action=action_override,
                risk_decision=body,
                detail=outcome_detail,
                master_address=self._master,
            )
        await self._notify(kind=kind, body=body, now=now)

    async def _notify(self, *, kind: str, body: str, now: datetime) -> None:
        await notify(
            self._pool, operator_id=self._operator_id, kind=kind, body=body, now=now
        )

    # --- reads ----------------------------------------------------------------

    def _apply_fill(
        self,
        state: SubState,
        coin: str,
        *,
        side: str,
        size_coin: Decimal,
        price: Decimal,
        leverage: Decimal,
    ) -> None:
        """Keep THIS cycle's cached view in step with what we just did.

        The cycle reads each sub's positions once (a weight-2 call per venue)
        and then may act several times on the same coin — most sharply a flip,
        whose close and open legs are the same coin in the same cycle. Without
        this the open leg would read the position its own close leg had just
        removed and skip itself as "coin occupied". The exchange remains the
        authority: this is the cycle's working copy, and the NEXT cycle's
        reconcile re-reads the truth regardless.

        LEVERAGE IS CARRIED, not defaulted to 1x, and under Base Stake sizing
        that is load-bearing rather than cosmetic: `Position.margin` derives
        the stake from notional over leverage, and the stake caps are judged
        against it. A synthesized 1x position would report a $1,000 position as
        $1,000 of margin, and the next entry in the same cycle would find the
        sub's cap already spent by a position that actually used $100."""
        if size_coin <= 0:
            state.positions.pop(coin, None)
            return
        state.positions[coin] = Position(
            coin=coin,
            side=Side(side),
            size_usd=size_coin * price,
            leverage=leverage,
            entry_price=price,
            unrealized_pnl=Decimal(0),
            size_coin=size_coin,
        )

    async def _sub_state(self, sub: CopySub) -> SubState:
        """One sub's equity and positions across every covered venue, from the
        one clearinghouseState per venue the reconcile already pays for.

        Through the shared `fetch_account_state` rather than a walk of its own
        (issue #181), because the equity is now a SUM and the sum has an
        all-or-raise rule that must not be re-implemented here: a venue that
        failed to answer contributes zero, which is indistinguishable from a
        sub that moved that balance out — and a budget measuring against a
        silently-short equity would wind a healthy copy down. Raising leaves
        the sub unreconciled for one cycle, which the caller already handles.

        The weight is billed here, per venue, because the budget is this
        module's to spend and the helper takes no view on it."""
        address = sub.require_address()
        for _ in POSITION_VENUES:
            await self._budget.spend(POSITIONS_WEIGHT)
        state = await fetch_account_state(self._read, address)
        return SubState(
            account_value=state.account_value,
            positions={position.coin: position for position in state.positions},
        )

    async def _open_orders(self, sub: CopySub) -> list[OpenOrder]:
        address = sub.require_address()
        orders: list[OpenOrder] = []
        for dex in POSITION_VENUES:
            await self._budget.spend(ORDERS_WEIGHT)
            orders.extend(await self._read.get_open_orders(address, dex=dex))
        return orders

    async def _mark(self, coin: str) -> Decimal:
        """The current mid for one coin, from its own venue's allMids."""
        dex, separator, _ = coin.partition(":")
        await self._budget.spend(MIDS_WEIGHT)
        mids = await self._read.get_mid_prices(dex if separator else None)
        mark = mids.get(coin)
        if mark is None:
            raise UnpriceableError(f"{coin} has no mid price on its venue")
        return mark

    async def _spec(self, coin: str) -> AssetSpec | None:
        """The asset id and size precision for one coin, from a per-cycle
        cache. The universe changes at listing speed, not at loop speed, so
        re-fetching it every cycle would spend weight 20+ for an answer that
        is almost always identical — but a cache that never expires would
        make a newly listed coin permanently un-copyable, so it is refreshed
        whenever a coin misses."""
        spec = self._specs.get(coin)
        if spec is not None:
            return spec
        for _ in range(2):  # core meta + perpDexs
            await self._budget.spend(META_WEIGHT)
        try:
            self._specs = await fetch_asset_specs(self._read)
        except Exception:
            log.warning("copy executor: asset universe unreadable", exc_info=True)
            return None
        return self._specs.get(coin)

    async def _no_local_position(self, sub: CopySub, coin: str, leg: str) -> _Skip:
        """Decision 8's residual, and decision 6's episode rule, sharing one
        exit: the Leader's event refers to a position we do not hold. WHY we
        do not hold it is what the operator needs, so the last ended episode's
        reason is quoted when there is one — "our bracket already took us out"
        (rule g1: no re-entry until the Leader closes and freshly re-opens)
        and "we never opened this one" are different sentences, and only one
        of them is something the operator might want to change."""
        ended = await ep.last_ended_episode(self._pool, sub_id=sub.id, coin=coin)
        reason = f"no local {coin} position for this {leg}"
        if ended is not None and ended.ended_reason is not None:
            reason += f" — the last one ended: {ended.ended_reason}"
            if ended.ended_reason == ep.ENDED_BRACKET:
                reason += (
                    " (our own TP/SL, so this copy episode is over until the leader "
                    "closes and re-opens)"
                )
        return _Skip(
            REASON_NO_LOCAL_POSITION,
            reason,
            {
                "coin": coin,
                "leg": leg,
                "sub_id": sub.id,
                "last_ended_reason": None if ended is None else ended.ended_reason,
            },
        )


def _short(address: str) -> str:
    """An address as the operator reads it in chat.

    Deliberately a second copy of `epigone.bot.format.short_address` rather
    than an import of it: that module pulls in aiogram, and the execute
    process — which holds a signing key and never talks to Telegram — has no
    business importing the bot runtime to render six characters. The
    duplication is one line; the dependency would be a package."""
    return f"{address[:6]}…{address[-4:]}" if len(address) > 12 else address


def _unreadable(what: str, leg: str) -> str:
    """A read-failure skip's sentence, told straight for the leg it happened
    on.

    An ordinary entry leaves its event outstanding, so "not judged this cycle"
    is exactly true — the next cycle asks again. A FLIP'S OPEN LEG cannot:
    its close leg claimed the single event both legs come from, so there is no
    next ask, and the honest sentence is the outcome (decision 3's chosen
    failure direction — the close filled, so we end FLAT) rather than a
    promise the mechanism will not keep."""
    if leg == "flip_open":
        return (
            f"{what}: the flip's open leg could not be judged, and this event is "
            f"already claimed by its close leg — no retry. The close filled, so the "
            f"copy is FLAT until the leader's next event"
        )
    return f"{what}: entry not judged this cycle — the next cycle asks again"


def _held(state: SubState, coin: str) -> Decimal:
    position = state.positions.get(coin)
    if position is None or position.size_coin is None:
        return Decimal(0)
    return position.size_coin


def _money(value: Decimal) -> str:
    return f"{value:.2f}"


def _figure(value: Decimal | None) -> str | None:
    """One dollar figure for an audit detail payload. `fixed_point` rather than
    `str`, for the reason `_round`'s docstring in the policy gives: a round
    NUMERIC from Postgres stringifies as `5E+2`, and the trail is read by the
    same human the chat is."""
    return None if value is None else fixed_point(value)


__all__ = ["EXECUTOR_CONSUMER", "CopyExecutor", "SubState"]
