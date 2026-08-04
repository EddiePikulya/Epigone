"""The copy executor's cycle (issue #136, ADR-0007).

One cycle, in the order the ADR forces:

1. **beat** the heartbeat — the watchdog's only view of us (ADR-0002), and a
   stale one is what trips the dead-man's switch;
2. **reconcile** every sub against the exchange (decision 10). First, because
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
6. **drain the backlog**: the copy-enabled Leaders' unclaimed `source='poll'`
   events, oldest first.

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
risk policy) and fire ONE SHOT; exits are gated by nothing, execute at any
age, and retry. Closing late is strictly safer than never closing.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.execute import episodes as ep
from epigone.execute import subs as subs_store
from epigone.execute.notices import ACTION, PAGER, PROVISIONING, SKIP, notify
from epigone.execute.policy import (
    BRACKET_VERIFY_INTERVAL,
    ENTRY_STALENESS_GUARD,
    EXIT_RETRY_ATTEMPTS,
    EXIT_RETRY_DELAY_SECONDS,
    LEADER_EQUITY_FLOOR,
    RiskPolicyV0,
)
from epigone.execute.pricing import (
    UnpriceableError,
    ioc_limit_price,
    open_size,
    relative_size,
    round_size,
    scale_fraction,
    trigger_price,
)
from epigone.execute.subs import CopySub
from epigone.gateway import (
    POSITION_VENUES,
    AssetSpec,
    HyperliquidGateway,
    OpenOrder,
    Position,
    Side,
    fetch_asset_specs,
)
from epigone.gateway.execution import (
    ExecutionError,
    Grouping,
    OrderFilled,
    OrderRejected,
    OrderResting,
    OrderResult,
    OrderSpec,
    Tif,
    TpSl,
    Trigger,
)
from epigone.position_events import POLL_SOURCE, ClaimableEvent, claim_event, outstanding_events
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
FILLS_WEIGHT = 20  # userFills, per endpoint

# Micro-USD is subAccountTransfer's unit (finding 6, measured).
USD_MICRO = 1_000_000


@dataclass(frozen=True)
class SubState:
    """One sub's live truth for this cycle: equity from the core venue, and
    positions across every venue Epigone covers."""

    account_value: Decimal
    positions: dict[str, Position]


@dataclass(frozen=True)
class _Skip:
    """A decision not to act, carrying the sentence the operator reads and the
    reason the trail records. Every skip still CLAIMS the event."""

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
        policy: RiskPolicyV0,
        *,
        operator_id: int,
        master_address: str,
        signer_address: str,
        source: str = POLL_SOURCE,
    ) -> None:
        self._pool = pool
        self._clock = clock
        self._read = read_gateway
        self._exec = exec_gateway
        self._provisioning = provisioning
        self._audit = audit
        self._budget = budget
        self._policy = policy
        self._operator_id = operator_id
        self._master = master_address.lower()
        self._signer = signer_address.lower()
        # ADR-0007 decision 4: 'poll' today; flipping to 'ws' is a #158
        # cutover item, which is why it is a constructor argument and not a
        # literal buried in the query.
        self._source = source
        self._specs: dict[str, AssetSpec] = {}
        self._brackets_checked_at: datetime | None = None
        # Episodes already reported as unclassifiable. Decision 10 says
        # "re-flag until resolved", and the AUDIT TRAIL does exactly that —
        # a row every loop. The operator's CHAT does not: a divergence that
        # nothing can classify does not resolve itself, so a message every
        # cycle would bury every other notice within minutes. Re-paging is the
        # #52 monitor's job, on its own throttled cadence.
        self._flagged: set[int] = set()

    # --- the cycle ------------------------------------------------------------

    async def run_cycle(self) -> None:
        now = self._clock.now()
        await heartbeat.beat(self._pool, heartbeat.EXECUTOR_PROCESS, now)
        # Reconciliation covers EVERY mapping; everything below covers only the
        # enabled ones. /uncopy stops event consumption, not the money
        # (decision 12) — a disabled sub still holds positions, its bracket can
        # still fire, and it can still be liquidated, so decision 10's "each
        # loop the executor compares every sub's live state" means every sub.
        # Leaving a disabled sub unreconciled would also strand a live episode
        # that a later /copy would then read as "already in a copy episode".
        all_subs = await subs_store.all_subs(self._pool, self._operator_id)
        subs = [sub for sub in all_subs if sub.enabled]
        states = await self._reconcile(all_subs, now)
        if await is_halted(self._pool):
            # Halted: reconciliation has run (it signs nothing), and every
            # step below signs something. The backlog is NOT drained — an
            # event claimed during a halt would be an event /resume can never
            # replay, and decision 9 is explicit that resume drains the
            # backlog under the already-locked rules.
            log.info("copy executor: halted — reconciled only, nothing signed")
            return
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
        leg instead of minting a second sub and stranding the first."""
        for sub in subs:
            if sub.is_provisioned:
                continue
            verdict = self._policy.judge_provisioning(
                allocation_usd=sub.allocation_usd, base_notional_usd=sub.base_notional_usd
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
            try:
                if address is None:
                    address = await self._provisioning.create_sub_account(sub.sub_name)
                    await subs_store.record_sub_address(self._pool, sub.id, address)
                await self._provisioning.sub_account_transfer(
                    address,
                    is_deposit=True,
                    usd_micro=int(sub.allocation_usd * USD_MICRO),
                )
            except ExecutionError:
                # The gateway already left its own attempt/outcome rows. The
                # mapping stays unfunded and the next cycle resumes from
                # whichever leg is still outstanding.
                log.exception(
                    "copy sub provisioning failed for %s; retrying next cycle", sub.sub_name
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
                        f"sub {_short(address)} · funded ${sub.allocation_usd} · "
                        f"base ${sub.base_notional_usd} · mode {sub.copy_mode}"
                    ),
                    now=now,
                )
            log.info("copy sub %s provisioned at %s", sub.sub_name, address)
            return

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

    async def _reconcile(self, subs: list[CopySub], now: datetime) -> dict[int, SubState]:
        """Compare every sub's live state against its episodes, classify each
        divergence, adopt the actual state as the new baseline, and page only
        what warrants it. It NEVER places an order to close a gap: auto-
        correcting would fight the operator (they close, we re-open), fight
        liquidations, and turn bookkeeping bugs into live orders."""
        states: dict[int, SubState] = {}
        for sub in subs:
            if not sub.is_provisioned:
                continue
            try:
                state = await self._sub_state(sub)
            except Exception:
                # A read failure is not a divergence. Skipping this sub for
                # one cycle costs latency; guessing would cost money.
                log.warning(
                    "copy executor: could not read sub %s this cycle", sub.sub_name, exc_info=True
                )
                continue
            states[sub.id] = state
            for episode in await ep.live_episodes(self._pool, sub.id):
                await self._reconcile_episode(sub, episode, state, now)
        return states

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
        if await self._was_liquidated(sub, episode):
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

    async def _was_liquidated(self, sub: CopySub, episode: ep.CopyEpisode) -> bool:
        """Ask the FILLS, not the equity.

        ADR-0007's table names this case "position gone + equity cratered",
        which is a heuristic that needs a threshold nobody has calibrated — a
        $200 position liquidating inside a $2000 allocation moves equity by
        10%, which no honest threshold separates from a losing exit. The fill
        stream states it outright: Hyperliquid tags a liquidation in the
        fill's own `dir`. Strictly more precise, at the cost of one read that
        only happens on an unexplained disappearance."""
        for _ in range(2):  # userFills + userTwapSliceFills (FILL_ENDPOINTS)
            await self._budget.spend(FILLS_WEIGHT)
        try:
            fills = await self._read.get_fills_since(sub.require_address(), episode.opened_at)
        except Exception:
            log.warning("copy executor: fills unreadable for %s", sub.sub_name, exc_info=True)
            return False
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
                action="bracket_replaced_after_resume",
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
                source=self._source,
                traders=list(by_leader),
            )
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
            if leg == "scale_in":
                episode = await ep.live_episode(self._pool, sub_id=sub.id, coin=coin)
                assert episode is not None  # _entry_skip proved it
                position = state.positions[coin]
                assert position.size_coin is not None
                fraction = scale_fraction(
                    event.event.prev_size_coin, event.event.size_coin
                )
                size = relative_size(position.size_coin, fraction, spec.sz_decimals)
            else:
                size = open_size(sub.base_notional_usd, mark, spec.sz_decimals)
            price = ioc_limit_price(mark, is_buy=is_long, sz_decimals=spec.sz_decimals)
        except UnpriceableError as exc:
            await self._claim_and_skip(
                event,
                sub,
                _Skip(f"not mirrorable: {exc}", {"coin": coin, "leg": leg}),
                now,
                claim=claim,
            )
            return
        notional = size * mark
        verdict = self._policy.judge_entry(
            notional_usd=notional, base_notional_usd=sub.base_notional_usd
        )
        if not verdict.allowed:
            await self._claim_and_skip(
                event, sub, _Skip(verdict.decision, {"coin": coin, "leg": leg}), now, claim=claim
            )
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
            request={"coin": coin, "side": side, "size": str(size), "limit_price": str(price)},
            claim=claim,
        )
        if attempt is None:
            return
        if await self._halted_before_signing(event, sub, attempt, leg, now):
            return
        self._exec.decision = verdict.decision
        results = await self._exec.place_orders(
            [order], vault_address=sub.require_address()
        )
        await self._settle_entry(
            event, sub, state, results[0], size, mark, coin, side, leg, attempt
        )

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
        age = now - event.observed_at
        if age > ENTRY_STALENESS_GUARD:
            # Decision 8: risk-increasing actions only. Five minutes never
            # trips in healthy operation and always trips across an outage —
            # firing a burst of stale opens on restart is the failure this
            # exists to prevent.
            return _Skip(
                f"stale entry: observed {int(age.total_seconds())}s ago, past the "
                f"{int(ENTRY_STALENESS_GUARD.total_seconds())}s guard",
                {"coin": coin, "leg": leg, "age_seconds": int(age.total_seconds())},
            )
        if await self._spec(coin) is None:
            return _Skip(
                f"{coin} has no asset id in the universe — cannot be ordered",
                {"coin": coin, "leg": leg},
            )
        episode = await ep.live_episode(self._pool, sub_id=sub.id, coin=coin)
        if leg == "scale_in":
            if episode is None:
                return await self._no_local_position(sub, coin, leg)
            if state.positions.get(coin) is None or state.positions[coin].size_coin is None:
                return _Skip(
                    f"no live {coin} position to scale — the exchange shows none",
                    {"coin": coin, "leg": leg},
                )
        else:
            if episode is not None:
                return _Skip(
                    f"already in a {coin} copy episode — a fresh open on a coin we "
                    f"already hold would double the position",
                    {"coin": coin, "leg": leg},
                )
            if coin in state.positions:
                # Decision 10's table: a position with no episode is the
                # operator's own, and we never touch it.
                return _Skip(
                    f"coin occupied: {coin} is held in this sub with no copy episode — "
                    f"the operator's own position, left alone",
                    {"coin": coin, "leg": leg},
                )
        floor_skip = await self._liveness_skip(event, coin, leg)
        if floor_skip is not None:
            return floor_skip
        return None

    async def _liveness_skip(
        self, event: ClaimableEvent, coin: str, leg: str
    ) -> _Skip | None:
        """Decision 7: on ENTRY events only, the Leader must still have real
        money on the exchange. Not a sizing input — under fixed Base Notional
        the Leader's equity never enters sizing — but a SIGNAL QUALITY gate:
        38% of quality-screened wallets had emptied their accounts while their
        stored metrics still looked alive (2026-07-29 research)."""
        await self._budget.spend(POSITIONS_WEIGHT)
        try:
            state = await self._read.get_account_state(event.trader_address)
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
                f"leader equity unreadable — entry not taken this cycle ({coin})",
                {"coin": coin, "leg": leg, "retry": True},
            )
        if state.account_value < LEADER_EQUITY_FLOOR:
            return _Skip(
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
            )
        else:
            await ep.adopt_size(self._pool, episode.id, episode.size_coin + filled)
        self._apply_fill(
            state, coin, side=side, size_coin=_held(state, coin) + filled, price=price
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
                "episode_id": episode.id,
            },
            body=(
                f"📈 Copied {leg.replace('_', ' ')} — {coin} {side} "
                f"({_short(sub.leader_address)}): requested ${_money(requested * mark)}, "
                f"filled ${_money(filled * price)} @ {price}"
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
                    _Skip(f"{coin} has no asset id — cannot be closed", {"coin": coin}),
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
                    event, sub, _Skip(f"not mirrorable: {exc}", {"coin": coin, "leg": leg}), now
                )
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
        )
        await self._settle_exit(sub, episode, leg, target, filled, remaining, attempt, coin)
        return True

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
            await notify(
                conn,
                operator_id=self._operator_id,
                kind=SKIP,
                body=(
                    f"⏭ Skipped {event.event.kind} on {event.event.coin} "
                    f"({_short(sub.leader_address)}) — {skip.reason}"
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
        self, state: SubState, coin: str, *, side: str, size_coin: Decimal, price: Decimal
    ) -> None:
        """Keep THIS cycle's cached view in step with what we just did.

        The cycle reads each sub's positions once (a weight-2 call per venue)
        and then may act several times on the same coin — most sharply a flip,
        whose close and open legs are the same coin in the same cycle. Without
        this the open leg would read the position its own close leg had just
        removed and skip itself as "coin occupied". The exchange remains the
        authority: this is the cycle's working copy, and the NEXT cycle's
        reconcile re-reads the truth regardless."""
        if size_coin <= 0:
            state.positions.pop(coin, None)
            return
        state.positions[coin] = Position(
            coin=coin,
            side=Side(side),
            size_usd=size_coin * price,
            leverage=Decimal(1),
            entry_price=price,
            unrealized_pnl=Decimal(0),
            size_coin=size_coin,
        )

    async def _sub_state(self, sub: CopySub) -> SubState:
        """One sub's equity and positions across every covered venue.

        Equity comes from the CORE venue: the allocation was funded there and
        that is where margin is measured. Positions walk POSITION_VENUES like
        every other position read in the system, so a copy on the xyz builder
        DEX reconciles like a core one."""
        address = sub.require_address()
        positions: dict[str, Position] = {}
        await self._budget.spend(POSITIONS_WEIGHT)
        core = await self._read.get_account_state(address, dex=None)
        for position in core.positions:
            positions[position.coin] = position
        for dex in POSITION_VENUES:
            if dex is None:
                continue
            await self._budget.spend(POSITIONS_WEIGHT)
            for position in await self._read.get_open_positions(address, dex=dex):
                positions[position.coin] = position
        return SubState(account_value=core.account_value, positions=positions)

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


def _held(state: SubState, coin: str) -> Decimal:
    position = state.positions.get(coin)
    if position is None or position.size_coin is None:
        return Decimal(0)
    return position.size_coin


def _money(value: Decimal) -> str:
    return f"{value:.2f}"


__all__ = ["EXECUTOR_CONSUMER", "CopyExecutor", "SubState"]
