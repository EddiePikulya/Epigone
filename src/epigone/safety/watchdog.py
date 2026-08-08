"""The watchdog: the PRIMARY dead-man's switch (issue #135, ADR-0005).

scheduleCancel — the protocol-native switch — is volume-gated out of reach
(deadman.py), so THIS process is what stands between a dead executor and a
book of unattended resting orders. Its whole design is independence from the
thing it watches:

- its own PROCESS (epigone.safety.main; the executor cannot take it down),
- its own AGENT KEY (the keystore's watchdog lane — its own nonce set, so a
  wedged executor signer cannot wedge the kill path),
- its own GATEWAY instance (one signer, one nonce lane — the execution
  seam's contract),
- and only the DB heartbeat row as its view of the executor (ADR-0002:
  processes meet in Postgres) — WITHOUT hard-depending on that database for
  the decision→wire path (below).

One cycle: beat own heartbeat (best-effort — a failed beat never skips the
protective work) → read halt state and executor liveness → trip on a stale
executor heartbeat → sweep whatever halt is active → verify, on a slow
cadence, that this watchdog's agent key is still approved ON-CHAIN
(extraAgents). A halt whose sweep isn't finished is re-swept every cycle
until it is. The sweep is ACCOUNT-WIDE: core plus every builder dex in the
live perpDexs listing (degrading to the covered POSITION_VENUES — a partial
sweep that says so — when the listing endpoint is down; partial coverage
never stamps swept_at).

AN INCIDENT DOES NOT TOUCH POSTGRES BEFORE THE WIRE (PR #143 rounds 2–5).
The failure modes are CORRELATED: the likeliest reason the executor is dead
is infrastructure trouble — precisely when a DB-dependent watchdog would be
blind. Rounds 2–4 bounded one hanging database leg at a time (command
timeout, then release, then rollback) and each time the same bug reappeared
one layer deeper — asyncpg's transaction exit awaits an UNTIMED
cancel_waiter before any per-op timeout arms — so round 5 removed the
dependency instead of bounding another leg:

- once an incident is DECLARED — a DB-blind window or a real stall trip —
  the cycle does no Postgres state work before the cancel pass reaches the
  wire: no heartbeat, no reads, no halt row, and the rate budget drops to
  its in-process bucket (FallbackBudget.incident_mode) without attempting
  the shared row. For a DB-BLIND incident the audit wrapper also defers
  its attempt row to after the call (AuditedExecutionGateway.wire_first) —
  fully Postgres-free to the wire, by construction. A real-stall trip,
  whose liveness reads answered that same cycle, deliberately KEEPS its
  bounded, best-effort write-ahead attempt row (round 6): against a
  healthy database the evidence is worth one plain bounded INSERT, and
  losing it to a crash-after-cancel would be a hole nothing else covers.
  Issue #201 extends that SAME carve-out, and only it, to the liveness
  pulse: a real-stall trip's own cancel pass is a multi-minute enumeration
  and beats through it, under a bounded per-leg time budget (see "A SWEEP
  PULSES" below); a DB-blind window still runs entirely dark;
- everything durable — halt row, audit events, sweep verification — runs
  AFTER the cancel attempt, best-effort, under a hard real-time ceiling
  (DB_BLOCK_CEILING_SECONDS; safe because Pool.release is shielded, so a
  cancelled block's connection is still terminated within the round-4
  acquire budget). Reconciliation is bookkeeping and never gates
  protection: a failing reconcile keeps the incident open and the next
  cycle cancels again;
- NORMAL operation keeps its bounded DB touches (epigone.safety.db:
  connect, per-query, lock waits, release budget), the same wait_for
  ceiling around every state block of run_cycle, and a per-attempt ceiling
  inside the shared rate bucket itself (SharedWeightBudget's
  attempt_ceiling, wired in main.py — the seam every budget spend passes
  through, the gateway's pre-sign spend included), so even the leg the
  pool timeouts cannot reach (the mid-transaction rollback) costs a
  bounded cycle, never a hung loop that could starve the blind machinery
  of the cycles it needs;
- cannot-read-Postgres is ITSELF a trip: unreadable CONTINUOUSLY past
  `db_blind_after` — a failure streak, reset by any successful read, so
  one dropped connection after a long healthy cycle never trips it
  (default 3× the executor-stall threshold — a deliberate, conservative
  call: cancelling resting orders is cheap and recoverable, the executor
  re-places when it returns, and nothing here closes positions or spends —
  whereas sitting blind through a correlated outage is the exact incident
  this layer exists to prevent). On recovery the incident reconciles: the
  halt row under a DISTINCT headline ("DB-blind sweep" vs a real stall's
  "unrecorded trip"; a trip reconciled within its own cycle keeps the
  plain stall reason), plus a blind-window audit event for any window that
  spanned cycles, even when a standing halt is joined (durable in every
  case except a watchdog death inside the recovery-to-reconcile window —
  the runbook's one carve-out).

A COLD START DURING AN OUTAGE IS THE SAME INCIDENT, DECLARED AT LAUNCH
(issue #145). The guarantee above used to cover an already-running process
only, because startup needed Postgres for the pool, the migrations, and the
keystore row holding the agent key — so a crash, a reboot, or a deploy
*during* an outage left no cancel path at all. epigone.safety.coldstart
removes that: the key comes from an encrypted local cache, the migration
check is deferred to the reconnect, and the watchdog is handed an incident
that is already open (`declare_cold_start`) so its FIRST cycle cancels. The
kind is preserved end to end — "cold-start DB-blind sweep" — because the
operator's follow-up differs from a running-process window.

THE SWEEP NEVER TRUSTS A CANCEL'S WORD. `swept_at` is stamped only when a
FRESH enumeration of the book answers empty — cancel results, however clean,
don't count, and an AmbiguousExecutionError just means the next cycle
re-enumerates and re-cancels what actually remains (cancel-by-oid re-issued
against a dead order answers MISSING_ORDER, which the verify pass shrugs
at). Treating "the cancel call failed cleanly" as "no orders live" is the
silent-live-order hazard this module exists to not have.

Positions are NOT closed — the sweep applies the documented unwind policy
(v0: hold-and-alert, docs/runbooks/halt-and-unwind.md), records the open-
position snapshot on the halt row, and the monitor's halt alert carries it
to the operator.

A SWEEP PULSES (issue #201). The sweep is account-wide on both axes and
every (account, dex) pair costs ORDERS_WEIGHT through a 900/min bucket
shared with the stream and ingest, so a real one is MINUTES of wall clock
inside a single cycle. The 2026-08-07 shakedown found what that did to the
watchdog's own liveness signals: for the eight minutes it spent sweeping a
/kill it never beat its heartbeat (the #52 monitor read the busiest process
in the system as dead) and never pushed its dead-man's schedule (which
consequently FIRED un-refreshed mid-halt, discharging the last-resort net
while the watchdog was still working). Both signals now ride a PULSE
emitted at every enumeration step: the heartbeat beat, plus an injected
`keepalive` — in production the dead-man's push (safety/main.py). Three
properties are load-bearing:

- the pulse is GATED ON THE INCIDENT'S KIND, the same split round 6 made
  for the write-ahead attempt row. A DB-BLIND window (running-process or
  cold-start) never pulses: that path reaches the wire with zero Postgres
  behind it, both legs of a pulse write to Postgres, and a watchdog blind
  because the database is unreachable cannot truthfully beat to it anyway.
  A REAL-STALL trip does pulse — its liveness reads answered that very
  cycle, so the database is healthy, and its own cancel pass is a
  multi-minute enumeration that would otherwise read as death. The
  reconcile's verify enumeration always pulses: by then the incident is
  cleared;
- its two legs are INDEPENDENT, and each runs on its own PER-CYCLE TIME
  BUDGET. Independent because coupling them recreates the incident: a
  refused Postgres connection fails in milliseconds, so a beat failure that
  short-circuited the push would leave a full-speed-looking sweep with an
  unrefreshed schedule. Budgeted because the throttle cannot bound a SLOW
  leg — it spaces pulses from their START, so anything slower than the
  interval is charged again at the very next enumeration step, and a wedged
  database or a slow-failing exchange endpoint adds tens of minutes to a
  real sweep. Budgeted on MEASURED WALL CLOCK rather than on error type,
  because the slow shapes are typed every which way (an exchange POST that
  502s at 25s raises AmbiguousExecutionError, and the deadman re-queues an
  ambiguous push at once) — and measuring time keeps for free the property
  that makes going quiet safe at all: a leg that fails FAST keeps retrying,
  so one blip never silences a signal for a ten-minute grind. The budget
  REFILLS ON THE CLOCK, every PULSE_LEG_REFILL_SECONDS of sweep, because a
  cycle is a whole sweep and a budget that lasted one let a single slow
  failure early in a grind silence a leg long enough for the armed schedule
  to reach its horizon mid-halt anyway. PULSE_LEG_BUDGET_SECONDS states the
  bound, PULSE_LEG_REFILL_SECONDS the window; `_pulse` carries the argument;
- the keepalive runs on THIS task, not a sibling one. Deliverable 2 of
  issue #201 asked for an independent dead-man task; it is deliberately not
  one, because the deadman shares this process's single AuditedExecution-
  Gateway and that wrapper carries MUTABLE PER-ACTION STATE — `decision`
  (the risk_decision prose every audit row is stamped with) and the
  incident posture `wire_first`. A sibling task calling `maintain()` would
  overwrite `decision` between a sweep's enumeration and its cancels, so
  kill-path rows would carry the dead-man's prose instead of the halt's:
  a liveness bug traded for a corrupted audit trail on the one path whose
  evidence matters most. (NOT a nonce-ordering argument — NonceSource is an
  atomic counter and the exchange accepts out-of-order landing at this
  concurrency; the shared audit state is the real constraint.) Pulsing on
  the sweep's own task removes the emergent race — the schedule is pushed
  every few seconds of a HEALTHY grind rather than once at the end of it —
  while keeping every action on the lane strictly ordered. Not "never left to
  lapse": under a saturated exchange the worst gap between two push attempts
  still exceeds the half-horizon of slack, and PULSE_LEG_REFILL_SECONDS walks
  that residual out step by step;
- and the last of that residual is walked out by a DEADLINE (issue #212). A
  budget and a throttle are the right shape for advisory work, and the push
  stops being advisory when its schedule is about to lapse — so the armed
  horizon crosses the Keepalive seam (`KeepaliveDeadline`,
  `DeadMansSwitch.armed_until`), and at any boundary where the step about to
  start could outlive that horizon the push is exempt from both gates —
  paced at SWEEP_PULSE_INTERVAL so the exemption can never hot-loop.
  PRIORITY_PUSH_WINDOW_SECONDS states the window, the guarantee it buys (no
  armed schedule lapses without a full-ceiling attempt having been made
  against it — for as long as every await between two boundaries is itself
  bounded, which the pacing sleep below is not), what a wedged leg pays for
  it, and what it does NOT close.

AND EVERY READ BETWEEN TWO PULSES IS BOUNDED (issue #204). Pulsing at every
enumeration step only bounds the liveness gap if the step itself is bounded,
and it was not: `parse_retry_after` handed the server our wall clock (fixed
at the source — gateway/backoff.py caps it), and even without that a
429-saturated read is RATE_LIMIT_MAX_TRIES answers at up to a 30s
REQUEST_TIMEOUT each — long enough for one read to straddle a push-due
boundary and let the last-resort net discharge under a working watchdog, and
429 pressure is most plausible exactly while an account-wide sweep grinds.
So every read this process makes goes through `_safety_read`: pulse, then a
hard SAFETY_READ_CEILING_SECONDS on the read itself. `_asset_ids_for` was
the worst of it — 2+N back-to-back HTTP reads, standing between the
enumeration and the cancels it feeds, with no pulse and no ceiling — and now
runs its fetch through `_PulsedUniverseReads`, which puts each of those
reads on that same seam.

Progress is also LOGGED per (account, dex), so a long sweep is legible in
the container logs while it runs — "sweeping, 60% done" was previously
indistinguishable from "wedged", `swept_at` being the only signal and
landing only at the very end.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

import asyncpg

from epigone.budget import SHARED_WEIGHT_PER_MINUTE, Budget
from epigone.clock import Clock
from epigone.gateway import (
    POSITION_VENUES,
    GatewayError,
    HyperliquidGateway,
    OpenOrder,
    PerpAsset,
    Position,
    fetch_asset_ids,
)
from epigone.gateway.execution import CancelSpec
from epigone.safety import heartbeat
from epigone.safety.audit import WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.budget import FallbackBudget
from epigone.safety.db import SAFETY_DB_TIMEOUT_SECONDS
from epigone.safety.halt import (
    HOLD_POLICY,
    WATCHDOG_SOURCE,
    Halt,
    active_halt,
    mark_swept,
    request_halt,
)

log = logging.getLogger(__name__)

# Read-side weights, billed per venue call like the stream passes bill theirs
# (each consumer owns its billing; the values mirror stream/poller.py and
# stream/orders.py — change those, change these).
ORDERS_WEIGHT = 20  # frontendOpenOrders, per venue
POSITIONS_WEIGHT = 2  # clearinghouseState, per venue
META_WEIGHT = 20  # meta (per venue), perpDexs, extraAgents — the info default

# The hard REAL-TIME ceiling on every durable/state block of a cycle
# (round 5): asyncpg's per-op timeouts cannot reach a transaction exit — a
# ROLLBACK awaits an UNTIMED cancel_waiter before any command timeout arms —
# so instead of bounding one more library leg, every DB block is wrapped in
# asyncio.wait_for at this ceiling. The composition is safe: Pool.release is
# asyncio.shield'ed, so the cancelled block's connection is still terminated
# within the acquire budget (the round-4 pool). Real time, not the injected
# clock: this bounds actual awaits, and the fakes finish instantly.
DB_BLOCK_CEILING_SECONDS = 4 * SAFETY_DB_TIMEOUT_SECONDS

# How often a grinding sweep says "alive" (issue #201). The sweep pulses at
# every enumeration step and this throttles the resulting work: one
# single-row heartbeat upsert plus one dead-man check per interval, not per
# REST call. Well under the monitor's watchdog-staleness window and well
# under the dead-man horizon's half-cadence push, so neither signal can go
# stale between two pulses of a sweep that is making progress.
SWEEP_PULSE_INTERVAL = timedelta(seconds=5)

# The keepalive leg of a pulse is ADVISORY work riding the protective path,
# so it gets a hard real-time ceiling of its own: nothing about pushing the
# dead-man's schedule may stall the sweep that is pushing it. 30s is one
# full exchange REQUEST_TIMEOUT — a healthy attempt fits; a retry storm gets
# cut — and cutting is safe, because a repeated scheduleCancel set REPLACES
# the schedule (deadman.py), which is the same self-reconciling retry its
# ambiguity path already relies on. The next pulse retries five seconds
# later, inside the half-horizon of slack the push cadence leaves (150s at
# the default 300s horizon), so a slow exchange costs retries, not a lapsed
# schedule. Real time, not the injected clock, like every other ceiling here.
KEEPALIVE_CEILING_SECONDS = 30.0

# How much of a sweep's own wall clock each pulse LEG may spend per cycle
# (review round 2 of PR #203). A ceiling bounds ONE attempt; this bounds
# what the attempts add up to, which is the number that decides whether a
# sweep finishes in minutes or in an hour — SWEEP_PULSE_INTERVAL spaces
# pulses from their start, so anything slower than the interval is charged
# again at the very next enumeration step.
#
# Measured, never inferred from the exception: the slow shapes are typed
# every which way (an exchange POST that 502s at 25s raises
# AmbiguousExecutionError, and the deadman re-queues an ambiguous push
# immediately), so only the clock tells the truth about what a leg costs.
#
# One pulse interval's worth. A healthy beat is milliseconds, so this is
# hundreds of healthy pulses and no working system ever reaches it, while a
# single wedged attempt spends it outright. The bound it buys: a leg costs a
# sweep at most this budget plus the one attempt that overran it, per REFILL
# WINDOW (below) — so at most PULSE_LEG_BUDGET_SECONDS + that leg's ceiling
# + the shielded drain the ceiling cannot itself cancel, whatever the
# exchange and the database are doing.
#
# "+ the shielded drain" is not pedantry (round-3 review of PR #203, F5):
# asyncio.wait_for AWAITS the cancellation it raises, and epigone.safety.db
# shields Pool.release precisely so a cancelled block still terminates its
# connection — so a beat wedged inside asyncpg costs its ceiling plus that
# release budget before this counter ever sees it. Bounded, and deliberately
# so; the number is just larger than the ceiling alone.
PULSE_LEG_BUDGET_SECONDS = SWEEP_PULSE_INTERVAL.total_seconds()

# How much sweep wall clock passes before a spent pulse leg is given its
# budget back (round-3 review of PR #203, F1). Without this the budget lasted
# the whole CYCLE, and a cycle is a whole sweep: one 30s slow-fail of a due
# dead-man push early in a multi-minute grind silenced the push leg for the
# rest of it, and the armed schedule could still reach its horizon mid-halt —
# the 2026-08-07 incident, narrowed but not closed.
#
# The window is chosen against the number that matters — the half-horizon of
# slack (150s at the default 300s horizon) between the dead-man's push falling
# due and the schedule lapsing. THE RESIDUAL IS NARROWED, NOT CLOSED, and the
# honest composition says why (merge-gate review of PR #209; an earlier
# revision of this comment claimed 140s < 150s by counting one in-flight step
# instead of two and by forgetting that the push falls due asynchronously to
# the sweep). Walk it from the moment D at which a push becomes due:
#
#   D        the sweep is mid-step. That step is ALREADY IN FLIGHT and is not
#            interruptible: up to 65s (a cancel POST, self-bounded by the
#            gateway's 429 loop at RATE_LIMIT_TOTAL_BUDGET_SECONDS +
#            REQUEST_TIMEOUT; a read is shorter, SAFETY_READ_CEILING_SECONDS
#            binding first)
#   D+65     pulse. The refill grants the leg its budget, the push attempt
#            wedges, and it is cut at KEEPALIVE_CEILING_SECONDS — plus the
#            drain the cut cannot itself cancel (the F5 correction, stated at
#            PULSE_LEG_BUDGET_SECONDS, applies to THIS term too)
#   D+95     budget spent; the next step is another saturated 65s cancel POST
#   D+160    pulse, refill, attempt two. Even an INSTANT success lands after
#            D+150 — the schedule has already lapsed. A push that itself
#            needs its ceiling lands nearer D+190.
#
# So ~190s against 150s of slack under transient exchange saturation, and
# larger under correlated failure: the pre-sign budget PACING SLEEP is
# deliberately un-ceilinged (SharedWeightBudget's docstring — a token-deficit
# wait may honestly run long and must not degrade the lane), the audited
# gateway's attempt/outcome rows go to a database that may itself be wedged
# to its 5s timeouts, and each cut leaves a shielded drain. ~250s when the
# exchange and the database are both bad at once — which is the correlated
# case this whole module exists for. "Every await between two pulses is
# bounded" is therefore not literally true; the pacing sleep is the exception,
# on purpose.
#
# THE WALK ABOVE IS THE BUDGET'S OWN ARITHMETIC AND IT NO LONGER ENDS THERE
# (issue #212): from D+65 on, every one of those boundaries is inside
# PRIORITY_PUSH_WINDOW_SECONDS of the horizon, and inside that window a push
# attempt stops asking this budget for permission. That constant re-walks the
# same numbers with the exemption in place and states what survives it. Read
# the rest of this comment as what the budget alone leaves — which is still
# the right frame for reading the budget's own size — rather than as the
# module's residual.
#
# WHY THIS IS SHIPPED ANYWAY, and what it costs when it fires: the residual
# discharges in the FAIL-SAFE direction. The dead-man firing cancels every
# resting order on the exchange, during a halt whose whole job was cancelling
# resting orders — so the outcome is the intended one arriving by the
# belt-and-braces route. It costs one of the account's 10 daily triggers and
# an operator moment of "why did that fire", which the runbook now names so a
# responder reads it as expected-rare rather than as a second incident.
#
# And what the window DOES buy is large: the quiet term used to be the REST OF
# THE SWEEP — minutes, because a per-CYCLE budget lasts a whole sweep — and is
# now at most this window. Minutes-to-45s is the difference between a residual
# that needs conjunctive bad luck and one that the 2026-08-07 incident hit on
# its first try. What this window could never buy — an attempt at the boundary
# where it actually matters, rather than at whichever boundary the refill clock
# happened to land on — is what the deadline buys instead
# (PRIORITY_PUSH_WINDOW_SECONDS); the two are complementary, and this one still
# governs every pulse outside that window, which is all of a normal sweep.
#
# The price is bounded and paid only when a leg is genuinely wedged: at worst
# one budget plus one overrunning attempt per window, so a wedged keepalive
# roughly halves a sweep's pace but cannot tax it per enumeration step, which
# is the failure PR #203's budget existed to stop. Real time like every other
# ceiling here — this measures actual awaits, and the fakes finish instantly.
PULSE_LEG_REFILL_SECONDS = 45.0

# The longest step this process can START and then not be able to cut short.
# The cancel POST is the binding one: it bounds ITSELF (the gateway's 429 loop
# at RATE_LIMIT_TOTAL_BUDGET_SECONDS ≈ 35s plus one REQUEST_TIMEOUT ≈ 30s) and
# deliberately cannot be cut from outside, because cutting a write orphans its
# attempt row (`_push_keepalive`'s own comment argues the exception). A read is
# shorter — SAFETY_READ_CEILING_SECONDS binds it at 45s — so one number covers
# every step, and it is this one. Stated here rather than imported, like
# SAFETY_READ_CEILING_SECONDS and for the same reason: this module's whole
# history is bounds that had one more layer underneath them, so the safety path
# states its own numbers and a change to the gateway's shows up as a diff here.
STEP_BOUND_SECONDS = 65.0

# How close to its horizon the armed dead-man's schedule may come before a
# push stops being advisory work and becomes the thing the sweep does FIRST —
# exempt from the leg budget and from the pulse throttle alike (issue #212,
# the residual PR #209's merge-gate review left open and the operator's
# 2026-08-08 decision closed).
#
# THE WINDOW IS A STEP PLUS A PUSH, not a step alone. The check happens at an
# enumeration boundary, and what must fit before the horizon is the step about
# to start PLUS the ceiling of the attempt that would follow it. A window of
# one step bound alone lets a boundary at horizon−1s be the first one that
# fires, which grants the attempt a full KEEPALIVE_CEILING_SECONDS it does not
# have. (The issue's "~60s" is the step term only; the ceiling term is this
# comment's correction to it.)
#
# NOT ONE ATTEMPT PER ARMED SCHEDULE — and the issue's own arithmetic is why,
# which is worth spelling out because "one budget-exempt push attempt" is what
# the decision comment says. Walk the issue's worst case from the moment D at
# which a push falls due (armed_until = D+150 at the default horizon):
#
#   D        a 65s cancel POST is already in flight and cannot be cut
#   D+65     boundary, 85s left: inside this window, so an exempted attempt —
#            but the ORDINARY leg would have attempted here too (the refill at
#            PULSE_LEG_REFILL_SECONDS grants it a budget at exactly this
#            point). It wedges and is cut at its ceiling
#   D+95     boundary, 55s left. The ordinary leg is spent; a one-per-schedule
#            exemption is spent too — so the next 65s step starts and the
#            schedule lapses at D+150 with the sweep mid-POST
#
# A single exemption therefore buys NOTHING over the refill in the exact case
# the issue was opened about. What closes it is the attempt at D+95: it has
# 55s of horizon in front of it and its ceiling is 30s, so it either lands —
# saving the schedule with 25s to spare — or proves the exchange could not
# accept a scheduleCancel across two full ceilings inside 95s. So the rule is
# ONE ATTEMPT PER BOUNDARY INSIDE THE WINDOW, paced (below) rather than
# counted, and the guarantee is:
#
#   NO ARMED SCHEDULE LAPSES WITHOUT A FULL-CEILING PUSH ATTEMPT HAVING BEEN
#   MADE AGAINST IT WITH THE WHOLE CEILING AHEAD OF THE HORIZON —
#
# conditional, and the condition is STEP_BOUND_SECONDS: the induction is "the
# last boundary outside the window had more than 95s left, so after a step of
# at most 65s the next boundary still has more than 30s". An await longer than
# a step bound between two boundaries breaks it, and one exists on purpose (the
# pacing sleep, below).
#
# PACED LIKE AN ORDINARY PULSE (SWEEP_PULSE_INTERVAL, on the injected clock —
# the same clock the deadline is on) so the exemption can never become a hot
# loop: a push that fails in milliseconds costs the sweep nothing but must not
# be re-issued at every one of a fast sweep's enumeration steps.
#
# WHAT IT COSTS, honestly, and the two shapes differ:
#
# - every attempt WEDGES: attempts start only while the horizon is still
#   ahead, so at most ceil(WINDOW / KEEPALIVE_CEILING) = 4 of them can be
#   started per armed schedule — ≤ ~2 minutes of sweep wall clock per 300s
#   horizon. Real, and deliberately NOT the per-step tax PR #203's budget
#   exists to prevent: bounded per horizon rather than per step, and smaller
#   than what the budgeted path alone already permits over the same span (one
#   ceiling per PULSE_LEG_REFILL_SECONDS window ≈ 6 per horizon);
# - every attempt FAILS FAST: wall clock is ~nil, but the pace is the only
#   bound left, so a 95s window admits up to ~19 attempts — 19 signed POSTs,
#   each spending its pre-sign weight through the same shared bucket the sweep
#   is paying from, against an exchange the premise says is saturated. That is
#   the honest upper end; it buys the case where the exchange starts answering
#   again mid-window, and it is why the pace is a pace and not "every
#   boundary".
#
# WHAT IT DOES NOT CLOSE, stated so no reader has to rediscover it:
#
# - if every attempt inside the window wedges for its full ceiling, the
#   schedule still lapses. What the fix removes is the COMPOSITION — a push
#   skipped because a budget was spent, or throttled, while a push would have
#   succeeded; what remains needs the exchange to be unable to accept a
#   scheduleCancel across the window, which is a push that was impossible
#   rather than one that was deprioritised;
# - the pre-sign budget PACING SLEEP is deliberately un-ceilinged
#   (SharedWeightBudget's docstring), so a token-deficit wait can outlast the
#   whole window between two boundaries. There IS a pulse on the far side of
#   every pacing sleep (`_walk_scope` pulses, spends, and `_safety_read`
#   pulses again), so the attempt happens the moment the sleep ends — but a
#   sleep longer than the window is a lapse nothing in this process prevents;
# - a DB-BLIND window does not pulse at all (`_pulse_allowed`), so it does not
#   push either, priority or otherwise. That is #201's gating decision left
#   deliberately untouched: a blind sweep is already cancelling every resting
#   order account-wide, so a schedule that fires under it produces the same
#   outcome by the belt-and-braces route;
# - and none of this touches R4 from the same review — a watchdog whose
#   cancels fail while its pushes succeed keeps the exchange-side net
#   postponed indefinitely. That is inherent to any keepalive design and
#   belongs to the mainnet risk register, not here.
#
# The window assumes the deadman's own push cadence leaves more slack than the
# window is wide (half of the 300s default horizon: 150s > 95s). Under a
# horizon configured below ~190s the two coincide, and an exempted attempt can
# find the deadman not yet due and do nothing; the default is nowhere near
# that, and the whole slack arithmetic here reads differently at that horizon.
PRIORITY_PUSH_WINDOW_SECONDS = STEP_BOUND_SECONDS + KEEPALIVE_CEILING_SECONDS

# The hard REAL-TIME ceiling on ONE read of the safety path (issue #204).
#
# The kill path's liveness guarantee is that no await between two sweep
# pulses outlives the dead-man's slack. Every pulse sits between two reads,
# so the guarantee is exactly a bound on a single read — and there was none:
# a saturated 429 loop is RATE_LIMIT_MAX_TRIES answers at up to a 30s
# REQUEST_TIMEOUT each plus the sleeps between them, and 429 pressure is most
# plausible precisely while an account-wide sweep grinds through the shared
# bucket. Capping Retry-After (gateway/backoff.py) bounds the sleeps; this
# bounds the whole read.
#
# 45s is one and a half REQUEST_TIMEOUTs: a healthy read is milliseconds, a
# read that rides out a 429 or two fits comfortably, and a saturated one is
# cut. Cutting is safe because enumeration is idempotent and unverified — the
# sweep leaves the halt unswept, says so, and the next cycle re-enumerates —
# whereas NOT cutting it is how a single read straddles a push-due boundary.
#
# DELIBERATELY TIGHTER than the gateway's own bound on the same call
# (RATE_LIMIT_TOTAL_BUDGET_SECONDS + REQUEST_TIMEOUT ≈ 65s), and that is the
# point rather than a redundancy: this module's whole history is bounds that
# turned out to have one more layer underneath them (the round 2–5 asyncpg
# chase in the docstring above), so the safety path states its own number
# instead of inheriting one. A read cut here that the gateway would have
# completed at 50s costs one cycle's retry.
#
# The composed gap this participates in is written out in full at
# PULSE_LEG_REFILL_SECONDS; on this axis a read is not the binding term,
# because the cancel POST — which cannot be cut from outside without
# orphaning its audit row — is longer.
SAFETY_READ_CEILING_SECONDS = 45.0

# A failed capability probe (network blip, info outage, unwritable verdict)
# retries on this short fuse instead of waiting out the full check interval —
# but never every cycle, which would burn 20 weight per 10s on a persistent
# outage. The fuse advances on EVERY failure shape (round 2 item 4): a bare
# TimeoutError must not re-fire the probe per-cycle just because it isn't a
# GatewayError.
CAPABILITY_RETRY = timedelta(minutes=5)


# Whatever ELSE must not go stale while a sweep grinds (issue #201). One
# implementation in production — DeadMansSwitch.maintain, wired in
# safety/main.py — kept as a callable seam so the watchdog does not have to
# know about the upgrade path it is the fallback for, and so tests can pulse
# something trivially observable. Optional at construction because a
# watchdog without one is a real configuration (the deadman is the UPGRADE
# path, ineligible below the $1M volume gate), not just a test shortcut.
Keepalive = Callable[[], Awaitable[None]]

# …and WHEN the thing the keepalive refreshes would lapse if it stopped being
# refreshed (issue #212): the armed schedule's horizon, or None while nothing
# is armed (never probed, volume-gated, or an accepted set still awaited).
# `DeadMansSwitch.armed_until` in production.
#
# A SECOND, NARROWER SEAM rather than a widened Keepalive. Keepalive says "do
# the thing that must not go stale" and is deliberately opaque about what the
# thing is — the watchdog is the fallback for an upgrade path it does not model
# — and a deadline is exactly one more fact, optional in the same way and for
# the same reason (a watchdog below the $1M volume gate has no schedule to be
# near the horizon of). Keeping them two callables also keeps every existing
# caller — the tests that pulse something trivially observable included —
# passing a plain function rather than an object satisfying a protocol.
KeepaliveDeadline = Callable[[], datetime | None]

# `Watchdog._safety_read` as a value, so a collaborator can be handed the
# seam without being handed the watchdog (`_PulsedUniverseReads`). Generic in
# the read's own result type: the seam changes WHEN a read is awaited, never
# what it answers.
T = TypeVar("T")


class _SafetyRead(Protocol):
    async def __call__(self, read: Awaitable[T]) -> T: ...


@dataclass(frozen=True)
class _AccountOrder:
    """One resting order and WHICH BOOK it rests on (issue #136): `account` is
    None for the master and a sub-account address otherwise — exactly the
    vault flag the cancel must carry."""

    account: str | None
    order: OpenOrder


@dataclass(frozen=True)
class _BlindIncident:
    """An in-process trip that Postgres couldn't record: either the database
    was unreadable past the blind threshold (`db_blind=True`), or a REAL
    observed stall tripped but the halt row was unwritable (`db_blind=False`
    — the distinction the reconciled halt's headline must preserve, so the
    operator can tell them apart). Held until a cycle can write the halt
    row — the reconcile — so the state machinery and the operator alert
    catch up with what the wire already did.

    `cold_start` (issue #145) splits the db_blind case once more: the
    process came up with Postgres already unreachable, so it has NEVER seen
    halt state or executor liveness, and its key came from the local cache
    rather than the keystore. Same protective behaviour, different operator
    follow-up — the process was restarted mid-outage — so the reason it
    reconciles under says so."""

    since: datetime
    reason: str
    db_blind: bool
    cold_start: bool = False


class Watchdog:
    """One cycle's logic, dependency-injected for tests; epigone.safety.main
    wires the real deps and loops it."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        clock: Clock,
        read_gateway: HyperliquidGateway,
        exec_gateway: AuditedExecutionGateway,
        audit: ExecutionAudit,
        budget: Budget,
        *,
        master_address: str,
        signer_address: str,
        executor_stale: timedelta,
        db_blind_after: timedelta,
        capability_interval: timedelta,
        keepalive: Keepalive | None = None,
        keepalive_deadline: KeepaliveDeadline | None = None,
    ) -> None:
        self._pool = pool
        self._clock = clock
        self._read = read_gateway
        self._exec = exec_gateway
        self._audit = audit
        self._budget = budget
        self._master = master_address.lower()
        self._signer_address = signer_address.lower()
        self._executor_stale = executor_stale
        self._db_blind_after = db_blind_after
        self._capability_interval = capability_interval
        self._keepalive = keepalive
        self._keepalive_deadline = keepalive_deadline
        # When this process last made a budget-exempt push attempt (issue
        # #212). Injected-clock time, because it paces attempts against a
        # deadline that is on that same clock — the leg budgets' monotonic
        # seconds measure something else, what an attempt COST.
        self._priority_pushed_at: datetime | None = None
        # When this process last emitted a liveness pulse (issue #201) —
        # the cycle-top beat counts, so a short cycle adds no extra writes —
        # and, per leg, how much wall clock it has spent since the budgets
        # were last refilled (`_pulse`; refilled at every cycle top and every
        # PULSE_LEG_REFILL_SECONDS of a grinding sweep, monotonic seconds).
        self._pulsed_at: datetime | None = None
        self._beat_spent = 0.0
        self._keepalive_spent = 0.0
        self._budgets_refilled_at = time.monotonic()
        self._capable: bool | None = None  # last on-chain verdict; None = unchecked
        self._next_capability_at: datetime | None = None  # None → due now
        # Postgres-independence state (module docstring): the onset of the
        # CURRENT unbroken streak of liveness-read failures (None = the last
        # read succeeded — any success resets it, so the blind threshold
        # means "continuously unreadable", never "one failure landing long
        # after the last success"; round 3 item 3), the unrecorded trip
        # awaiting its halt row, and how many cancel passes ran unrecorded.
        self._db_failing_since: datetime | None = None
        self._blind: _BlindIncident | None = None
        self._blind_passes = 0

    def declare_cold_start(self, since: datetime, reason: str) -> None:
        """The process came up blind (issue #145): Postgres was unreachable at
        launch, so epigone.safety.main loaded the agent key from the local
        cache and hands the watchdog an incident that is ALREADY open. The
        first cycle therefore cancels — there is nothing to wait for. A
        running process waits out `db_blind_after` because a blip is not an
        incident; here the outage has already outlived the startup grace
        (WATCHDOG_COLDSTART_GRACE_SECONDS, the same span), so the same rule
        has already been satisfied by the time this is called."""
        self._declare(
            _BlindIncident(since=since, reason=reason, db_blind=True, cold_start=True)
        )

    async def run_cycle(self) -> None:
        now = self._clock.now()
        # Each cycle refills both pulse-leg budgets (issue #201, `_pulse`):
        # a leg that wedged during the last one gets a fresh one here, so
        # the strike is never a permanent give-up. A long sweep refills them
        # again from inside itself, on the clock (`_refill_leg_budgets`).
        self._refill_leg_budgets(force=True)
        # INCIDENT-FIRST (round 5, the structural rule): an open incident
        # reaches the wire before this cycle touches Postgres AT ALL — no
        # beat, no reads, no state. Durable catch-up comes after the cancel.
        if self._blind is not None:
            await self._incident_cycle(now)
            return
        self._beat_spent += await self._beat(now)
        # The cycle-top beat is charged to the same budget the sweep's
        # pulses spend, so it must report exhaustion the same way (round-3
        # review of PR #203, F4): a beat that wedges HERE is the commonest
        # shape of a quiet leg, and it used to go over budget silently.
        _report_if_spent("heartbeat", self._beat_spent)
        try:
            halt, stall_reason = await asyncio.wait_for(
                self._read_liveness(now), DB_BLOCK_CEILING_SECONDS
            )
        except Exception:
            # Deliberately ANY failure of the liveness reads, not just
            # connection errors: a watchdog that cannot answer "is the
            # executor alive" is blind whatever the cause, and blind fails
            # protective (a spurious blind trip cancels recoverable orders;
            # the inverse mistake strands them).
            log.warning(
                "watchdog: Postgres cannot answer the liveness question", exc_info=True
            )
            self._handle_unreadable(now)
            if self._blind is not None:  # declared just now: cancel THIS cycle
                await self._incident_cycle(now, reconcile=False)  # reads just failed
            return
        self._db_failing_since = None  # any successful read breaks the streak
        if halt is None and stall_reason is not None:
            # A REAL stall is an incident too (round 5): the cancel goes to
            # the wire before the halt row is attempted — the executor is
            # already presumed dead (that IS the trip), and the sweep's
            # verify-by-enumeration catches anything racing the halt write.
            self._declare(_BlindIncident(since=now, reason=stall_reason, db_blind=False))
            log.error(
                "watchdog TRIPPED: %s — cancelling before any state write", stall_reason
            )
            await self._incident_cycle(now, declared_now=True)
            return
        if halt is not None and halt.swept_at is None:
            await self._sweep(halt)
        # After the protective work, never before it: the capability probe is
        # advisory, and its failures must not delay a sweep by one second.
        try:
            await self._verify_capability(now)
        except Exception:
            log.exception("capability probe failed; retrying next cycle")

    # --- liveness (issue #201) ---

    async def _beat(self, now: datetime) -> float:
        """Say ALIVE, best-effort (round 2 item 1c): losing the liveness
        signal is bad; skipping the cycle's protective work over it is
        worse. Bounded by the same real-time ceiling as every other state
        block, and it stamps the pulse clock.

        Answers what the attempt COST THE SWEEP, in real seconds: the strike
        is a time budget and not an error taxonomy (`_pulse`), so every exit
        — landed, refused, cancelled at the ceiling — is measured the same
        way and the caller needs to know nothing about which happened."""
        self._pulsed_at = now
        started = time.monotonic()
        try:
            await asyncio.wait_for(
                heartbeat.beat(self._pool, heartbeat.WATCHDOG_PROCESS, now),
                DB_BLOCK_CEILING_SECONDS,
            )
        except TimeoutError:
            log.error(
                "heartbeat write hit its %.0fs ceiling", DB_BLOCK_CEILING_SECONDS
            )
        except Exception:
            log.exception("heartbeat write failed — continuing the protective cycle")
        return time.monotonic() - started

    async def _push_keepalive(self) -> float:
        """The pulse's other leg: whatever else must not go stale while a
        sweep grinds — in production the dead-man's schedule. Same contract
        as `_beat` — it answers what it cost, and the budget decides.

        A rejected push is free to retry five seconds later and the schedule
        must not be abandoned over one 429; the audited gateway is
        best-effort, so this leg keeps working through a Postgres outage,
        which is exactly when the exchange-side net matters most."""
        assert self._keepalive is not None
        started = time.monotonic()
        try:
            # THE ONE PLACE THIS PROCESS CANCELS AN AUDITED WRITE FROM OUTSIDE,
            # and it is worth naming because three other docstrings here (the
            # module's #204 section, SAFETY_READ_CEILING_SECONDS, and
            # HttpExecutionGateway._post) argue that a write must bound ITSELF
            # rather than be cut, precisely so no attempt row is left without
            # an outcome. In production this keepalive IS an audited write — the
            # deadman's scheduleCancel — so a push cut here can orphan its
            # attempt row (AuditedExecutionGateway._audited catches Exception,
            # and the CancelledError wait_for raises is not one).
            #
            # Accepted, not overlooked, and the trade runs the other way than
            # it does for the cancel POST. The cancel is the protective action
            # itself, so its evidence is the trail's most valuable row. This is
            # ADVISORY work riding the protective path, and the thing being
            # protected is the pulse loop's own liveness: an un-cut push wedges
            # the sweep, which is the failure PR #203 exists to prevent. A
            # repeated scheduleCancel set REPLACES, so nothing is lost but the
            # row — an orphaned attempt with no outcome, which reads correctly
            # as "we tried and never learned whether it landed".
            await asyncio.wait_for(self._keepalive(), KEEPALIVE_CEILING_SECONDS)
        except TimeoutError:
            log.error(
                "sweep keepalive (dead-man's push) hit its %.0fs ceiling",
                KEEPALIVE_CEILING_SECONDS,
            )
        except Exception:
            # The sweep is the protective work and nothing advisory may stop
            # it. The deadman's own state machine is self-reconciling (a
            # repeated set REPLACES), so a cheap failure just retries.
            log.warning(
                "sweep keepalive (dead-man's push) failed", exc_info=True
            )
        return time.monotonic() - started

    def _pulse_allowed(self) -> bool:
        """Whether a sweep in the CURRENT posture may say it is alive.

        Normal operation: always. A DB-BLIND incident (running-process or
        cold-start): never — that path reaches the wire with zero Postgres
        behind it, both legs of the pulse write to Postgres, and a watchdog
        blind because the database is unreachable cannot truthfully beat to
        that database anyway. A REAL-STALL trip: yes, on round 6's own
        precedent — its liveness reads answered THIS cycle, so the database
        is healthy, and round 6 already decided that against a healthy
        database one bounded write before the wire is worth its evidence
        (there it was the write-ahead attempt row; here it is the beat that
        keeps the monitor from reading a trip's own multi-minute cancel pass
        as a dead watchdog)."""
        return self._blind is None or not self._blind.db_blind

    async def _pulse(self) -> None:
        """One liveness pulse from INSIDE the sweep (module docstring): beat
        the heartbeat and push the dead-man's schedule while the enumeration
        grinds. Silent in the postures `_pulse_allowed` refuses, and silent
        within SWEEP_PULSE_INTERVAL of the last pulse.

        TWO INDEPENDENT LEGS, EACH ON ITS OWN PER-CYCLE TIME BUDGET (review
        of PR #203, rounds 1–2).

        Independent, because coupling them recreates the very incident: a
        refused Postgres connection fails in milliseconds, so a beat failure
        that short-circuited the push would leave a full-speed-looking sweep
        with an unrefreshed schedule — and the audited gateway is
        best-effort exactly so a protective EXCHANGE action survives a
        database outage.

        Budgeted, because the throttle cannot bound a SLOW leg:
        SWEEP_PULSE_INTERVAL spaces pulses from their START, so an attempt
        that takes longer than the interval is charged at every enumeration
        step — a wedged database or a slow-failing exchange endpoint adds
        tens of minutes to a real sweep, which is round 1's bug at a
        different scale.

        On MEASURED TIME and not on error type, because the cost is wall
        clock and the failures that burn it are typed every which way: an
        exchange POST that 502s at 25s raises AmbiguousExecutionError, and
        the deadman deliberately re-queues an ambiguous push at once rather
        than advancing its due time, so a TimeoutError-keyed strike would
        never fire on the single most likely slow shape. Measuring time
        keeps the property that makes a strike safe at all — a leg that
        fails FAST keeps retrying, because it costs the sweep nothing and
        one blip must not silence a signal for a ten-minute grind — and it
        gets that for free rather than by another rule.

        The budget REFILLS ON THE CLOCK, not once per cycle (round-3 review
        of PR #203, F1): a cycle is a whole sweep, so a per-cycle budget let
        one slow failure early in a multi-minute grind silence a leg for the
        whole of it — long enough for an armed dead-man schedule to reach its
        horizon mid-halt. PULSE_LEG_REFILL_SECONDS states the window and the
        argument for its size; the bound it costs is in
        PULSE_LEG_BUDGET_SECONDS.

        AND ONE PUSH OUTRANKS BOTH GATES (issue #212). A budget and a throttle
        are the right shape for advisory work, and the dead-man's push stops
        being advisory when its schedule is about to lapse: at that boundary
        the sweep makes one exempted attempt BEFORE the step, because after
        the step there may be no schedule left to push — for as long as the
        step is the longest thing between two boundaries, which the pacing
        sleep is not. PRIORITY_PUSH_WINDOW_SECONDS states when, what the
        exemption guarantees, and what it does not close."""
        if not self._pulse_allowed():
            return
        self._refill_leg_budgets()
        pushed = await self._priority_push(self._clock.now())
        # Re-read: the attempt above can have spent a whole KEEPALIVE_CEILING,
        # and both the throttle below and the beat it gates want the time it
        # is NOW, not the time the pulse was entered.
        now = self._clock.now()
        if self._pulsed_at is not None and now - self._pulsed_at < SWEEP_PULSE_INTERVAL:
            return
        # The pulse clock advances even when both legs are spent, so a quiet
        # sweep does not re-enter this body at every step.
        self._pulsed_at = now
        # THE KEEPALIVE GOES FIRST. Both legs are advisory to the sweep, but
        # only one has a deadline it can miss: the dead-man's schedule lapses
        # a half-horizon after its push falls due (150s), while the beat's
        # consumer — the monitor's staleness check — allows 300s. Beating
        # first would put a wedged database's ceiling in front of the push on
        # every pulse, which is the one term the gap arithmetic at
        # PULSE_LEG_REFILL_SECONDS cannot afford.
        if (
            not pushed  # the priority attempt above WAS this pulse's push
            and self._keepalive is not None
            and self._keepalive_spent < PULSE_LEG_BUDGET_SECONDS
        ):
            await self._charge_push()
        if self._beat_spent < PULSE_LEG_BUDGET_SECONDS:
            self._beat_spent += await self._beat(now)
            _report_if_spent("heartbeat", self._beat_spent)

    async def _priority_push(self, now: datetime) -> bool:
        """The armed schedule is close enough to its horizon that the step
        about to start could outlive it: push NOW, exempt from the leg budget
        and from the pulse throttle (issue #212). Answers whether it pushed,
        so the pulse does not immediately push a second time.

        WHY THE EXEMPTION IS SAFE AGAINST THE BUDGET IT OVERRIDES: that budget
        exists to stop a wedged leg taxing every enumeration STEP of a
        multi-minute sweep (PR #203 round 1), and this cannot become that. It
        fires only inside PRIORITY_PUSH_WINDOW_SECONDS of a horizon, only
        while that horizon is still ahead, and no more often than
        SWEEP_PULSE_INTERVAL — so its worst case is bounded per armed
        schedule, not per step, and the constant's comment does that
        arithmetic. The cost is still CHARGED to the leg's counter — exempt at
        the gate, not free — so it can never hand the budgeted path a longer
        purse than it had.

        Paced on the INJECTED clock, unlike the leg budgets' monotonic
        seconds, because the thing being paced against is the deadline, and a
        deadline and its own pacing must be read off one clock or a fake can
        satisfy neither.

        The deadline is re-read at every boundary rather than cached: the
        keepalive is the thing that moves it, so a cached one would be stale
        by exactly the push it is deciding about — a landed push closes the
        window by 300s, which is the whole reason a healthy system never
        reaches this code."""
        if self._keepalive is None or self._keepalive_deadline is None:
            return False
        armed_until = self._keepalive_deadline()
        if armed_until is None:
            return False  # nothing armed: no horizon to be near
        left = (armed_until - now).total_seconds()
        # `left > 0`: a horizon already past is a schedule that has fired or
        # lapsed, and jumping the queue saves nothing — the ordinary budgeted
        # leg re-arms it on the deadman's own overdue cadence rather than the
        # sweep paying a ceiling at every step until the exchange answers.
        # No lower guard: a healthy push is milliseconds, so an attempt with
        # 5s left is still a schedule saved, and refusing it to protect the
        # sweep's pace would be trading the thing this exists for.
        if not 0 < left <= PRIORITY_PUSH_WINDOW_SECONDS:
            return False
        if (
            self._priority_pushed_at is not None
            and now - self._priority_pushed_at < SWEEP_PULSE_INTERVAL
        ):
            return False
        # Stamped BEFORE the attempt: a wedge is cut by the ceiling inside
        # `_push_keepalive` and a failure is swallowed there, so the stamp has
        # to be about attempts rather than successes or a fast-failing push
        # would be re-issued at every step of a fast sweep.
        self._priority_pushed_at = now
        log.error(
            "dead-man's schedule is %.0fs from its horizon and the next sweep step "
            "could outlive it — a budget-exempt push attempt now (issue #212)",
            left,
        )
        await self._charge_push()
        return True

    async def _charge_push(self) -> None:
        """Push, and bill what it cost to the leg's counter. One place because
        the exempted path and the budgeted one differ in WHETHER they push,
        never in what a push costs — a priority attempt is exempt at the gate,
        not free (`_priority_push`), and both must report the same way when
        the counter goes over."""
        self._keepalive_spent += await self._push_keepalive()
        _report_if_spent("dead-man's push", self._keepalive_spent)

    def _refill_leg_budgets(self, *, force: bool = False) -> None:
        """Give both pulse legs their budgets back — at every cycle top
        (`force`) and every PULSE_LEG_REFILL_SECONDS of a grinding sweep.

        Both legs refill together on ONE window because the window is a
        property of the sweep's wall clock, not of either leg; the legs stay
        independent where it matters, which is that each spends and strikes
        on its own counter. Monotonic seconds, like every spend it undoes:
        the injected clock can be frozen or fast-forwarded by a fake, and a
        budget that refilled on it would refill never or always."""
        elapsed = time.monotonic() - self._budgets_refilled_at
        if not force and elapsed < PULSE_LEG_REFILL_SECONDS:
            return
        self._budgets_refilled_at = time.monotonic()
        self._beat_spent = 0.0
        self._keepalive_spent = 0.0

    async def _safety_read(self, read: Awaitable[T]) -> T:
        """ONE read of the safety path: say alive first, then a hard
        real-time ceiling on the read itself (issue #204).

        Both halves are the same guarantee seen from either side. The pulse
        is what makes the gap between two liveness signals a gap of one read
        rather than one enumeration pass; the ceiling is what makes that gap
        finite at all, which uncapped it was not — a 429-saturated read can
        run for minutes, and a minute of that spent straddling a push-due
        boundary is a dead-man's switch fired under a working watchdog.

        The pulse here is deliberately a SECOND one on the enumeration path:
        `_walk_scope` already pulses before paying the shared bucket, because
        the pacing sleep is itself time between signals. Within
        SWEEP_PULSE_INTERVAL the second one costs nothing, and past it — a
        slow pace, a slow read — it is exactly the signal that was missing."""
        await self._pulse()
        return await asyncio.wait_for(read, SAFETY_READ_CEILING_SECONDS)

    async def _read_liveness(self, now: datetime) -> tuple[Halt | None, str | None]:
        halt = await active_halt(self._pool)
        stall = None if halt is not None else await self._executor_stall(now)
        return halt, stall

    # --- incident operation (rounds 2–5) ---

    def _declare(self, incident: _BlindIncident) -> None:
        """Open an incident: from here until reconcile, cancel passes run
        with ZERO Postgres before the wire — the rate budget drops to its
        in-process bucket (FallbackBudget.incident_mode) instead of
        attempting the shared row, and the audit wrapper defers its attempt
        row to after the call (wire_first)."""
        self._blind = incident
        self._blind_passes = 0
        self._set_incident_posture(incident)

    def _clear_incident(self) -> None:
        self._blind = None
        self._blind_passes = 0
        # The failure streak dies with the incident (round 6 item 1): the
        # reconcile's successful writes ARE the interruption, so a later
        # blip must open a FRESH streak — carrying the old onset would
        # re-trip instantly with a false "without interruption" span in the
        # durable record.
        self._db_failing_since = None
        self._set_incident_posture(None)

    def _set_incident_posture(self, incident: _BlindIncident | None) -> None:
        # In-process test budgets (WeightBudget) lack the flag — and are
        # already Postgres-free, so there is nothing to switch off.
        if isinstance(self._budget, FallbackBudget):
            self._budget.incident_mode = incident is not None
        # Wire-first audit only while the database is actually UNREADABLE
        # (round 6 item 3): a real-stall trip's liveness reads answered this
        # very cycle, so its write-ahead attempt row would land and keeps
        # its evidential value — deferring it would open a carve-out (a
        # crash between cancel and deferred write, against a HEALTHY
        # database) that nothing else covers.
        self._exec.wire_first = incident is not None and incident.db_blind

    def _handle_unreadable(self, now: datetime) -> None:
        """A liveness read failed. Open (or age) the CONTINUOUS failure
        streak — any successful read resets it (round 3 item 3: one dropped
        connection after a long healthy cycle must never read as an outage)
        — and declare the blind incident once the streak outlives the
        threshold: past it, an unknowable executor is indistinguishable
        from a dead one, and blind fails protective."""
        if self._db_failing_since is None:
            self._db_failing_since = now  # a new failure streak opens
        blind_for = now - self._db_failing_since
        if blind_for <= self._db_blind_after:
            log.warning(
                "watchdog: Postgres unreadable for %ds unbroken (blind trip at %ds)",
                int(blind_for.total_seconds()),
                int(self._db_blind_after.total_seconds()),
            )
            return
        incident = _BlindIncident(
            since=now,
            reason=(
                f"Postgres unreadable without interruption since "
                f"{self._db_failing_since:%Y-%m-%d %H:%M:%S} UTC — blind for "
                f"{int(blind_for.total_seconds())}s > "
                f"{int(self._db_blind_after.total_seconds())}s; the executor's "
                f"liveness is unknowable, which is indistinguishable from death"
            ),
            db_blind=True,
        )
        self._declare(incident)
        log.error(
            "watchdog BLIND TRIP: %s — cancelling resting orders without halt state",
            incident.reason,
        )

    async def _incident_cycle(
        self, now: datetime, *, reconcile: bool = True, declared_now: bool = False
    ) -> None:
        """One cycle of a declared incident: THE WIRE FIRST — the cancel
        pass performs zero Postgres work (in-process budget, wire-first
        audit) — then the durable catch-up (halt row, events, sweep
        verification), best-effort and hard-ceilinged so no wedged rollback
        can stall the next cycle. `reconcile=False` skips the catch-up on
        cycles where the liveness reads JUST failed — the database has
        already answered the question. `declared_now` carries the explicit
        "this trip opened this very cycle" fact into the reconcile, which
        keys the plain-reason/no-window-event shape on it rather than
        inferring it from timestamp equality."""
        assert self._blind is not None
        kind = _incident_kind(self._blind)
        try:
            await self._unrecorded_pass(f"{kind}: {self._blind.reason}")
        except Exception:
            # An exchange-side failure must not block the reconcile attempt:
            # the halt row is valuable even while cancels are failing.
            log.exception("incident cancel pass failed; reconcile still attempted")
        if reconcile:
            await self._try_reconcile(now, declared_now=declared_now)

    async def _try_reconcile(self, now: datetime, *, declared_now: bool = False) -> None:
        """Close the incident's books, AFTER the wire: halt row + events
        under a hard real-time ceiling (a wedged transaction rollback hangs
        in asyncpg BEFORE any per-op timeout arms — the ceiling cancels the
        block, and the shielded pool release then terminates the wedged
        connection within the acquire budget). Reconciliation is BOOKKEEPING
        and must never gate protection (round 3 item 2): failure keeps the
        incident open and next cycle cancels again."""
        assert self._blind is not None
        try:
            halt = await asyncio.wait_for(
                self._settle_blind_debt(now, same_cycle_trip=declared_now),
                DB_BLOCK_CEILING_SECONDS,
            )
        except Exception:
            log.exception(
                "incident reconcile failed (bounded) — the incident stays open and "
                "the next cycle cancels again"
            )
            return
        if halt is not None and halt.swept_at is None:
            # The incident pass WAS the cancel; this sweep only verifies and
            # stamps — re-cancelling what the pass just killed would be
            # redundant wire work between two enumerations of the same book.
            await self._sweep(halt, skip_cancel=True)

    async def _unrecorded_pass(self, decision: str) -> None:
        """One cancel pass that Postgres cannot record, counted AFTER it
        completes: the counter is audit evidence (blind_window_reconciled
        reports it), so it counts completions, not attempts. The chosen
        error direction: a pass that dies mid-flight counts zero even if
        some cancels landed before the failure — the trail may UNDERcount
        blind activity, never claim passes that didn't finish. Welded to
        the pass here so a call site can neither forget nor miscount it."""
        await self._cancel_resting(decision)
        self._blind_passes += 1

    async def _settle_blind_debt(
        self, now: datetime, *, same_cycle_trip: bool = False
    ) -> Halt | None:
        """Postgres can record the incident: write the halt row (joining any
        halt that appeared meanwhile) under a headline that PRESERVES which
        trip it was — "DB-blind sweep" for the unreadable-database trip,
        "unrecorded trip" for a real observed stall — so the operator can
        tell them apart. A trip reconciled in the SAME cycle it was declared
        (the healthy-database normal case since round 5 put the cancel
        before the halt write) keeps the plain stall reason and skips the
        window event: nothing ran in the dark, and the trail's cancel rows
        landed normally. An incident that SPANNED cycles leaves the durable
        blind-window event whenever this reconcile runs (round 3 item 4):
        joining a standing halt writes no new halt state by design, and the
        window's own best-effort audit rows likely failed with the same
        outage. The one hole — the watchdog dying between DB recovery and
        this cycle, taking the in-process marker with it — is carved out in
        the runbook, not papered over here. The normal sweep machinery then
        verifies the book and stamps swept_at with the position snapshot."""
        assert self._blind is not None
        # A trip can only be "same cycle" via the explicit declared_now flag
        # from the trip branch (not timestamp equality — a frozen or
        # coarse clock must never turn a spanning window into a quiet trip).
        if same_cycle_trip:
            assert not self._blind.db_blind  # blind windows always span cycles
            headline = "trip"
            reason = self._blind.reason
        else:
            # The trip parenthetical says "unconfirmed", not "unwritable"
            # (round 6 item 4): a reconcile ceiling can fire BETWEEN the
            # halt row's commit and clearing the incident, in which case the
            # next cycle joins its own, successfully written row — the label
            # must be true for both that case and a genuinely failing write.
            headline = f"{_incident_kind(self._blind)} reconciled" if self._blind.db_blind else (
                "unrecorded trip reconciled (halt row write had not been confirmed)"
            )
            reason = (
                f"{headline}: {self._blind.reason}; resting orders were being "
                f"cancelled unrecorded since "
                f"{self._blind.since:%Y-%m-%d %H:%M:%S} UTC; Postgres answered "
                f"again at {now:%Y-%m-%d %H:%M:%S} UTC"
            )
        halt, created = await request_halt(
            self._pool, self._clock, self._audit, source=WATCHDOG_SOURCE, reason=reason
        )
        if not same_cycle_trip:
            await self._audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="blind_window_reconciled",
                risk_decision=headline,
                detail={
                    "reason": self._blind.reason,
                    "blind_since": self._blind.since.isoformat(),
                    "recovered_at": now.isoformat(),
                    "unrecorded_cancel_passes": self._blind_passes,
                    "halt_id": halt.id,
                    "joined_standing_halt": not created,
                    # Issue #145: which KIND of blind window this was, as a
                    # queryable field and not only inside the prose reason.
                    "cold_start": self._blind.cold_start,
                },
                master_address=self._master,
            )
            log.error(
                "watchdog: unrecorded window reconciled into halt #%d (joined=%s)",
                halt.id,
                not created,
            )
        elif created:
            log.error("watchdog: trip recorded as halt #%d", halt.id)
        self._clear_incident()
        return halt

    async def _executor_stall(self, now: datetime) -> str | None:
        """The trip condition: the executor HAS run (its row exists) and its
        heartbeat is older than the threshold. No row means no executor was
        ever deployed — nothing to protect, not an emergency (decommissioning
        deletes the row for the same reason; see the runbook)."""
        beaten = await heartbeat.last_beat(self._pool, heartbeat.EXECUTOR_PROCESS)
        if beaten is None:
            return None
        age = now - beaten
        if age <= self._executor_stale:
            return None
        return (
            f"executor heartbeat stale: {int(age.total_seconds())}s > "
            f"{int(self._executor_stale.total_seconds())}s"
        )

    # --- the sweep ---

    async def _sweep(self, halt: Halt, *, skip_cancel: bool = False) -> None:
        """Cancel-all with verify-by-enumeration (module docstring): cancel
        what rests, then list AGAIN over a fresh venue listing, and only an
        empty, COMPLETE-coverage listing stamps the sweep done — with the
        position snapshot and the unwind policy recorded. Any failure leaves
        the halt unswept for the next cycle; enumeration is idempotent and
        cancels tolerate re-issue.

        ACCOUNT-WIDE ON BOTH AXES. Venues: every builder dex in the live
        perpDexs listing, not just POSITION_VENUES (PR #143 review).
        ACCOUNTS: the master plus every sub-account (issue #136, ADR-0007
        decision 1) — A4 is the first thing in this codebase that can place
        an order on a sub, and "adding a venue to trading means adding it to
        the sweep" applies to accounts exactly as it applies to dexs. The sub
        list comes from the EXCHANGE (`subAccounts`), not from Epigone's
        `copy_subs` table, so the cold-start blind path — which has no
        database at all — sweeps them under this same code.

        `skip_cancel` is the incident-reconcile shape (round 5): the
        incident's own pass already cancelled, so this call only enumerates
        and stamps — an order that somehow still rests leaves the halt
        unswept and the NEXT cycle's full sweep cancels it."""
        if skip_cancel:
            dexs, accounts, complete = await self._sweep_scope()
            orders = await self._open_orders(dexs, accounts, phase="verify")
        else:
            dexs, accounts, complete, cancelled = await self._cancel_resting(
                f"halt #{halt.id} ({halt.source}): {halt.reason}"
            )
            if cancelled:
                # Cancels went out: the deciding enumeration must be FRESH —
                # listing included, so a dex appearing mid-sweep can't hide
                # an order from the verify. An already-empty book needs no
                # second look (its first enumeration IS the verify).
                dexs, accounts, complete = await self._sweep_scope()
                orders = await self._open_orders(dexs, accounts, phase="verify")
            else:
                orders = []
        if orders or not complete:
            log.warning(
                "halt #%d sweep incomplete: %d order(s) resting, coverage %s; "
                "retrying next cycle",
                halt.id,
                len(orders),
                "complete" if complete else "PARTIAL (perpDexs or subAccounts unavailable)",
            )
            return
        positions = await self._open_positions(dexs, accounts)
        # Durable write, hard-ceilinged like every state block (round 5): a
        # partition striking mid-transaction here must cost a bounded cycle,
        # not a hung loop that can never reach the blind machinery.
        await asyncio.wait_for(
            mark_swept(
                self._pool,
                self._clock,
                self._audit,
                halt=halt,
                positions=[_position_json(p) for p in positions],
                unwind_policy=HOLD_POLICY,
            ),
            DB_BLOCK_CEILING_SECONDS,
        )
        log.error(
            "halt #%d swept: book empty across %d venue(s) × %d account(s); %d open "
            "position(s) HELD per %s (docs/runbooks/halt-and-unwind.md)",
            halt.id,
            len(dexs) + 1,
            len(accounts),
            len(positions),
            HOLD_POLICY,
        )

    async def _cancel_resting(
        self, decision: str
    ) -> tuple[list[str], list[str | None], bool, list[_AccountOrder]]:
        """Enumerate account-wide and cancel whatever rests — the shared
        kernel of the normal sweep and the DB-blind sweep. No verification,
        no stamping: callers own what "done" means. Returns what it saw —
        (venues, accounts, coverage-complete, the orders it cancelled) — so
        the normal sweep can treat an already-empty first enumeration as its
        verify instead of re-billing a second one.

        Cancels are grouped PER ACCOUNT because a cancel names a book: one
        action per account, carrying that account's vault flag (the master's
        is None). A sub whose cancel fails leaves the halt unswept and the
        next cycle re-enumerates — the same idempotent retry the venue axis
        already relies on.

        `decision` is set on the gateway immediately before each cancel, not
        once up front (issue #201): the enumeration in between pulses, and a
        pulse's dead-man push sets `decision` for its own action. Per-cancel
        assignment makes the attribution correct whatever runs between two
        cancels, instead of correct-by-luck."""
        dexs, accounts, complete = await self._sweep_scope()
        orders = await self._open_orders(dexs, accounts, phase="cancel pass")
        if orders:
            asset_ids = await self._asset_ids_for(orders)
            for account in accounts:
                theirs = [o.order for o in orders if o.account == account]
                if not theirs:
                    continue
                await self._pulse()
                self._exec.decision = decision
                await self._exec.cancel_orders(
                    _cancels_for(theirs, asset_ids), vault_address=account
                )
                log.warning(
                    "sweep progress: cancelled %d resting order(s) on %s",
                    len(theirs),
                    account or "the master",
                )
        return dexs, accounts, complete, orders

    async def _sweep_scope(self) -> tuple[list[str], list[str | None], bool]:
        """What this sweep must cover, on both axes: (builder dexs, accounts,
        coverage-complete).

        Either enumeration can fail, and a failure on either degrades the same
        way (round 2 item 3): fall back to what is certainly covered, say so,
        and never stamp swept_at on partial coverage — a partial sweep that
        announces itself beats a total abort that cancels nothing.

        The account axis is issue #136's: `None` is the master (no vault
        flag), and every sub-account follows. Read from the exchange rather
        than from Epigone's own mapping table so the cold-start blind path,
        which has no database, enumerates them identically."""
        await self._pulse()
        await self._budget.spend(META_WEIGHT)
        try:
            dexs = await self._safety_read(self._read.get_perp_dexs())
            dexs_complete = True
        except Exception:
            dexs = [dex for dex in POSITION_VENUES if dex is not None]
            dexs_complete = False
            log.error(
                "perpDexs listing unavailable — sweep coverage degraded to the "
                "covered venues %s (PARTIAL; swept_at withheld)",
                dexs,
                exc_info=True,
            )
        accounts: list[str | None] = [None]
        await self._budget.spend(META_WEIGHT)
        try:
            accounts += list(
                await self._safety_read(self._read.get_sub_accounts(self._master))
            )
            accounts_complete = True
        except Exception:
            accounts_complete = False
            log.error(
                "subAccounts listing unavailable — sweeping the master ONLY; any "
                "Copy Sub-account's resting orders are NOT covered this pass "
                "(PARTIAL; swept_at withheld)",
                exc_info=True,
            )
        # The scope, up front: the operator (and the log reader) can size the
        # grind before it starts instead of inferring it from silence. INFO
        # like the progress lines it heads — this module's warning/error
        # levels are for what CHANGED (an incomplete sweep, a cancel, a
        # stamped halt), and narration must not compete with them.
        steps = _scope_steps(dexs, accounts)
        log.info(
            "sweep scope: %d venue(s) × %d account(s) = %d enumeration(s) per pass, "
            "~%d weight (~%ds at the shared %d/min pacing); coverage %s",
            len(dexs) + 1,
            len(accounts),
            steps,
            steps * ORDERS_WEIGHT,
            round(steps * ORDERS_WEIGHT * 60 / SHARED_WEIGHT_PER_MINUTE),
            SHARED_WEIGHT_PER_MINUTE,
            "complete" if dexs_complete and accounts_complete else "PARTIAL",
        )
        return dexs, accounts, dexs_complete and accounts_complete

    async def _asset_ids_for(self, orders: list[_AccountOrder]) -> dict[str, int]:
        # Map exactly the dexs the enumerated orders sit on (namespaced
        # `dex:COIN` coins), plus the core universe fetch_asset_ids always
        # reads. One map serves every account: asset ids are a property of the
        # venue, not of the book. Billing: core meta + perpDexs (when any dex
        # is needed) + one meta per needed dex.
        #
        # The spends are paid UP FRONT (the shared bucket paces them as a
        # group), but the reads are not: fetch_asset_ids makes 2+N HTTP calls
        # and they used to run back-to-back with no pulse and no ceiling
        # between them (issue #204) — a stretch of the cancel path, sitting
        # between the enumeration and the cancels it feeds, that was invisible
        # to every liveness signal. `_PulsedUniverseReads` puts each of those
        # reads on the same seam every other safety-path read runs through.
        needed = sorted(
            {o.order.coin.split(":", 1)[0] for o in orders if ":" in o.order.coin}
        )
        spends = 1 + (1 + len(needed) if needed else 0)
        for _ in range(spends):
            await self._pulse()
            await self._budget.spend(META_WEIGHT)
        return await fetch_asset_ids(
            _PulsedUniverseReads(self._read, self._safety_read), dexs=needed
        )

    async def _walk_scope(
        self, dexs: list[str], accounts: list[str | None], weight: int
    ) -> AsyncIterator[tuple[str | None, str, str | None, str]]:
        """Walk the sweep's (account, dex) grid — the shape both reads take,
        in one place so the three things every step owes are structural
        rather than remembered: PULSE (issue #201 — a step is where a long
        sweep says it is alive), PAY the budget, and count itself. Yields
        (account, address, dex, "i/N") with the pulse and the spend already
        done, so the caller is left with only its own read."""
        steps = _scope_steps(dexs, accounts)
        step = 0
        for account in accounts:
            address = account or self._master
            for dex in [None, *dexs]:
                step += 1
                await self._pulse()
                await self._budget.spend(weight)
                yield account, address, dex, f"{step}/{steps}"

    async def _open_orders(
        self, dexs: list[str], accounts: list[str | None], *, phase: str
    ) -> list[_AccountOrder]:
        """The account-wide order enumeration — the expensive axis, and the
        one the whole sweep's wall clock is made of. Every step reports
        itself, so a multi-minute pass is legible in the logs while it
        runs instead of only when it finishes."""
        orders: list[_AccountOrder] = []
        async for account, address, dex, at in self._walk_scope(dexs, accounts, ORDERS_WEIGHT):
            found = await self._safety_read(self._read.get_open_orders(address, dex=dex))
            orders.extend(_AccountOrder(account=account, order=order) for order in found)
            log.info(
                "sweep progress: %s %s — %s on %s: %d resting order(s), %d so far",
                phase,
                at,
                address,
                dex or "core",
                len(found),
                len(orders),
            )
        return orders

    async def _open_positions(
        self, dexs: list[str], accounts: list[str | None]
    ) -> list[Position]:
        positions: list[Position] = []
        async for _account, address, dex, at in self._walk_scope(
            dexs, accounts, POSITIONS_WEIGHT
        ):
            positions.extend(
                await self._safety_read(self._read.get_open_positions(address, dex=dex))
            )
            log.info(
                "sweep progress: position snapshot %s — %s on %s: %d open",
                at,
                address,
                dex or "core",
                len(positions),
            )
        return positions

    # --- the on-chain capability probe (advisory) ---

    async def _verify_capability(self, now: datetime) -> None:
        """The beating-but-impotent guard (PR #143 review): verify ON-CHAIN —
        via the public extraAgents readback, no signing, no trading action —
        that this watchdog's agent key is still approved and unexpired, so a
        mid-run deregistration or an unrestarted rotation pages the operator
        BEFORE an incident needs the cancel, not during. Verdict state lands
        beside the heartbeat (migration 0025) for the #52 monitor; verdict
        TRANSITIONS go to the audit trail. EVERY failure shape — read errors
        of any type, an unwritable verdict — advances the retry fuse, so no
        outage flavor can re-fire the probe per-cycle (round 2 item 4)."""
        if self._next_capability_at is not None and now < self._next_capability_at:
            return
        await self._budget.spend(META_WEIGHT)
        try:
            # Ceilinged like every other read on this process (issue #204).
            # Advisory work, but it runs on the watchdog's ONE task: a wedged
            # extraAgents read holds up the next cycle, and with it the next
            # heartbeat and the next dead-man push.
            agents = await self._safety_read(self._read.get_extra_agents(self._master))
        except Exception:
            self._defer_capability_retry(now, "extraAgents read failed")
            return
        approval = next((a for a in agents if a.address == self._signer_address), None)
        if approval is None:
            capable = False
            detail = (
                f"agent {self._signer_address} is NOT among {self._master}'s approved "
                f"agents — deregistered on-chain (or never approved); every cancel "
                f"this watchdog issued would be rejected"
            )
        elif approval.valid_until <= now:
            capable = False
            detail = (
                f"agent {self._signer_address} approval EXPIRED "
                f"{approval.valid_until:%Y-%m-%d %H:%M} UTC — rotate the watchdog "
                f"lane and restart the service (agent-key-rotation runbook)"
            )
        else:
            capable = True
            detail = (
                f"approved on-chain as {approval.name or 'unnamed'} "
                f"until {approval.valid_until:%Y-%m-%d}"
            )
        try:
            await asyncio.wait_for(
                self._store_verdict(capable, detail, now), DB_BLOCK_CEILING_SECONDS
            )
        except Exception:
            self._defer_capability_retry(now, "verdict unrecordable (Postgres?)")
            return
        self._next_capability_at = now + self._capability_interval

    async def _store_verdict(self, capable: bool, detail: str, now: datetime) -> None:
        if capable != self._capable:
            # Event before state (the deadman's rule): a lost event write
            # leaves the verdict unclaimed and re-derived on retry.
            await self._audit.record_event(
                actor=WATCHDOG_ACTOR,
                action="watchdog_capable" if capable else "watchdog_impotent",
                risk_decision="on-chain capability probe (extraAgents)",
                detail={"verdict": detail},
                master_address=self._master,
            )
            self._capable = capable
            if not capable:
                log.error("watchdog IMPOTENT: %s", detail)
        await heartbeat.record_capability(
            self._pool, heartbeat.WATCHDOG_PROCESS, capable=capable, detail=detail, now=now
        )

    def _defer_capability_retry(self, now: datetime, what: str) -> None:
        log.warning(
            "capability probe: %s; retrying in %ds",
            what,
            int(CAPABILITY_RETRY.total_seconds()),
            exc_info=True,
        )
        self._next_capability_at = now + CAPABILITY_RETRY


class _PulsedUniverseReads:
    """The watchdog's read gateway, seen the way the asset-map fetch must see
    it on the safety path (issue #204): every universe read routed through
    `_safety_read`, so `fetch_asset_ids`' 2+N back-to-back HTTP calls each
    pulse first and each carry a ceiling.

    A wrapper rather than a hook inside the helper, because the offset
    arithmetic that makes an asset id correct lives in exactly one place
    (gateway.fetch_asset_specs) and must keep living there; what the sweep
    needs to change is how each read is AWAITED, which is precisely what a
    wrapper can say and a parameter cannot. Satisfies gateway's narrow
    PerpUniverseReader — the two methods that fetch actually makes.

    Handed the gateway and the seam rather than the watchdog: what it needs
    is a way to read and a way to read SAFELY, and naming exactly those two
    keeps it from being a second door into the watchdog's internals."""

    def __init__(self, read: HyperliquidGateway, through: "_SafetyRead") -> None:
        self._read = read
        self._through = through

    async def get_perp_assets(self, dex: str | None = None) -> list[PerpAsset]:
        return await self._through(self._read.get_perp_assets(dex))

    async def get_perp_dexs(self) -> list[str]:
        return await self._through(self._read.get_perp_dexs())


def _report_if_spent(leg: str, spent: float) -> None:
    """A pulse leg that has just gone over its budget says so once, where a
    reader of the sweep's own progress lines will see it: the signal it
    carries is about to stop, and the reason is that it was costing the sweep
    more than the sweep can pay. Said again after each refill, because each
    refill is a fresh attempt and a fresh exhaustion."""
    if spent >= PULSE_LEG_BUDGET_SECONDS:
        log.error(
            "sweep pulse: the %s leg has spent %.1fs (budget %.1fs) — quiet until "
            "the budget refills in at most %.0fs of sweep, or at the next cycle; "
            "the protective work continues",
            leg,
            spent,
            PULSE_LEG_BUDGET_SECONDS,
            PULSE_LEG_REFILL_SECONDS,
        )


def _scope_steps(dexs: list[str], accounts: list[str | None]) -> int:
    """How many (account, dex) enumerations one pass over this scope costs.
    The `+ 1` is the core venue, which is never in the builder-dex listing —
    stated once here so the sweep's size, its ETA and its progress counter
    cannot drift apart."""
    return len(accounts) * (1 + len(dexs))


def _cancels_for(orders: list[OpenOrder], asset_ids: dict[str, int]) -> list[CancelSpec]:
    """One account's cancels, by oid. An order whose coin has no asset id
    fails the WHOLE sweep loudly rather than being skipped: a skipped order is
    a live order the trail would show as swept-over."""
    cancels: list[CancelSpec] = []
    for order in orders:
        asset = asset_ids.get(order.coin)
        if asset is None:
            raise GatewayError(
                f"open order {order.order_id} is on coin {order.coin!r} with no "
                f"asset id in the universe — cannot cancel it; sweep aborted"
            )
        cancels.append(CancelSpec(asset=asset, oid=order.order_id))
    return cancels


def _incident_kind(incident: _BlindIncident) -> str:
    """The operator-facing name of an incident, used verbatim in the cancel
    pass's risk decision and (with " reconciled") in the halt headline. Three
    kinds, because the follow-up differs: an unreadable database, an
    unreadable database that was ALREADY unreachable when this process
    launched (issue #145 — the process restarted mid-outage), and a real
    observed stall whose halt row could not be confirmed."""
    if not incident.db_blind:
        return "unrecorded trip"
    return "cold-start DB-blind sweep" if incident.cold_start else "DB-blind sweep"


def _position_json(position: Position) -> dict[str, str]:
    """The halt row's position snapshot entry: what the operator needs to act
    from the alert, Decimals as strings (the JSONB convention)."""
    return {
        "coin": position.coin,
        "side": position.side.value,
        "size_usd": str(position.size_usd),
        "leverage": str(position.leverage),
        "entry_price": str(position.entry_price),
        "unrealized_pnl": str(position.unrealized_pnl),
    }
