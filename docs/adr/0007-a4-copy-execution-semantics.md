# ADR 0007: A4 copy-execution semantics

Date: 2026-08-04
Status: accepted (settled in the 2026-08-03/04 /grill-with-docs session for
#136; every decision below is the operator's, recorded with the reasoning
that survived the grill)

## Context

Issue #136 (A4, operator copy executor) states what to build but leaves the
money-losing edges open. #138's amended agenda lists them. This ADR records the
operator's decisions, one per section, with the reasoning that survived the
grill. ADR-0005 (key custody) and ADR-0006 (position-event seam) are inputs and
are not revisited here.

## Decisions settled so far

### 1. Capital model: one Copy Sub-account per Leader

Each copy-enabled Leader gets a dedicated Hyperliquid sub-account, funded with
a fixed allocation. The allocation IS the exposure cap — margin-enforced by the
exchange, not by Epigone bookkeeping. Verified foundations (gateway TESTNET
FINDINGS 3/6): one master-approved agent key trades every sub via
`vaultAddress`; `subAccountTransfer` funds them programmatically; each sub has
its own clearinghouseState/PnL, so per-Leader copy stats are exchange-native.

- Multi-Leader subs are DEFERRED. The grill worked through the same-coin
  collision options (first-owner-wins / attribution ledger / communal position
  with add-on-open, flatten-on-any-close, skip-opposite-side) and the operator
  chose to start single-Leader; the communal-position analysis is preserved in
  the grill transcript for when grouping returns.
- A sub-account is NOT a key-compromise boundary (an agent key can drain subs
  to the master — finding 3). It is a risk/margin boundary only. ADR-0005's
  no-external-exit invariant is unchanged.
- `createSubAccount` sits behind a $100k cumulative-volume gate. Testnet
  master: already past it. Mainnet: farming past the gate (~$65 at measured
  6.5bp) is a mainnet-switch checklist item, recorded in #138.
- The A3 watchdog sweep and /kill must enumerate and cancel PER SUB, including
  the cold-start blind path. A4 is the first thing that can place orders on a
  sub, so this scope lands in A4 (the "adding a venue means adding it to the
  sweep" rule from the runbook, applied to sub-accounts).
- Max sub-account count: PROBED (2026-08-04, `scripts/testnet_subaccount_cap_probe.py`,
  gateway TESTNET FINDING 10) — **10 sub-accounts per master, flat**. The 11th
  creation is refused `"Too many sub-accounts."`, and the ceiling did not move
  at $1M cumVlm, so unlike the agent cap it is not volume-scaled. Under
  one-sub-per-Leader that is a hard ceiling of 10 concurrent Leaders, minus
  whatever non-copy subs the master already holds; subs cannot be deleted, so
  a retired Leader's sub is REUSED rather than replaced. Far above phase A's
  handful — it constrains nothing today, and it is the number that would
  decide whether deferred multi-Leader subs ever need to return.

### 2. Sizing: fixed Base Notional per sub, relative mirroring

Option (a) of the grill: each Copy Sub-account has a Base Notional (e.g. $200).
A Leader's open is copied at Base Notional regardless of the Leader's absolute
size — the Leader's size NEVER determines copy size; only relative changes do.
A 50% scale-in scales the copy position by 50%; a 30% trim sells 30%. The
5%-of-their-account vs 40%-of-ours conflict dissolves by construction, and the
model is robust to stale leader-equity data (38% of screened wallets had
emptied their accounts while stored metrics looked alive, 2026-07-29 research).
No separate aggregate cap needed in v0: the sub's allocation bounds everything,
including scale-in compounding — a scale the margin can't absorb simply rejects.

### 3. Flip execution: close-then-open, two orders

The event is one row (ADR-0006); execution is two orders. The close leg is
REDUCE-ONLY (structurally cannot over-close or reverse). The open leg then goes
through the FULL fresh-open pipeline — halt re-check, staleness guard, risk
policy — as if it were a new open. Close-fills-but-open-declined ends FLAT with
an audit row: the missed-copy failure direction, chosen deliberately. There is
exactly one code path that opens positions; a halt landing between the legs
stops the open leg cleanly (this is the executor-side answer to A3's
`skip_cancel` residual race window mid-flip).

### 4. Order type: IOC-only, bounded slippage; nothing entry-shaped rests

Every copy action is an IOC limit at mark with a hardcoded slippage cap
(constant recorded at implementation; ~1% starting proposal). Fidelity-to-
leader beats fill price at $100–400 notionals on a 10–20s-stale signal. No
working-order management loop exists in A4; the "leader closes while our entry
rests" edge DISSOLVES because no entry ever rests. Resting orders in a sub are
limited to TP/SL triggers (if adopted — open question), which is exactly the
surface the watchdog sweep already handles.

- **Authoritative-source filter (forced, not optional):** the executor's
  backlog query filters `source = 'poll'`. The seam's `outstanding_events` /
  `claim_event` do not filter on source (hazard noted at #166 review), and the
  WS shadow lane dual-writes every (trader, coin) — an unfiltered executor
  would copy every trade TWICE during the shadow phase. Flipping this filter
  to `'ws'` is a #158 cutover checklist item.

### 5. Partial/zero fills: policy split by direction

- **Entries** (open, scale-in, flip's open leg): ONE shot, accept-and-audit.
  Requested vs filled recorded; under-copy corrects at the leader's next event
  or reconciliation. Missed-copy bias, consistent with ADR-0006.
- **Exits** (close, scale-out, flip's close leg): the residue is risk the
  leader no longer holds — bounded retry of the REDUCE-ONLY remainder
  (starting proposal: up to 3 attempts over ~30s; constants recorded at
  implementation), then leave the position and raise a reconciliation finding.
  Reduce-only makes exit retries structurally safe; all retry hazards
  (re-pricing a moved market, staleness, state) are entry-shaped.
- **PAGER CASE (do not lose):** a close that remains UNFILLED after retries
  exhaust means the book could not absorb a ~$200 reduce-only IOC within the
  slippage cap — pathological. Distinct audit reason, wired so the #52 monitor
  can page on it rather than drowning in generic partial-fill noise.

### 6. TP/SL: per-sub Copy Mode — `default` | `bracket`; `mirror` deferred

- **`default`**: pure copy. No local triggers; exits happen when the Leader's
  close/trim events say so.
- **`bracket`**: the sub carries its own optional TP% and SL%, applied at OUR
  fill time as exchange-native trigger orders. Needs no leader data, so
  ADR-0006's deliberate exclusion of TP/SL from the event seam costs nothing.
- **`mirror`** (deferred): copying the Leader's actual TP/SL requires tracking
  their order changes — resting-order territory that is unworkable on REST
  cadence (ADR-0006 §Scope arithmetic) and belongs to the WS order seam
  (#168). A one-shot fetch at copy time was considered and rejected: a STALE
  mirrored stop looks like the leader's risk management but is not.
- **Episode rule (g1):** when a bracket trigger fires and the Leader is still
  in the position, that Copy Episode is OVER. Subsequent leader events for the
  position (scales, trims, the eventual close) are claimed + skipped with an
  audit reason until the Leader fully closes and a fresh open event arrives.
  No re-entry: the bracket is an explicit local override of the Leader's exit
  timing, and re-entering would churn against it. The skips are visible in the
  audit trail if the operator reconsiders.
- Consequence for reconciliation: a position can now disappear WITHOUT a
  leader event (trigger fired). Reconciliation must classify "position gone,
  trigger order gone/filled" as a bracket exit, not as divergence.

### 7. Leader liveness: absolute live-equity floor, entries only

At signal time, on ENTRY events only (open, flip's open leg), fetch the
Leader's live `clearinghouseState` and require account equity >= a hardcoded
v0 floor of **$10,000**. Below the floor: claim + skip + audit reason.

- **Exits are never gated** — on liveness, or anything else. If we hold a copy
  and the Leader closes, we close; gating an exit is holding risk on a
  technicality.
- The relative alternative (live >= X% of screened equity) was rejected as
  circular: its denominator is exactly the stale stored data this gate exists
  to distrust (stored values ran 2–6x live equity, 2026-07-29 research).
- Under fixed Base Notional sizing, leader equity never enters sizing — this
  gate protects SIGNAL QUALITY (is this still the trader whose stats earned
  the copy?), not sizing.
- Caveat recorded: `clearinghouseState` is core-only; a leader keeping most
  equity on a builder dex reads low. Acceptable for v0.
- Cost: weight-2 fetch on rare entry events of a small copy set. Negligible.

## Out of A4 scope, captured during the grill

- **Withdrawal alerts for all tracked wallets** (operator idea, 2026-08-04):
  alert when a tracked Trader pulls meaningful equity out — the 38%-emptied-
  accounts finding as a push notification instead of a copy-time gate. For
  every tracked wallet, not just Leaders. Goes through /to-tickets after this
  grill; likely detection: equity-drop-not-explained-by-PnL in the existing
  poll, or the WS ledger-updates channel.

### 8. Staleness guard: 5 minutes, risk-increasing actions only

`ENTRY_STALENESS_GUARD = 5 minutes` — one hardcoded module-level constant
(changeable in one line; per-sub configurability deliberately deferred until a
real leader demands it). Age is measured now-minus-`observed_at` at claim time.

- **Guarded (risk-increasing):** open, scale-in, flip's open leg. Older than
  the guard: claim + skip + audit reason, never traded. 5 min is 15–30x the
  normal 10–20s signal latency — it never trips in healthy operation, always
  trips across a real outage.
- **Exempt (risk-reducing):** close, scale-out, flip's close leg — execute at
  ANY age. Closing late is strictly safer than never closing; a blanket guard
  would entrench risk (a close skipped as stale leaves a position no future
  event will ever close, because the leader is already flat).
- A stale flip therefore half-executes: close leg fires, open leg skipped →
  flat, both halves audited. Same direction-asymmetry as fills (decision 5)
  and liveness (decision 7).
- Residual, follows from the episode model: a skipped stale entry means later
  leader events reference a position we never opened — those claim + skip
  with a "no local position" audit reason; per-loop reconciliation keeps this
  honest.

### 9. /resume: consent to trade, nothing more — plus bracket re-placement

`/resume` lifts the halt; the backlog then drains under the ALREADY-LOCKED
rules — stale entries die on the 5-minute guard, exits execute at any age. No
resume-specific event handling exists. A 2-hour halt plays out as: leader
closed during it → we close on resume (late but out); leader opened during
it → skipped as stale; post-resume events → copied normally.

- Re-syncing to the leader's current book on resume is REJECTED on the
  glossary's own rule: positions that predate our look are not events —
  entering mid-thesis at a price the leader didn't pay is a different
  product, not copying.
- Flatten-on-resume is rejected as contradicting hold-and-alert: flattening
  is the operator's per-position decision in the UI during the halt.
- **(r1a) Bracket re-placement:** the halt sweep cancels TP/SL triggers and
  hold-and-alert keeps positions, so a bracket-mode sub's survivors come out
  of a halt unstopped. On /resume, the executor re-places brackets on every
  still-held position in bracket-mode subs — percentages from sub config,
  anchored to the position's entry price from clearinghouseState — audited
  as `bracket_replaced_after_resume`. Rationale: resume is consent to trade
  WITH THE SUB'S CONFIGURED POLICY; silently degrading `bracket` to
  `default` is a decision nobody made.
  - **SUPERSEDED by amendment D-1 (2026-08-04)** — see the amendments
    section. r1a's *effect* is unchanged; what changed is that restoration is
    a per-cycle invariant rather than a resume-only action, and the audit
    reason is renamed accordingly.

### 10. Reconciliation: exchange is truth; classify, audit, never auto-fix

Each loop the executor compares every sub's live `clearinghouseState` against
its Copy Episodes. On divergence it CLASSIFIES the cause, adopts the actual
state as the new baseline, audits, and pages only what warrants it. It NEVER
places an order to close a gap — auto-correcting would fight the operator
(they close, we re-open), fight liquidations, and turn bookkeeping bugs into
live orders.

Self-damping principle: relative operations apply to the ACTUAL held size,
never a bookkept expected size. Leader trims 30% → sell 30% of what we really
hold. Divergences converge instead of compounding.

| finding | meaning | action |
| --- | --- | --- |
| position gone, bracket trigger gone/filled | bracket fired | episode over (g1), audit |
| position gone, no trigger | operator closed manually / late exit | episode over, audit — operator wins |
| position gone + equity cratered | liquidation | episode over, audit, **page** |
| size ≠ expected | partial, residue, manual resize | adopt actual as baseline, audit |
| position with no episode | operator's own manual position | never touch; leader open on that coin skips with "coin occupied" |
| unclassifiable | possible bug | adopt nothing, **page**, re-flag until resolved |

### 11. Operator visibility: every copy action and every problem lands in Telegram

The operator runs this from Telegram; the audit trail alone is not a
notification channel. A4 therefore surfaces to the operator's chat:

- **Executed copy actions**: each copy open/scale/close/flip-leg reports what
  was done — sub, coin, side, requested vs filled notional (e.g. "copied:
  $200 → filled $200" / "filled $80 of $200"). Delivered as the executor's
  OWN messages: ADR-0006's separation holds in both directions — execution
  never reads `position_alerts`, and copy status is never written onto alert
  rows. Message formatting is implementation detail.
- **Every skip, with its reason**: staleness, liveness floor, coin occupied,
  risk-declined, no-local-position. Events are rare and the operator is one
  person; full verbosity is the point (they act manually on what they see).
- **Pager cases** ride the existing 🚨 monitor path: unfilled close after
  retries (decision 5), liquidation, unclassifiable divergence (decision 10),
  plus the halt pages A3 already sends.

### 12. Provisioning: bot commands, fully automated, operator-only

`/copy <leader> <allocation> <base> <mode> [tp% sl%]` creates the sub via the
agent key (verified capable — TESTNET FINDINGS 3), funds it with
`subAccountTransfer`, registers the mapping (a `copy_subs` table: leader →
sub address, Base Notional, Copy Mode, bracket %s, enabled flag), and starts
copying. `/uncopy` flips the flag off.

- **Operator-only, hard-gated.** Copy commands answer ONLY the operator's
  Telegram id (single-operator phase A; the bot has other users). Every other
  user gets a refusal; the gate is on the command handler AND the executor
  never reads mappings not owned by the operator id. Multi-user copy is
  Phase B (#127) and out of scope.
- `/copy` confirms before acting — it moves money; same confirm-tap pattern
  as `/resume`.
- Enable/disable is a DB flag read each loop — no restart (acceptance
  criterion), and a disabled mapping's events simply stop entering the
  backlog (claim-means-handled does not apply to events that never qualify).
- **Disable never auto-flattens.** `/uncopy` stops event consumption, reports
  what is still open in the sub, and leaves the positions to the operator —
  consistent with decision 10's never-auto-fix.
- Guardrail: the funding transfer is master↔sub internal; external
  withdrawal remains impossible under ADR-0005. Automating it adds no
  authority the agent key did not already hold.

## Executor constraints inherited from prior work (not decisions — obligations)

- **Source filter:** backlog query filters `source = 'poll'` (decision 4);
  flip to `'ws'` is a #158 cutover item.
- **The A3 `skip_cancel` residual race** (#136 comment, PR #143 round-6):
  re-check halt state as late as possible before signing; a halt observed
  AFTER signing is a reconciliation obligation — verify against live state,
  cancel what landed — never an assumption that the order didn't happen. The
  write-ahead audit's raise-pre-wire-when-Postgres-unreachable behavior is
  load-bearing (it makes livelock against a DB-blind sweep impossible) and
  must not be weakened.
- **Per-sub sweep:** the watchdog/kill sweep — including the cold-start blind
  path (#145) — must enumerate and cancel across every copy sub (decision 1).
- **End-to-end `run_cycle` regression test** (carry-forward from #143):
  drive a mid-transaction black hole through the real cycle with a wall-clock
  bound, wherever the execution loop's integration tests land.
- **Probe before implementing:** max sub-account count — DONE, 10 per master
  (decision 1, finding 10); testnet
  master is past the $100k createSubAccount gate, mainnet farming (~$65) is a
  mainnet-switch checklist item in #138.
- **Testnet-only:** the live gate is unchanged — no mainnet code path
  reachable before #137 (A5) merges; the switch is hand-run.

## Amendments

Decisions settled after the original grill, during implementation review of
#136 (PR #176). Each supersedes the numbered decision it names; the decision
text above is left standing so the reasoning that produced it stays readable.

### D-1 (2026-08-04). Bracket restoration is a per-cycle INVARIANT, not a
### resume-only action — supersedes decision 9's (r1a)

**Decision.** The executor holds "every live position in a bracket-mode sub
has its TP/SL resting on the book" as a property it restores on a slow
cadence, whatever removed them. It does not detect the /resume transition.

**Why the operator chose it over r1a as written.** r1a names one cause — the
halt sweep — but it is not the only one. A restart loses in-process
transition state entirely, so an executor that came up after a crash would
never restore anything; a partially-filled bracket, an exchange-side
cancellation, and the operator's own cancel in the Hyperliquid UI all leave
the same unstopped position with no resume to hang the fix on. One invariant
covers every cause including the halt, and it is the only version that
survives a restart. The cost is a periodic `frontendOpenOrders` read per
bracket-mode sub, which is why the cadence is slower than the loop.

**What it does not change.** A bracket placed at fill time is still placed at
fill time (decision 6) — the invariant is for restoration, not for the
initial placement, and waiting a cycle to stop a fresh position would be
exactly the window a stop exists to cover.

**Consequences, all binding:**

- **The audit reason is RENAMED to `bracket_restored`.**
  `bracket_replaced_after_resume` would be false for most of its firings once
  a resume is the minority cause, and a trail that misnames why it acted is
  worse than one that says less.
- **Every restoration notices to Telegram**, like every other copy action
  (decision 11). The operator must be able to see a bracket coming back —
  particularly the ones that come back after something they did.
- **The operator's route to an unstopped position is `default` mode or
  `/uncopy`**, not "cancel the trigger and hope". Cancelling a bracket leg in
  the exchange UI is now a temporary state: the executor restores it within
  the cadence. That is the intended behaviour — a sub configured `bracket`
  stays bracketed — and it is recorded here because it is genuinely
  surprising if you expect the UI to be authoritative.
- **A halted cycle restores nothing** and says so. Brackets are the one order
  shape this executor leaves RESTING, so they carry the same late halt
  re-check the order legs do; between a halt and its resume a bracket-mode
  position is unstopped, which the halt-and-unwind runbook now states.

### D-2 (2026-08-04). The liveness floor is decision 7's letter: open and
### flip's open leg ONLY — narrows the implementation, not the decision

**Decision.** The $10,000 live-equity gate fires on `open` and on a flip's
open leg. It does NOT fire on `scale_in`.

**Why.** Decision 7 already says "on ENTRY events only (open, flip's open
leg)"; the first implementation read "entry" as "anything risk-increasing"
and gated scale-ins too. That is a different gate with a different meaning.
The floor asks "is this still the trader whose stats earned the copy?" —
a question about STARTING to follow someone. A scale-in is the continuation
of a position we already opened on a Leader we already judged, and refusing
to follow it leaves us holding a half-mirrored position rather than no
position, which is the worse of the two states. It also spends a weight-2
fetch on the most common event kind.

**What it does not change.** Decision 8 is untouched: a scale-in is
risk-increasing, so it stays staleness-guarded. The asymmetry the ADR runs on
is intact — this narrows one gate to its stated scope, it does not open
exits to gating.
