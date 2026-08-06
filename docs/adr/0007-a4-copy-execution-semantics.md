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
  **At the cap, provisioning ADOPTS an orphaned sub instead of failing** —
  amendment D-3 below, added once the cap probe itself had spent all ten
  slots on the testnet master and left `/copy` with nothing to mint.

### 2. Sizing: fixed Base Notional per sub, relative mirroring

> **SUPERSEDED by amendment D-4 (2026-08-05, issue #137)** — Base Notional
> becomes Base Stake and the Leader's leverage becomes a sizing input. The
> text below is left standing so the reasoning that produced it stays
> readable, and D-4 says which half of it survived.

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
  - **NARROWED by amendment D-7 (2026-08-06)** — a cycle that drains a backlog
    summarises its skips instead. Live operation, pager cases, copied actions
    and the audit trail are all unchanged; see the amendments.
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

### D-3 (2026-08-05, issue #178). At the sub-account cap, provisioning ADOPTS
### an orphaned sub — extends decision 12's provisioning leg

**Decision.** When `createSubAccount` is refused `"Too many sub-accounts."`
(finding 10's flat cap of 10), provisioning does not fail: it adopts an
existing sub of the master that is **unmapped** — no `copy_subs` row, of any
operator, enabled or disabled, points at it — and **position-free** on every
venue Epigone covers. The mapping records that address and goes through the
ordinary funding leg, which already treats the allocation as a TARGET BALANCE
and moves only the difference, so an orphan that inherited a balance is
funded correctly without a special case. Fresh creation is still attempted
first on every pass; adoption is the refusal's fallback, never the first
choice.

**Why the cap needed an answer at all.** The probe that established the cap
(decision 1's "probe before implementation") consumed all ten slots on the
testnet master with empty probe subs. Every `/copy` on that master therefore
refuses at the create leg, and no Leader can be copied — the A4 shakedown
would have had nothing to run against. On mainnet the same shape arrives more
slowly (ten Leaders, or ten subs spent on anything else) and is permanent
when it does, because subs cannot be deleted. The venue leaves exactly one
recovery: re-use what is already there.

**The two refusals to keep apart.**

- **Cap refused, an orphan is adoptable** → adopt, audit `copy_sub_adopted`,
  and say ADOPTED to the operator. It reads differently from a creation
  because it *is* different: the sub was taken over, not minted for this
  Leader, and only the operator can say whether that is what they wanted. The
  address, the audit row and that notice commit in ONE transaction at the
  moment of adoption — not at the end of the run — so a funding leg that
  defers cannot cost the operator the sentence that says which of the two
  happened. The ready notice repeats it when the run does finish.
- **Cap refused, nothing adoptable** → fail loudly: `copy_provisioning_cap_
  exhausted`, the mapping DISABLED, nothing funded, and the same "was NOT set
  up" sentence the risk-declined path uses — with its own fix, since freeing a
  slot is a different action from lowering an allocation. The notice states
  what does NOT free one: `/uncopy` keeps its sub by design, and the exchange
  deletes nothing. A master whose ten subs are all mapped is genuinely at
  decision 1's ceiling, and the way past it is retiring a mapping for good or
  using another master.

A read that fails is neither: an unreadable listing, a candidate whose
positions will not load, or a listing that comes back EMPTY while the exchange
is refusing at the cap (a contradiction, and no evidence of anything) all DEFER
to the next cycle with the mapping left pending. "I cannot tell" is not "there
is none", and disabling a Leader over a transient read failure is the wrong
direction of mistake.

**Never adopted, both from decision 10's never-touch rule.** A sub mapped to
any Leader, *including a disabled one* — /uncopy stops event consumption, not
ownership, and a later /copy re-enables that row onto that same sub — and a
sub holding an open position, whoever opened it. Resting orders are not part
of the test: the disqualifier is a live position, and an inherited resting
order belongs to no Copy Episode, so reconciliation never acts on it and the
next /kill sweep cancels it with everything else on that book.

**The name follows the Leader, best-effort.** Probed for this ticket
(`scripts/testnet_subaccount_rename_probe.py`, gateway TESTNET FINDING 11):
`subAccountModify` renames an existing sub and the listing reads the new name
back immediately. So an adopted `capprobe_003` is renamed for its Leader. It
is cosmetic — nothing in Epigone keys off the exchange-side name — so a
refusal is reported in the ready notice and the provisioning run carries on.
Renaming frees no slot; the sub is still permanent.

**What it does not change.** The late halt re-check still guards every
signature — and adoption ADDS one, because it adds a signature. Provisioning
now gates three legs (`create`, `rename`, `fund`) rather than two, since the
listing read and the per-candidate position reads put seconds to tens of
seconds between the create check and the rename; a `/kill` landing in that
window must not get a `subAccountModify`. A halt there skips the rename and
KEEPS the adoption — a row in Epigone's own database is not something the
exchange saw, so the next cycle resumes at the funding leg instead of taking
over a second sub, and only the cosmetic name is lost. One sub per pass still
holds. And the cap is still a cap — adoption re-uses slots, it does not create
them, so ten remains the ceiling on concurrent Leaders (decision 1).

### D-4 (2026-08-05, issue #137). Sizing is BASE STAKE × MIRRORED LEVERAGE —
### supersedes decision 2's fixed Base Notional

**Decision.** The dollars configured per Copy Sub-account are the Operator's
**margin**, isolated per position, not the position's size. A copied open's
position is **Base Stake × the mirrored leverage**, where the mirrored leverage
is `min(the sub's mode answers, the operator's backstop, the asset's own
maximum)`. The sub's mode is `mirror` (the Leader's own leverage on that
position) or `fixed N`. Backstop default 20x. Relative mirroring of scale
events is unchanged: a 50% scale-in still scales the copy by 50% of what we
actually hold.

**Why the operator reopened decision 2.** Fixed Base Notional dissolved the
"5% of their account vs 40% of ours" conflict by refusing to let the Leader
influence size at all — and in doing so it threw away the one thing about a
Leader's position that is genuinely information rather than an artifact of
their bankroll. A Leader's SIZE says how rich they are; their LEVERAGE says how
convinced they are. Base Notional copied a 20x conviction trade and a 2x parking
position at the same $200, which is not mirroring a strategy, it is mirroring a
ticker. Base Stake keeps the property that made fixed sizing right — the money
at risk is the Operator's own constant, decided by the Operator — and lets the
Leader's conviction scale the exposure on top of it.

**Isolated margin is what makes the sentence true.** "The stake is the worst
case" is a claim about a mechanism, not a wish: under cross margin a losing
position reaches the whole sub's balance. So the executor sets
`updateLeverage(isolated)` per (sub, coin) before the first order of an
episode. That is a SIGNING action and takes the full discipline every other
signature takes — the audit wrapper, and a late halt re-check immediately
before the wire. It is set on EVERY open rather than cached: leverage is
exchange-side state that `/copy`, the operator's own UI, and an adopted sub can
all change, and a cache of what we last set would be wrong exactly when it
matters. A refusal ENDS the entry, because a position opened at whatever
leverage the account happened to carry is not the position the policy judged.

**The cap is not optional.** Notional, liquidation distance, funding cost and
fee drag all scale with stake × leverage, so an uncapped mirror hands the
Leader a dial that multiplies the Operator's exposure without touching the
Operator's configuration. The backstop is the operator's; the asset maximum is
the venue's; the lower wins. A `mirror` open whose event carries no Leader
leverage is SKIPPED, not defaulted — 1x would silently shrink the copy tenfold
and the backstop would silently maximise it, and neither is a decision anyone
made.

**What it does not change.** Decision 7's liveness floor still gates entries on
Leader equity, and still is not a sizing input — the Leader now contributes
leverage to sizing, never equity. Decision 5's fill asymmetry, decision 3's
flip pipeline, decision 10's self-damping (relative operations against the size
the exchange reports) and decision 4's IOC-only rule are untouched. The
per-position exposure ceiling is still exchange-enforced: a stake the sub's
margin cannot absorb simply rejects.

**Consequences, all binding:**

- **`copy_subs.base_notional_usd` is RENAMED to `base_stake_usd`** (migration
  0036), not added beside its predecessor. The two cannot coexist honestly: a
  row carrying both is a row where nothing says which number sized the last
  order, and a silently reinterpreted $200 is a 20× larger position.
- **EXISTING MAPPINGS ARE PINNED TO `fixed 1x` BY THE MIGRATION.** A rename
  makes CODE fail loudly; it does nothing for DATA. A mapping written before
  A5 stores a number that meant a POSITION, and the new column's default would
  have put it in `mirror` mode — so the next copied open behind a 20x Leader
  would have been twenty times the position the operator configured, with no
  action from them and no message to them. At `fixed 1x`, position = stake × 1
  = the number already stored, so a pre-A5 mapping sizes IDENTICALLY across the
  upgrade; the only thing that changes for it is that its margin is now
  isolated, which can only reduce what it can lose. Raising such a sub to
  `mirror` is a deliberate act: re-run `/copy` for that Leader, confirm tap and
  all. (Operator decision at PR #182's merge gate; the migration carries the
  reasoning beside the statement.)
- **`/copy` grows a leverage argument** — `/copy <leader> <allocation> <stake>
  <leverage> <mode> [tp% sl%]` — and its prompt states the stake as margin and
  spells out the position it buys.
- **Fractional leverage rounds DOWN**, the same conservative direction every
  size rounds. `updateLeverage` takes an integer on the wire, so `/copy` and
  `/limits` both refuse a fractional one rather than truncating it silently.
- **The episode records the leverage it opened at** (`copy_episodes.leverage`),
  as bookkeeping — the exchange's own figure on the live position remains the
  authority, exactly as it does for `size_coin`.

### D-5 (2026-08-05, issue #137). Per-order risk is a POLICY MODULE with
### operator-tunable global limits — the A5 half of decision 2's "no aggregate
### cap needed in v0"

**Decision.** Four gates, judged before signing, each recorded verbatim in
`execution_audit.risk_decision`:

1. **Liquidity Floor** — a coin is copyable iff its live market clears 24h
   notional volume ≥ $100k AND open-interest notional ≥ $100k (defaults;
   operator-tunable down to 0 = off). Deliberately NOT a curated allowlist:
   which coins to trade is the Leader's decision, and Epigone's only veto is
   market health. Judged ONCE per Copy Episode, at the open that starts it.
2. **Stake caps** — max stake per coin per sub and max aggregate stake per sub,
   in MARGIN dollars, measured against what the EXCHANGE says is committed. An
   order over a cap is CLAMPED to the headroom left and audited
   `allowed-clamped` with both figures; a clamp whose position falls under the
   venue's $10 minimum order value becomes a denial.
3. **Mirrored leverage**, per D-4.
4. **Exits are unconditionally exempt from all of it.** Only the halt outranks
   an exit. Denial prose says "did not enter", never "did not exit".

**Why the floor speaks once, at the open.** A live episode must never be
INTERRUPTED by it (a scale-in on a coin that has since gone thin still copies —
refusing leaves us half-mirrored on a thesis still running) and never TRAPPED
by it (an exit always signs — a copy stuck in a thin market is precisely the
position we most need out of). A flip ENDS the episode, so a flip's opening leg
is a fresh entry and the floor speaks again: a sub-floor coin makes the copy go
flat and sit out, which is the correct answer rather than an edge case.

**Where the numbers live, and why it is split.** Per-sub knobs (Base Stake,
leverage mode) are properties of one Leader's mapping, so they sit on
`copy_subs` and are set with `/copy`. The global knobs — floor volume, floor
OI, per-coin stake cap, per-sub aggregate stake cap, backstop leverage — are
Epigone's own stance, so they are ONE `risk_limits` row set with `/limits`,
re-read by the executor every cycle (no restart), with every change audited old
→ new. A limit that needs a redeploy is a limit nobody changes during the
incident that needed it.

**Stated naive choice, recorded as the known gap.** The stake caps are
per-sub and have NO cross-sub coordination. Two Leaders in two subs holding the
same BTC short look independent to this policy and are not — the 2026-07-29
wallet research found exactly that shape among shortlisted wallets. A
correlation-aware aggregate is the version that would bound true exposure; v0
ships the naive form because each sub's allocation is separately funded and
exchange-enforced, which bounds the error at one operator's scale. Recorded in
migration 0036 beside the columns.

**Also recorded, not built:** `l2Book` depth checking (the floor reads volume
and open interest, not the book itself), and the rule that **the floor defaults
must be revisited whenever the stake caps are raised** — a $100k floor bounds
slippage for a $4,000 position and does not for a $40,000 one.

**Deferred with its enabler shipped:** the daily-loss pause, filed as **issue
#181**. Small stakes plus operator alerting cover the interim; what ships now
is per-cycle **sub-equity history** (`copy_sub_equity`), recorded from the
equity the reconcile already reads and used to discard — because the honest way
to pick a daily-loss threshold is to look at what a sub's equity actually does
across a day of copying. #181 also owns that table's retention window, since
the window it needs is the one that decides how far back the pause looks.

**The live gate is now a flag, not an absence.** `EXECUTOR_ALLOW_MAINNET`
wires the capability `HttpExecutionGateway` has always demanded. Mainnet takes
TWO deliberate acts — the flag AND the mainnet URL — plus a funded account, and
the default is neither. The WATCHDOG opens on the same variable by name: it is
the executor's dead-man's switch and `/kill`'s only sweeping hand, so a live
executor beside a testnet-refused watchdog must not be reachable by setting one
variable.

### D-6 (2026-08-05, issue #184). Leader liveness reads the SIGNAL network, not
### the network the executor trades — restates decision 7's subject

**Decision.** The $10,000 live-equity floor fetches the Leader's
`clearinghouseState` from the network the SIGNAL came from, through a second,
read-only info gateway pinned to `EXECUTOR_SIGNAL_INFO_URL` — default: the
tracking product's MAINNET info endpoint, the same constant the pollers use.
Every other executor read — Liquidity Floor market stats, mark prices, asset
specs, sub-account state, fills — stays pinned to the book the executor trades
and still derives from the exchange URL.

**Why it took a deployment to notice.** Decision 7 never said which network,
because until the A5 shakedown there was only one: signal network and trade
network coincide on a mainnet deployment, and the read was bit-for-bit correct
by accident. The shakedown mirrors REAL mainnet Leaders onto the TESTNET book —
real signal, mock money — and tracked Leaders hold $0 on testnet. Every entry
skipped with `leader below the liveness floor: $0 < $10,000`, and no Copy
Episode could ever open. Verified live 2026-08-05: all three staged shakedown
candidates read $0 on testnet.

**Why the signal network is the right answer, not a harness accommodation.**
The gate asks "is this still the trader whose stats earned the copy?" The stats
were earned on the network tracking observed, the 38%-emptied finding is about
THAT account, and a Leader who emptied the account their stats came from is
exactly what the gate must still catch — on mainnet it does, unchanged, because
there the two networks are the same one. What the fix removes is a reading in
which the gate judged an account the Leader never had a reason to fund.

**The one-network doctrine is RE-SCOPED, not repealed.** Orders and
order-adjacent reads remain single-network by construction: a size priced on
one network and sent to another must stay untypeable, so nothing on that side
became configurable. The Leader's account is a signal-network entity, and its
liveness read is the one documented exception — served by a gateway that is
read-only, holds no signer, and has exactly one caller.

**Rejected: reading the poll-pass equity capture** (#170's `trader_equity`,
written in the same transaction as the signal). Genuinely attractive — no API
spend, freshness at signal time — but it changes decision 7's letter from
"fetch live" to "read the capture" and imports staleness and missing-row
semantics the live fetch does not have. Operator chose the live signal-side
fetch (2026-08-05).

**What it does not change.** Amendment D-2 stands unaltered: the floor fires on
`open` and a flip's open leg only, never on `scale_in`, and exits are gated by
nothing — now provably so against BOTH gateways' answers. Unreadable equity
still skips with retry semantics (event unclaimed on plain entries, leg-aware
wording on flips), now against the signal-side gateway. The floor constant, the
audit wording carrying the observed figure, and the weight-2 spend against the
shared budget are all untouched — only the endpoint moved. The builder-dex
caveat in decision 7 stands as recorded.

**Consequences, all binding:**

- **`EXECUTOR_SIGNAL_INFO_URL` defaults to MAINNET** — the only env knob in the
  executor family whose default is not testnet, because it names where the
  Leaders are, not where the money is. It signs nothing and can place nothing.
  A value that is not `/info`-shaped degrades to the default with a logged
  warning, per the executor-config convention.
- **The start-up audit row records it**, beside the exchange URL, so the trail
  answers "which account was being judged" as well as "which book was traded".
- **The test harness runs the shakedown topology by default**: the copy
  executor's suite gives the leader equity to the SIGNAL fake only, so a
  liveness read that slipped back onto the trade gateway reads $0 and fails
  loudly rather than passing on a coincidence.

### D-7 (2026-08-06, issue #190). A cycle that DRAINS A BACKLOG summarises its
### skips — narrows decision 11's "full verbosity", changes nothing it records

**Decision.** When one executor cycle produces more than `SKIP_DIGEST_THRESHOLD`
(= 5) skip notices, the operator's chat gets ONE summary for that cycle —
totals by Leader and by reason, with a pointer to `copy_skipped` on the audit
trail — instead of one message per event. At or below the threshold, nothing
changes: each skip sends its own sentence with its own full reason, exactly as
decision 11 specifies.

**Why the threshold is a constant and not a knob.** It is not a preference
about how loud the chat should be; it is the line between the two regimes the
chat has. Live, the poller hands the executor one Leader's one or two events
per cycle. A drain is the whole retained backlog at once — measured at ~20 on
the first `/copy` of a long-tracked Leader (observed 2026-08-06), and unbounded
above that after downtime. Anything from five to a dozen separates them
identically, so there is nothing for an operator to tune; exposing it would
only invite the two settings that are actually wrong (0, which coalesces the
trickle decision 11 exists for, and ∞, which restores the storm).

**Why decision 11's reasoning survives it.** "Events are rare and the reader is
one person who acts manually on what they see" is a statement about LIVE
operation, and it is still true there. A drain is the case it does not
describe: twenty identical sentences about twenty coins do not give the
operator twenty things to act on, they give them one — "the backlog was stale"
— buried in a scroll that also buries the copied actions and pages around it.
The summary reports that one thing, and says where the twenty are.

**Three boundaries, all load-bearing:**

- **The AUDIT TRAIL IS UNTOUCHED.** One `copy_skipped` row per event, with the
  full per-event sentence, verbatim, in both regimes — the row still commits in
  the same transaction as the claim. Coalescing is a DELIVERY decision about
  the doorbell, never about the record.
- **🚨 pager notices are never coalesced**, at any volume. Volume is the moment
  a page is most likely to be missed, not least.
- **Copied actions are never coalesced.** Summarising what did NOT happen is a
  readability decision; summarising what DID would hide money moving.

**Consequences:**

- **`_Skip` carries a `category`** beside its sentence — the coarse bucket the
  summary counts. Its WORDS are the ones decision 11 and the runbook already
  use (`stale entry`, `coin occupied`, `no local position`, `risk-declined`,
  …), because a summary that renamed them would be a second vocabulary for the
  same six things. The sentence stays what the trail records and what an
  under-threshold cycle sends.
- **Skip notices are held in memory until the end of the cycle** — in BOTH
  regimes, since only the end of a cycle knows which regime it was — rather
  than written per event and collapsed afterwards. Collapsing would race the
  bot's own delivery: a long cycle can have its first notices already in
  Telegram when its last event is handled, and deleting those would leave the
  operator with five sentences AND a summary that counts them again. The cost
  is bounded and named: a crash mid-cycle loses CHAT LINES for events already
  claimed, never their audit rows, which still commit with the claim.
- **Within one cycle, skips now arrive after copied actions and pages**, where
  they used to interleave, because they are all written at the flush. Each
  still carries the timestamp of the moment it was DECIDED, not of the flush.
- **A failed notice write leaves the rest held** rather than dropping the
  tail: these events are already claimed, so nothing would re-offer them.
- **Only per-event skips coalesce.** Provisioning skips and bracket skips are
  at most one per sub per cycle and cannot storm, so they still send
  individually.

### D-8 (2026-08-06, issue #193). The liveness floor is TEMPORARILY $100 for the
### A5 shakedown — a suspension of decision 7's number, not a revision of it

**TEMPORARY. Revert guarded by issue #192**, which closes only by restoring
`LEADER_EQUITY_FLOOR = Decimal("10000")`. This amendment is written to be
DELETED with it, unlike every amendment above, which are permanent records.

**Decision.** For the shakedown period, `LEADER_EQUITY_FLOOR` is `$100`. The
real value, and the one it returns to, is `$10,000`.

**Why.** A5 needs one Leader whose signal timing the operator can drive on
demand — open a position, watch the full copy path run, close it — and the only
such Leader is the operator's own mainnet wallet, at ~$300 of equity. Under
decision 7's number that wallet is exactly what the gate exists to refuse, so
the shakedown could observe the gate but never anything downstream of it.

**Why decision 7's reasoning is untouched.** The floor asks "is this still the
trader whose stats earned the copy?" of a Leader chosen for their stats. The
operator's own wallet was never chosen that way, so the question the floor
protects is not being answered differently — it is not being asked. Nothing
about the 38%-of-wallets research, the absolute-not-relative form, or the
entries-only scope changes; only the period in which the number is honest.

**Why it stays a CONSTANT and does not become a knob.** A knob is the failure
mode this amendment exists to avoid. An env var or a `/limits` entry would let
$100 outlive the shakedown silently, at which point the gate has been disabled
rather than suspended, and nothing in the repo would say so. A constant edit is
loud: it shows in the diff, it shows in this amendment, and it shows in #192.
