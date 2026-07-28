# Runbook: execution halt & the position-unwind policy (issue #135, ADR-0005)

## The one fact this whole page turns on

**Neither kill mechanism closes positions.** The watchdog's cancel-all and
the protocol's scheduleCancel both kill *resting orders* only. A halt leaves
every open position exactly where it was — and because the sweep also
cancels resting TP/SL trigger orders, those positions are left **without
their protective stops**. That gap, not the order sweep, is what matters in
a real incident. This runbook is the documented answer to it.

## The v0 policy: hold-and-alert

On any halt (watchdog trip or operator `/kill`), Epigone:

1. cancels every resting order, verified by re-enumerating the book until it
   reads empty (never on a cancel call's word — the AmbiguousExecutionError
   contract),
2. snapshots the open positions onto the halt row and the audit trail,
3. **holds** the positions — it does not flatten them — and
4. pages the operator (🚨 via the health monitor, with the snapshot count and
   sweep status, re-paged on the reminder cadence until resolved).

Recorded per halt as `unwind_policy = "hold-and-alert"` so the trail says
which policy version governed each incident.

### Why hold, not auto-flatten (the rationale the ticket asks for)

- **Phase A is operator-only.** The person the halt pages is the person who
  owns the master wallet — and the master wallet on app.hyperliquid.xyz
  keeps working whatever state Epigone is in (ADR-0005: our authority is a
  revocable trade-only agent key; the user-side escape hatch is the design's
  whole point). A human with full context and a working UI beats an
  automated market order into whatever liquidity happens to exist.
- **A halt is evidence of malfunction, not of a bad position.** The likeliest
  trips are a wedged executor, a DB blip, a deploy gone long. Auto-flattening
  on a false positive realizes slippage and spread for nothing — an unforced
  error the operator can never undo, where holding for the minutes until a
  human looks is usually free.
- **Flattening is the least-tested path at the worst moment.** A
  reduce-only-market flatten needs live mid prices, slippage bounds, and
  per-coin liquidity judgment — precisely the risk-policy machinery A5
  builds. Running that for the first time mid-incident, signed by the
  watchdog, against a possibly-degraded exchange, is how a halt becomes a
  loss. v0 refuses to pretend that machinery exists.
- **The honest residual** (stated, not hidden): held positions are exposed
  and unstopped until the operator acts. The alert says so; the steps below
  are the operator's playbook. This trade-off flips when nobody is on call —
  see "Revisit" below.

## Operator playbook

### When the halt alert lands

1. Read the alert: who halted (`watchdog` or `kill`), why, whether the sweep
   is done ("orders swept" vs "sweep PENDING — orders may still rest"), and
   how many positions are held.
2. Open the master wallet on app.hyperliquid.xyz (testnet:
   app.hyperliquid-testnet.xyz) and look at the held positions. Decide per
   position: flatten (market close from the UI), re-protect (place a stop
   from the UI), or hold.
3. Diagnose the trip (`docker compose logs watchdog`, the `execution_audit`
   table — attempt rows with no outcome row mean a crash mid-submission;
   `ambiguous` outcome rows mean a reconcile was owed and the sweep loop is
   doing it by re-enumeration).
4. Only then `/resume` (it asks to confirm). Resume lifts the halt and
   re-places **nothing**; if the executor heartbeat is still stale the
   watchdog re-halts within one cycle — resume is consent, not an override.

### `/kill` (operator-initiated halt)

`/kill [reason]` in the bot, owner-only. The halt row lands immediately (any
executor stops at its next loop's `is_halted` check); the watchdog sweeps
the book within one watchdog cycle (default 10s). The reply and this page
are deliberately blunt that positions are held, not closed.

### Decommissioning the executor on purpose

The watchdog trips on a *stale* executor heartbeat — an executor stopped
forever keeps its last beat forever, which reads as death (the fail-safe
direction). If you retire the executor deliberately: halt or drain it first,
then delete its liveness row so the watchdog stands down:

```sql
DELETE FROM process_heartbeats WHERE process = 'executor';
```

(Heartbeat rows are current state, not history — deleting one is legitimate;
the audit trail is where history lives, and it is append-only.)

## The mechanisms, for reference

| | watchdog (PRIMARY) | scheduleCancel (upgrade path) |
|---|---|---|
| kills resting orders | ✅ cancel-all, verified by enumeration | ✅ exchange-side, all at once |
| closes positions | ❌ (this policy) | ❌ |
| needs alive | its own process **plus Postgres** (heartbeat + halt state ride the ADR-0002 seam) — never the executor | nothing — fires exchange-side even if the whole host and its database are gone |
| available today | ✅ | ❌ $1M cumulative-volume gate (verified live 2026-07-28; operator ~$58k) |
| status | always on (`--profile execution`) | eligibility-probed every 6h, self-activates on acceptance, transitions on the audit trail |

Stated precisely (PR #143 review, rounds 2–3 — no sentence here is broader
than what the code guarantees, and the whole section covers an
**already-running watchdog only**; see the cold-start boundary below):

- **What a Postgres outage does NOT stop:** cancelling — because once an
  incident is declared, the cancel path does not use Postgres AT ALL
  (round 5): no heartbeat, no reads, no halt row, the rate budget runs on
  its in-process bucket without attempting the shared row, and the audit
  wrapper defers its attempt row to after the wire (wire-first audit) for
  the incident's duration. Durable bookkeeping — halt row, events, sweep
  stamps, the deferred audit pair — happens AFTER each cancel attempt
  under a hard ~20s ceiling, and a failing reconcile just leaves the
  incident open: the next cycle cancels again, every cycle, until writes
  recover. Getting TO the declaration is bounded too: normal-operation DB
  touches ride the safety pool's connect/per-query/lock-wait/release
  budgets (~2× the 5s bound per fully hung touch), every state block of
  the cycle sits under the same hard ceiling, and the shared rate bucket
  ceilings each of its own database attempts — together covering even the
  transaction-exit leg asyncpg cannot time (an untimed cancel-wait before
  ROLLBACK, the round-5 finding that ended the bound-one-more-leg
  approach).
- **What a Postgres outage DOES change:** the watchdog cannot *read*
  executor liveness or halt state, so it cannot distinguish stall from
  health. Its answer is the **blind trip**: unreadable *continuously* —
  an unbroken failure streak; any successful read resets the clock, so a
  single dropped connection never trips it — past
  `WATCHDOG_DB_BLIND_SECONDS` (default 180 = 3× the stall threshold), it
  attempts a cancel pass every cycle until the database answers — and
  incident cycles touch Postgres only AFTER the cancel, under a hard
  ceiling, so "every cycle" stretches by at most the post-cancel reconcile
  ceiling (~20s), never by TCP retransmission timescales. Worst-case time
  from outage onset to the first blind cancel LANDING: up to one poll
  interval before the streak opens, plus the streak threshold, plus up to
  one more poll interval and one waiting cycle's ceilinged DB blocks (the
  trip fires at the first cycle START past the threshold), plus the venue
  enumeration's HTTP legs on the trip cycle itself (exchange I/O at its
  30s-per-request timeout — the only thing left between the trip and the
  wire). What is LOST while blind: `swept_at` verification
  stamps, the position snapshot, heartbeats, and per-cancel audit rows for
  that window.
- **What recovery reconciles, durably:** a halt row under a headline that
  preserves which trip it was ("DB-blind sweep" vs a real stall's
  "unrecorded trip" — when no halt already stands; a standing /kill halt
  is joined, not duplicated), plus a `blind_window_reconciled` audit event
  recording the window's span and how many unrecorded cancel passes
  completed. ONE carve-out (the blind marker is process memory, and while
  the database is down there is nowhere durable to put it): if the
  watchdog itself dies in the narrow window between the database
  recovering and its next cycle's reconcile (≈ one poll interval), the
  blind window survives only in the dead process's logs — no halt row, no
  event. In every other case a blind window cannot exist only in process
  logs.
- **What still needs Postgres:** `/kill` recording a halt (the bot tells
  you plainly when it could not), `/resume`, the monitor's view, the
  executor's own `is_halted` gate — and the watchdog's **cold start**:
  startup needs the pool, migrations, and the keystore, so a watchdog
  crash/reboot/deploy *during* the outage leaves no cancel path until
  Postgres returns (filed separately; not covered by this page's
  guarantees). The only layer needing NOTHING of ours is scheduleCancel —
  exactly why it stays implemented-and-probing despite being volume-gated
  inactive.
- The executor's ORDER path keeps the opposite discipline end to end
  (write-ahead audit, shared budget with its burst cap and send gate, hard
  halt gate): evidence and pacing before money is spent; action before
  losses continue. The asymmetry is the design — which is also why the
  safety lane's degraded in-process bucket deliberately trades the shared
  burst discipline for availability during the incident window.

**Sweep coverage:** the cancel-all is ACCOUNT-WIDE — the core venue plus
every builder dex in the live `perpDexs` listing, re-fetched each sweep, so
a venue added to trading is swept with no code change. If the listing
endpoint itself is down, coverage degrades to the covered POSITION_VENUES:
those venues are still swept, but `swept_at` is deliberately withheld — so
a halt alert that keeps saying "sweep PENDING" for more than a cycle or two
means either orders that won't die or degraded venue coverage; the watchdog
log says which. The boundary that
remains: **sub-accounts.** The #142 probes showed an approved agent can
create and fund sub-accounts — separate accounts whose books this master's
sweep never sees. Until A5's risk policy either forbids sub-account use by
the executor or adds their books to the sweep, treat any sub-account
activity as OUTSIDE the kill switch's reach.

The watchdog's own health is a 🚨 health-monitor check on two axes: liveness
(`HEALTHCHECK_WATCHDOG_STALE_SECONDS`, default 300) and CAPABILITY — every
~6h (`WATCHDOG_CAPABILITY_CHECK_HOURS`) it verifies on-chain, via the public
`extraAgents` readback, that its agent key is still approved and unexpired,
so a beating-but-impotent watchdog (mid-run deregistration, an unrestarted
rotation) pages before an incident instead of during one. An executor
heartbeat with no watchdog ever run is likewise 🚨 — trading never runs
unguarded.

## Revisit (when this policy is wrong)

Hold-and-alert leans on an operator being reachable within minutes. That
assumption breaks at exactly the points ADR-0005 already gates:

- **A5 (risk policy v0, mainnet gate):** flatten-on-halt becomes buildable —
  the caps/liquidity machinery it needs is A5's deliverable. Decide then
  whether `unwind_policy = "flatten-reduce-only"` becomes the default or a
  per-halt operator choice.
- **Phase B (external users):** unattended user positions cannot wait for a
  human. Hold-and-alert is not acceptable there; treat this policy as a
  Phase-A-only decision, re-made before the first external account.
