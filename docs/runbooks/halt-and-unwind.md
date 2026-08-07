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
| needs alive | its own process — never the executor, and (since issue #145) **not Postgres**: it cancels blind through an outage and can even cold-start into one | nothing — fires exchange-side even if the whole host and its database are gone |
| available today | ✅ | ❌ $1M cumulative-volume gate (verified live 2026-07-28; operator ~$58k) |
| status | always on (`--profile execution`) | eligibility-probed every 6h, self-activates on acceptance, transitions on the audit trail |

Stated precisely (PR #143 review, rounds 2–3 — no sentence here is broader
than what the code guarantees). It covers a watchdog that is **running or
starting**: issue #145 removed the already-running caveat this section used
to carry, so a crash, an OOM, a reboot, or a deploy *during* the outage no
longer strands the account (see "Cold start during an outage" below):

- **What a Postgres outage does NOT stop:** cancelling — because once an
  incident is declared, the cancel path does no Postgres state work before
  the wire (round 5): no heartbeat, no reads, no halt row, and the rate
  budget runs on its in-process bucket without attempting the shared row.
  A DB-BLIND incident also defers the audit attempt row to after the wire
  (wire-first audit) — fully Postgres-free to the wire; a real-stall trip,
  whose reads answered that same cycle, keeps its bounded best-effort
  write-ahead attempt row on purpose (round 6). Durable bookkeeping
  happens AFTER each cancel attempt as separately ceilinged blocks — the
  deferred audit pair and the reconcile each under their own hard ~20s
  ceiling, each exiting at ceiling plus the ~5s release budget — so a
  worst-case incident cycle's post-cancel database time is roughly TWO
  ceilings (~50s all-in), bounded, never open-ended; a failing reconcile
  just leaves the incident open and the next cycle cancels again, every
  cycle, until writes recover. Getting TO the declaration is bounded too:
  normal-operation DB touches ride the safety pool's
  connect/per-query/lock-wait/release budgets (~2× the 5s bound per fully
  hung touch), every state block of the cycle sits under the same hard
  ceiling, and the shared rate bucket ceilings each of its own database
  attempts — together covering even the transaction-exit leg asyncpg
  cannot time (an untimed cancel-wait before ROLLBACK, the round-5 finding
  that ended the bound-one-more-leg approach).
- **What a Postgres outage DOES change:** the watchdog cannot *read*
  executor liveness or halt state, so it cannot distinguish stall from
  health. Its answer is the **blind trip**: unreadable *continuously* —
  an unbroken failure streak; any successful read resets the clock, so a
  single dropped connection never trips it — past
  `WATCHDOG_DB_BLIND_SECONDS` (default 180 = 3× the stall threshold), it
  attempts a cancel pass every cycle until the database answers — and
  incident cycles touch Postgres only AFTER the cancel, in separately
  ceilinged blocks, so "every cycle" stretches by at most the sum of the
  post-cancel ceilings (deferred audit pair + reconcile, ~50s all-in with
  their release budgets), never by TCP retransmission timescales. Worst-case time
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
  you plainly when it could not), `/resume`, the monitor's view, and the
  executor's own `is_halted` gate. The watchdog's own **start** no longer
  does (issue #145) — see below. The only layer needing NOTHING of ours is
  scheduleCancel — exactly why it stays implemented-and-probing despite
  being volume-gated inactive.
- The executor's ORDER path keeps the opposite discipline end to end
  (write-ahead audit, shared budget with its burst cap and send gate, hard
  halt gate): evidence and pacing before money is spent; action before
  losses continue. The asymmetry is the design — which is also why the
  safety lane's degraded in-process bucket deliberately trades the shared
  burst discipline for availability during the incident window.

### Cold start during an outage (issue #145)

A watchdog that is *started* while Postgres is unreachable — a crash, an OOM,
a host reboot, a deploy landing mid-incident — protects the account anyway:

- **How it gets a key without the database.** Every successful DB-backed
  start writes the **watchdog lane's** agent key to a local file
  (`WATCHDOG_KEY_CACHE_FILE`), sealed under the same KEK as the keystore
  (`KEYSTORE_KEK_FILE`), AAD-bound so no field of it can be edited and no
  keystore blob transplanted in. A start that cannot reach Postgres loads
  that copy. It is the watchdog lane only — never the executor's, never a
  master key (ADR-0005 is untouched) — and the file must live on storage
  that survives a container restart (the compose service mounts a named
  volume at `/var/lib/epigone`).
- **When it goes blind.** Startup probes Postgres on a bounded connect and
  keeps probing for `WATCHDOG_COLDSTART_GRACE_SECONDS` (default 180 — the
  same span as the blind threshold, so one rule covers both shapes: a
  five-second blip during a deploy is not an incident). If the database
  answers, startup is exactly as before. If it does not, the process
  cold-starts blind and the incident is declared **at launch**, so the
  FIRST cycle cancels. A start that *begins* DB-backed and then stalls (a
  wedged advisory lock, the host black-holing right after the probe) is
  abandoned at the same grace and goes blind too — hanging unstarted is the
  one outcome this must never have.
- **Worst case, outage onset to the first blind cancel, for a cold start:**
  the grace window — plus, at most, one poll interval if the database
  answered right at its end and the DB-backed attempt then stalled — plus
  one cycle, plus the venue enumeration's HTTP legs (30s/request, the read
  gateway's own bound). Nothing in that path touches Postgres.
- **What a cold-started watchdog cannot do:** place orders. Its gateway is
  wrapped cancel-only in code (`epigone.safety.cancel_only`), so
  placement/modify/leverage raise instead of signing. It cancels, and it
  pushes scheduleCancel. That is the whole authority.
- **Deferred, not skipped.** The migration check runs on the reconnect,
  before any other write. It runs inside the cycle's hard DB ceiling, so a
  migration that cannot finish inside it — typically one queued behind
  another process's advisory lock — leaves the watchdog blind and cancelling
  every cycle until the schema settles. That is the fail-safe direction and
  it is self-healing, but the logs are where you will see it.
- **The process-start stamp lands on that reconnect too**, carrying the
  ACTUAL launch time rather than the reconnect time — otherwise a cold start
  would silently restart the #52 monitor's never-verified-capability grace
  clock and defer the page that says this watchdog has never proved it can
  act.
- **What you will see in the trail** once Postgres returns: a
  `watchdog_cold_start_reconnected` event (launch time, blind duration,
  whether the key cache still matches the keystore), plus the usual
  `blind_window_reconciled` event and halt row — under the headline
  **"cold-start DB-blind sweep reconciled"**, distinct from a running
  process's "DB-blind sweep reconciled". The distinction is the follow-up:
  a cold-start window means the process was restarted mid-outage, so check
  *why* it restarted.
- **The rotation caveat.** The cache is a copy, not the source of truth. If
  you rotate the watchdog lane while the process is down, a cold start uses
  the older key until the reconnect refreshes the cache; the reconnect logs
  the mismatch loudly and asks for a restart, and the on-chain capability
  probe is what decides whether the older key can still act.
- **A cache it cannot write does not change anything.** Refreshing the
  cache is best-effort: if the volume is read-only or full, a healthy
  DB-backed start still starts (loudly logged) rather than degrading into a
  blind one that would cancel the book while Postgres is fine. What suffers
  is the *next* cold start, which falls back to an older copy or refuses.
- **It still refuses to start with no usable key at all** (no cache, or an
  expired one): a watchdog that beats a heartbeat it cannot act behind is
  false safety. Under `restart: unless-stopped` it retries, and comes up
  the moment Postgres answers.

**Sweep coverage:** the cancel-all is ACCOUNT-WIDE ON TWO AXES.

*Venues:* the core venue plus every builder dex in the live `perpDexs`
listing, re-fetched each sweep, so a venue added to trading is swept with no
code change.

*Accounts (issue #136, ADR-0007 decision 1):* the master plus **every
sub-account**, enumerated from the exchange's own `subAccounts` endpoint on
each sweep. A4 made the old boundary untenable — the copy executor places
orders on a Copy Sub-account per Leader, so "sub-accounts are outside the
kill switch's reach" would have meant the kill switch missed exactly the
books with copy money in them. Each account's cancel carries that account's
vault flag, because a cancel names a book. The list comes from the EXCHANGE
rather than from Epigone's own `copy_subs` table for one reason: the
cold-start blind path has no database, and it must sweep subs too.

If EITHER listing endpoint is down, coverage degrades to what is certainly
covered — the POSITION_VENUES on the venue axis, the master alone on the
account axis — those are still swept, but `swept_at` is deliberately
withheld. So a halt alert that keeps saying "sweep PENDING" for more than a
cycle or two means either orders that won't die or degraded coverage; the
watchdog log says which axis.

**A sweep takes MINUTES, and that is not a wedge (issue #201).** Coverage on
two axes means one enumeration per (account, dex) pair at 20 weight each,
paced through the 900/min bucket shared with the stream and ingest — eleven
accounts across four venues is ~1800 weight per pass and the sweep does up
to two passes (cancel, then a fresh verify). Two to ten minutes of wall
clock inside a single watchdog cycle is normal. What that used to look like
from outside was indistinguishable from a hang, so the sweep now says what
it is doing:

- `sweep scope: N venue(s) × M account(s) = K enumeration(s) per pass, ~W
  weight (~Ts …)` — logged before the grind starts. That `~Ts` is the ETA;
  if the wall clock passes it by a wide margin, something else is wrong.
- `sweep progress: <phase> i/K — <address> on <dex>: n resting order(s)` —
  one line per enumeration, for both the cancel pass and the verify pass,
  plus a line per account whose orders were actually cancelled and one per
  position-snapshot read.
- the watchdog's `process_heartbeats` row keeps beating THROUGHOUT (every
  few seconds), so the #52 monitor no longer reads a sweeping watchdog as a
  dead one, and the dead-man's `scheduleCancel` schedule is pushed forward
  from inside the same loop rather than being allowed to fire mid-halt.

So: heartbeat fresh + progress lines advancing = working, wait. Heartbeat
fresh + progress lines stopped = stuck on one REST call or on budget
pacing. Heartbeat stale = the process itself is in trouble; that is the
monitor's watchdog check, and it means what it says again.

Note the monitor's own limit here, unchanged by #201: its staleness
THRESHOLD is 300s (`HEALTHCHECK_WATCHDOG_STALE_SECONDS`) but it only runs
every 15 minutes (`HEALTHCHECK_INTERVAL_MINUTES`), so a genuinely dead
watchdog can go up to a check cadence unreported — long enough for the
dead-man's 300s schedule to fire first. Tightening the cadence to inside
one dead-man period is issue #201's sixth candidate and is NOT done;
until it is, do not treat "the monitor has not paged" as "the watchdog is
alive" during an incident you are already watching.

A DB-BLIND window is deliberately silent on all of these: that path reaches
the wire with zero Postgres behind it by construction, so it neither beats
nor pushes the dead-man until it reconciles. A blind watchdog looks dead to
the monitor because it cannot reach the database to say otherwise — which
is the correct reading, and the reason the DB-blind alert exists. A trip
whose halt row could not be confirmed is the other kind of incident and
DOES beat through its cancel pass: its liveness reads answered that cycle,
so the database is healthy. If a beat fails mid-pass it goes quiet for the
rest of that incident by design — reaching the wire outranks saying so.

One thing a sweep still does NOT do: close positions. A Copy Sub-account's
positions are HELD exactly like the master's, and bracket triggers are
resting orders, so a halt CANCELS a bracket-mode sub's stops. The executor
restores them — as a per-cycle invariant, not only on resume (ADR-0007
amendment D-1) — but it restores NOTHING while the halt stands, because
brackets are the one order shape it leaves resting and a halt means it signs
nothing. So between the halt and the resume, a bracket-mode position is
UNSTOPPED, and the executor says so in the chat when it declines to place
one. If the halt is going to stand for a while and a sub holds something you
wanted stopped, act from the master wallet.

A halt also blocks PROVISIONING: `/copy` mappings waiting for their
sub-account are neither created nor funded while it stands. Unlike an IOC,
a funding transfer cannot be un-sent and a sub-account cannot be un-minted,
so both legs carry the same late halt re-check the order legs do.

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
