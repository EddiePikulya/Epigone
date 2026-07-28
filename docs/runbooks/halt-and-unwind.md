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
| needs Epigone alive | its own process only — not the executor | no — fires even if the host is gone |
| available today | ✅ | ❌ $1M cumulative-volume gate (verified live 2026-07-28; operator ~$58k) |
| status | always on (`--profile execution`) | eligibility-probed every 6h, self-activates on acceptance, transitions on the audit trail |

The watchdog's own liveness is a 🚨 health-monitor check
(`HEALTHCHECK_WATCHDOG_STALE_SECONDS`, default 300); an executor heartbeat
with no watchdog ever run is likewise 🚨 — trading never runs unguarded.

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
