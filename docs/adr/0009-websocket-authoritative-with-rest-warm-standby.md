# ADR-0009: Websocket-authoritative position events, REST as a warm standby

**Status:** accepted (2026-08-06, issue #158)
**Supersedes in part:** ADR-0006 (the 499-Trader capacity argument, already
corrected by ADR-0008), ADR-0007 decision 4 (the consumer's `source` filter)

## Context

Since #157 two lanes have observed the same Traders and written the same
`position_events` rows, distinguishable by `source`. Only the REST poll pass was
ever consumed; the websocket lane was a shadow, kept deliberately unreachable
from alerting so that being wrong cost nothing.

The four-day comparison over real traffic (reported on #158, 2026-08-06; 297
events, 136 matched pairs) settled what the shadow phase was for:

- **sides agreed 100%**, no phantom events on either side;
- the websocket **led the poller by a median 4.2s** (p10 −0.9s, p90 10.7s) —
  about the poll interval, as expected;
- the websocket is **finer**: 20 ws-only events, all scale-ins/outs, because it
  sees individual fills where a 10s poll window coalesces them into one diff;
- **5 poll-only events.** Two are flip-boundary classification differences. At
  least one — `0x037e…` CASHCAT, 11:01:23 on 08-06 — is a change the websocket
  lane never produced while demonstrably alive on that same trader and coin. The
  2026-08-06 amendment notes it cannot be distinguished from the benign
  divergence class (a gradual change crossing the 25% significance threshold in
  one 10s poll diff and never in any single ~5s push), so the honest reading is
  "at most 1–3 real misses in four days, possibly zero".

That last finding is the design driver, whichever way it resolves. A websocket
that is connected, delivering, and silently missing changes trips no heartbeat
and no liveness canary. The only thing that catches it is periodically
re-reading truth and comparing.

## Decision

### 1. The websocket owns event production; REST keeps running as a warm standby

REST polling does not stop and does not idle. It drops to
`STANDBY_POLL_INTERVAL_SECONDS` (60s) where it keeps reading every wallet,
diffing, recording what it saw, and comparing against what the websocket
produced — and it automatically takes production back when the lane goes bad.

Two reasons, both load-bearing:

- **A fallback that only runs during incidents is least exercised when most
  needed.** Escalation changes a cadence; it does not wake dormant code.
- **Continuous reconciliation is the only detector for a lying lane.** See the
  CASHCAT event above.

### 2. Ownership is one row, read under a lock, and the poller decides alone

`lane_authority` (migration 0037) holds owner / since / reason / healthy_since.
The poller reads the ws lane's existing `process_heartbeats` row at the top of
every tick and concludes for itself — no supervisor, no second state, and
therefore no window where one component believes the lane is healthy while
another has already given up.

Exclusivity is enforced by Postgres rather than by timing: producers read the
row `FOR SHARE` inside the transaction that writes their events; a transfer
takes it `FOR UPDATE`. A transfer cannot commit while a producer's write is in
flight, and a producer starting after a transfer blocks, then sees the new owner
and writes nothing authoritative. `tests/test_position_cutover.py` demonstrates
this by running both concurrently rather than asserting it.

**Escalate fast, de-escalate slow.** A heartbeat stale by
`WS_HEARTBEAT_STALE_SECONDS` (45s) transfers production on the next tick.
Ownership returns only after `WS_RECOVERY_SECONDS` (300s) of uninterrupted
freshness AND after the lane has re-established absolute state for every tracked
wallet since the degradation began (its own `ws_lane_state.resynced_at`, which
the lane stamps by re-reading REST in place). Any staleness resets the probation.

**Bounded, documented failover:** staleness (45s) + one tick (10s) + the pass —
under two minutes end to end, and the reason the tick stays at 10s even while
polling runs at 60s.

### 3. Consumers filter on `authoritative`, not on `source`

`position_events.authoritative` is new. `source` says which transport observed
the event; `authoritative` says whether that lane owned production at that
instant. Both lanes write everything they see, forever; the non-owner's rows
carry `authoritative = FALSE` and nothing consumes them.

This replaces ADR-0007 decision 4's mandatory `source='poll'` filter (and #158's
own checklist item "flip the executor's source to 'ws'"). A source filter cannot
survive failover in either position: pinned to `poll` the executor goes blind the
moment the websocket takes over; pinned to `ws` it goes blind the moment the
poller takes it back — and a copy that opens and never closes is the worst
outcome this system can produce.

It also keeps the shadow comparison alive **through and after** the transition,
which the ticket asks for and which matters because four days is thin for tail
behaviour.

### 4. Drift is an incident, never a silent write — but only on the second look

When the poller's diff finds a change the websocket has not produced, it does
not quietly write it: that would put two writers on one Trader. It escalates
(transferring ownership, with the wallet and coins named in `reason`), re-checks
under the exclusive lock, and only then produces the event. The health monitor's
`position_lane` check reads that reason and puts it in front of the operator.

**One look of patience first.** The websocket is routinely a few seconds BEHIND
the poller while perfectly healthy — it holds entry bursts (decision 5) and
re-sends state on a ~5s cadence — so at the 60s standby cadence a real share of
all changes are seen here first. Escalating on sight would fire the incident
most days, and an incident that fires most days is one nobody reads. So the
first sighting withholds the verdict AND the memory advance
(`position_poll_state.reconcile_pending`): the coin's anchor stays put, the next
pass re-diffs the identical change, and it decides then. The wallet is re-polled
on the next 10s tick rather than after another standby interval, so a genuine
miss costs ~10s of latency, not ~60s.

Compared on **direction**, not on kind or size, and not on the coin alone. The
lanes legitimately describe the same reality with different kinds (the
granularity difference above; the flip-boundary decomposition), so comparing
kinds would escalate on lanes that agree. Comparing the coin alone fails the
other way: an entry the websocket did produce would vouch for an exit it did
not, and an exit nobody produces is a copy position that never closes. `flip`
counts as both directions, which is exactly what absorbs the two lanes'
different flip decompositions.

**This is the disposition of the comparison's third condition** ("flip-boundary
kind normalization so the two producers' vocabularies match during any ownership
transfer"). The vocabularies already match — both lanes call the same
`epigone.position_diff`, one rule, one threshold, since #157. What the
comparison saw was not two vocabularies but one vocabulary applied to
observations taken at different cadences: a flip the poller reports as
`scale_out` + `flip` is a flip the websocket saw mid-way. No normalization can
remove that, because neither description is wrong. What the transfer actually
requires is that the two descriptions be interchangeable at the boundary —
neither doubling a leg nor dropping one — and direction-matching is what
delivers it. Downstream, ADR-0007's self-damping rule finishes the job: every
relative operation applies to the size the EXCHANGE reports, never to a
bookkept expectation, so a differently-decomposed flip converges on the next
cycle rather than compounding.

### 5. Burst coalescing: entries debounce, exits never do

*The open decision this ticket left to implementation.*

The websocket's finer view is not finer information about the Leader's intent —
it is the transport showing fills the poll window merged. Producing each one
would make the copy path mirror one entry with three orders: three sets of fees,
and three chances of a sliver falling under the exchange's minimum notional and
being skipped entirely.

**Decision: hold entry-side scale-ins for `WS_COALESCE_WINDOW_SECONDS` (3s) and
emit the whole burst as one event. Closes, flips and scale-OUTs are produced at
the first observation that shows them, always.** Opens are not held either — an
open is never a burst, and delaying one only costs price.

Implemented by *not advancing the anchor*: the coin's snapshot stays where the
burst began, so the next observation diffs the whole burst as one change
measured from it (`ws_position_snapshots.coalescing_since` records the window).
Nothing is buffered in memory, so a lane that dies mid-burst loses nothing — the
anchor is on disk and the change re-diffs exactly once.

**Why the cost is affordable:** an entry is delayed by at most the window plus
the wait for the next ~5s push. That is less than the latency the cutover buys
(median 4.2s), so a coalesced entry still reaches the copy path sooner than it
does today. The debounce spends part of the win, never more than the win.

Rejected alternatives: aggregating in the copy path instead (the executor would
need a timer and a pending-order concept — state that can be wrong, in the
process that signs); and a size floor on emitted scales (silently drops small
real changes, and the exchange minimum is a property of the copy account, not of
the Leader's trade).

### 6. Capacity: global ownership, tracked set capped at 15

One IP may subscribe **15 unique users** (ADR-0008, measured). Per the 2026-08-06
amendment, v1 keeps ownership **global** and caps the tracked set at 15 rather
than splitting ownership per wallet: per-wallet ownership doubles the machinery
for a scale this deployment does not have (13 tracked, single-operator posture).

Consequence: a poll set past 15 keeps the poller authoritative — the handback is
blocked with that reason — because a lane that is authoritative for wallets it
cannot see is a silent hole in alerting and in the copy path. The way past 15 is
more source IPs (#29, riding #188's egress selection), which is a later ticket.

The follow caps (15 admin / 5 everyone else) are **left as they are**: the poll
set is the union of tracked wallets and linked wallets, so the cap that binds is
the union's size, and the honest control is the ws-side ceiling refusing to hand
over production rather than a per-user number that does not measure the same
thing. The ticket asked for the choice to be stated; this is it.

### 7. Copy-enabled Leaders take the scarce resources first

`epigone.poll_set.leaders_first` orders the poll set for every consumer that has
something scarce to spend on it, in every mode rather than only degraded ones —
a rule that only runs during incidents is a rule nobody has tested.

- **The REST pass**: ordering *is* the prioritisation the ticket asks for. The
  pass is paced by the shared weight budget, so a set too large for the
  escalated cadence stretches at its tail, and a Leader must never be in the
  tail.
- **The websocket lanes**: which 15 wallets get slots (and which 8 get order
  connections) is a decision, per #158's 2026-08-04 comment — "selection needs
  to be deliberate, not alphabetical". It was an address-sorted prefix, which
  decides by leading hex digit; it is now Leaders first. That also retires
  #168's deferred finding A3.

### 8. The kill switch

`WS_AUTHORITATIVE=0` pins production to the poller at its escalated cadence —
the pre-cutover world — while the websocket lane keeps shadowing. It takes
effect on the next tick, with no probation, because undoing a cutover must be
immediate. Unset means enabled: a deployment that forgets the variable must not
silently run a fast transport nobody listens to.

## Consequences

- Position Alerts, copy execution and the `/tracked` experience are unchanged in
  shape and continue across a cutover, a failover and a recovery — both lanes
  publish through one seam (`epigone.position_publish`), so a User cannot tell
  which transport saw their Trader.
- Entries reach the copy path ~1–4s sooner than before despite the debounce;
  exits reach it ~4s sooner with no debounce at all.
- The standby cadence hands most of the position lane's share of the 900/min
  budget back to ingest, so fine-metric refresh gets faster than before.
- One new failure mode to watch: a lane that flaps just under the staleness
  threshold stays authoritative while producing gaps. Reconciliation catches
  that as drift within one standby interval, which is what it is for.
- `authoritative` is now the column any future consumer must filter on. A
  consumer that forgets it double-copies; the docstring on
  `outstanding_events` says so at the point of use.
