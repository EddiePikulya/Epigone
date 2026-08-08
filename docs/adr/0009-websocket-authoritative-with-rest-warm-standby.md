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

**Two classes of divergence, and only one of them is a fault.** The poller
seeing a change the websocket has not produced means one of two entirely
different things, and conflating them was the first version's real defect.

*Latency divergence* — the lane is a few seconds behind. It holds entry bursts
(decision 5) and re-sends state on a ~5s cadence, so at the 60s standby cadence
a real share of changes are seen here first. It resolves itself on the next
look. So the first sighting withholds the verdict AND the memory advance
(`position_poll_state.reconcile_pending`): the coin's anchor stays put, the next
pass re-diffs the identical change, and it decides then. The wallet is re-polled
on the next 10s tick rather than after another standby interval, so a genuine
miss costs ~10s of latency, not ~60s. Every completed pass rewrites the whole
pending set — including to empty — because a doubt about a change that has since
evaporated would otherwise sit in the row for days and then escalate the first
unrelated change that ever raced the lane, with no patience at all.

*Threshold divergence* — **the benign class #158's 2026-08-02 comment names**,
and it never resolves, because there is nothing to resolve. The significance
threshold measures against the LAST OBSERVATION, and the lanes observe at
different cadences: a position creeping up ~8% per push crosses 25% against a
60s-old anchor and never against any single push. The lane emits nothing, and
its memory is completely current. Waiting a second look does not help; the
comment requires this be classified as expected, not as a lane error, and a
version that escalated it would have thrashed ownership daily and taught the
operator to ignore the incident channel — this design's own stated anti-goal.

**The discriminator is the other lane's own memory, plus its unconsumed rows.**
Before doubting anything, the poller diffs what it just read against
`ws_position_snapshots` — the websocket lane's anchor, which IS its claim about
what it has seen — using the same `position_diff` both lanes share. Coins where
that diff yields an event are coins the lane is behind reality on *by its own
account*: **MISSED**. A Trader the lane has never baselined vouches for nothing
— an empty memory agrees with a flat wallet the way an empty room agrees with an
empty room, and absence of memory is absence of watching. The read is strictly
read-only: each lane's diff memory has exactly one writer, or that writer's next
diff would be against state it never observed.

The anchor alone is not enough, and the gap it leaves is a *silent* one, which
makes it the more dangerous half. The lane advances its anchor whether or not
anyone consumes what it wrote — so a change it observed while the POLLER owned
production leaves an unconsumed row and a moved anchor. Read by the anchor
alone, that is indistinguishable from the benign class, and it would be vouched
away by both lanes forever: no event, no alert, no incident, and if it was an
exit, a copy position that never closes. The window is small (a change observed
after the poller's last poll-owned pass and before the handback commits) but it
follows essentially every deploy.

So an unconsumed websocket row moving the same direction inside the window is
evidence too: **STRANDED** — nobody produced it and nobody now will. Anything
else — no row at all, and an anchor that agrees with reality — is the benign
class: the lane's rules made the change a non-event, and it is recorded as a
shadow row and produced by nobody, which is what those rules already decided.

The incident says which of the two it is, because they send a human to different
places: a lane that dropped a change is a lane to investigate, while a change
that fell between two owners is a transfer doing what transfers do. And the tick
that moves ownership always polls, whatever the cadence says, so a straddler is
found within a tick rather than a standby interval.

**A held doubt is judged in the window that raised it** *(issue #200)*.
Withholding the verdict is only patience if the second look asks the same
question, and it did not, quite. The lookback was re-derived on every pass from
`last_polled_at − RECONCILE_GRACE` — and the pass that HOLDS a doubt advances
`last_polled_at` itself, so the window walked forward while the doubt stood
still. The transfer-tick poll above keeps that walk to one tick on the happy
path, but the pass that catches a straddler is exactly the pass that can skip a
wallet: one `RateLimitedError`, or a five-failure abort cutting the tail of
`leaders_first`, costs that wallet its look. The doubt is then raised a standby
interval later and confirmed ten seconds after that, against a window starting
well past the unconsumed row that proved the straddle — reclassified benign,
shadow-recorded, swallowed. The round-2 hole again, reached by a different
route, and for an exit that is a copy position that never closes.

So the doubt carries its own window: `position_poll_state.reconcile_since`
(migration 0038) records the look the doubt was raised against, written and
cleared by the same statement as `reconcile_pending` so neither can outlive the
other. The evidence cannot age out of it, and not merely by a margin — a change
is a change *because* it was observed after that look, so a window starting
there contains what raised it however late the confirm arrives.

**Per coin, not per wallet.** A wallet holding a doubt is usually holding other
positions too, and one cursor for all of them would let a minute-old websocket
row vouch for a brand-new change on a different coin: the same swallow, arriving
through the repair. Each change is judged from the window that begins at the
observation it is measured from — which for a held coin is where its anchor was
deliberately left, and for every other coin is this pass's previous look.

*Rejected: freezing `last_polled_at` while a doubt stands* — removing the
coupling rather than pinning it, as #158's round-3 note proposed. It produces
the same window for the doubted coin and costs no migration, and it was rejected
for two reasons. One cursor cannot say that one coin's evidence is older than
another's, so it buys the per-wallet widening above. And it turns a column that
states a fact — when this pass last read this wallet — into a claim about
judgement, inherited by every reading of it, present and future.

That second reason is what the note itself flagged, and the code answers it more
narrowly than the note assumed: **the Withdrawal Alert staleness gate does not
read `last_polled_at`.** It measures from the equity observation's own timestamp
(`trader_equity.observed_at`, against `MAX_OBSERVATION_GAP_INTERVALS` × the
interval in force), and that observation lands unconditionally on every pass,
held doubt or not. So freezing the cursor would not have widened withdrawal
detection today; it would have left a second freshness timestamp beside one that
stayed honest, with nothing in the code saying which readings must use which.
`test_a_held_doubt_keeps_its_window_without_holding_the_wallets_freshness` pins both
halves — the doubt keeps its window, the wallet keeps its freshness, and the
withdrawal is detected across the hold exactly as it would have been without it.

One consequence, stated because it reverses a claim this document's own
implementation notes made: the transfer-tick poll is no longer a correctness
precondition of the stranded repair. The confirm window no longer moves, so a
late hold is late and nothing more. It stays, as the latency property it looks
like — a straddler found within a tick beats one found within a standby
interval — and the margin it spends is now pinned as a relation
(`RECONCILE_GRACE` against one straddle plus one confirm tick, with a push
cadence left over) rather than asserted in a comment.

Compared on **direction**, not on kind or size, and not on the coin alone. The
lanes legitimately describe the same reality with different kinds (the
granularity difference above; the flip-boundary decomposition), so comparing
kinds would escalate on lanes that agree. Comparing the coin alone fails the
other way: an entry the websocket did produce would vouch for an exit it did
not, and an exit nobody produces is a copy position that never closes. `flip`
counts as both directions, which is exactly what absorbs the two lanes'
different flip decompositions.

**Every direction the change moves, not merely one of them** (issue #196). A
`flip` counting as both directions was first read as "either will do", and that
left the coin-alone hole open through the one kind that spans both: an entry the
lane did produce answering for the exit inside a flip the lane missed. It is the
only known interleaving that leaves a copy on the WRONG SIDE of its Leader
rather than merely late, so the vouch has to be a complete account — a poll-side
flip is told only when both directions are. A lane that decomposes flips pays
nothing for it: it emits both legs inside its ~5s push cadence, well within the
one-look hold that every doubt serves anyway. The **stranded** question takes the
opposite quantifier deliberately, because it is the opposite question: there a
row earns silence and only a complete account should; here a row is evidence the
lane saw a change nobody produced, and half a flip is still evidence.

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
  `outstanding_events` says so at the point of use. It defaults FALSE, so a
  WRITER that forgets it produces rows nobody consumes — the safe direction,
  and the one that matters during a rolling deploy, when a pre-cutover
  container can still be writing into an already-migrated database.
- A gradual accumulation that never crosses the threshold against any single
  websocket observation now produces no event at all, where the 60s poller
  would have called it a scale-in. That is the websocket's rules being the
  rules, which is what "authoritative" means; the same change was already
  invisible at the old 10s cadence for all but the steepest ramps.
