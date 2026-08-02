# ADR 0006: Persist position events as the execution seam, not per-follower alert rows

Date: 2026-07-30
Status: accepted

Numbered 0006 because 0004 is reserved by the prediction-market spike
(PR #69, unmerged) and 0005 is the autotrade key architecture this extends.

## Context

Issue #136 (Autotrade A4, the operator copy executor) is the next ticket in
ADR-0005's Phase A. It says the executor consumes "the position-event signal
for copy-enabled leaders … pick the clean seam: the underlying events, not
per-follower alert delivery rows." That seam does not exist. The events are
real, but they are ephemeral.

Today `epigone.stream.poller` fetches each polled wallet's positions across
`POSITION_VENUES`, diffs them against `position_snapshots`, and builds a list
of in-memory `_Event` objects — OPEN, CLOSE, FLIP, SCALE-IN, SCALE-OUT, the
last two gated by `SCALE_SIGNIFICANCE_THRESHOLD` (25% of the last snapshot's
notional). `_queue_alerts` then fans that list out into `position_alerts`:
**one row per event per follower**, filtered at queue time by each Track's
`muted` flag and its effective `min_size_usd` floor (the #10 rule —
suppression at queue time, never at delivery, so unmuting never backfills).
Snapshot writes and alert inserts share one transaction per trader, which is
what makes an event detected exactly once across stream restarts.

Then the `_Event` list goes out of scope. The only durable trace of a
leader's trade is a set of per-follower notification rows and a snapshot of
the *new* state. The event itself — the thing that happened — is never
written down.

So A4 has to get its signal from somewhere, and the choice is being forced
now rather than after the executor exists, because the path of least
resistance (read `position_alerts`) permanently welds execution to Telegram
notification delivery.

### The forward-compatibility driver

A WebSocket lane for copy-enabled wallets is on the roadmap, and it changes
what "the right seam" means. Verified against Hyperliquid's docs on
2026-07-30:

- Every user-scoped subscription takes an arbitrary `user` address —
  `webData3`, `orderUpdates`, `userEvents`, `userFills`,
  `clearinghouseState`, and notably `allDexsClearinghouseState`, which
  covers every dex in one subscription where the REST poller spends two
  `clearinghouseState` calls per wallet per pass.
- The per-IP WS limits are **10 connections, 30 new connections/min, 1000
  subscriptions, 2000 outbound messages/min, 100 in-flight post messages** —
  and they are a **separate budget from the 1200 weight/min REST cap**
  (Epigone paces against 900/min of that, issue #28).
- Correction to the record: `docs/research/ecosystem-survey.md` and ADR-0002
  both say user-specific subscriptions are "capped at 10 unique addresses per
  IP". That is wrong. The 10 is a *connection* cap; addresses are bounded by
  the 1000-subscription cap. ADR-0002's conclusion is unaffected (it turned
  on nothing being CPU-bound), but the figure should not be carried forward.

That lane would take copy latency from the current poll-shaped ~10–20s
(10s interval, plus the wallet's position in the pass, plus the 2s outbox
tick) to ~1–3s, and it is the *only* way limit-order mirroring becomes
viable. The REST arithmetic says so plainly: `frontendOpenOrders` is weight
20 per venue per call, so 40 per wallet per cycle across core + xyz. At the
shipped 100s cadence that is 24 nominal weight/min/wallet; to approach WS
latency you would need a seconds-scale cadence, and at 10s the order pass
alone is 240/min/wallet — the entire 900/min bucket is gone at **~3 wallets**,
with ingest starved to nothing. (At 30s it is 80/min/wallet, ~11 wallets by
weight but only ~5 by the pass's own `7N−5` second duration limit. Every
route to fast order mirroring over REST dead-ends in single-digit wallets.)

The consequence for *this* decision: if A4 consumes a persisted event table,
the WS lane later becomes **a second producer writing the same rows**, and the
executor is not touched. If A4 reads alerts or re-diffs snapshots inline,
adding WS means rewriting the executor — the one component where a rewrite
costs real money if it is wrong. This is the strongest argument in the ADR
and it is a structural one, not a preference.

## Decision

**Persist the event.** A new `position_events` table, written by the poller in
the same transaction that updates `position_snapshots`. `_queue_alerts`
becomes one consumer of the event list; the A4 executor becomes another.

### Schema (migration 0029)

(Written as 0028; corrected when issue #155 — the coin-unit prefactor this ADR
asks for, below — shipped first and took that number. A migration number is
frozen once it ships and an unwritten one is not, so the ADR moves.)

`position_events` carries everything `_Event` and `position_alerts` carry
today, plus what execution needs and neither currently records:

| column | why |
| --- | --- |
| `id` (identity), `trader_address`, `coin` | identity and subject; `coin` is venue-namespaced (`xyz:META`), exactly as the snapshot key is |
| `kind` | `open` \| `close` \| `flip` \| `scale_in` \| `scale_out` |
| `side`, `size_usd`, `leverage`, `entry_price` | the new leg (open, flip, scale) |
| `prev_side`, `realized_pnl`, `pct_return`, `opened_at` | the closed leg (close, flip) and the scale's live return |
| `prev_size_usd` | the size a scale grew or shrank from |
| **`size_coin`, `prev_size_coin`** | **new** — the position in coin units |
| `observed_at` | when Epigone saw it; equals the alert row's `created_at` for the same event |
| `source` | `poll` \| `ws`, defaulting to `poll` — the second-producer hook, and how WS-vs-poll latency gets measured |

The two new columns are the substantive schema finding. `clearinghouseState`
returns `szi` (signed coin size) and `parse_positions` reads it — but only for
its sign, to derive `Side`. The magnitude is discarded, so nothing downstream
of the gateway knows a position in coin units. An executor cannot place an
order from a dollar notional alone; it needs units, or a mark price to convert
with. Recording `size_coin` gives both, since mark is `size_usd / size_coin` —
so no separate mark column, which would only invite the two to disagree. This
requires widening `Position` with the absolute `szi` and adding a nullable
`size_coin` to `position_snapshots`, because a CLOSE event is built entirely
from the snapshot. Existing snapshot rows backfill on their next poll (the
upsert rewrites every column); the only gap is a position that both exists
pre-migration and closes in the very first pass after it, which yields one
event with a NULL `size_coin`. Consumers treat NULL units as "size not
mirrorable" rather than guessing.

The same CHECK constraints `position_alerts` carries (an open has a side, a
close has a prev_side, a flip has both, a scale has both sizes) come along —
they encode the diff's shape and are as true of the event as of the alert.

What deliberately does **not** come along is `tpsl`. That kind exists on
`position_alerts` (migration 0021) as an anchor-editing enrichment queued by
the *order* poller; it is not a position event, and putting it here would
smuggle presentation into the seam. Likewise absent: `telegram_message_id`,
`scale_arrows`, `attempts`, `delivered_at`, `user_telegram_id` — every one of
those is delivery state, which is exactly what we are separating out.

### One transaction, write-ahead

The event insert joins the existing per-trader transaction alongside the
snapshot upsert, the poll-state update, and the alert fan-out. This is free —
the transaction is already open — and it is load-bearing. A crash between the
event write and the snapshot write would either lose the event (snapshot
advanced, no record) or replay it (event written, snapshot un-advanced, the
next pass diffs the same change again). Atomicity is the whole reason the
poller's exactly-once property holds today, and the event row inherits it.

This is the same write-ahead discipline `safety/audit.py` established for
`execution_audit` under ADR-0005: the durable record is written before or
with the effect, never after, so a crash leaves an over-recorded state that
can be reconciled rather than an under-recorded one that cannot be detected.

### Consumers claim; they do not cursor

Progress is tracked in `position_event_claims (event_id, consumer,
claimed_at)`, primary-keyed on the pair, cascading on event delete. A
consumer's work queue is the events with no claim row for its own name.

The obvious cheaper design — a per-consumer cursor table holding
`last_event_id` — is **rejected on the ADR's own central argument.** Identity
values are allocated at INSERT and become visible at COMMIT, so two producers
committing concurrently can publish out of id order; a cursor that has
advanced past the higher id silently skips the lower one forever. Today there
is exactly one producer and the hazard is theoretical. But the entire reason
for persisting events is that a WS lane joins as a second producer, so
adopting a scheme that is correct *only* while there is one producer defeats
the decision. A claim table is order-independent by construction: an event is
outstanding until someone claims it, whenever it became visible.

A single `consumed_at` flag or boolean on the event row is rejected for the
same reason in a different dress — it presumes exactly one consumer, and the
premise of the seam is that there are at least two.

**Idempotency across restarts (#136's acceptance criterion) is write-ahead
claiming.** The executor inserts its claim in the same transaction that writes
the `execution_audit` *attempt* row, then goes to the wire. A crash between
claim and response leaves a claimed event, an attempt row, and no outcome
row — which is precisely the "reconcile me" signal `safety/audit.py` already
defines, and A4 already reconciles positions against live `clearinghouseState`
every loop. The trade being made here is explicit: **a claimed-but-unsent
event is a missed copy; an unclaimed-but-sent event is a double copy.** We
choose the missed copy, because reconciliation surfaces it and the leader's
next event re-syncs, whereas a double copy is an unrequested doubled position
with real money behind it.

A claim means *handled*, not *traded*. An event the risk policy declines, or
one for a leader whose copy flag is off, is still claimed — with the audit row
recording the decision — otherwise the queue never drains. The executor's
query additionally joins to the copy-enabled set, so uncopied leaders' events
never enter its backlog in the first place.

### Ordering

The guarantee is: **events for the same `(trader_address, coin)` are totally
ordered by `id`, and consumers process in `id` order.** Cross-trader and
cross-coin ordering is not guaranteed and does not need to be — every consumer's
state is keyed per position, so BTC's event order is irrelevant to ETH's.

Same-coin order is what matters: mirroring a SCALE-IN before the OPEN it
scales, or a CLOSE before the OPEN, produces nonsense. It holds today because
one producer commits one transaction per trader per pass. When the WS lane
lands, it must hold to the same invariant — **one producer per (trader, coin)
at a time** — which falls out naturally from the intended split: a
copy-enabled wallet is streamed *or* polled, never both. That constraint is
recorded here so the WS ticket inherits it rather than rediscovering it.

On flips specifically: a flip is **one row**, not a CLOSE row followed by an
OPEN row. `_flip_event` composes both legs into a single `_Event`
(`prev_side` + `realized_pnl` for the closed leg, `side` + `size_usd` +
`entry_price` for the new one), and `position_alerts` has always stored it
that way. So the ordering hazard a two-leg flip would create simply does not
arise — and this ADR declines to split it for the executor's convenience.
Splitting would require guaranteeing both ordering *and* atomic co-visibility
of the pair, which a single row provides for free. The executor reads a flip
as one instruction: close what you hold, open the other side.

### Retention

`POSITION_EVENT_RETENTION = 7 days`, pruned by `observed_at` in the poll pass
that wrote events — the `record_rate_limit` precedent (`epigone.budget`
prunes `rate_limit_events` past a day as it inserts, rather than running a
sweeper). Claims cascade away with their events.

Retention here is a safety property, not housekeeping. A consumer seven days
behind is not behind, it is broken, and replaying week-old copy signals would
mirror trades whose theses have expired — actively worse than doing nothing.
The executor pairs this with a much tighter staleness guard: an event older
than a few minutes (A4 to pick and record the constant; ~5 minutes is the
starting proposal) is claimed and skipped with an audit row, never acted on.
That is what makes "the executor was down for an hour" a safe event rather
than a burst of stale orders.

Volume makes the pruning cheap: events are rare — a wallet opens, scales, or
closes a handful of times a day — so even a 50-wallet poll set at 20
events/wallet/day is ~7k rows at steady state.

### Scope: positions only

This ADR covers position events. The order poller (`stream/orders.py`) has a
superficially similar shape but a materially different one underneath:
`order_alerts` is **one batched row per follower per wallet per cycle**, not
one per event (issue #115's noise rule), with the batch stored as rendered
JSONB. An `order_events` table would need per-order rows and a different
schema, and it would be premature besides — the weight arithmetic above shows
limit-order mirroring cannot work on a REST cadence at all. The order-event
seam should be designed *with* the WS lane, where `orderUpdates` pushes each
order individually, rather than retrofitted onto a 100s poll it can never
serve. When it comes, it follows this ADR's pattern: persisted events, claim
table, alert fan-out as one consumer.

### Sequencing: the table ships beside the alert path, which is not refactored

Phase 1 (the implementation ticket) adds the table and the event write, and
leaves `_queue_alerts` **exactly as it is**. Both writes iterate the same
in-memory `events` list inside the same transaction, so they cannot diverge:
there is no window in which the table says one thing and the alerts another.

Refactoring `_queue_alerts` to read back from `position_events` is not
scheduled — not "later", but *not planned at all* unless a concrete need
appears. It would buy nothing (the list is already in hand), and it would put
a schema change and a read-path rewrite on the live alerting path that users
depend on, in the same change. The apparent duplication is one loop over a
list, not duplicated logic; the actual diff semantics remain in exactly one
place, which is the property that matters.

The suppression rules stay where they are, in the alert layer, and are
explicitly *not* modelled in the event table. Mute and `min_size_usd` are
notification preferences. The executor must see a $500 open from a leader the
operator copies even if every follower's floor is $10k — which is precisely
the failure mode option 1 has.

## Alternatives honestly weighed

- **Executor reads `position_alerts`.** Rejected by #136 and rejected again
  here on the merits. Those rows are per-follower delivery records: duplicated
  once per follower (so the executor must dedupe to recover the event), and —
  the disqualifying part — *conditionally absent*. An event is dropped at
  queue time if the follower is muted or if the position is below their
  `min_size_usd` floor, so a leader nobody tracks with a low enough floor
  produces **zero rows** and the executor silently misses the trade. Copy
  trading would inherit notification semantics: change an alert floor, change
  what gets traded. It also couples execution to a table whose columns exist
  to serve Telegram message editing (`telegram_message_id`, `scale_arrows`,
  `tpsl_line`, `attempts`).
- **Executor re-diffs `position_snapshots` itself.** No new table, no new
  write. Rejected because it puts the baseline rule, the flip rule, the
  scale-significance threshold, and the venue-namespacing convention in a
  second place, and the two copies will drift — the threshold has already been
  tuned once (#10) and the venue tuple changed twice this month (#21, and the
  mkts drop in #149). Worse, both differs would race on the same snapshot
  rows: the poller advances the snapshot the instant it diffs, so an executor
  reading afterwards sees the *new* state and the event is already gone. It
  would need its own shadow snapshot table, at which point it has re-derived
  option 3 with an extra copy of the state and no atomicity between them.
- **A generic events/outbox table for all domains.** Considered and dropped as
  premature: two producers and two consumers do not justify an abstraction,
  and the columns here (`prev_size_usd`, `realized_pnl`, `opened_at`) are
  position-shaped. If order events land later and a third domain follows,
  generalise then.
- **Cursor table instead of claims.** Covered above: cheaper, one row per
  consumer, and unsound the moment a second producer commits concurrently —
  which is the exact future this ADR exists to protect.

## Consequences

- A4 gets a seam it can be built against now, and the WS lane later writes the
  same rows with `source = 'ws'` without the executor changing. That is the
  payoff, and it is the reason this ADR is worth its cost.
- The costs, stated plainly: one new table and one new claim table; one extra
  INSERT per event inside a transaction that is already open (events are rare,
  so this is not measurable); two nullable columns added to
  `position_snapshots` and `Position`; a retention rule to keep honest; and a
  claim-scan query that is a `LEFT JOIN` over an unindexed-by-absence
  predicate — fine over a few thousand rows, and it is retention that keeps it
  a few thousand rows. If the table ever grows past that, the query needs an
  index strategy or the claims model needs revisiting.
- Execution and notification are now genuinely independent. Changing a mute,
  a min-size floor, or the whole alert renderer cannot change what gets
  traded — and this ADR takes that as a rule, not an accident: no execution
  path may read `position_alerts`.
- The diff logic stays in exactly one place. The event table is a record of
  what the poller decided, not a second opinion about it.
- The exactly-once property is now stated as a contract rather than an
  emergent property of one transaction: producers write events atomically with
  the state they diffed; consumers claim before acting. A4's "no double-copy
  across restarts" is discharged by that contract, at the accepted cost of
  favouring a missed copy over a doubled one.
- Order events remain unsolved, deliberately. `order_alerts` keeps its batched
  per-follower shape until the WS lane makes a per-order seam meaningful.

## Update, 2026-08-02 (issue #157): the shadow phase, and what the one-producer invariant binds

The WS lane landed as this ADR anticipated — a second producer writing these
same rows with `source = 'ws'`, and the executor untouched. Two clarifications
the implementation forced, recorded here so a later reader does not find the
document contradicting the code:

- **"One producer per (trader, coin) at a time" binds from CUTOVER, not now.**
  The Ordering section states that invariant and derives it from "a copy-enabled
  wallet is streamed *or* polled, never both". The shadow lane deliberately
  violates it: it subscribes to the whole poll set and dual-writes every
  `(trader, coin)`, because comparing the transports is the entire point and
  requires both descriptions of the same change. That is the case the Ordering
  section itself allows two paragraphs later ("a dual-written (trader, coin) is
  that comparison working, not a bug"), and it is safe only because NOTHING
  consumes `'ws'` rows. The invariant becomes binding the moment a consumer
  reads a source it did not filter for — which is #158's decision to make, not
  a property the shadow phase has.
- **Order events stayed unsolved, as §Scope requires.** #157's issue text asked
  for resting-order changes "recorded as position events"; the implementation
  session narrowed this to positions on this ADR's reasoning, and the operator
  confirmed the narrowing at merge review (2026-08-02), deferring order
  persistence to a dedicated follow-up ticket rather than dropping it. The
  lane subscribes to `orderUpdates` (it costs a subscription and serves the
  liveness signal) and counts what arrives, but persists nothing — the order
  seam's schema remains a decision to be made with the cutover, not a detail to
  be improvised inside a shadow lane.

One factual correction to the Context section while the record is open: the
"~1-3s" latency it projects is what the transport ALLOWS, not a measurement.
`allDexsClearinghouseState` was observed pushing absolute state on a ~5s cadence
for an idle account; whether a change also triggers an immediate push is
unsettled and is #158's to measure. See docs/research/ecosystem-survey.md.
