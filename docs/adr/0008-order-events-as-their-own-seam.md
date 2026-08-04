# ADR 0008: Order events as their own seam, attributed by connection

Date: 2026-08-03
Status: accepted

Extends ADR-0006, which built `position_events` and closed with §Scope:
"Order events remain unsolved, deliberately… The order-event seam should be
designed *with* the WS lane, where `orderUpdates` pushes each order
individually, rather than retrofitted onto a 100s poll it can never serve. When
it comes, it follows this ADR's pattern: persisted events, claim table, alert
fan-out as one consumer." This is that design (issue #168).

## Context

Issue #157 shipped the websocket shadow lane. It subscribes `orderUpdates` per
Trader, counts what arrives (`LaneStats.order_messages`), and persists nothing —
the operator confirmed the positions-only narrowing at merge review
(2026-08-02) and deferred the order seam to a ticket of its own.

#168 asks for that seam: resting-order changes for tracked Traders, arriving
over the subscription the lane already holds, persisted with an order-domain
vocabulary. Its stated premise is that this is cheap — "the subscription already
exists and is free… this ticket adds persistence, not subscriptions".

**That premise is false, and the way it is false decides the whole design.**

### What the transport actually does (measured 2026-08-03, mainnet, read-only)

Reproducible as `scripts/testnet_ws_probe.py orders` and `… users`; the numbers
are in docs/research/ecosystem-survey.md.

**1. An `orderUpdates` frame does not say whose order it is.** The payload is

```json
{"channel": "orderUpdates", "data": [
  {"order": {"coin": "xyz:CL", "side": "B", "limitPx": "80.366", "sz": "3.732",
             "oid": 509258420256, "timestamp": 1785781659536,
             "origSz": "3.732", "cloid": "0x0e2b…"},
   "status": "open", "statusTimestamp": 1785781659536}]}
```

There is no `user` field at any level — unlike `allDexsClearinghouseState`,
which carries one, which is exactly why the position lane can multiplex every
Trader onto a single connection. Subscribe two Traders' order feeds to one
connection and the frames arrive interleaved and anonymous. The transport hands
you the order and withholds whose it is.

So the lane as shipped *cannot* persist what it is already receiving. The
subscription is free; the attribution is not.

**2. There is an undocumented per-IP cap of 15 unique users.** A 16th address
is refused with `Cannot track more than 15 total users.` Measured: it is per
**IP**, not per connection (a brand-new address on a freshly opened second
connection is refused while the first holds its 15); it counts distinct
**addresses**, not subscriptions (a second subscription type for an
already-tracked address is free, and so is the same address on a *different*
connection); unsubscribing frees a slot at once, and closing a connection frees
all of its slots within ~2s.

This retires ADR-0006's "499 Traders on one connection" and the
`MAX_SUBSCRIBED_TRADERS` constant derived from it. The real ceiling for the
whole lane process, on one IP, is **15 Traders** — which the tracked set can
already exceed (15 wallets per User × 2–3 Users). That is a pre-existing #157
defect, not one this ticket introduces; what this ticket owes it is honesty
(below).

**3. `webData3` is not a way out.** It exists (`webData2` does not), but its
payload carries `perpDexStates` and `userState` only: no `user` field and no
open orders at all. It neither names its subject nor carries the resting book.

**4. The feed is a firehose.** One market-making address alone: **442 frames /
1471 order updates in 60s** (`open` 649, `canceled` 628, `badAloPxRejected` 105,
`iocCancelRejected` 68, `filled` 21). Fifteen such addresses on one connection
produced 8066 frames in 90s. Position events are rare — "a wallet opens, scales,
or closes a handful of times a day" (ADR-0006 §Retention). Order events are not,
and a seam sized as if they were would be sized wrong by four orders of
magnitude.

## Decision

### 1. Attribution is structural: one connection per Trader

An order update is attributed by **which connection it arrived on**, because
that is the only fact about it that identifies a Trader. The order lane
therefore holds one websocket connection per shadowed Trader, subscribed to
exactly one `orderUpdates`.

Alternatives weighed and rejected:

- **Attribute by `oid` against the resync's known-order set.** Works for
  `canceled` / `filled` on an order we already knew; fails for exactly the event
  that matters most, a *placement*, whose oid is by definition new. Mirroring a
  leader's plan without seeing placements is not a seam, it is a log of endings.
- **Attribute the whole frame when it contains one known oid.** Frames do appear
  to be per-user batches — a requote arrives as a cancel of a known oid beside
  the replacement placement — so this would attribute much of the traffic. It is
  rejected anyway: it is a heuristic sitting under a seam that copy execution is
  meant to ride on, it silently fails for a frame of pure placements, and the
  "frames are per-user" property is undocumented and unenforced. ADR-0006 chose
  a missed copy over a doubled one; a *misattributed* order is worse than both.
- **Reconstruct order state from `webData3` or a fast REST `frontendOpenOrders`
  poll.** Neither carries the lifecycle vocabulary: absolute state can only tell
  you an order is *gone*, never whether it filled or was cancelled — the
  distinction the REST order poller already cannot make (`stream/orders.py`:
  "a known id disappears: pruned silently"). Getting that distinction is the
  entire reason the websocket is the right transport here. And ADR-0006 §Context
  already did the weight arithmetic that kills the REST route.

The cost of the decision is a scarce resource: **10 connections per IP.** The
shared connection (positions for every Trader, plus liveness) keeps one, and one
is held back as reconnect overlap — a replacement connection is opened before
its predecessor's slot is certainly released — leaving `MAX_ORDER_LANES = 8`.
Unique-user allowance is *not* additionally spent: an order lane subscribes an
address the shared connection already tracks, and the same address on a second
connection is free (finding 2).

Eight is far below the 15-Trader position ceiling and enormously below the
tracked set. It is enough, because the seam exists for copy execution and copy
execution is operator-scale (ADR-0005, #127: one operator, a handful of
leaders). It is stated as a constant with a loud refusal above it, so the day it
binds is a log line rather than a silently partial dataset.

### 2. Its own table, its own vocabulary (migration 0031)

Not `position_events`: that table's `kind` speaks open/close/flip/scale and its
CHECK constraints encode position shape. An order lifecycle shares none of it.

`order_events` records **one update to one order**, exactly as the wire
delivered it:

| column | why |
| --- | --- |
| `id` (identity), `trader_address` | identity and subject; the address comes from the connection, never the payload |
| `order_id` | the exchange's `oid` — the thing a cancel names |
| `coin` | venue-namespaced (`xyz:CL`), as everywhere else |
| `is_buy` | `side` "B" is a buy, "A" a sell, decoded once at the parser like `parse_open_orders` |
| `limit_price`, `size`, `original_size` | where it rests, what is left, what it started as (`limitPx`, `sz`, `origSz`) |
| `cloid` | the client order id when the placer set one; opaque, stored verbatim |
| `kind` | `placed` \| `filled` \| `canceled` \| `rejected` \| `triggered` \| `gone` \| `other` |
| `status` | the exchange's raw status string, verbatim; NULL for a resync-derived event, which no status ever described |
| `origin` | `stream` \| `resync` — how Epigone learned of it |
| `placed_at`, `status_at`, `observed_at` | the order's own timestamp, the exchange's `statusTimestamp`, and when Epigone saw it |

**`kind` is a classification of `status`, and `status` is kept anyway.** The
observed statuses already include four that no public list would have led us to
guess (`badAloPxRejected`, `iocCancelRejected`, and the `*Canceled` family), and
new ones can appear at any time. So an unrecognised status maps to `other` and
rides with its raw string intact, on the `OpenOrder.tpsl` precedent: "a trigger
from a family we have never seen renders its raw orderType rather than a guessed
'SL' — self-describing beats silently wrong". A consumer can be conservative
about `other`; it can never be *misled* by it.

**There is no `modified` kind.** Hyperliquid's modify is an atomic
cancel/replace that mints a new oid (verified live, #115), and over this feed it
arrives as precisely that: a `canceled` and an `open`, usually in one frame,
sharing a `statusTimestamp`. Collapsing the pair would require the same
coin/side/size-tolerance matching heuristic `stream/orders.py::_without_modifies`
uses — which exists to suppress *alert noise*, and which ADR-0006 is explicit
must not migrate into the execution seam ("the suppression rules stay where they
are, in the alert layer"). An executor mirroring a leader wants the truth of the
wire: cancel that one, place this one. `status_at` is stored precisely so a
consumer that *wants* to recognise a modify can pair the two by instant, without
this table having guessed on its behalf.

This is the one place the ticket's wording ("place, modify, cancel, fill") is
not met literally: a modify is persisted as its two real halves rather than as a
kind of its own, and nothing is lost by it.

**What the stream cannot tell us is left NULL rather than guessed.**
`WsBasicOrder` carries no `orderType`, `isTrigger`, `triggerPx`, `reduceOnly` or
`isPositionTpsl`, so a streamed placement cannot say whether it is a stop, a
take-profit, or a plain resting limit. The REST resync reads all of it
(`frontendOpenOrders` → `OpenOrder`), so resync-derived events carry those
columns and streamed ones carry NULL — the `size_coin` convention of ADR-0006,
where NULL means "not observed", never a default. This is a real hole in what
the transport can support and it belongs in the record: an executor mirroring a
streamed placement knows the price and size but not the order's *kind*, and must
treat that as not-mirrorable rather than assume a plain limit.

### 3. Reconnect resync, and the honest name for a gap

Same discipline as the position lane, for the same reason: a websocket delivers
from the moment you subscribe, so a reconnect that resumed streaming would lose
the gap silently. Every order-lane connection therefore begins with a
point-in-time `frontendOpenOrders` read across `POSITION_VENUES`, diffed against
the lane's own memory (`ws_order_state`), and only then subscribes.

The diff, and what each case emits:

- **First observation of a Trader emits nothing.** A ladder that predates the
  first look is not news — the baseline rule both existing lanes obey.
- **Resting now, unknown before** → `placed`, `origin = resync`. It appeared
  during the gap; the moment is unknown, which is what `origin` records.
- **Known before, absent now** → `gone`, `origin = resync`. A resting order that
  left the book while nobody was watching is genuinely ambiguous — filled or
  cancelled, and absolute state cannot say which. `gone` is that ambiguity named
  rather than a coin flip between `filled` and `canceled`, and rather than the
  silence the acceptance criterion forbids.
- **Known, still resting, smaller** → `filled`, `origin = resync`, carrying the
  new remaining size. A resting order's remaining size can only shrink by
  filling: a modify would have minted a new oid. This one inference is safe, so
  it is made.
- **Known, still resting, unchanged** → nothing.
- **Known, still resting, LARGER** → nothing emitted; memory takes the new size
  silently. This should be unreachable — the only thing that changes a resting
  order's size is a fill, which shrinks it, and a modify mints a new oid rather
  than growing an old one. It is written down because `order_diff` has to do
  *something* if the exchange ever does it, and the choice matters: emitting the
  `filled` that the "smaller" branch would produce would report a fill that did
  not happen, into a seam meant for copy execution. Recording what was observed
  and saying nothing about it is the conservative half of that pair.

The subscription's own opening behaviour then diffs to nothing, because the
resync already recorded the gap — neither lost nor doubled, exactly as #157's
position resync works.

**The one window resync cannot close, and how it heals.** Resync-then-subscribe
covers everything before the REST read and everything after the subscription
takes effect. It does not cover what happens *between* them. On the position
lane that gap is self-healing and nobody has to think about it: every
`allDexsClearinghouseState` push carries absolute state, so the next push — ~5s
away, even for an idle account — re-states the truth and the diff corrects
itself. `orderUpdates` carries transitions. A cancel that lands in the window is
mentioned once, to nobody, and never again; `ws_order_state` then believes in a
resting order that is not there, indefinitely, while `allMids` keeps the socket
looking healthy and no `gone` is ever emitted. That is the same hazard class
§4 disconnects the rate ceiling over — frozen memory behind a live socket —
reached from a third direction, and it must not be left undeclared.

The fix is to bound how long any connection is trusted:
**`ORDER_CONNECTION_MAX_SECONDS = 15 minutes`**, after which a perfectly healthy
connection is retired and replaced. The replacement resyncs before it
subscribes, so whatever the window swallowed surfaces then as `placed` /
`filled` / `gone` with `origin = resync`, exactly like any other gap.

Recycling was chosen over the two alternatives because it reuses the ordering
argument already proven rather than adding a second one:

- **A verify-resync taken after subscribing** would be a point-in-time REST read
  applied on a live socket, i.e. precisely the "staler answer lands after a
  newer push" race that made resync-before-subscribe the rule. Making it safe
  needs the streamed frames buffered and reconciled against the read, which is
  new machinery under the most correctness-critical part of the lane.
- **A positive subscription acknowledgement** (`subscriptionResponse`, which the
  probe confirms is real) would catch a refusal but not this: the subscription
  here succeeds, and the change simply happened before it did.

What this buys is a *bound*, not an elimination — staleness of at most 15
minutes on a window that is itself sub-second and will usually catch nothing. It
costs 0.53 new connections/min across all 8 lanes (of 30 per IP) and two paced
REST calls per lane per period.

**A refused subscription ends the connection.** The other way a lane can look
healthy while recording nothing is being told no. A server `error` frame on an
order-lane connection is unambiguous in a way it is not on the shared position
connection: this connection serves one Trader and sends exactly two
subscriptions and its own pings, so the error is about one of them. If it is the
order feed — the concrete case being the per-IP allowance of 15 unique users
already full when this lane asks, e.g. a 16th address racing the position lane's
refresh — the lane would go on receiving `allMids`, stay liveness-healthy
forever, record nothing and emit no `gone`. So the frame ends the connection.
It earns `SUBSCRIPTION_COOLDOWN_SECONDS = 5 minutes` rather than the ordinary
reconnect floor, because a refusal is usually structural: re-asking every minute
would spend 8 connections a minute on an answer that only changes when a user
slot frees.

**The stream is reduced to covered venues before it is diffed.** `orderUpdates`
is account-wide and carries every dex; the REST resync that anchors it sees
`POSITION_VENUES` only. Diffing the wider observation against the narrower
anchor would emit a `placed` for every uncovered-venue order and then a `gone`
for it on the next reconnect, forever. Spot orders are dropped by the same
convention `parse_open_orders` uses (`@N`-indexed and `BASE/QUOTE`-named coins) —
Epigone is perp-only.

### 4. Volume: a rate ceiling, and 24h retention

ADR-0006 could size `position_events` on "events are rare". This table cannot.
A single active address was measured at 1471 order updates/minute — over 2
million a day, from one Trader.

Two bounds, and they are different in kind:

- **`ORDER_UPDATE_RATE_LIMIT = 120 updates/minute per Trader`,** measured on a
  rolling minute. This is not a performance knob, it is the **Bot rule at the
  seam**: CONTEXT.md defines a Bot as an account whose profile indicates
  automated market-making rather than copyable skill, and excludes it from the
  Universe. Two order updates per second sustained is that account. A Trader
  whose plan changes 24 times a second does not have a plan to mirror, and
  filling the seam with their requotes would bury the leaders it exists to serve.
  It is not free of false positives, and that cost is declared as deviation 5
  below rather than buried here.
- **`ORDER_EVENT_RETENTION = 24 hours`,** pruned on write, the same
  `record_rate_limit` precedent ADR-0006 followed. Position events keep 7 days
  because a copy executor that was down for a day still wants them; a resting
  order from yesterday has either filled or been cancelled, and mirroring it
  would place an order against a book that has moved on. The staleness argument
  is strictly stronger here, so the window is strictly shorter.

**Breaching the ceiling ends the connection**; it does not mute the Trader on a
live one. That distinction was got wrong first and is worth recording, because
the wrong version looks harmless: a lane that simply stopped recording would
hold `ws_order_state` frozen at whatever was resting when the ceiling hit while
the exchange moved on, and — since `allMids` keeps the socket alive
indefinitely — nothing would ever force the resync that repairs it. The lane
would sit on one of ten connections believing a stale book, and the divergence
would eventually surface as a burst of phantom `gone`s. That is precisely the
silent gap §3 exists to prevent, arrived at from the other direction. So a
refusal disconnects, and the next connection re-establishes absolute state.

`REFUSED_COOLDOWN_SECONDS = 15 minutes` is what keeps the refusal meaningful:
reconnecting on the ordinary backoff would let a maker write a full ceiling's
worth every cycle forever. Worst case with all three bounds is 8 lanes ×
120 events per 15 minutes × 24h ≈ **92k rows**, against the 2.1M/day/Trader the
measured maker would have produced. A real leader produces a few hundred a day.

One further allowance the first draft of this ADR missed, recorded because it
binds the same way the others do: **30 new connections per minute, per IP**, also
shared with the position lane. Eight order lanes reconnecting on the position
lane's 2-second floor would spend ~240/min between them — eight times the whole
cap. Order lanes therefore carry their own `ORDER_RECONNECT_MIN_SECONDS` of a
minute (8/min between them, about a quarter of the allowance), and take the
smaller share deliberately: position events feed Position Alerts and the cutover
comparison, while these rows are read by nobody.

### 5. Nothing consumes it, and nothing can consume it by accident

The shadow discipline of #157 is inherited whole: the rows are written, nothing
reads them, Position Alerts and REST polling behave exactly as before, and the
order lanes run inside the same separate process (ADR-0002) that cannot reach
alerting.

#168 raises a specific hazard: `outstanding_events` / `claim_event` in
`position_events.py` do not filter on `source`, so a seam that shared their
tables would be silently picked up by the position-event consumer path. **A
separate table closes that structurally rather than by convention** — no query
in `position_events.py` names `order_events`, and none can be made to by
accident. Progress for future order consumers is tracked in its own
`order_event_claims`, following ADR-0006's pattern for ADR-0006's reason (claims
rather than a cursor, because identity values publish at COMMIT and a second
producer can therefore publish out of id order).

`outstanding_order_events` / `claim_order_event` ship with the table and with
no caller, exactly as `position_events`' pair did — the seam is what is being
delivered, and a seam with a write side and no read side is half a seam.

### 6. What this forces on the position lane

Two changes, both corrections rather than features:

- **`MAX_SUBSCRIBED_TRADERS` drops from 499 to 15**, the measured per-IP unique-user
  allowance. The lane's docstring already promised it "refuses to subscribe past
  the cap (loudly, rather than letting the server start rejecting)" — against
  the real number it has been doing the opposite. This makes the promise true.
  It also means a tracked set above 15 is now a visible, logged refusal, which is
  the point: #157's dataset has been silently partial above 15 wallets.
- **The shared connection stops subscribing `orderUpdates`.** Those frames are
  unattributable there by finding 1, so counting them measured nothing per
  Trader, and they are a large fraction of the lane's inbound traffic. A Trader
  now costs the shared connection one subscription, not two.

## Deviations from #168

What the ticket asked for and this design does not do, each deliberate and each
argued above. Numbered so a review can name one.

1. **No `modified` kind.** A modify is persisted as its two real halves, a
   `canceled` and a `placed`, because that is what Hyperliquid does (§2).
2. **"No change to the subscription budget" does not hold.** Subscriptions per
   Trader actually *dropped* (2 → 1 on the shared connection), but the lane now
   spends a whole **connection** per Trader. Forced by the attribution finding
   (§1).
3. **An out-of-scope correction to #157's position lane** — `MAX_SUBSCRIBED_TRADERS`
   499 → 15, and `orderUpdates` off the shared connection — against the ticket's
   "the position lane behaves exactly as before" (§6). *Signed off by the
   operator, 2026-08-04, on the probe evidence.*
4. **Order coverage (8 Traders) is below position coverage (15).** Both are the
   transport's limits rather than Epigone's; raising either needs another IP
   (§1).
5. **The rate ceiling applies the Bot rule per minute, and will sometimes catch
   a human.** CONTEXT.md defines a Bot by *statistical profile* and excludes it
   once, at Universe vetting. `ORDER_UPDATE_RATE_LIMIT` applies the same word to
   *one minute of behaviour*, at the seam, to an account vetting already let
   through — and those two judgements do not always agree. A tracked human
   Leader who mass-cancels a ladder of more than 120 orders in a minute (a real
   thing a real leader does on changing their mind about a market) is refused
   mid-burst, blacked out for 15 minutes, and the transitions inside that
   blackout are never recorded: the post-cooldown resync collapses them into
   `placed` / `gone`.

   It is kept, for the reason §4 gives — the measured alternative is 2.1M rows a
   day from a single address, which would bury exactly the leaders this seam
   exists for — but with three properties that make it survivable rather than
   silent:

   - **The refusal is loud.** `OrderLaneStats.refused` marks the connection, its
     closing line logs at WARNING naming the Trader and the blackout length, and
     the lane logs at ERROR when it stands down. An operator can tell a refused
     Leader from a refused market maker by reading the log, which is the whole
     point of not swallowing it.
   - **The post-cooldown state is honest, not merely present.** The resync that
     follows re-establishes absolute state, so orders that really did leave are
     reported `gone` exactly once and orders still resting are left alone —
     no phantom `gone`, no phantom `placed`, and every inferred row carrying
     `origin = resync` so a consumer can see it was inferred. What is lost is
     resolution (which orders were cancelled, and when), never accuracy.
   - **It is a verdict on a minute, not on an account.** The ceiling is a rolling
     minute and the cooldown ends; the next connection re-judges from scratch.
     Nothing about the Trader is remembered or marked.

   The lasting fix is not a bigger number here — it is that the seam has no
   consumer yet, and the consumer that eventually reads these rows (#158, copy
   execution) is what can say whether a Leader's mass-cancel burst needed to be
   recorded transition by transition, or whether the resync's summary was always
   enough. That decision is theirs to take, on this dataset.

## Consequences

- The order seam exists, with a vocabulary that distinguishes a fill from a
  cancel — which no REST-shaped design could have provided at any cadence.
- Order coverage is bounded at 8 Traders where position coverage is bounded at
  15, and both bounds are the transport's, not Epigone's. Raising either means
  more IPs, which is a real option and a real decision, and is out of scope here.
- The record on websocket capacity is now measured rather than inferred. ADR-0006's
  499 figure is retired; anyone sizing a future lane starts from 15 per IP.
- A streamed placement's order *type* is unknown (NULL). Whoever builds order
  mirroring must decide whether to treat a type-unknown placement as
  not-mirrorable or to pay a REST read to resolve it. This ADR does not decide
  that, because the consumer does not exist yet — but it records that the choice
  is coming and why it exists.
- `gone` is a kind consumers must handle. It is the honest cost of reconnecting
  to a feed that only reports transitions: some transitions happen while nobody
  is listening.
- **Every order lane reconnects on a 15-minute cycle even when nothing is
  wrong**, which is how the resync→subscribe window heals. Reconnect log lines
  are therefore routine rather than a signal, and a reader of #158's dataset
  should expect a small, regular pulse of `origin = resync` rows that is the
  lane working rather than the transport failing.
