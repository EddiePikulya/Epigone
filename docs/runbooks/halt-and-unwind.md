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
accounts across four venues is 44 enumerations, 880 weight per pass, and the
sweep does up to two passes (cancel, then a fresh verify) for ~1760 weight
all told plus the scope and asset-id reads. Two to ten minutes of wall
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
  from inside the same loop rather than being allowed to fire mid-halt;
- and, if configured, the external dead-man ping goes out from the same
  enumeration steps (issue #213), so a grinding sweep does not read as a
  dead process from outside the host either.

So: heartbeat fresh + progress lines advancing = working, wait. Heartbeat
fresh + progress lines stopped = stuck on one REST call or on budget
pacing. Heartbeat stale = the process itself is in trouble; that is the
monitor's watchdog check, and it means what it says again.

The monitor now looks at watchdog liveness on ITS OWN TIMER (issue #205),
every 60s (`HEALTHCHECK_WATCHDOG_CHECK_SECONDS`) rather than only on the
15-minute check cycle (`HEALTHCHECK_INTERVAL_MINUTES`); the expensive checks
keep the slow cycle. So the worst case from a watchdog freezing to the
operator holding a 🚨 DM is one staleness threshold plus one liveness
cadence — 360s at the defaults — instead of the threshold plus a quarter of
an hour. That is what the 2026-08-07 shape needed, where the watchdog froze
at 18:20:15, the 18:24 pass saw four minutes of staleness (under threshold),
the next look would have been 18:39, and the operator found the freeze with
an ad-hoc DB query instead.

Read that number honestly: 360s is longer than the dead-man's 300s horizon,
so a watchdog that dies with an armed schedule can still have that schedule
fire before the page lands. What has changed is that the page now arrives in
the same minutes as the incident rather than after it, and that the gap is
made of the STALENESS THRESHOLD, which is a knob. If you want the page ahead
of the fire, lower `HEALTHCHECK_WATCHDOG_STALE_SECONDS` — the watchdog beats
every cycle (~10s) and every ~5s inside a sweep, so 300s is thirty missed
beats and there is a lot of room; the reason it is not lower by default is
that it must also survive a deploy restart and a cold start's grace window
without paging.

**That 360s no longer silently becomes infinity (issue #213).** The
monitor's pool used to be opened with no `command_timeout` and no
acquire bound — unlike the watchdog's — so a black-holed database host did not
make the monitor report DB-down, it made the monitor loop *hang*: no fast
tick, no slow cycle, no page, silently, for as long as kernel TCP
retransmission took to give up. That was never exotic. A dead database host is
among the likeliest reasons the watchdog is dead in the first place, so the
blind spot was CORRELATED with the thing being watched.

The monitor's runtime pool now carries **three** of the four bounds the
watchdog's has — connect, per-query, and the acquire/release budget
(`epigone.db.create_pool`, at 10s for this lane). Not the fourth: the
watchdog's server-side `lock_timeout` guards `SELECT … FOR UPDATE` behind a
dead holder, the monitor takes no row locks, and its realistic lock wait is a
migration's ACCESS EXCLUSIVE during a deploy — already bounded client-side,
and a server-side error there would be a *false* 🚨 on every schema-changing
deploy. Each gather additionally runs under a hard 30s real-time ceiling;
that one is defence in depth rather than a hole the pool provably cannot
reach (both gathers are a bare `fetchrow` with no transaction, so the pool
bounds should suffice on their own — it is kept because a ceiling that never
fires costs nothing and this axis keeps growing new layers). Migrations still
run on a plain unbounded pool that closes before the bounded one takes over —
a migration legitimately runs long — the same startup shape the watchdog uses.

**Read the latencies honestly, because they did not all improve.**

- The **watchdog-stale** page is unchanged at **360s** worst case. It can only
  fire when Postgres is answering, and when Postgres answers nothing here
  spends a ceiling. The bounds bought no speed on this path — they bought the
  path *existing* on the other one.
- The **DB-down** page belongs to the SLOW cycle, deliberately (the fast tick
  refuses to page from a snapshot that knows one table). So a black-holed
  database is reported within one `HEALTHCHECK_INTERVAL_MINUTES` — **up to
  ~15 minutes**, plus a ceiling — where before it was reported *never*. If
  fifteen minutes is too long for the mainnet posture, that interval is the
  knob, and lowering it is what raises the cost of the expensive checks.
- Every touch against a black-holed host spends its bound, so ticks during
  such an outage come roughly every 90s rather than 60s. Nothing pages off
  those ticks while the database is down, so this costs latency on the
  *recovery* signal, not on an alarm.

What it does NOT fix, and cannot: the monitor still reaches you through its
own process, its own Postgres reads, and its own Telegram session, all on this
host. A host that takes the database and the monitor out together still pages
nobody *from here*. That is what the next section is for.

**So keep the old rule, in its stronger form: do not treat "the monitor has
not paged" as "the watchdog is alive."** What #205 bought is narrower and
worth having — *if* the monitor is running and Postgres is answering, silence
now means the watchdog beat within the last minute or so, where before it
meant nothing for up to a quarter of an hour. What it does not buy: evidence
that the dead-man's schedule has not fired, or evidence of anything at all
when the database is unreachable. During an incident you are already watching,
read the `process_heartbeats` row and the sweep's progress lines directly;
they are the primary signal, and an ad-hoc query against the database is still
the check that does not go through the monitor.

### The out-of-band page path (issue #213)

Every alarm above runs through Epigone. The watchdog writes a heartbeat to
Postgres, the monitor reads it from the same host and DMs you from a container
beside it — so the single most likely cause of a genuinely dead watchdog is
also the thing that stops anyone being told. A checker inside the failure
domain cannot report the domain failing, however well bounded it is.

So the watchdog also pings **outward**, on the inverted logic a dead man's
switch needs. You create a check on an external dead-man service
(healthchecks.io and friends), set its ping URL in
`WATCHDOG_EXTERNAL_PING_URL`, and the watchdog says "still here" every 60s from
the same pulse that beats its heartbeat — at every cycle top and at every
enumeration step of a sweep, so a multi-minute grind keeps pinging. **When the
pings stop, the external service pages you from its own infrastructure.**
Nothing of ours has to be alive for that page to happen.

**What it covers.** The watchdog process ceasing to run cycles, for any reason
it cannot survive: a crash loop, an OOM kill, a container stopped and not
restarted, a kernel panic, the host powered off, the host's network gone, the
Hetzner box gone. Also a watchdog *wedged* — stuck inside a single await
between two pulses — because a wedged process stops pinging just as a dead one
does. This is the only alarm Epigone has that survives losing the host.

**What it does NOT cover.** Read this list before treating a green check as
reassurance:

- **A watchdog that is alive but impotent.** A revoked or expired agent key,
  a pool of orders on a network it cannot reach — none of that stops the
  pings. That is the monitor's on-chain capability check, and it is still
  inside the failure domain.
- **Postgres being down.** The watchdog deliberately KEEPS pinging while
  DB-blind: it is alive, it is cancelling, and it is telling the truth about
  itself. So external silence never means "the database is down", and a
  database outage produces **no external page at all** — DB-down remains the
  monitor's job, bounded now but still in-domain. This is a deliberate
  trade: the alternative (stop pinging when blind) would page "watchdog
  dead" for a watchdog that is working, which is the wrong sentence at 3am.
- **Anything but the watchdog.** One check, one process. The executor, the
  bot, ingest and the websocket lane are not in it — **and neither is the
  monitor**. A monitor that crash-loops, wedges or is stopped by hand on an
  otherwise healthy host is silent, and takes the DB-down page and every other
  check with it; the only present signal is the daily digest not arriving,
  which is a once-a-day negative someone has to notice. **Issue #220** tracks
  that residual (the cheap fix is a second dead-man check for the monitor
  itself, reusing this same code) and flags for the operator whether it should
  also be a mainnet gate.
- **A monitor that never finishes starting.** Its migration run is bounded
  (5 minutes) so a host black-holing mid-startup crash-loops the container
  rather than hanging it silently before the first cycle. A crash loop is
  *visible* in `docker compose ps` and the logs — it is not a page. Same
  residual, same issue.
- **The external service itself.** It is a third-party dependency with its
  own uptime and its own delivery path (email, SMS, push). If *it* is down,
  most such services do not page — silence there is ambiguous, not safe.
  Point its notifications at something that is not this host and not the
  Epigone bot.
- **False pages, in the safe direction.** Our egress failing — an outbound
  firewall rule, DNS, the service rate-limiting us — reads exactly like a
  dead watchdog. That is the direction a dead-man's switch is supposed to
  fail, and it is the reason the ping interval (60s) is a small fraction of
  the grace period you set on the check.

**And it is not faster than the horizon either.** The page arrives no sooner
than the grace period you configure, so at a 5-minute grace it lands 5–6
minutes after the watchdog stops — longer than the dead-man's 300s horizon,
exactly like the monitor's 390s. Both alarms tell you what happened; neither
outruns the exchange-side net, and that net firing is the fail-safe outcome
anyway.

**Operator setup — do this before the mainnet switch; it is a gate.**

1. Create a check on the dead-man service. Period **1 minute**, grace **5
   minutes** (a handful of dropped pings must be absorbed; five minutes of
   silence is thirty missed pings).
2. Point its notifications somewhere that does not depend on this host or on
   the Epigone bot — a personal email and an SMS/push channel.
3. Put the ping URL in the server's `.env` as `WATCHDOG_EXTERNAL_PING_URL`.
   **Treat it as a secret:** on healthchecks.io and services like it the path
   IS the credential, and anyone holding it can forge this watchdog's
   liveness. Epigone logs the host and never the path — including on the
   failure line, which reports the exception's type rather than its text
   precisely because several aiohttp errors quote the whole URL. Use `https`;
   a plain-`http` URL is accepted (a self-hosted checker is legitimate) but
   warned about at startup, because it puts the credential on the wire.
4. `docker compose --profile execution up -d watchdog` and confirm the first
   log line reads `out-of-band ping ARMED`. If it reads `NO
   out-of-band page path`, the variable did not reach the container.
5. Confirm the check went green on the service's own dashboard, then stop the
   watchdog for six minutes and confirm you are actually paged. An untested
   page path is not a page path.

**A malformed URL refuses to start the watchdog under
`EXECUTOR_ALLOW_MAINNET`.** It is the one knob in the watchdog's config that
does not degrade to a safe default, and the exception is deliberate: degrading
answers a fat-fingered secret with a live mainnet watchdog that nothing
outside this host is watching — the exact state this gate exists to prevent —
behind a log line nobody reads twice. The container crash-loops instead, and
the fix is one line of `.env`. On testnet it warns and carries on.

**Unset is a supported configuration, and the watchdog says so at 🚨 volume in
its first log line.** Note that unset is deliberately *not* treated as
malformed: an operator who has not armed the path may have decided not to, and
that is a policy call rather than a parser's. **If you want mainnet to refuse
to boot with no ping URL at all, say so and it is a two-line change** — it was
left this way to be ruled on rather than assumed. Everything else runs
identically without it: this leg cannot slow the kill path even in principle,
because `ping()` is a synchronous call with no `await` for a stall to be
inherited through (the request runs as its own task under a 5s aiohttp timeout
and a 10s outer ceiling, and those bounds protect the ping's own health, not
the caller's). But "unset" means the only thing that would notice this process
dying is the monitor, which reads the same database on the same host. That is
the state this issue exists to end.

**The #188 floating-IP secondary is not this and does not replace it.** It
stays parked.

A DB-BLIND window is deliberately silent on all of these: that path reaches
the wire with zero Postgres behind it by construction, so it neither beats
nor pushes the dead-man until it reconciles. A blind watchdog looks dead to
the monitor because it cannot reach the database to say otherwise — which
is the correct reading, and the reason the DB-blind alert exists. A trip
whose halt row could not be confirmed is the other kind of incident and
DOES beat through its cancel pass: its liveness reads answered that cycle,
so the database is healthy.

**The one signal a DB-blind window does NOT silence is the external ping**
(issue #213, the section above): it goes to something that is not the
database, so it keeps going out through the whole blind window. That is
deliberate — the watchdog is alive and cancelling, and saying so is true —
and it is why external silence never means "the database is down".

In every posture, each of the two IN-BAND legs of the pulse — the heartbeat
and the dead-man's push — runs on its own small TIME budget: a leg whose attempts add up past it
goes quiet, because reaching the wire outranks saying so, and gets the budget
back every 45 seconds of sweep (and at every cycle top), so going quiet is
under a minute and never the rest of a grind. The budget is spent on
measured wall clock, not on how an attempt failed, so a leg that is merely
SLOW (a wedged database, an endpoint that 502s after twenty seconds) stops
taxing the sweep, while a leg that fails FAST keeps trying — a refused
connection or a single 429 costs the sweep nothing and must not silence a
signal for the whole grind. The two legs never gate each other: a dead
database still lets the schedule be pushed, a wedged exchange still lets the
heartbeat beat.

So `sweep progress` lines advancing with a heartbeat that has stopped means
the heartbeat's own leg is down, not that the watchdog died. Two readings fit
that, and the log line says which: if the last `sweep pulse: the heartbeat
leg has spent …` line is recent, the leg is being SLOW right now — a wedged
or failing database. If there is no such line, or the last one is a minute or
more old and the beats have not resumed, the beat is not slow but failing
outright and the log will carry the `heartbeat write failed` warnings to
match. A leg that was slow earlier in the cycle and is healthy again resumes
on its own at the next refill; that shape looks like a gap in the heartbeat
followed by beats, and needs no action.

Nearly every await between two pulses is now bounded (issue #204):

- every READ the watchdog makes is ceilinged at 45s;
- the exchange's `Retry-After` is capped at 30s wherever we sleep on it, and
  the whole 429 retry loop is bounded at 35s of wall clock in BOTH gateway
  directions — which is what bounds a CANCEL POST at ~65s (that budget plus
  one 30s request timeout). A cancel is bounded from inside rather than
  cancelled from outside on purpose: cancelling a write mid-flight would
  leave the audit trail's attempt row with no outcome, on the one path whose
  evidence matters most;
- the pulse-leg refill above is the third.

**The dead-man residual, and what the sweep now does about it (issue #212).**
The push falls due asynchronously to the sweep, so before the fix the
arithmetic was: the step already in flight when it falls due (up to 65s) + a
wedged push cut at its 30s ceiling + the next saturated step (another 65s)
before the leg's budget refills and it can try again — ~190s against 150s of
slack under a saturated exchange. The sweep now watches the deadline itself:
when the armed schedule is within 95s of its horizon (one step bound plus one
push ceiling), a push attempt jumps the leg budget and the pulse throttle
both, so a step about to start is never the reason the last attempt was
skipped — as long as that step is the longest thing between two of the sweep's
pulses, which the un-ceilinged pacing sleep below is not. In the log that
reads

```
dead-man's schedule is 55s from its horizon and the next sweep step could
outlive it — a budget-exempt push attempt now (issue #212)
```

which during a long sweep is **expected and self-healing**, not a fault: it
means the grind walked a horizon down and the schedule was re-armed in front
of it. Attempts are paced at 5s and the window is 95s wide, so **one or two
per horizon is the healthy shape and ~19 in a row is the ceiling** — a burst
approaching that means every push is failing, and the
`sweep keepalive (dead-man's push) failed` / `hit its ceiling` lines beside it
say which way. If you also see
`dead-man's switch: retrying at once rather than in 6h`, the exchange rejected
a `scheduleCancel` outright while a schedule was still standing: the switch is
deliberately ignoring its 6-hourly re-probe cadence until that schedule either
gets re-armed or runs out, and the reject's own message is on the audit trail.

**What is still NOT closed, so a fire is still possible.** If every attempt
inside that 95s window wedges for its full 30s ceiling — up to four of them —
the schedule still lapses; that needs an exchange that cannot accept a
`scheduleCancel` at all across the window, rather than a push that was merely
deprioritised. That is true of the cancel pass as well as the enumeration:
attempts continue until the window is dealt with, so committing to a cancel
POST (which nothing can cut once sent) never costs the schedule its remaining
attempts. The pre-sign budget's pacing sleep is also deliberately
un-ceilinged, so a token-deficit wait longer than the window outlasts
everything above — that is the one gap in "a step is never the reason an
attempt was skipped", and "every await is bounded" is still not literally
true.
And a DB-blind sweep does not push at all, by design (it runs with zero
Postgres behind it) — under one of those the schedule lapses within a horizon,
which is the same cancel-all the blind sweep is already performing.

**So: a `scheduleCancel` that fires mid-halt is expected-rare, not a second
incident.** It discharges in the fail-safe direction — it cancels every
resting order on the exchange, during a halt whose whole job was cancelling
resting orders, so the intended outcome arrives by the belt-and-braces route.
The cost is one of the account's **10 daily triggers** and a confusing line in
the trail. If you see one during a sweep that was otherwise progressing: note
it, check the trigger budget, and keep reading the sweep's own progress lines.
Do not go looking for a separate fault.

A read cut at its ceiling shows up as a sweep that did not finish: the halt
stays unswept, the watchdog logs the failure, and the next cycle
re-enumerates. Repeated cuts on the same venue mean that endpoint is
saturated or down, not that the watchdog is broken. A cancel that runs out
its 429 budget surfaces as the usual rate-limited streak, which the sweep
already treats as "not swept, retry next cycle".

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
(`HEALTHCHECK_WATCHDOG_STALE_SECONDS`, default 300, re-evaluated every
`HEALTHCHECK_WATCHDOG_CHECK_SECONDS`, default 60) and CAPABILITY — every
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
