# Runbook: operator copy execution (issues #136 and #137, ADR-0007)

The A4/A5 product loop: a Leader the operator explicitly enabled opens, scales
or closes a position, and Epigone mirrors it into that Leader's own funded
sub-account. **ADR-0007 is the document that decides everything here** —
including amendments D-4 and D-5, which are what A5 changed; this page is what
to type, what you will see, and what to do when something pages.

**Live gate: TESTNET BY DEFAULT, and going live is a manual act in three
parts.** `EXECUTOR_ALLOW_MAINNET=1` passes the capability the gateway demands,
`EXECUTOR_EXCHANGE_URL` must point at mainnet, and the master must hold real
money. Either switch alone changes nothing: the URL without the flag is
refused at gateway construction, and the flag without the URL logs a warning
and keeps trading testnet. The same variable opens the WATCHDOG's gateway —
one flag for the pair, because a live executor whose dead-man's switch cannot
reach the live book is the one configuration nobody should be able to type.
The executor's start-up audit row records which network it came up on.

## Which network answers which question

**One network for the book, one read for the Leader** (ADR-0007 amendment D-6,
issue #184). The doctrine that everything comes from the book you trade holds
for everything the ORDERS depend on, and has exactly one documented exception.

| what is read | from | configured by |
| --- | --- | --- |
| mark prices, market stats (Liquidity Floor), asset specs, sub-account state, fills | the network you TRADE | derived from `EXECUTOR_EXCHANGE_URL` — not separately settable, by design |
| the Leader's live equity, for the liveness floor | the network the SIGNAL came from | `EXECUTOR_SIGNAL_INFO_URL`, default **mainnet** |

Orders and order-adjacent reads are single-network by construction: a size
priced on one network and sent to another must stay impossible to type, so
nothing on that side is configurable. The Leader's account is a different kind
of thing — it belongs to the network Epigone TRACKED them on — and the liveness
gate asks about that account.

**On a normal mainnet deployment this is invisible**: both urls are mainnet and
behavior is bit-for-bit what it always was. It matters on the testnet
shakedown, which mirrors real mainnet Leaders onto the testnet book: without
it, every entry skipped with `$0 < $10,000` and no episode could open. Set
`EXECUTOR_SIGNAL_INFO_URL` only if you are tracking Leaders somewhere other
than mainnet; a value that is not `/info`-shaped is refused, logged, and falls
back to the mainnet default. The gateway behind it is read-only and holds no
key — it cannot place anything anywhere.

## The one fact this page turns on

**Tracking is not copying.** Tracking a wallet tells you what it does. Copying
it spends money. The only path from one to the other is an explicit `/copy`,
and the default for every tracked wallet is off.

## Before the first copy

1. **An executor-lane agent key.** `python -m epigone.keystore generate --lane
   executor …`, then the approval ceremony
   (`docs/runbooks/agent-key-ceremony.md`). The execute process refuses to
   start without one — a beating executor that cannot place would read as
   healthy to the watchdog.
2. **The watchdog running.** The executor beats the heartbeat the dead-man's
   switch reads. Start `--profile execution` so both come up together; an
   executor heartbeat with no watchdog is itself a 🚨 health check.
3. **Volume on the master.** `createSubAccount` sits behind a $100k
   cumulative-volume gate. The testnet master is past it; a mainnet master is
   a mainnet-switch checklist item (#138).

## Setting up a copy

```
/copy <leader> <allocation> <stake> <leverage> <mode> [tp% sl%]
/copy 0xabc… 1000 100 mirror default
/copy 0xabc… 1000 100 5 bracket 10 5
```

- **allocation** — dollars transferred into a dedicated sub-account for this
  Leader. This IS the exposure cap, margin-enforced by the exchange, not by
  Epigone bookkeeping. A scale the margin cannot absorb simply rejects.
- **stake** — YOUR margin behind each copied OPEN, isolated per position. The
  POSITION is stake × leverage, so $100 behind a 10x Leader is a $1,000
  position — and because the margin is isolated, that $100 is the most that
  position can lose. The Leader's own size never changes it; only their
  leverage and their relative moves are mirrored (a 50% scale-in scales the
  copy 50%, a 30% trim sells 30% — of what we actually hold).
  **This argument changed meaning in A5.** It used to be Base Notional: the
  size of the position itself. A pre-A5 habit of typing `200` now means $200
  of margin, which at a 10x Leader is a $2,000 position.
  **Mappings that already existed were pinned to `fixed 1x` by migration
  0036**, precisely so the change of meaning could not move them: at 1x the
  stored number is still the position, to the cent. If you want one of them
  mirroring, re-run `/copy` for that Leader with the leverage you want — the
  upgrade will not do it for you.
- **leverage** — `mirror` (the Leader's own leverage on that position) or a
  whole number for fixed. Either is an ASK: the final leverage is the lowest
  of it, `/limits max_leverage`, and the asset's own maximum. A `mirror` open
  whose event carries no Leader leverage is skipped rather than guessed.
- **mode** — `default` exits when the Leader exits. `bracket` wraps each
  position in our own TP/SL at OUR fill price. **Read the episode rule below
  before choosing `bracket`.**

`/copy` confirms before acting, because it moves money. What the tap writes is
a ROW, not an exchange call: the bot process holds no signer (ADR-0005), so
the execute process creates and funds the sub on its next loop and reports
back in the chat. Until that message arrives, nothing is being copied.

`/copies` lists the mappings. `/uncopy <leader>` stops one.

**`/uncopy` never flattens.** It stops consuming events and tells you what is
still open in that sub. Closing those is your decision, from the master
wallet — the same never-auto-fix rule reconciliation obeys.

**Re-copying reuses the sub-account.** Sub-accounts cannot be deleted and a
master holds at most **10** of them (probed 2026-08-04; the 11th is refused
`"Too many sub-accounts."`), so a second `/copy` for the same Leader
re-enables the existing mapping rather than burning another slot.

**At the cap, `/copy` ADOPTS a sub instead of minting one.** When all ten
slots are spent, the executor takes over an existing sub of the master that is
**not mapped to any Leader** (a disabled mapping still owns its sub) and
**holds no open position**, renames it for its Leader, and funds it to the
allocation from whatever it was already holding. Nothing about copying changes
afterwards — it is an ordinary Copy Sub-account from that point. You will know
it happened because a notice says **ADOPTED** the moment it does — before the
funding leg, so you hear it even if funding has to retry — and again in the
ready message; the audit trail carries `copy_sub_adopted`.
Check that sub's balance if the figure matters to you: it inherited whatever
was in it, and the top-up only moves the difference.

**When nothing is adoptable, `/copy` fails loudly and does nothing.** All ten
subs mapped or holding positions means the notice says the copy **was NOT set
up**, the mapping is disabled, and no money moved. Note what does NOT free a
slot: `/uncopy` does not — a disabled mapping still owns its sub, because that
is what makes re-copying reuse it — and neither does deleting anything on the
exchange, since sub-accounts are permanent. A master whose ten subs are all
mapped is genuinely full at ten Leaders (decision 1's ceiling, arrived at).
The ways out are to retire a Leader's mapping for good (a database change
today; there is deliberately no command that throws a sub away), to flatten
and un-map a sub held by something outside Epigone, or to run this operator
from another master.

## What you will see in the chat

Everything. Events are rare and you are one person acting manually on what you
see, so the executor reports:

- **every copied action** — sub, coin, side, requested vs filled;
- **every skip, with its reason** — stale entry, leader below the liveness
  floor, coin occupied, risk-declined, no local position, not mirrorable;
- **pager cases**, which also ride the 🚨 health-monitor path.

**Except on a drain.** When one cycle disposes of more than five leader events
without acting on any of them — the first `/copy` of a Leader you have tracked
for weeks, the cycle after a `/resume`, the first cycle back from downtime —
you get ONE summary instead: how many died, why, per Leader. Each of those
events still has its own `copy_skipped` row on the audit trail with its full
sentence, which is where to look when the counts are not enough. Copied
actions and 🚨 pages are NEVER summarised, however busy the cycle:

```
⏭ 21 leader events skipped, summarised — a backlog drain, not that many
separate problems. Each one has its own row on the audit trail, action
copy_skipped, carrying its own full reason.
• 0xabcd…1234 — 18: 15 stale entry, 3 no local position
• 0xbeef…5678 — 3: 3 coin occupied
```

One side effect worth knowing before you read a busy cycle's chat: skips are
now delivered at the END of the cycle, so within a single cycle every copied
action and 🚨 arrives before every skip, even where the skip was decided
first. The skip's own timestamp is still the moment it was decided.

These are the executor's OWN messages, on their own queue. They are never
Position Alerts and never carry alert preferences: a mute or a per-Track size
floor cannot suppress a copy report, and cannot change what gets traded.

## The rules that will surprise you

**A bracket exit ENDS the copy episode.** If your TP or SL fires while the
Leader is still in the position, that is it — their later scales and their
eventual close are skipped (visibly, with a reason) until they fully close and
freshly re-open. The bracket is an explicit local override of the Leader's
exit timing; re-entering would churn against it.

**A stale entry is dropped, a stale exit is not.** Entries older than 5
minutes are skipped — that is what stops a restart after downtime firing a
burst of stale opens. Exits execute at ANY age, because a close skipped as
stale leaves a position no future event will ever close.

**A stale flip half-executes.** Close leg fires, open leg skipped, you end
flat. Both halves are audited. Same asymmetry.

**A Leader under $10,000 of live equity is not copied into.** ⚠️ **TEMPORARY
(issue #193, revert guarded by #192): during the A5 shakedown the floor is
$100, not $10,000**, so the operator's own ~$300 wallet can drive the copy path
on demand. Everything below describes the gate either way. Opens and a
flip's open leg only — not scale-ins, and never exits (ADR-0007 amendment
D-2). This is a signal-quality gate, not a sizing input: it asks "is this
still the trader whose stats earned the copy?", which is a question about
starting to follow someone. 38% of quality-screened wallets had emptied their
accounts while their stored metrics still looked alive. The equity read is the
one that comes from the SIGNAL network (amendment D-6, table above) — the
account that earned the stats, not the account on the book we trade.

**A bracket you cancel comes back.** Restoration is a per-cycle INVARIANT
(amendment D-1): every live position in a `bracket` sub has its triggers, and
the executor restores them within a minute whatever removed them — a halt
sweep, a restart, or you cancelling them in the Hyperliquid UI. If you want a
position unstopped, the route is `default` mode or `/uncopy`, not cancelling
the trigger. Every restoration reports in the chat.

**Re-copying tops the sub up to its allocation.** `/uncopy` never flattens, so
the sub comes back holding whatever last time left in it. The allocation is a
TARGET BALANCE — it is the exchange-enforced exposure cap — so a re-copy moves
only the difference. An over-funded sub is left alone: taking money out is not
something provisioning decides.

**An exit too small for the exchange is skipped, not retried.** Under the $10
minimum order value there is no order to send, so the residue stays and
reconciliation keeps reporting it rather than three guaranteed rejects
appearing as a market problem.

**A position we did not open is never touched.** If you hold something in a
copy sub yourself, the Leader's events on that coin skip with "coin occupied".

**Reconciliation never trades.** Each loop the executor compares every sub's
live state against its episodes, classifies any divergence, adopts the actual
state as the new baseline, and audits. It will NEVER place an order to close a
gap — auto-correcting would fight you, fight liquidations, and turn
bookkeeping bugs into live orders.

## When something pages

| what you see | what happened | what to do |
| --- | --- | --- |
| 🚨 CLOSE UNFILLED after 3 reduce-only attempts | the book would not absorb a reduce-only IOC inside the slippage cap — pathological | the position is LEFT AS IS. Close it from the master wallet if it matters. |
| 🚨 LIQUIDATED | the sub was liquidated; the episode is closed | nothing to save. The allocation is what it cost. |
| 🚨 …exchange says <side>, episode says <side> | a divergence nothing could classify — a bug or an outside actor | the executor adopted NOTHING and will re-flag every loop. Investigate before re-enabling. |
| 🚫 was NOT set up | the v0 risk policy declined the allocation or base | the mapping is disabled and nothing was funded. Re-run `/copy` inside the ceilings. |

## Interaction with `/kill`

`/kill` halts everything. The executor stops signing at its next loop — it
still reconciles (which places no orders), but drains no backlog and provisions
nothing. Events arriving during the halt are NOT claimed, so `/resume` drains
them under the ordinary rules: stale entries die on the 5-minute guard, exits
execute at any age.

The sweep cancels resting orders across the master **and every sub**. For a
bracket-mode sub that means its TP/SL are cancelled while the position is
HELD — so between the halt and the resume, that position is unstopped. On
`/resume` the executor re-places brackets on every still-held position in a
bracket-mode sub. If a halt is going to stand for a while, act from the master
wallet rather than waiting.

## The risk policy (A5)

Every risk-increasing order is judged before it is signed, and the verdict —
allow, clamp or decline — is recorded verbatim in
`execution_audit.risk_decision`. **Exits are exempt from all of it.** Only a
halt outranks an exit; a denial in the trail therefore always says "did not
enter" and never "did not exit".

**The Liquidity Floor** decides whether a coin is copyable at all: 24h notional
volume AND open-interest notional must both clear their thresholds. It is not
a curated coin list — which coins to trade is the Leader's decision, and
Epigone's only veto is market health. It speaks **exactly once per Copy
Episode, at the open that starts it**, which has three consequences worth
knowing before it surprises you:

- a live episode is never interrupted — scale-ins copy even after the coin has
  gone thin;
- an exit is never blocked — a copy stuck in a thin market is the position you
  most need out of;
- a **flip ends the episode**, so its opening leg is a fresh entry judged
  fresh. A flip into a sub-floor coin therefore closes and **sits out flat**.

**The stake caps** bound the MARGIN behind one coin and one sub, measured
against what the exchange says is committed (your own manual positions in that
sub included — it is one margin pool). An order over a cap is **clamped** to
the headroom left and audited `allowed-clamped` with both the asked and the
given figure; only a clamp whose position falls under the exchange's $10
minimum becomes a denial. **No cross-sub coordination**: two Leaders holding
the same coin in two subs look independent to this policy and are not. That is
a stated v0 choice, recorded as a known gap — what bounds it today is that each
sub's allocation is separately funded.

## `/limits` — the global knobs

```
/limits                      show every knob, and when one last moved
/limits <knob> <value>       move one (audited old → new)
```

`floor_volume`, `floor_oi`, `coin_stake`, `sub_stake`, `max_leverage`. The
executor re-reads the row **every cycle**, so a change lands without a restart
— which is the point: a limit you have to redeploy to change is a limit nobody
changes during the incident that needed it.

Both floors turn **off** at 0; the stake caps and the leverage backstop do not
(a zero cap is "copy nothing", which `/uncopy` says reversibly). Per-sub knobs
— stake and leverage mode — live on `/copy`, not here.

**Revisit the floors whenever you raise the stake caps.** A $100k floor bounds
slippage for a $4,000 position and does not for a $40,000 one.

## Constants, and where to change them

The numbers the OPERATOR tunes are in `/limits` (above). The numbers ADR-0007
settled live in `epigone/execute/policy.py`, one line each, beside the
reasoning that chose them: the 5-minute entry staleness guard, the $10,000
leader liveness floor (temporarily $100 — see the ⚠️ above and issue #192),
the 1% slippage cap, the 3-attempt exit retry, the
bracket verification interval, the $10 exchange minimum, and the $2,000
allocation funding ceiling (a typo catcher on the one irreversible money move,
not a risk limit). Per-sub configurability is deliberately not offered for any
of them.

## The daily-loss pause is NOT in yet

A5 ships its enabler, not the pause (filed as **#181**): every cycle records
each sub's equity in `copy_sub_equity`, which is what a threshold will
eventually be chosen from. Until #181 lands, small stakes and the notices in
your chat are the interim cover — nothing stops a sub that is having a bad day
except `/uncopy` or `/kill`.
