# Runbook: operator copy execution (issue #136, ADR-0007)

The A4 product loop: a Leader the operator explicitly enabled opens, scales or
closes a position, and Epigone mirrors it into that Leader's own funded
sub-account. **ADR-0007 is the document that decides everything here**; this
page is what to type, what you will see, and what to do when something pages.

**Live gate: TESTNET ONLY.** Nothing in the codebase passes `allow_mainnet`,
so a mainnet URL is refused at gateway construction. The switch happens only
after #137 (A5) merges, and it is hand-run by the operator.

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
/copy <leader> <allocation> <base> <mode> [tp% sl%]
/copy 0xabc… 1000 200 bracket 10 5
```

- **allocation** — dollars transferred into a dedicated sub-account for this
  Leader. This IS the exposure cap, margin-enforced by the exchange, not by
  Epigone bookkeeping. A scale the margin cannot absorb simply rejects.
- **base** — the fixed dollar size of a copied OPEN. The Leader's own size
  never changes it; only their relative moves are mirrored (a 50% scale-in
  scales the copy 50%, a 30% trim sells 30% — of what we actually hold).
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

**A Leader under $10,000 of live equity is not copied into.** Opens and a
flip's open leg only — not scale-ins, and never exits (ADR-0007 amendment
D-2). This is a signal-quality gate, not a sizing input: it asks "is this
still the trader whose stats earned the copy?", which is a question about
starting to follow someone. 38% of quality-screened wallets had emptied their
accounts while their stored metrics still looked alive.

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

## The v0 risk policy

Hardcoded and deliberately conservative; A5 (#137) replaces the module, not the
seam. Today: allocation ≤ $2,000, base notional ≤ $400, order notional ≥ $10
(the exchange's own floor), and exits are never declined. Every verdict —
allow or decline — is recorded verbatim in `execution_audit.risk_decision`.

## Constants, and where to change them

All of them live in `epigone/execute/policy.py`, one line each, beside the
reasoning ADR-0007 gave them: the 5-minute entry staleness guard, the $10,000
leader liveness floor, the 1% slippage cap, the 3-attempt exit retry, the
bracket verification interval, and the policy ceilings above. Per-sub
configurability is deliberately not offered for any of them.
