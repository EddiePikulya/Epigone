# ADR 0005: Epigone-native execution via Hyperliquid agent keys; master keys never on Epigone servers

Date: 2026-07-27
Status: proposed

Numbered 0005 because 0004 is reserved by the prediction-market spike
(PR #69, unmerged).

## Context

Epigone's next big feature is Epigone-managed copy trading: the bot opens,
closes, and manages Hyperliquid positions on behalf of users — operator first,
designed for anyone later. Issue #127 asked the foundation question only:
**which key architecture makes "a non-crypto person can start trading from a
Telegram bot" and "the design is honestly safe" both true?** The evidence is
in docs/research/autotrade-spike.md; product mechanics (sizing, leader
selection) are a separate spike.

Three prior decisions frame this one:

- **ADR-0001** put direct Hyperliquid APIs in the serving path and rejected
  the Bullpen CLI for it. This ADR extends that posture from reads to writes.
- **ADR-0002** structured Epigone as Python processes over a Postgres seam;
  execution must arrive as another process on that seam, not a new shape.
- **Issue #106** (cancelled) tried copy trading through the Bullpen executor
  and died on the right objection: it made the operator custodian of users'
  credentials for a third-party Alpha product. Separately, the operator
  personally hit login/auth failures with the Bullpen CLI — first-hand
  evidence that a third-party *consumer* product in the critical path is
  fragile even at the front door. This spike re-opens #106's question with a
  dependency rule derived from that experience: **infrastructure vendors
  (builder-facing APIs, SLAs, published pricing) are acceptable dependencies;
  consumer products are not.** The execution path itself is Epigone-native.

The spike's decisive findings (all sourced in the research doc):

1. **Hyperliquid splits its action surface by signature scheme.** Every
   trading action (orders, cancels, TP/SL, leverage, scheduleCancel) is an
   "L1 action" an approved **agent key** may sign; every fund-moving or
   authority-granting action (`withdraw3`, `usdSend`, `approveAgent`,
   `approveBuilderFee`, …) is "user-signed" — **master key only**. Verified
   in docs for the load-bearing cases (builder-fee approval verbatim),
   structurally certain for the rest; a handful of gray-zone internal
   vault/subaccount actions are flagged for testnet probing, none an
   external exit. Live probes show top accounts running 26–103 named agents
   each: this is the exchange's intended automation primitive.
2. **No layer above Epigone can constrain individual orders.** Orders are
   signed over an opaque action hash, so neither Hyperliquid approvals nor
   any provider policy engine (verified for Turnkey, which otherwise parses
   EIP-712 fields and supports Hyperliquid `approveAgent` policies
   first-class) can inspect an order's asset/size/side. Per-order risk limits
   can only live in Epigone's own executor.
3. **Users can verify and revoke our authority from outside our system**:
   agent approval, expiry (≤180 days), and revocation live on
   app.hyperliquid.xyz; `userRole`/`extraAgents` make the grant publicly
   auditable. Builder fees are likewise user-approved, capped (10bp perps),
   and revocable — a proven revenue mechanic ($30M+ cumulative across
   builders; pvp.trade ~$8M).
4. **The incident record condemns exactly one design**: every major Telegram
   trading-bot loss (Maestro, Unibot, Banana Gun, SIGMA; ~$4M+ combined)
   involved server-held master keys or approval/auth bugs around them — none
   involved a Hyperliquid agent key. And a leaked *trade-only* key is still
   severe (counter-trading extraction, 3Commas ~$22M precedent), so
   "agent-only" bounds the blast radius; it does not excuse skipping the
   risk layer.
5. **Non-custodial normie onboarding has a floor of one Telegram Mini App
   tap.** Every embedded-wallet provider requires user-presence auth to
   establish user-controlled keys; the only fully headless path (Dynamic
   server wallets) is custodial by construction.

## Decision

### The invariant: Epigone servers only ever hold agent keys

The master key — the thing that owns funds and grants authority — never
touches Epigone infrastructure in any phase. Epigone generates, encrypts
(KMS-envelope), and holds only **trade-only agent keys**, one per user
account, each approved by that user's master wallet, each expiring ≤180 days,
each revocable by the user outside our system. Agent addresses are never
funded and never reused after deregistration (nonce-replay hazard).

### Execution is Epigone-native, direct against the exchange endpoint

A new `execute` process joins `ingest`/`stream`/`bot` on the Postgres seam
(ADR-0002): it consumes copy signals, applies the risk policy, signs with
agent keys via the official `hyperliquid-python-sdk` signing helpers, and
talks to `POST /exchange` through an `ExecutionGateway` beside the existing
read gateway (ADR-0001's shape, extended to writes). One agent key per user
account, one signer per process lane — this is also what Hyperliquid's nonce
model (per-signer nonce sets) wants.

### The risk policy lives in the executor, as a first-class module

Because finding 2 says nowhere else can: per-coin allowlist with a liquidity
floor (thin books are the extraction vector), max position notional, max
leverage, max daily loss halt, and a scheduleCancel dead-man's heartbeat with
an explicit position-unwind policy. These ship in Phase A, before the first
external user — they are the honest substance behind "feels safe", not
polish.

### Onboarding is phased by custody tier, operator-first

- **Phase A — operator only.** The operator's own wallet approves the agent
  key via the Hyperliquid UI; no provider, no new custody, no regulatory
  change (own funds = proprietary trading). This proves the engine.
- **Phase B — bring-your-own-wallet users, behind a regulatory gate.**
  Crypto-native users sign `approveAgent` + `approveBuilderFee` from their
  existing wallet on a minimal Epigone signing page; funds stay in their own
  Hyperliquid account. Builder codes (2–5bp within the 10bp cap) switch on
  here as revenue. The gate: counsel review of the portfolio-management/CTA
  flags and US/sanctions geofencing — discretionary trading of a third
  party's account is likely regulated activity in the EU (MiFID II, since
  perps) and CTA-shaped in the US regardless of custody.
- **Phase C — embedded wallets for normies.** An infrastructure provider
  (Turnkey or Privy — selected hands-on in a Phase C spike, which requires
  the signups this spike was barred from) holds the user's **master** key in
  a TEE, keyed to Telegram-Mini-App auth; it signs the same two approvals the
  BYOW user signs, plus withdrawals — policy-pinned where the provider
  supports it (e.g. Turnkey policies constraining `approveAgent` to Epigone's
  agent address and allowlisting withdrawal destinations). Epigone's trading
  path is unchanged: the same server-held agent key, the same executor, the
  same risk module. The provider is an onboarding dependency, never an
  execution dependency — if it is down, trading continues; only
  onboarding/withdrawal ceremonies wait.

## Alternatives honestly weighed

- **Provider signs every order (no server-held agent key).** Strictly
  dominated: order content is an opaque hash, so per-order signing buys zero
  policy leverage while adding vendor signing latency (Turnkey markets
  50–100ms), per-signature cost, and provider rate caps (Turnkey: hard
  10 RPS per sub-org) to the hot path — and putting an infrastructure
  vendor in the execution critical path violates the dependency rule's
  spirit. Rejected on evidence, not taste.
- **Self-managed encrypted master keys (full custody).** Lowest onboarding
  friction (no Mini App tap), and it is what most incumbent Telegram bots do.
  Rejected: it is the architecture of every major incident in the record, it
  flips the FinCEN hosted-wallet analysis against us, it makes "we cannot
  touch your funds" false — and #106 was cancelled over a weaker version of
  exactly this.
- **BYOW-only (no embedded wallets ever).** Honestly safe and nearly free —
  but it excludes the "non-crypto person" half of the #127 question
  entirely. Kept as Phase B, rejected as the end state.
- **Bullpen (or any consumer product) as executor.** Re-rejected. #106's
  custody objection stands; the operator's own Bullpen CLI auth failures show
  the front-door fragility; ADR-0001/0004 already established the pattern:
  closed Alpha consumer products with no builder ToS/SLA don't belong in the
  serving path. Turnkey/Privy differ in kind, not degree: builder-facing
  APIs, published pricing, SLAs, audits — and even they are confined to
  onboarding, not execution.
- **Hyperliquid vaults (protocol-native pooled copy).** The heaviest
  regulatory shape (pooled fund) and a different product than per-user
  accounts. Out of scope; revisit only with counsel.

## Consequences

- The serving path stays Epigone-native end to end (extends ADR-0001): the
  only new third-party dependency is an onboarding-time key-management
  vendor in Phase C, deliberately kept off the execution path.
- "Honestly safe" becomes a checkable claim: funds sit in the user's own
  Hyperliquid account; our authority is a trade-only key the user can see
  (`extraAgents`), verify (`userRole`), and revoke on Hyperliquid itself;
  withdrawals are cryptographically outside our reach. The residual truths
  we state rather than hide: a compromised Epigone can still trade affected
  accounts badly (bounded by the risk module, revocation, expiry), and
  Phase C users trust the provider's TEE for master-key custody.
- Epigone owns the entire risk layer — the cost of finding 2. Caps,
  allowlists, kill switches, and the audit trail are product code with
  product-grade tests, not configuration.
- Operational burdens accepted: agent-key rotation every ≤180 days (runbook +
  automated re-approval ceremony), scheduleCancel heartbeat discipline
  (10 triggers/day budget), address-based rate budgets per user account, and
  an execution lane added to the shared IP weight budget.
- Phase B cannot ship on engineering merit alone: it sits behind an explicit
  regulatory gate (EU portfolio-management/MiFID II and US CTA flags,
  geofencing) — scheduled work, not a disclaimer.
- Testnet probes in Phase A resolve the spike's open questions (real agent
  cap, scheduleCancel eligibility, gray-zone L1 actions) before any design
  hardens around them.
