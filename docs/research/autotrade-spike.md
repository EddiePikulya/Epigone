# Autotrade spike: automated trading on Hyperliquid research (2026-07-27)

Investigation for #127: how to automate trading on Hyperliquid such that (a) a
non-crypto-native user can onboard trivially from a Telegram bot and (b) the
design is genuinely secure and *feels* safe. This re-opens the question the
cancelled Bullpen-executor direction (#106) answered badly — that path made the
operator custodian of users' credentials for a third-party Alpha product. This
spike is the same question done right: Epigone-native execution with a proper
key-management architecture. Decision summary lives in ADR-0005; this doc is
the evidence, the backlog sketch, and the open questions.

Product mechanics (sizing, leader selection, risk engine tuning) are out of
scope — separate spike. Safety rules honored: read-only probes only, no
exchange write actions, no key generation, no service signups, no credentials.

Confidence tags: **[V]** verified from primary docs or live probe, **[S]**
secondary source, **[I]** inferred/synthesized.

## 1. Live probe evidence (read-only info API, 2026-07-27)

All probes: `POST https://api.hyperliquid.xyz/info` (documented types) and the
`stats-data` leaderboard already used by ingest. No exchange actions.

- **Leaderboard:** 40,988 rows — the Universe seed is alive and unchanged.
- **`extraAgents` on the 15 top-monthly-volume accounts:** every one runs
  fleets of *named* agent wallets — 26 to 103 concurrent agents per master,
  with names like `api_wallet_38`, `trade_key_151`, `agent86`, `b-sign-003`.
  Agent-key execution at scale is the established practice of the most active
  accounts on the exchange, not an exotic feature. **[V]**
- **Expiry in practice:** observed `validUntil` horizons span 12–176 days out,
  consistent with the documented 180-day maximum; big operators visibly rotate
  (staggered expiries within one account). **[V]**
- **Cap discrepancy (open question):** docs say "1 unnamed + up to 3 named
  agents, plus 2 named per subaccount". Cross-checking with `subAccounts`
  counts, several probed accounts blow through that formula — e.g. one master
  holds 103 active agents with only 4 subaccounts (formula cap: 12), another
  102 with 7. The documented limit is stale or enforced differently. Do not
  design around "3 named agents"; verify the real cap on testnet at build
  time. **[V probe vs V docs — contradiction]**
- **`userRole` maps an agent to its master publicly:**
  `{"role": "agent", "data": {"user": "0xf5d8…"}}` — anyone (including a
  user outside our system) can verify which master an agent key serves, and a
  master can enumerate its approved agents via `extraAgents`. This is the
  hook for revocation-you-can-verify-yourself UX. **[V]**
- **Agent wallets hold nothing:** a probed agent address's own
  `clearinghouseState` shows `accountValue: 0.0`. Agents are pure signers;
  equity lives entirely with the master. **[V]**

## 2. The execution layer: exchange endpoint mechanics

Primary sources: Hyperliquid GitBook (exchange endpoint, signing, nonces and
API wallets, error responses, rate limits) and the official
`hyperliquid-python-sdk` source (`signing.py`, `exchange.py`).

### Two signing schemes — the fact the whole architecture pivots on

`POST /exchange` actions split into two schemes (SDK: `sign_l1_action` vs
`sign_user_signed_action`) **[V]**:

| Scheme | Action types | Who may sign |
| --- | --- | --- |
| **L1 actions** — action hash = keccak(msgpack(action) ‖ nonce ‖ vault flag), wrapped as EIP-712 over a phantom `Agent(string source, bytes32 connectionId)` struct | `order`, `cancel`, `cancelByCloid`, `modify`, `batchModify`, `updateLeverage`, `updateIsolatedMargin`, `scheduleCancel`, `twapOrder`, `twapCancel`, `vaultTransfer`, `noop`, `reserveRequestWeight` (+ `setReferrer`, `createSubAccount`, `subAccountTransfer` — see gray zone below) | master **or an approved agent key** |
| **User-signed actions** — human-readable EIP-712 (domain `HyperliquidSignTransaction`) | `usdSend`, `spotSend`, `withdraw3`, `usdClassTransfer`, `sendAsset`, `approveAgent`, `approveBuilderFee`, `convertToMultiSigUser`, `tokenDelegate`, … | **master wallet only** |

Everything that moves funds out or changes who may sign is user-signed —
master-only. Everything an agent can do is trading. The builder-codes doc says
it plainly for `approveBuilderFee`: *"This action must be signed by the user's
main wallet, not an agent/API wallet"* **[V]** — and structurally, a
user-signed payload recovers the signer as the acting account, so an agent's
signature resolves to the wrong (empty) account **[I, structurally certain]**.

Gray zone: `subAccountTransfer` / `createSubAccount` / `setReferrer` are
L1-signed in the SDK, and the docs don't state whether the chain accepts them
from an agent signer. Treat them as master-only in threat modeling until a
testnet probe says otherwise. One deliberate exception exists:
`agentSendAsset`, the single agent-signable transfer, restricted to
*self-directed* moves ("Destination must match the source address"). **[V]**

### Nonces

- Per **signer** (master key or each agent key separately): the 100 highest
  nonces are stored; a new nonce must exceed the smallest stored one and be
  unused; freshness window (T − 2 days, T + 1 day). **[V]**
- Docs' own concurrency advice: **one API wallet per trading process**, atomic
  ms-timestamp counter per signer, batch every ~0.1s. Separate agent keys give
  separate nonce sets — the reason multi-user execution wants one agent per
  user, not a shared signer. **[V]**
- `noop` exists to burn a nonce (kill in-flight orders); pruned agents' nonce
  sets allow **replay of previously signed actions** — never reuse a
  deregistered agent address. **[V]**

### Order surface (all agent-signable)

- Limit orders with tif `Gtc` / `Ioc` / `Alo` (post-only); market orders are
  aggressive IOC with slippage-bounded price (SDK `market_open/close`).
- Trigger orders `{isMarket, triggerPx, tpsl: "tp"|"sl"}`; grouping
  `normalTpsl` (tied to parent order) / `positionTpsl` (against the position).
- `reduceOnly`, 128-bit `cloid` client order ids (cancel + status by cloid),
  batched orders in one action, optional `builder: {"b": addr, "f": int}`
  per order.
- `modify`/`batchModify`, `updateLeverage`, `updateIsolatedMargin`, TWAPs.

Everything a copy executor needs — entry, exit, TP/SL, leverage, cancels — is
inside the agent-signable set. **[V]**

### scheduleCancel — the protocol-native dead-man's switch

*"Schedule a cancel-all operation at a future time … The time must be at least
5 seconds after the current time. Once the time comes, all open orders will be
canceled and a trigger count will be incremented. The max number of triggers
per day is 10"* (reset 00:00 UTC); sending without a time removes the
schedule. **[V]** The executor heartbeats it forward; if Epigone dies, resting
orders die with it. Note: it cancels **orders**, it does not flatten
**positions** — position unwind on failure needs its own logic. **[I]**
Secondary sources mention a $1M cumulative-volume eligibility gate; the
current official docs contain no such requirement — unverified either way,
probe on testnet. **[S, flagged]**

### Error/reject semantics

Batch responses carry per-order `statuses` (`resting{oid}`, `filled{totalSz,
avgPx, oid}`, `error{…}`), but pre-validation failures return **one error for
the whole batch** — both cases must be handled. Documented reject classes:
tick price, `MinTradeNtl` ("minimum value of $10"), margin, reduce-only
violations, bad ALO/trigger prices, no-liquidity market orders, open-interest
caps; cancels: `MissingOrder`. **[V]**

## 3. Agent wallets: the security primitive

What the spike set out to verify, verified:

- **CAN:** sign every trading action (orders, cancels, TP/SL, leverage,
  margin, TWAPs, scheduleCancel), for the master and — via `vaultAddress` —
  for its subaccounts ("on behalf of the master account or subaccounts"
  **[V]**) and plausibly vaults **[I]**.
- **CANNOT:** withdraw, transfer (`withdraw3`, `usdSend`, `spotSend`,
  `usdClassTransfer`, `sendAsset`), approve another agent, approve a builder
  fee, or convert the account to multi-sig — all user-signed, master-only.
  **[V]**
- **Approval:** master signs `approveAgent` (user-signed) with agent address,
  optional name, optional `valid_until` — **max 180 days**. Docs state 1
  unnamed + 3 named (+2/subaccount); live probes contradict the formula (§1).
  **[V, contradiction flagged]**
- **Revocation:** re-approving over a slot (same name, or new unnamed)
  deregisters the old key; expiry prunes; "the registering account loses all
  funds" prunes. The user can do all of this from app.hyperliquid.xyz's API
  page — **outside our system entirely**, which is exactly the
  feels-safe property #127 asks for. **[V docs / S for UI]**
- **Agent addresses hold no state** (probe §1) and should never be funded or
  reused after deregistration (replay risk). **[V]**
- **Observability:** `userRole` (weight 60) publicly resolves agent → master;
  `extraAgents` lists a master's agents with expiries (documented by
  QuickNode/Chainstack references **[S]**, verified live **[V]**).

**The honest blast-radius statement.** "Trade-only" does not mean "harmless".
A leaked agent key cannot move funds out directly, but it can trade the
account into the ground — and the documented attack class on trade-only keys
is **counter-trading extraction**: the attacker builds a position in their own
account, then uses the victim's key to push a thin-book market through their
orders; value exits via the market, not via withdrawal (precedents: Binance
VIA/BTC 2018; 3Commas API-key leak Dec 2022, ~$22M **[S]**). On Hyperliquid
the same play maps to illiquid perps at high leverage. Plan for: leaked agent
key ≈ potential near-total loss of the affected account, bounded by (a)
per-account isolation — one agent per user, one user per master account, (b)
user-side revocation, (c) expiry, (d) Epigone-side caps and symbol allowlists
(exclude thin books), (e) anomaly kill switch. **[V pattern / I mapping]**

## 4. Builder codes: the revenue mechanic

The fee-attribution primitive fits Epigone's copy model exactly **[V]**:

- User signs `approveBuilderFee {maxFeeRate: "0.001%", builder}` once — from
  the **main wallet** (never an agent), revocable any time, max 10 active
  builder approvals per user.
- Each order optionally carries `builder: {"b": builder_addr, "f": fee}` with
  `f` in **tenths of a basis point** (f=10 → 1bp). Caps: **0.1% (10bp) on
  perps**, 1% on spot. Charged on order notional in the collateral asset.
- Builder eligibility: ≥100 USDC in the builder's perps account; revenue is
  claimed "through the usual referral reward claim process"; accrued totals
  via the `referral` info query; approval checkable via `maxBuilderFee`.
- TWAP payloads carry no builder field — assume TWAPs earn nothing. **[I]**
- Market proof: builder-code revenue is real money — cumulative revenue
  crossed $10M in late 2025 and multiple sources now put it at $30–40M+
  lifetime: Phantom ~$20.6M, pvp.trade ~$8M, Insilico ~$3.3M from ~3k users
  (≈$1,100/user). Dashboards: ASXN Hyperscreener builder-codes tab, Allium.
  **[S]**

For Epigone: attach the builder field to every copied order at the multi-user
phase; a 2–5bp perp fee is squarely inside observed market practice. The
approval rides the same one-time master-wallet signing ceremony as
`approveAgent` — no extra funnel step. **[I]**

## 5. Rate limits and the Python SDK

### Rate limits relevant to execution **[V]**

- **IP-based:** 1200 weight/min shared with info (the existing
  `SharedWeightBudget` models this); an exchange action weighs
  `1 + floor(batch_length / 40)` — order placement is cheap against the IP
  budget.
- **Address-based (the one that matters for trading):** 1 request per 1 USDC
  cumulatively traded per address, with an initial 10,000-request buffer; when
  exhausted, a 1-request-per-10s trickle. Batches count as *n* against the
  address limit. Cancels get headroom (`min(limit + 100k, 2 × limit)`).
  Extra capacity is purchasable on-chain: `reserveRequestWeight` at 0.0005
  USDC/request. A copy executor trades real notional, so the budget grows
  with use; a *stalled* account with the buffer spent is the risk case.
- **Open orders:** cap 1,000 default (+1 per $5M volume, hard cap 5,000).
- **WebSocket post:** signed exchange actions can ride the existing WS
  connection (100 in-flight max) — a latency option for later.
- Stale-`expiresAfter` cancels cost 5× address weight — don't spam expiries.

### Official `hyperliquid-python-sdk` **[V]**

- Official (Hyperliquid-authored; PyPI maintainers include the founder's
  handle), MIT, ~1.8k stars, latest 0.24.0 (2026-06-04), Python 3.9–3.13,
  multi-release-per-year cadence. Already Epigone's dependency (ADR-0002).
- **Full coverage** of the action surface: `order` (with `builder=`),
  `bulk_orders`, `modify_order`, `market_open/close`, cancels incl. by-cloid,
  `schedule_cancel`, `update_leverage`, `update_isolated_margin`,
  `approve_agent` (generates a fresh agent key and returns it),
  `approve_builder_fee`, subaccount/vault/transfer/multi-sig actions.
- **The agent pattern is first-class:** `Exchange(wallet=agent_key,
  account_address=master_addr)` — sign with the agent, act as the master.
  Example: `examples/basic_agent.py`.
- Production caveats: nonces are per-call `get_timestamp_ms()` with no atomic
  counter (same-ms collisions under concurrency — reinforces
  one-agent-per-process); the WS manager is thread-based, not asyncio
  (Epigone's executor would use its own asyncio HTTP path through the gateway
  seam, as ingest/stream already do). **[V code / I implication]**

## 6. Key management for non-crypto users

The question: who holds the **master** key (funds + approval authority)? The
agent key is settled — Epigone generates and holds it server-side, encrypted;
it is the bounded, revocable credential. Four candidate answers for the
master, weighed honestly:

### 6.1 Turnkey — TEE infrastructure (what Bullpen builds on)

- **Model:** keys generated/used only inside AWS Nitro Enclaves running
  Turnkey's open-source QuorumOS; reproducible builds (StageX) + remote
  attestation make the enclave code third-party verifiable; Shamir-split
  quorum ceremony for the master enclave key. Honest read: *architecturally*
  non-custodial (Turnkey can't read keys; every action needs a credential
  Turnkey doesn't hold) but **availability-custodial** — export requires
  Turnkey to be up; no user-side recovery if Turnkey vanishes. **[V]**
- **Sub-organizations:** one per end user; parent org (us) gets **read-only**
  visibility; "Delegated Access" gives our backend a scoped user inside the
  user's sub-org that can sign without user presence — with no possibility of
  root escalation when set up frontend-first. **[V]**
- **Policy engine — decision-critical finding:** policies can parse **EIP-712
  payload fields** (`eth.eip_712`: primary_type, domain, message), and
  Hyperliquid is *first-class* — docs ship an
  `allow-signing-of-eip-712-payloads-for-hyperliquid-approveagent-operations`
  example. But Hyperliquid **orders** are L1 actions signed over an opaque
  hash (`connectionId = keccak(msgpack(action)+nonce)`), so **no provider
  policy anywhere can inspect an order's content** (asset, size, side).
  Policy leverage exists exactly where the master key signs: *which agent gets
  approved*, *which builder fee*, *where withdrawals may go*. Per-order
  trading limits can only live in Epigone's own executor. **[V + I,
  high-confidence composition]**
- **Auth:** passkeys, email OTP, OAuth; **no plain-Telegram-bot auth** — the
  productized path is a Telegram **Mini App** (`telegram-cloud-storage-stamper`
  + demo repo) storing the user's session key in Telegram Cloud Storage;
  reachable only from Mini App JS, not from a server-side bot. **[V]**
- **Pricing:** free tier 1,000 wallets / 25 sigs/mo; $0.10/sig
  pay-as-you-go; Pro $99/mo at $0.05/sig; Enterprise to $0.0015 with SLAs.
  Rate cap: hard 10 RPS per sub-org. With the agent architecture we sign via
  Turnkey only at onboarding (approveAgent + approveBuilderFee) and for
  withdrawals — cost and RPS are negligible. **[V]**
- **Ecosystem proof:** Turnkey's Hyperliquid blog lists Dexari, Hyperbot,
  pvp.trade, Kinetiq, Hyperbeat, Liminal, SuperX, Slash.trade; Bullpen's own
  FAQ confirms "non-custodial Turnkey wallet … export your private key at any
  time". Marquee customers include Moonshot and Polymarket. **[V/S]**

### 6.2 Privy — higher-level embedded wallets

- **Model:** TEE-based by default (enclave share + auth share; key exists
  only transiently inside the TEE), with an older audited Shamir device-share
  model. **[V]**
- **Delegated signing is the headline:** *session signers* — user consents
  once, then the backend can request signatures (stated use cases include
  "agentic trades"); user can **revoke at any time**
  (`removeSessionSigners`); "require signed requests" binds delegation to our
  server key. Fine-grained policy engine + key quorums are Enterprise-gated.
  **[V]**
- **Hyperliquid is productized:** an official Privy recipe covers agent
  wallets, order placement, deposits/withdrawals, subaccounts, and builder
  codes end-to-end. Privy also ships a **funding modal** (Meld/MoonPay/
  Coinbase onramps + bank transfers) — the onramp step comes bundled. **[V]**
- **Telegram:** login supported, but zero-click only **inside a Mini App**;
  no headless bot-only path. Same one-tap-into-Mini-App floor as Turnkey.
  **[V]**
- **Pricing:** free to 499 MAU (50k sigs/mo), then $299/mo (to 2,499 MAU),
  $499/mo (to 9,999). **[V]**

### 6.3 Dynamic, Web3Auth — evaluated, weaker fits

- **Dynamic** (TSS-MPC 2-of-2, TEE server share): publishes both a
  Hyperliquid agent-wallet recipe and a **Telegram server-wallet bot recipe**
  — the only fully headless (no Mini App) path found. But server wallets are
  developer-controlled: that shape is custodial-with-guardrails, the trust
  model we're rejecting. Growth $249/mo. Fallback candidate only if
  zero-web-view ever becomes non-negotiable. **[V]**
- **Web3Auth** (SSS 2-of-3, client-side key reconstruction; MPC
  enterprise-only): browser-first, no consented server-delegation story —
  poor fit for a server-automated bot. Cheapest tiers ($69/mo growth) don't
  compensate. **[V/I]**

### 6.4 Self-managed encrypted keys — the DIY bar

A competent small-operator design (KMS envelope encryption, per-user DEKs,
decrypt only in-process) defends against DB dumps, stolen backups, and casual
insider access. It does **not** defend against a live compromised host — the
process that can decrypt to sign can be made to sign anything. That is
acceptable for **agent** keys (bounded blast radius, revocable, expiring —
and this is exactly what Epigone V1 does for the operator's own agent key).
It is **not** acceptable for user **master** keys: holding those makes
Epigone a custodian — the incident-record pattern (§8), the regulatory
pattern (§9), and the #106 objection all fire at once. **[I]**

## 7. Onboarding funnel realities

Funding facts **[V unless noted]**:

- Canonical deposit: native **USDC on Arbitrum** → Bridge2 contract;
  credited <1 min; **minimum 5 USDC — smaller amounts are lost forever**.
  Official onboarding now also shows deposit addresses for
  Ethereum/Base/Polygon USDC and native BTC/ETH/SOL (via Unit — a 2-of-3
  MPC guardian federation, a real trust dependency **[S]**); CCTP-based
  direct-to-HyperCore USDC is live with the Arbitrum bridge slated for
  eventual deprecation **[S]**.
- Fiat onramps delivering Arbitrum USDC: Coinbase Onramp, MoonPay, Transak,
  Ramp — card fees ~2.9–4.5%, KYC (ID + selfie) beyond small limits **[S]**.
  Privy bundles Meld/MoonPay/Coinbase + bank rails in its funding modal
  **[V]**; a direct in-app HL fiat onramp (Swapped.com) is in testing **[S]**.

The three funnels, friction-marked (★ = observed drop-off point):

| | (a) Bring-your-own-wallet | (b) Embedded wallet + agent | (c) Custodial server key |
| --- | --- | --- | --- |
| Steps | /start → deep-link → connect MetaMask/Rabby ★ → fund from existing stack → sign `approveAgent` + `approveBuilderFee` → done | /start → one tap into Mini App (wallet exists) → fund via bundled onramp ★★ (fees/KYC) → one consent tap (delegation/agent approval) → done | /start → address issued instantly → user must still acquire crypto ★★ → done |
| Who can use it | crypto-natives only | anyone with a card | anyone with crypto |
| Custody | none — we hold a trade-only agent key | master in provider TEE, user-revocable; we hold trade-only agent key | we hold everything |
| Withdrawal power | user only | user (via provider auth; key export as backstop) | us — full theft blast radius |

Two structural truths: **the onramp/KYC step, not wallet creation, is the
funnel bottleneck in every variant**; and a one-tap Telegram **Mini App**
interaction is the floor for non-custodial normie onboarding — every provider
requires user-presence auth to establish user-controlled keys, and the only
way to remove that tap is to become custodial (c). **[I from V facts]**

## 8. Threat model: incident record and what it teaches

Documented incidents (all **[V/S]** with sources in the research briefs):

| Incident | Date | Loss | Failure mode |
| --- | --- | --- | --- |
| Maestro router exploit | Oct 2023 | ~280 ETH (~$500k, refunded) | router contract abused users' token approvals |
| Unibot router exploit | Oct 2023 | ~355 ETH (~$560–640k, refunded) | call-injection in 3-day-old router |
| Banana Gun hack | Sep 2024 | $3M / 11 users (refunded) | Telegram message-oracle flaw → manual transfers from bot-held wallets |
| SIGMA bot key leak | rep. May 2026 | ~$200k | bot-generated/imported **raw private keys** exfiltrated |
| Hyperliquid user drain | Oct 2025 | ~$21M | **master-key** compromise (user-side, not protocol) |
| 3Commas API-key leak (CEX precedent) | Dec 2022 | ~$22M | trade-only keys exploited via counter-trading extraction |

Patterns: every big Telegram-bot loss involved **server-held master keys or
router/message-auth bugs** — none involved a Hyperliquid agent key; no
verified breach of Turnkey/Privy/Web3Auth key infrastructure exists (absence
of incident ≠ absence of risk; a BitBot/Privy drain rumor from Feb 2024 did
not verify). The 3Commas precedent is why §3's blast-radius statement treats
a leaked trade-only key as severe anyway. **[V/S]**

Threat-model table for the recommended architecture (ADR-0005):

| Compromise | Blast radius | Mitigations |
| --- | --- | --- |
| Epigone server | agent keys → bad trades / counter-trading extraction on affected accounts; **no withdrawals** | per-user agent+account isolation; Epigone-side caps & symbol allowlist; scheduleCancel heartbeat; anomaly kill switch; user-side revocation; ≤180d expiry |
| Key provider (Turnkey/Privy) | master keys at risk *if* TEE+policy model fails — the residual trust ask | provider attestation/audits; policy pinning approveAgent to our agent address; withdrawal-destination allowlist; user key export |
| User device (Telegram account) | attacker can command trading via our bot as the user | Epigone caps; withdrawal destinations pinned to user's own addresses; provider re-auth for sensitive ops |
| Operator (us) turning malicious | same as server compromise — trading, not theft | the honest pitch: verifiable non-custody (`userRole`/`extraAgents` are publicly checkable), user-revocable outside our system |

## 9. Regulatory flags (noted, not resolved — not legal advice)

- **EU/Germany:** ESMA's copy-trading briefing (2023): auto-execution without
  client intervention = **portfolio management**. Perps are **MiFID II**
  territory (ESMA classifies perpetuals as derivatives; MiCA excludes
  financial instruments) — i.e. the *harder* licensing regime (BaFin
  investment-firm question), plus CFD-style product-intervention rules for
  retail. **[V]**
- **US:** discretionary trading of others' commodity-interest accounts for
  compensation is CTA-shaped; pooling is CPO-shaped. CFTC has fined DeFi
  protocols for offering leveraged derivatives to US persons (Opyn/ZeroEx/
  Deridex, Sep 2023). Hyperliquid's own ToS geoblocks US persons — as the
  access layer, Epigone must geofence US (and sanctioned) users itself.
  **[V]**
- **Custody line:** FinCEN's hosted-wallet test — "total independent control
  over value" → money transmitter. User-owned account + trade-only revocable
  agent key is the strongest available "not custody" fact pattern; a
  bot-created server-side wallet is the weakest. Non-custody does **not**
  cure the portfolio-management/CTA analysis, which attaches to discretion,
  not custody. **[V/I]**
- **The escalation ladder:** operator-only trading of own funds — no flags
  identified (proprietary trading). First **external user** with discretion →
  portfolio management (EU) / CTA (US) questions attach; any fee (builder
  revenue on their flow counts) strengthens "for compensation". First
  **pooled** fund (incl. an HL vault) → fund-vehicle territory, heaviest.
  First **performance fee** → cements compensation + narrows exemptions.
  **[V/I]** Multi-user phases therefore sit behind an explicit regulatory
  gate (counsel consulted, geofencing in place) — a backlog item, not a
  footnote.

## 10. Phased backlog sketch (operator-only first)

Phase gates are hard: A ships value to the operator with zero third-party
dependencies and zero new custody; B adds crypto-native external users only
after the regulatory gate; C adds normies only after B proves the engine.

**Phase A — operator-only V1 (own funds, own risk):**

| # | Slice | Depends | Size | Delivers |
| --- | --- | --- | --- | --- |
| A1 | **ExecutionGateway** (write-side twin of the read gateway): order/cancel/modify/TP-SL/leverage/scheduleCancel via the SDK's signing, typed action results, error taxonomy, address-budget accounting; fake for tests; **testnet harness** | — | L | The execution seam, testnet-proven (incl. probing the open questions: agent caps, scheduleCancel gate) |
| A2 | **Agent-key custody v0**: operator approves a named agent from their own wallet (HL UI); encrypted keystore (KMS/age envelope) for the agent key; rotation runbook for the 180-day expiry | — | S | Master key never touches the server, from day one |
| A3 | **Safety layer v0**: scheduleCancel heartbeat, `/kill` command (cancel-all + halt), append-only execution audit table, position-unwind-on-halt policy | A1 | M | Dead-man's switch + audit trail before the first real order |
| A4 | **Operator copy executor**: consume the existing stream alerts for followed Traders → sized copy orders (fixed sizing config), open/scale/close mirroring, TP/SL attachment | A1–A3 | L | The actual product loop, single-user |
| A5 | **Risk policy v0**: per-coin allowlist (liquidity floor), max position notional, max leverage, max daily loss halt — enforced in the executor, because (verified) no protocol or provider layer can | A4 | M | The only place per-order limits can exist, built as a first-class module |

**Phase B — external crypto-native users (BYOW), behind the regulatory gate:**

| # | Slice | Depends | Size |
| --- | --- | --- | --- |
| B1 | Regulatory gate: counsel review of §9, geofencing (US + sanctioned), ToS | A | — |
| B2 | Per-user agent onboarding: signing page for `approveAgent` + `approveBuilderFee` (user's own wallet), per-user agent isolation, `userRole`-based verification UX | A | M |
| B3 | Builder-code revenue plumbing: builder field on copied orders, accrual tracking, claim runbook | B2 | S |
| B4 | Multi-account executor: per-user nonce/signer separation, per-user risk policies, per-user kill/revocation status monitoring (detect user-side revocation gracefully) | B2 | L |

**Phase C — normie onboarding (embedded wallets):**

| # | Slice | Depends | Size |
| --- | --- | --- | --- |
| C1 | Provider selection spike: hands-on Turnkey vs Privy eval (the thing this spike could not do without signups) — policy pinning of approveAgent, Mini App auth flow, export UX | B | M |
| C2 | Telegram Mini App onboarding: one-tap wallet creation, bundled onramp, consent ceremony (agent + builder fee), key-export offer | C1 | L |
| C3 | Withdrawal-destination allowlist + user transparency surfaces (positions/PnL views, "verify our permissions yourself" deep links) | C2 | M |

## 11. Open questions

1. **Agent-count cap:** live probes contradict the documented
   1+3+2/subaccount formula (§1). What is the real cap? (Testnet probe, A1.)
2. **scheduleCancel eligibility:** does the $1M cumulative-volume gate from
   secondary sources exist? (Testnet probe, A1.) Also: policy for flattening
   *positions* (not just orders) on executor death.
3. **Gray-zone L1 actions:** are `subAccountTransfer`/`createSubAccount`
   accepted from agent signers? Changes the subaccount-per-strategy design
   space and the threat model. (Testnet probe, A1.)
4. **Turnkey vs Privy:** decided hands-on in C1 — key criteria: policy
   pinning of `approveAgent` to our agent address (Turnkey verified-in-docs;
   Privy policy engine is Enterprise-gated), Mini App auth DX, funding modal,
   export UX, pricing at our scale.
5. **Builder fee level:** 2–5bp within the 10bp perp cap — priced against
   copy-trade round-trip costs; needs modeling with real sizing (product
   spike).
6. **Regulatory:** the §9 ladder — which jurisdictions to serve at Phase B,
   what geofencing evidence standard, whether builder-fee-only monetization
   still reads as "for compensation" (it does, assume yes). Counsel, B1.
7. **Multi-account rate limits:** address-based budgets are per master
   account (1 req/1 USDC traded + 10k buffer) — fine per user, but confirm
   the executor's batching strategy under N users on one IP (IP weight is
   shared with ingest/stream; the existing `SharedWeightBudget` needs an
   execution lane).
8. **Vault mode:** HL vaults are the protocol-native pooled-copy primitive —
   explicitly out of scope (heaviest regulatory shape), revisit only with
   counsel.
