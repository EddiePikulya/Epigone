# Polymarket spike: prediction-market mode research (2026-07-19)

Investigation for #59: what core to build prediction-market mode on (Bullpen CLI vs
direct Polymarket APIs), the domain-model mapping, the mode seam, and the ticket
backlog. Decision summary lives in ADR-0004; this doc is the evidence and the plan.

## 1. Probe evidence (all run 2026-07-19, no login, `BULLPEN_READ_ONLY=1`)

Note for local re-runs: this machine's ISP DNS-poisons `*.polymarket.com`
(see `ansem-bullpen/bullpen-cli/research.md`); probes ran through
`tools/clean-dns-proxy.py` (`HTTPS_PROXY=http://127.0.0.1:18080`). The production
Hetzner box resolves cleanly — this is a workstation problem, not a serving-path risk.

### Bullpen CLI (`bullpen 0.1.112 (Alpha)`) — what works without login

| Command | Verified result |
| --- | --- |
| `polymarket data leaderboard [--period day/week/month/all]` | Top-trader PnL/volume rows with wallet addresses. The newer `--time-period 7d` (Bullpen-indexed) path returned `NETWORK_TIMEOUT` twice, then legacy `--period week` worked — the Bullpen-indexed endpoints are flaky. |
| `polymarket data leaderboard --sort copyability` | Bullpen-computed `copyability_score/tier`, `insider_score`, `is_bot`, `is_farmer`, `max_drawdown` per wallet. Genuine value-add (no public equivalent), no login needed today. |
| `polymarket positions --address <w>` | Full open positions: market, outcome, shares, `avg_price`, `current_price`, `unrealized_pnl`, `condition_id`, `redeemable`. Help text warns: *"pass only when support asks you to inspect a specific public wallet"* — inspecting arbitrary wallets is explicitly not the supported use. |
| `polymarket activity --address <w> --type trade` | Trade history: outcome, side BUY/SELL, price, shares, `usdc_size`, `condition_id`, timestamp, tx hash. Also `REDEEM`/`REWARD`/`MAKER_REBATE` records. |
| `polymarket data profile <w> --trades` | Profile + recent trades, but thin: `win_rate`, `volume`, `account_age_days` all null; `trades_count` wrong (14 for a whale with 8,555 trades per `wallet-stats`). |
| `polymarket wallet-stats <w>` | Rich Bullpen-server stats: activity bounds, `avg_hold_duration_hours` (1d/7d/30d/lifetime), `avg_position_size`, `avg_trades_per_day`, `is_likely_bot`, `trader_tier`, win rates, `copyability_score`, `insider_score`, category expertise. |

Requires `bullpen login` (personal device auth): `tracker` (follow wallets, feeds,
alerts, copy), `feed`, `data smart-money`, portfolio, all trading.

Dependency posture: the CLI is closed-source (Homebrew tap binary) with no
published license terms, SLA, pricing, or rate limits for third-party server use —
Bullpen monetizes the trading platform, and nothing suggests running their Alpha
CLI as the data backend of a public multi-user bot is a sanctioned (or stable) use.
As a personal research tool it is ordinary usage.

### Direct Polymarket APIs — the same data, unauthenticated

The CLI's read output says `"source": "polymarket"`: for every serving-path need, the
CLI is a veneer over Polymarket's own public APIs. Verified directly with `curl`:

| Endpoint | Verified result |
| --- | --- |
| `data-api.polymarket.com/positions?user=<w>` | Superset of the CLI's positions output: adds `realizedPnl`, `percentRealizedPnl`, `totalBought`, `initialValue`, `oppositeAsset`. |
| `data-api.polymarket.com/trades?user=<w>` | Same trade records the CLI renders (side, price, size, outcome, conditionId, timestamp). |
| `data-api.polymarket.com/v1/leaderboard?window=1d/1w/1m/all&rankType=pnl/vol&limit=50&offset=N` | Pages of 50; enumerates to rank ~10,050 then clamps (offset=20000 returned the same last rank 10050). Two rank types × four windows → union Universe seed of 10k–40k wallets. |
| `gamma-api.polymarket.com/events?slug=<event>` / `markets?...` | Market/event metadata incl. resolution: `closed`, `closedTime`, `outcomes`, `outcomePrices` (`["0","1"]` after resolution), `umaResolutionStatuses`. |

Observed end-to-end consistency check: a wallet's `positions` showed a $1.145M
England-Yes position at `current_price: 0`, and the Gamma event for that market showed
`closed: true, outcomePrices: ["0","1"]` — resolution flows through both surfaces.

## 2. API landscape (web research, verified live where noted)

Official docs: https://docs.polymarket.com. Per the docs, **Gamma and the Data API
are fully public — no authentication**; CLOB is public for market data (auth only
for trading).

| API | Base | Role | Rate limits (documented, Cloudflare-throttled not rejected) |
| --- | --- | --- | --- |
| Gamma | `gamma-api.polymarket.com` | markets/events/tags/search, resolution metadata | 4,000 req/10s general; `/markets` 300/10s; `/events` 500/10s |
| Data API | `data-api.polymarket.com` | per-wallet positions/trades/activity/value, holders, leaderboard | 1,000/10s general; `/trades` 200/10s; `/positions` 150/10s; `/closed-positions` 150/10s |
| CLOB | `clob.polymarket.com` | books, prices, midpoints, price history | `/book`, `/price`, `/midpoint` 1,500/10s |
| WSS | `ws-subscriptions-clob…/ws/market` (public), `/ws/user` (own-account auth only), `ws-live-data.polymarket.com` (RTDS) | realtime | — |

Key facts beyond the probes above:

- **Leaderboard:** documented params are `category` (11 values: OVERALL, POLITICS,
  SPORTS, CRYPTO, …) × `timePeriod` (DAY/WEEK/MONTH/ALL) × `orderBy` (PNL/VOL),
  `limit ≤ 50`. Documented offset cap is 1,000 but probing shows it serves to rank
  ~10,050 before clamping — treat depth >1k as soft. Union across boards seeds a
  universe of tens of thousands of wallets.
- **Trade history depth:** `/trades` offset is capped at 10,000 and **rejects (400)**
  past it — page inside `start`/`end` timestamp windows for deep history; each
  window gets a fresh offset budget. `takerOnly` defaults **true**; pass
  `takerOnly=false` to see maker fills (⇒ a maker-share-like metric *is* computable
  later if bot-exclusion tuning wants it). `/activity` distinguishes REDEEM from sells.
- **Realized PnL:** `/closed-positions?user=` gives per-market realized PnL
  (limit ≤ 50/page); `user-pnl-api.polymarket.com/user-pnl` serves the
  profile-chart PnL time series (works unauthenticated, but only semi-documented —
  don't put it in the serving path without a fallback).
- **Realtime for arbitrary wallets:** the CLOB user channel only covers one's own
  account. But the RTDS (real-time data stream) websocket
  `wss://ws-live-data.polymarket.com` topic
  `{"topic":"activity","type":"trades"}` — verified live — streams **every trade on
  the platform** unauthenticated, with `proxyWallet`, side, price, size, market
  fields, tx hash. It powers the site's live feed (de-facto stable) but is
  **undocumented** → use as the stream upgrade with `/trades?user=` polling as the
  reconciling fallback. This mirrors ADR-0002's perp plan ("global trades-feed
  filter when scale demands"). Practical polling math: at 150 req/10s on
  `/positions`, hundreds of tracked wallets fit comfortably at 30s cadence.
- **Resolution push:** WS market channel has a `market_resolved` event
  (`custom_feature_enabled: true`); Gamma polling remains the simple path;
  positions flip `redeemable: true` with `curPrice` 0/1. On-chain ground truth is
  the UMA optimistic oracle (undisputed ≈2h after proposal; disputed 4–6 days).
- **On-chain fallback is poor right now:** Polymarket migrated to v2 contracts
  (April 2026) and Goldsky-hosted public subgraphs are deprecated/incorrect for
  current data — the Data API is the canonical read path; Dune/Allium for offline
  audit only.
- **ToS/geo:** read APIs are positioned as public infrastructure (official builder
  program, "public good" language on the data pages). Geo-restrictions apply to
  **order placement**, not data ("data and information is viewable globally") —
  Germany (Hetzner prod) is close-only for *trading*, irrelevant to reads. Open
  item: eyeball the current browser-rendered ToS text before relying on it (the
  spike couldn't extract it verbatim).
- **SDKs:** official v2 clients exist (`py-clob-client-v2` etc.) but the Data API
  is plain JSON-over-HTTPS — a thin in-repo gateway (as with Hyperliquid) beats a
  dependency. Several competitor products (polymarketanalytics.com/traders, Wallet
  Master Radar, copy bots advertising 1–3s replication) run on these same public
  endpoints — validates both demand and API sufficiency.

## 3. Domain-model mapping (perp → prediction)

The mapping below is an *analysis* tool (what generalizes, what's new, what
doesn't apply) — not a schema plan. The schema conclusion (§5, ADR-0004) is the
opposite of a mirror: prediction mode gets its own native tables and metric
vocabulary, because the domains differ enough that a shared shape would serve
neither well.

| Perp concept | Prediction analog | Notes |
| --- | --- | --- |
| Trader = HL account | Trader = Polymarket proxy wallet | data-api also returns `name`/`pseudonym` → display name for free. Same 0x address on both venues is two distinct Traders. |
| Position = (coin, side, size_usd, leverage, entry) | Position = (market/conditionId, outcome, shares, avg_price 0–1, current_price) | No leverage, no liquidation. No short side: bearishness = buying the opposite outcome (in neg-risk events — multi-outcome events whose outcomes are structured as complements — selling one outcome equals buying the rest). `Side` enum → `Outcome` label. |
| Fill | Trade record (`side BUY/SELL`, price, shares, usdcSize, timestamp, txHash) | Plus non-trade activity: `REDEEM`, `REWARD`, `MAKER_REBATE` — rebate/reward income matters for bot detection, not for trade PnL. |
| Round-trip / closed trade (#58: non-flat → flat episode) | Position episode on one outcome token: first buy → (sold to zero \| **market resolves**) | Resolution is the natural close #59 anticipated: shares settle at 1 (win) or 0 (loss). Same episode framing as #58 — align the definitions. |
| Realized PnL = closedPnl on fills | Realized PnL = sell proceeds + resolution payout − cost basis | data-api positions carry `realizedPnl`/`cashPnl` per position, so we can cross-check our own computation. |
| Liquidation | — (does not exist) | |
| Funding | — (does not exist) | |

### Metric Library mapping

**Coarse (from leaderboard, zero marginal cost):** windowed `pnl` and `vol` map
directly onto the existing `coarse_metrics` rows (windows 1d/1w/1m/all ↔
day/week/month/allTime). Not available coarsely: ROI, account value (leaderboard
omits them; portfolio value needs a per-wallet call — treat as fine-pass data).

**Fine (from /positions + /trades per wallet):**

| Metric | Transfers? | Prediction definition |
| --- | --- | --- |
| win_rate | yes | closed episodes (sold-out or resolved) with PnL > 0 ÷ all closed episodes |
| avg_win / avg_loss / realized_pnl / trade_count | yes | same shapes over episodes |
| roi | yes (redefined) | realized PnL ÷ capital deployed (Σ `totalBought` cost basis from positions). Perp ROI comes deposit-adjusted from the leaderboard; prediction ROI we compute ourselves in the fine pass. |
| max_drawdown / sharpe | yes | over the realized-PnL time series, as in perps |
| avg_hold_seconds | yes | first buy → flat-or-resolution; aligns with #58/#48 episodes |
| avg_leverage | **no** | concept absent — NULL for prediction rows |
| maker_share | **defer** | computable (`/trades?takerOnly=false` exposes maker fills) but not needed for V1 screening; revisit with bot-exclusion tuning |
| resolution win rate | **new** | resolved episodes won ÷ resolved episodes (excludes traded-out) |
| calibration / avg entry price of wins vs losses | **new** | is the trader buying cheap correctness (entry 0.30 → won) or expensive certainty (entry 0.95)? Computable from episodes; strong "skill" signal. |
| unique_markets, avg time-to-resolution | **new** | breadth and patience signals, cheap to compute |
| category expertise | **new** | per-category (POLITICS/SPORTS/CRYPTO/…) PnL/win rate; the leaderboard API exposes 11 categories natively. "Best politics trader" is a screen perps can't express — lean into it. |

**Bot exclusion transfers, with new tells:** extreme trade counts (the probed
7d-leaderboard #1 did 8,555 trades in 14 days — `wallet-stats` activity bounds
2026-07-01 → 2026-07-14), `MAKER_REBATE`/`REWARD` income share,
holding both outcomes of the same market simultaneously, sub-hour average holds.
Bullpen's `is_likely_bot`/`is_farmer`/`copyability_score` serve as an offline
calibration reference for our heuristics (R&D only, per ADR-0004).

## 4. Alerts & copy semantics

Alert events (diff of successive `/positions` polls, same shape as the perp poller):

- **enter** — new outcome-token position appears.
- **scale_in / scale_out** — shares change ≥25% on the same outcome (reuse threshold from #10).
- **exit** — position sold to zero before resolution (realized PnL from sells).
- **resolve** — a held market resolved (from Gamma): won (payout = shares × $1) or lost (position → 0). Replaces perp `close` for held-to-resolution episodes; `flip` has no analog.

**Is copying actionable?** Yes, with honest caveats the alert must carry: the
trader's avg price vs the current price (the odds may have already moved — copying
at 0.90 what the trader bought at 0.55 is a different bet), and time-to-end-date
(near-resolution markets leave no room to be early). Slippage/liquidity is visible
via the CLOB book if needed later. Epigone stays **screen + alert**; auto-copy
remains a later phase (ADR-0001).

## 5. Schema impact (prediction-native tables — detail in ADR-0004)

Share only what is venue-independent; give the prediction domain its own tables
rather than NULL-padding the perp ones:

- **Shared:** `users`, `allowlist`, `tracks`, delivery mechanics. `traders` PK
  becomes `(address, market_type)` (identity, display name, bot flags are shared
  concepts; perp bookkeeping columns stay perp-only). `criteria` gains
  `market_type`.
- **New prediction-native tables:** `prediction_markets` (question, event,
  category, end date, resolution state), `prediction_positions` (outcome-token
  snapshots the stream diffs), `prediction_coarse_metrics` (address × window ×
  **category** → pnl/vol/rank), `prediction_metrics` (native fine vocabulary),
  `prediction_alerts` (enter/scale_in/scale_out/exit/resolve; mirrors delivery
  bookkeeping columns so one delivery loop drains both queues).
- **Untouched:** all perp pipeline tables (`position_snapshots`,
  `coarse_metrics`, `fine_metrics`, `position_alerts`, `fine_trades`,
  `fine_open_episodes`, `position_poll_state`) — no PK migrations on live perp
  data, ~zero perp regression risk.
- New: Polymarket gets its own rate limiter; `rate_budget` (HL weight system) is
  untouched.

## 6. Ticket backlog (vertical slices, blocking edges)

| # | Slice | Depends on | Size | Delivers |
| --- | --- | --- | --- | --- |
| P1 | **PolymarketGateway** (read-only): leaderboard, positions, trades, event/resolution lookup, own rate limiter, fake for tests | — | M | The provider seam, probed shapes typed |
| P2 | **Schema for prediction mode**: `(address, market_type)` on traders + `market_type` on criteria (backfill `'hyperliquid'`); new `prediction_markets` / `prediction_positions` / `prediction_coarse_metrics` / `prediction_metrics` / `prediction_alerts` tables | — | M | Native schema ready; perp tables untouched |
| P3 | **Prediction Universe seed**: leaderboard scan (11 categories × 4 windows × 2 rank types) → traders + prediction_coarse_metrics incl. per-category rank; screener ranks prediction traders by windowed/category PnL & volume | P1, P2 | M | First end-to-end vertical: screen prediction traders, incl. "best POLITICS this month" |
| P4 | **Prediction fine metrics**: episode extraction (align #58), resolution win rate, calibration, time-to-resolution, unique markets, hold time, realized PnL, drawdown, Sharpe; bot exclusion (rebate share, two-sided holdings, trade frequency) | P3 | L | Full native Criteria vocabulary for prediction mode |
| P5 | **Track + position alerts**: stream polls tracked prediction wallets, enter/scale/exit alerts, mode-aware rendering (prices in ¢, outcome labels) | P1, P2 | M | Tracking loop parity with perps |
| P6 | **Resolution alerts**: Gamma polling for tracked open positions' markets → resolve alerts with won/lost + payout | P5 | S | The prediction-native alert perps don't have |
| P7 | **Mode-aware Criteria UX**: "Perps or Predictions?" branch in the builder, per-mode metric list (incl. category picker), mixed tracked-list badges | P3 | S–M | Mode surfaced to users |
| P8 | *(later)* Realtime upgrade: RTDS trades-firehose filter for tracked wallets (polling stays as reconciler) + copy-context enrichment (price drift vs entry, time-to-end in alerts) | P5 | M | Sub-5s alerts, copyability judgment aids |
| P9 | *(later, R&D)* Bullpen `wallet-stats` offline cross-check harness for bot-exclusion calibration | P4 | S | Heuristic validation, not serving path |

Suggested order: P1 ∥ P2 → P3 → P5 → P4 → P6 → P7 (P3 before P5 only because a
seeded universe makes tracking demo-able; P5 technically needs just P1+P2).

## 7. Effort, risks

Rough total: ~2 of the 6 core slices are L, rest S/M — comparable to a compressed
replay of the perp pipeline over a friendlier API (positions come pre-aggregated;
no weight budget, no S3 question, leaderboard pages freely).

| Risk | Read |
| --- | --- |
| Undocumented/unversioned data-api | Same posture as HL's `stats-data` leaderboard (accepted in ADR-0001); gateway seam contains the blast radius. |
| Leaderboard depth ~10k per board (probed; documented cap is only 1k — treat extra depth as soft) | Universe smaller than HL's 40k but the union across the 88 category × window × rank-type boards + tracked-wallet referrals is ample; not a blocker. |
| Rate limits documented but unverified under our load | Docs promise generous, throttle-not-reject limits (§2); spike probes never approached them. Own limiter + backoff from day one; confirm behavior at scan volume in P3. |
| Reward/rebate farmers polluting leaderboards | Bot-exclusion (P4) is load-bearing; volume boards especially farmed. |
| Polling latency (~30s) for V1 alerts | Accepted at first, matches perp poller; the RTDS trades firehose (undocumented) is the low-latency upgrade path — P8, not Polygon indexing. |
| RTDS firehose + `user-pnl-api` are undocumented | Keep them out of the V1 serving path; adopt firehose only with the documented polling fallback wired in. |
| Resolution lag (UMA propose → finalize) | Alert on Gamma `closed` + `outcomePrices`; treat `umaResolutionStatuses` as advisory. |

## 8. CONTEXT.md glossary update proposal

Proposed additions (apply when the first prediction ticket lands, not in this spike):

| Term | Definition |
| --- | --- |
| **Market Mode** | The domain a Criteria operates over: `perp` (venue: Hyperliquid) or `prediction` (venue: Polymarket). Modes map 1:1 to venues today, and the `market_type` column stores the venue (`'hyperliquid'`/`'polymarket'`). Users, tracking, and alert delivery are shared across modes; the metric vocabulary and alert semantics are per-mode. |
| **Prediction Market** | A question with a defined resolution (e.g. "Will X win?") whose outcome shares trade at a price 0–1 that doubles as the market's probability estimate. Lives inside an Event on Polymarket. |
| **Outcome** | One tradable side of a prediction market (Yes/No, or a named side). A share of an outcome settles at $1 if that outcome wins, $0 otherwise. |
| **Resolution** | The moment a prediction market's outcome becomes final and shares settle. For prediction-mode Traders, resolution is a natural round-trip close (cf. #58). |
| **Redeem** | Claiming the $1/share payout of a winning outcome after resolution. |
| **Category** | Polymarket's topical grouping of markets (Politics, Sports, Crypto, …). A native screening dimension in prediction mode: Criteria can rank Traders within a Category. |

Proposed amendments:

- **Trader** already reads "a Hyperliquid account / prediction-market wallet" —
  extend with "(a Polymarket proxy wallet; the same address on different venues is
  a different Trader)".
- **Position Alert** is currently defined by the perp events (opens, closes, flips
  a position) — generalize to "opens, materially changes, closes, or (prediction
  mode) sees resolved a position", since `flip` has no prediction analog and
  `resolve` has no perp analog.
