# ADR 0004: Prediction-market mode on direct Polymarket APIs, behind a market-provider seam

Date: 2026-07-19
Status: proposed

## Context

Epigone's vision (CONTEXT.md) covers perp traders *and* prediction-market traders;
only perps ship today. ADR-0001 chose direct Hyperliquid APIs over the Bullpen CLI
for perps but explicitly deferred the Polymarket integration decision, keeping the
Bullpen CLI as "the candidate integration for phase 2 (Polymarket)". Issue #59
re-opened that question for the prediction domain specifically: the Bullpen CLI is
purpose-built for prediction markets, so it deserved a real evaluation before we
default to "direct again".

The spike (docs/research/polymarket-spike.md) probed both candidates live:

- **Bullpen CLI (0.1.112, Alpha), no login:** leaderboard, per-wallet positions,
  per-wallet trade history, and rich server-derived `wallet-stats`
  (copyability/insider scores, `is_likely_bot`, hold durations) all work read-only.
  But its read output is labeled `"source": "polymarket"` — for every serving-path
  data need it is a veneer over Polymarket's own public APIs. Its differentiated
  features (tracker/feeds/alerts, smart-money, copy subscriptions, trading) sit
  behind a personal `bullpen login` and Bullpen's proxy — the exact ADR-0001
  concerns. `positions --address` help text says arbitrary-wallet inspection is a
  support-only escape hatch, the Bullpen-indexed endpoints timed out intermittently
  during probing, and driving a CLI binary as a subprocess from a multi-user server
  is an awkward serving-path shape for an Alpha tool we don't control.
- **Direct Polymarket APIs, unauthenticated:** `data-api.polymarket.com` serves
  positions, trades, activity, closed-position PnL, and a paginated leaderboard
  (~10k wallets per window/rank-type, 11 categories) for arbitrary wallets;
  `gamma-api.polymarket.com` serves market/event metadata including resolution
  (`closed`, `outcomePrices`, `closedTime`); the CLOB API serves prices/books and
  a public market-data websocket, and the RTDS (real-time data stream) websocket
  streams a platform-wide trades feed (undocumented topic, verified live). Documented rate limits are
  generous (`/positions` 150 req/10s, `/trades` 200 req/10s, Gamma 4,000 req/10s)
  and throttle rather than reject. The direct position payloads are a superset of
  what the CLI shows (adds `realizedPnl`, `totalBought`, `initialValue`).

Epigone's architecture already has the seams a second market needs: a
Protocol-based gateway (`HyperliquidGateway`), three processes communicating only
through Postgres (ADR-0002), and a Metric-Library-driven screener. Nothing in the
schema anticipates a second market yet — `position_snapshots` keys on
`(trader_address, coin)`, `fine_metrics` bakes in perp-only columns, and `traders`
assumes one venue.

## Decision

### Core tooling: direct Polymarket APIs; Bullpen CLI stays an R&D tool

The serving path talks to Polymarket directly — the same shape as ADR-0001:

- **Universe discovery:** `data-api /v1/leaderboard` (11 categories × windows
  1d/1w/1m/all × rank types pnl/vol; probed to ~10k ranks per board, though the
  documented offset cap is 1k — treat the extra depth as soft).
- **Coarse metrics:** the same leaderboard rows (windowed PnL, volume).
- **Fine metrics:** `data-api /positions` + `/trades` per wallet (shares, avg/current
  price, realized+unrealized PnL, full trade history).
- **Resolution:** `gamma-api` events/markets (`closed`, `outcomePrices`).
- **Realtime tracking:** poll `data-api /positions` per tracked wallet (same
  polling shape as the perp `stream` process, and comfortable within 150 req/10s).
  The authenticated CLOB user channel cannot watch third-party wallets, but the
  RTDS websocket's platform-wide trades feed — filtered client-side by tracked
  wallets — is the later low-latency upgrade, exactly mirroring ADR-0002's perp
  plan ("global trades-feed filter when scale demands"). It is undocumented, so it
  ships only after polling works and stays as the reconciler beneath it.

The Bullpen CLI is *not* in the serving path, for the same reasons as ADR-0001 —
Alpha maturity, proxy routing, personal login for its best features, support-only
arbitrary-wallet access, and a closed-source binary with no published terms, SLA,
or rate limits for third-party server use — plus one new fact: its no-login reads
are Polymarket's public APIs anyway, so it adds a dependency without adding data. It stays a
personal R&D/vetting tool, where its `wallet-stats` (copyability/insider scores,
`is_likely_bot`) are a genuinely differentiated cross-check for tuning our own
bot-exclusion and vetting heuristics.

### Mode architecture: a market provider behind the existing seams, not a parallel pipeline

One new Protocol, `PolymarketGateway`, lives in `gateway/` beside
`HyperliquidGateway`. The existing processes stay:

- `ingest` runs a per-market seed/fine pass (leaderboard scan → coarse; wallet
  history → fine), selecting the gateway by market.
- `stream` polls tracked wallets per market and writes that mode's alert queue
  (`position_alerts` / `prediction_alerts`), both drained by one delivery loop.
- `bot` renders market-aware alerts and screener results; users, invite gate,
  tracking, delivery, and the health monitor are shared unchanged.

Rate budgeting is per-provider: `SharedWeightBudget` models Hyperliquid's weight
system and is not shared with Polymarket, which gets its own simple limiter.

### Schema: prediction-native tables; share only what is genuinely shared

Prediction markets are not perps with different labels. The position shape differs
(outcome shares priced 0–1 vs levered directional size), the entities differ
(markets that *resolve* are first-class; nothing in perps resolves), and the
metrics that make a prediction trader "best" differ (resolution win rate,
calibration, category expertise — not leverage or maker share). Forcing prediction
data through the perp tables would produce NULL-heavy hybrid rows, a `coin` column
holding outcome-token ids, and a metric vocabulary that serves neither venue well.
So the mode seam splits by concept, not by table reuse:

**Shared — the concept is venue-independent:**

- `users`, `allowlist`, `tracks`, and alert-delivery mechanics (attempts,
  `delivered_at`, muting, min-size floors) stay single.
- `traders`: PK becomes `(address, market_type)` — identity, display name, bot
  flags are shared concepts; the same 0x address on two venues is two Traders.
  Perp pipeline bookkeeping columns (`refresh_tier`, `fine_*`) simply stay
  perp-only.
- `criteria`: gains `market_type` — a Criteria screens exactly one mode, with that
  mode's Metric Library.

**Prediction-native — new tables designed for the domain:**

- `prediction_markets`: conditionId, question, event, category, end date,
  `closed`, winning outcome — resolution state lives here, and alerts render
  from it.
- `prediction_positions`: (address, outcome token) → shares, avg price, current
  price, redeemable — the tracked-wallet snapshot the stream process diffs.
- `prediction_coarse_metrics`: (address, window, **category**) → pnl, volume,
  rank. Category (POLITICS/SPORTS/CRYPTO/… — 11 on the leaderboard API) has no
  perp analog and is a first-class screening dimension: "best politics traders
  this month" is a native query, not a filter bolted on later.
- `prediction_metrics`: the native fine vocabulary — resolution win rate,
  calibration (entry price of wins vs losses), avg time-to-resolution, unique
  markets, hold time, plus the venue-generic shapes (realized PnL, ROI over
  deployed capital, win rate, drawdown, Sharpe).
- `prediction_alerts`: enter/scale_in/scale_out/exit/**resolve** with
  outcome/price/payout fields; it mirrors the delivery bookkeeping columns so the
  bot's delivery loop drains both queues through one code path.

The perp tables are untouched — no PK migrations on live production data beyond
the `traders`/`criteria` discriminators — so perp regression risk is ~zero.

### UX: mode is a property of a Criteria, not of the user

No global toggle. The screener builder asks "Perps or Predictions?" first and then
offers that mode's metric vocabulary (including category for predictions); tracked
Traders carry their market type and render with mode-appropriate alerts (outcome
labels, prices in cents, resolution countdowns). A user can screen and track both
venues side by side.

## Consequences

- No third-party dependency, auth, or proxy in the prediction serving path;
  the same operational posture as perps (free, unauthenticated, undocumented-API
  risk accepted and mitigated by the gateway seam).
- Epigone builds its own prediction analytics (closed-trade extraction, resolution
  win rate, bot exclusion) rather than consuming Bullpen's — that work is the
  product's moat, and Bullpen's scores remain available offline as a calibration
  reference.
- Two metric vocabularies are maintained deliberately: the screener dispatches to
  perp or prediction tables per Criteria instead of querying one hybrid table.
  The cost is a second Metric Library section; the win is that each mode's
  metrics can be what makes *that* venue's traders worth copying, not the
  intersection of two domains.
- Realtime latency for prediction alerts is polling-bound (~30s) at V1, matching
  the perp poller; the RTDS trades-firehose upgrade can take it to seconds without
  new infrastructure. On-chain Polygon indexing is *not* the fallback — Polymarket's
  v2 contract migration (April 2026) deprecated the public subgraphs, so the Data
  API is the canonical read path.
- Geo/ToS posture: read data is globally viewable and the read APIs are positioned
  as public builder infrastructure; Polymarket's geo restrictions apply to order
  placement (Germany, where prod runs, is close-only for *trading* — irrelevant to
  reads). Open item before implementation starts: eyeball the current
  browser-rendered ToS text, which the spike could not extract.
- Copy remains screen + alert: alerts carry the trader's entry price vs the current
  price and time-to-resolution so a follower can judge whether the trade is still
  copyable; auto-execution stays deferred to the eventual copy-trading phase
  (ADR-0001).
- `market_type` stores the venue (`'hyperliquid'` / `'polymarket'`); "Market Mode"
  (perp / prediction) is the user-facing term, mapping 1:1 to venue today. Kalshi
  or other venues would follow the same seam (a new venue value, a new gateway
  Protocol), but are out of scope until Polymarket proves out.
