# Ecosystem survey: APIs, data sources, and competitors (2026-07-09)

Research pass before writing the Epigone spec. Question: what existing APIs/CLIs/data sources can help, and who else is building in this space?

## 1. Data sources

### Official Hyperliquid (free — our serving path, per ADR-0001)
- **Info API** (`api.hyperliquid.xyz/info`): per-address `clearinghouseState` (weight 2), `userFills`/`userFillsByTime` (weight 20+, 2k fills/page), `portfolio` (windowed PnL/volume — the two-stage-scan coarse pass, see spec-defaults.md).
- **Websocket**: market-level trades feed (counterparty addresses included → firehose path); user-specific subscriptions capped at 10 unique addresses/IP.
- **Undocumented leaderboard**: `stats-data.hyperliquid.xyz/Mainnet/leaderboard` — used by client libraries (e.g. hyperliquid-go); our Universe seed. Risk: undocumented, could change without notice.
- **⭐ Official S3 archives** (major find for `ingest`):
  - `s3://hl-mainnet-node-data/node_fills_by_block` — every fill on the exchange streamed from a node (older formats: `node_fills`, `node_trades`).
  - `s3://hyperliquid-archive` — L2 book snapshots (`market_data/[date]/[hour]/...`) and `asset_ctxs/[date].csv.lz4`, updated ~monthly, no timeliness guarantee.
  - **Implication:** fine-metric computation (win rate, Sharpe, drawdown per account) can run as offline batch over bulk-downloaded fills for the *entire* Universe, bypassing the 1200/min API budget entirely. The rate-limited API is then only needed for incremental freshness and realtime tracking. To verify at build time: bucket access mode (requester-pays?), volume, lag.

### Third-party historical mirrors (backfill alternatives)
- **Reservoir** (via Hydromancer): free public S3 archive — fills, 1s OHLCV, **daily position & balance snapshots**, 20-level L2 depth, all markets incl. HIP-3.
- **Dwellir**: raw node archives (replica_cmds, node_fills…) from Jan 2025. **Artemis**: 3 open tables on S3 from Aug 2025. **Tardis.dev / 0xArchive / HyperliquidRPC**: paid historical APIs.

### Paid intelligence APIs (shortcut/validation, not core dependency)
- **Nansen API** (~$49/mo): documented HL leaderboard, positions, trades, smart-money endpoints. Could seed/validate the Universe if stats-data breaks.
- **HyperTracker API** (coinmarketman/hypertracker.io, free tier + paid): claims 1.5M+ wallets, unified API for traders/wallets/markets/vaults — closest to "Epigone's ingest as a service." Dependency/cost trade-off vs building on free official data.
- **Apify scrapers**: leaderboard/vault scrapes; last-resort fallback.

## 2. Competitive landscape (Telegram + HL trader tracking)

| Product | Shape | Overlap with Epigone |
| --- | --- | --- |
| **pvp.trade** (~50k MAU) | TG bot: trade HL from group chats, clans, copy/counter friends, leaderboards | Social **execution**; discovery is "your friends", not criteria-based screening |
| **Hyperbot** (hyperbot.network) | Whale tracker + one-click copy + TG alerts + web dashboard | Tracks *whales* (size-based), not user-defined criteria |
| **Dextrabot** | Find & copy top HL wallets, TG alerts, SL/TP on copies | Closest in pitch; "top wallets" curated by them, not by user's own metrics |
| **Cielo Finance** | Multi-chain wallet tracker with TG bot; HL on Pro plan | Generic wallet tracking; no HL-native trader-quality screening |
| **HyperTracker** | Web dashboard + paid API, 1.5M wallets | Data/analytics product, not a TG-first product |
| **Buildix / ASXN Hyperscreener / CoinGlass** | Web analytics dashboards / screeners | Screeners exist — but web-based, market-focused, not follow+alert loops |
| HyperEVM bot, HypurrQuant | TG execution/DCA/sniper bots | Different category (execution) |

**Read:** the space is active (validates demand), and every neighbor is either execution-first, whale-size-first, or web-dashboard-first. **No incumbent does user-defined-criteria screening → follow → realtime alerts as a Telegram-native loop.** That is Epigone's wedge; the spec should defend it (criteria expressiveness + alert quality) rather than competing on execution features incumbents already own.

## 3. SDKs / code
- **hyperliquid-python-sdk** (official — chosen, ADR-0002); **CCXT** also supports HL (fallback/cross-check).
- **hyperliquid-go** (cordilleradev) — reference if a Go stream rewrite ever happens; also documents the stats-data leaderboard call.
- **thunderhead-labs/hyperliquid-stats** — open-source HL stats infra worth reading for metric definitions.
- **Chainstack tutorial** "Hyperliquid on-chain activity tracker Telegram bot" — validates the exact V1 architecture.
- **Bullpen CLI** — R&D bench + phase-2 (Polymarket) + phase-3 (managed copy-trading) bridge; see ADR-0001 and research in ansem-bullpen repo.

## Verdict — what we actually use (decided 2026-07-09)

**Production path (all free, all Hyperliquid-official except one):** info API (poll/portfolio/fills), stats-data leaderboard (Universe seeding only — non-critical, quarantined), official S3 fills archive (offline fine-metrics backfill; Reservoir mirror as plan B), websocket trades feed (scale-up only, not V1).

**Dev-time only:** Bullpen CLI (metric cross-check harness; phase-2/3 bridge).

**Named fallbacks, not used:** Nansen / HyperTracker APIs (only if stats-data dies). **Ruled out:** Apify, Tardis, 0xArchive, Dwellir (redundant with S3), CCXT (redundant with official SDK), hyperliquid-go (unless Go rewrite). Competitors and open-source repos are intel/reading, not dependencies.

## Decision impact
- **ADR-0001 unchanged** (direct HL APIs in serving path) — strengthened: S3 archives are also official/direct.
- **spec-defaults.md updated**: fine-metrics backfill via S3 bulk data instead of rate-limited API paging where feasible.
- **New spec consideration**: differentiation = criteria expressiveness + alert quality (see §2).
