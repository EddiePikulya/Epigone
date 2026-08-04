# Ecosystem survey: APIs, data sources, and competitors (2026-07-09)

Research pass before writing the Epigone spec. Question: what existing APIs/CLIs/data sources can help, and who else is building in this space?

## 1. Data sources

### Official Hyperliquid (free — our serving path, per ADR-0001)
- **Info API** (`api.hyperliquid.xyz/info`): per-address `clearinghouseState` (weight 2), `userFills`/`userFillsByTime` (weight 20+, 2k fills/page), `portfolio` (windowed PnL/volume — the two-stage-scan coarse pass, see spec-defaults.md).
- **Websocket**: market-level trades feed (counterparty addresses included → firehose path); user-specific subscriptions (`webData3`, `orderUpdates`, `userEvents`, `userFills`, `clearinghouseState`, `allDexsClearinghouseState`) take a `user` param and work for **any** address. ~~capped at 10 unique addresses/IP~~ — **corrected 2026-07-30** (ADR-0006): the 10 is a *connection* cap, not an address cap. Real limits are **10 connections, 30 new connections/min, 1000 subscriptions, 2000 outbound messages/min, 100 in-flight posts** — all a **separate budget from the 1200 weight/min REST cap**, so a websocket lane costs no REST weight. `allDexsClearinghouseState` covers every dex in one subscription where the REST poller spends one `clearinghouseState` call per venue.

#### Websocket findings, measured 2026-08-02 (issue #157's two unknowns, settled)

Probed against the funded testnet harness and — where testnet was too quiet to reach the volumes in question — against mainnet market data (public, read-only, no keys, no account actions; the allowances are per-IP and account-independent).

- **The 2000 messages/min allowance is OUTBOUND-ONLY.** One connection was driven to **6702 inbound messages/min sustained over 200s (peak minute 7497)** — 3.3–3.7× the allowance — while sending nothing but a keepalive ping every 25s. Zero error frames, zero throttling, never disconnected. So the Trader ceiling is not set by message volume. ~~At 2 subscriptions per Trader against 1000, that is **499 Traders on one connection**~~ — **wrong, corrected 2026-08-03 below**: an undocumented per-IP cap of **15 unique users** binds ~33× earlier than the subscription cap ever does.
- **There IS a ping/pong, and there IS an undocumented ~60s idle timeout.** `{"method": "ping"}` → `{"channel": "pong"}`. A connection with no traffic **in either direction** is closed at ~60s: three connections (zero subscriptions; one quiet user subscription; one that pinged only at t=0) were all cut at 60.6s with close code 1006 — abrupt, no close frame. Inbound traffic **does** reset the timer (a connection receiving ~106 msg/min survived 240s sending nothing after subscribe), so a busy market subscription incidentally keeps the socket open. Epigone pings anyway rather than depend on someone else's feed for its own liveness.

Two further findings from the same probes, worth carrying:

- **`allDexsOrderUpdates` does not exist** — the server rejects it as unparseable. `orderUpdates` takes no `dex` and is account-wide as it stands, so a Trader is still covered by 2 subscriptions total (`allDexsClearinghouseState` + `orderUpdates`).
- **`allDexsClearinghouseState` pushes on a ~5s cadence even for a completely idle account** (43 pushes in 210s on an account with no positions), carrying absolute state each time — it is not a pure change feed. Whether a change ALSO triggers an immediate push was not determined: settling it needs an account that actually trades during the probe, which is #158's measurement with the funded harness. Until it is, the "sub-second latency" figure in ADR-0006 should be read as an upper bound argument (the transport allows it), not a measured property.
- **Undocumented leaderboard**: `stats-data.hyperliquid.xyz/Mainnet/leaderboard` — used by client libraries (e.g. hyperliquid-go); our Universe seed. Risk: undocumented, could change without notice.
- **⭐ Official S3 archives** (major find for `ingest`):
  - `s3://hl-mainnet-node-data/node_fills_by_block` — every fill on the exchange streamed from a node (older formats: `node_fills`, `node_trades`).
  - `s3://hyperliquid-archive` — L2 book snapshots (`market_data/[date]/[hour]/...`) and `asset_ctxs/[date].csv.lz4`, updated ~monthly, no timeliness guarantee.
  - **Implication:** fine-metric computation (win rate, Sharpe, drawdown per account) can run as offline batch over bulk-downloaded fills for the *entire* Universe, bypassing the 1200/min API budget entirely. The rate-limited API is then only needed for incremental freshness and realtime tracking. To verify at build time: bucket access mode (requester-pays?), volume, lag.

#### Websocket findings, measured 2026-08-03 (issue #168, the order seam's design inputs)

Probed against mainnet, read-only: public market data plus public account state, no keys and no signed actions. Reproducible as `scripts/testnet_ws_probe.py orders` / `users`. All four findings contradict something the repo previously believed, so each is stated with the observation that settles it.

- **⚠️ There is an undocumented per-IP cap of 15 UNIQUE USERS across all user-scoped subscriptions**, and it is the binding constraint on every websocket lane — not the 1000-subscription cap. Subscribing a 16th address returns `{"channel":"error","data":"Cannot track more than 15 total users."}`. Measured properties:
  - **Per IP, not per connection.** One connection holding 14 users; a brand-new user on a *second, freshly opened* connection was refused with the same error.
  - **It counts distinct addresses, not subscriptions.** A second subscription type (`allDexsClearinghouseState`) for an already-tracked user was accepted; the same user subscribed again on a *different* connection was also free. So splitting one Trader's feeds across connections costs no extra allowance.
  - **Unsubscribing frees a slot immediately**; closing a connection frees all of its slots within ~2s (re-tested at 2s: accepted). A reconnect therefore cannot lock the lane out of its own addresses.
  - This retires the "499 Traders on one connection" figure in ADR-0006 and in `epigone.ws.lane`. The real ceiling for the whole process, on one IP, is **15 Traders** — which today's tracked set can already exceed (15 wallets/User × 2–3 Users).
- **`orderUpdates` frames do NOT say which user they are about.** The payload is `{"channel":"orderUpdates","data":[{"order":{coin, side, limitPx, sz, oid, timestamp, origSz, cloid}, "status", "statusTimestamp"}, …]}` — no `user` field at any level, unlike `allDexsClearinghouseState`, which carries one. On a connection subscribed to several users the frames are therefore **unattributable**: the transport gives you the order and withholds whose it is. One connection per Trader is the only way to attribute an order update, which is what ADR-0008 decides on.
- **`webData3` is not an alternative attribution route.** It exists (`webData2` does not — the server rejects it as unparseable), but its payload carries only `perpDexStates` and `userState`: no `user` field, and **no open orders at all**. So it neither names its subject nor carries the resting book.
- **The order feed is a firehose for an active account.** One market-making address alone produced **442 frames / 1471 order updates in 60s** (~24/s), with statuses `open` 649, `canceled` 628, `badAloPxRejected` 105, `iocCancelRejected` 68, `filled` 21. Across 15 such addresses on one connection: 8066 frames in 90s. This is the *excluded-Bot* end of the population (the probe harvests the busiest addresses off the public trades feed, which is precisely how a market maker is spotted) rather than a copyable leader, but any order-persistence seam has to survive it — which is why ADR-0008 carries a rate ceiling and a 24h retention rather than position events' 7 days.

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
