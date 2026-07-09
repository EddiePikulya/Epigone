# ADR 0002: Python everywhere, as separate processes over a Postgres seam

Date: 2026-07-09
Status: accepted

## Context

Epigone's workload has three parts with different natural strengths: websocket/polling fan-in for tracked wallets (Go's home turf), metric computation over a ~40k-account universe (Python/numpy's home turf), and a menu-heavy Telegram dialog UX (best DX in TypeScript/grammY). We considered TypeScript everywhere, Go, full polyglot (best language per component), and a Python-core + TS-bot split.

Two facts frame the decision:

1. **Nothing is CPU-bound.** The universe scan is capped by Hyperliquid's 1200 weight/min rate limit (11+ hours per full pass in any language), and realtime tracking is capped by HL's websocket limits (max 10 unique users across user-specific subscriptions per IP), so the stream is either a ~60s polling loop or a modest global-trades-feed filter. Go's performance edge is never exercised at V1 scale.
2. **Telegram UX is framework-independent.** aiogram and grammY emit identical Telegram primitives; the framework choice affects developer ergonomics only, not what users see.

## Decision

One language — Python 3 + asyncio — across three separate processes communicating only through Postgres:

- `ingest/` — universe scan scheduler and Metric Library computation (numpy/pandas), on the official `hyperliquid-python-sdk`.
- `stream/` — tracked-wallet position watcher (polling first; global trades-feed filter when scale demands) writing to an alert queue table.
- `bot/` — aiogram (FSM dialogs for the screener builder), reads metrics, writes users/criteria/tracks, delivers alerts.

## Consequences

- One toolchain, no cross-language domain-model duplication; all criteria/metric semantics live in exactly one place.
- The Postgres schema is a deliberate seam: any single process (most plausibly `stream/`) can later be rewritten in Go against the same tables if a measured bottleneck appears — polyglot is graduated into, not architected in.
- We accept asyncio's fiddlier long-running-worker ergonomics (reconnects, silent task death) and mitigate with supervision/heartbeats.
- We forgo grammY's dialog DX; aiogram's FSM delivers the identical user-facing UX.
