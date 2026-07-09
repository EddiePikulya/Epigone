# ADR 0001: Direct Hyperliquid APIs as the data foundation (not Bullpen CLI)

Date: 2026-07-09
Status: accepted

## Context

Epigone's precursor MVP (`ansem-bullpen` repo, `bullpen-cli` worktree + `~/.bullpen-watch/watch.py`) was built on the Bullpen CLI: it discovered a 40,457-account universe from open Hyperliquid leaderboard stats, vetted 15 wallets by pulling fill history through `bullpen`, and polls `bullpen hyperliquid status` per wallet every minute for position alerts.

Bullpen CLI is attractive (agent-friendly JSON, one interface covering Hyperliquid *and* Polymarket, built-in tracker/copy-trading), but it is Alpha software, routes through Bullpen's proxy servers, and its best endpoints require a personal login — a poor fit for the serving path of a public multi-user Telegram bot.

## Decision

Epigone's server talks to Hyperliquid directly:

- **Universe discovery:** open Hyperliquid leaderboard/stats data (proven by the 40k-account scan).
- **Metrics:** documented public info API (`userFillsByTime`, `clearinghouseState`, …) per address.
- **Realtime tracking:** Hyperliquid websocket subscriptions per tracked address.

Bullpen CLI is kept as a personal research/vetting tool and is the candidate integration for phase 2 (Polymarket) and the eventual copy-trading phase.

## Consequences

- No third-party dependency, auth, or ToS risk in the serving path; HL info API is free and unauthenticated.
- Epigone must build its own ingestion (scan, fills paging at 2,000/page, websocket fan-in) rather than reusing Bullpen's tracker.
- The Polymarket phase will be a separate integration decision when it arrives.
- Vetting heuristics learned via Bullpen R&D (bot exclusion, closed-trade win-rate grouping) transfer into the Metric Library.
