# Epigone — Domain Glossary

> Ubiquitous language for the Epigone project. Glossary only — no implementation details.

| Term | Definition |
| --- | --- |
| **Epigone** | A Telegram bot that finds and tracks the best Hyperliquid perp traders and prediction-market traders, where "best" is defined by each user's own criteria — there is no single global leaderboard. Named after "epigone" (imitator of the greats): the product's thesis is *cloning the best to be the best*. |
| **Criteria** | A user-defined definition of "best trader": a set of filters over the Metric Library plus a sort key (screener-style). Each user can have their own; the same trader can rank #1 for one user and be invisible to another. |
| **Metric Library** | The fixed set of per-Trader metrics Epigone computes (e.g. PnL, win rate, Sharpe, max drawdown, trade count, account age, average leverage — exact list to be specified) over defined timeframes. Criteria are expressed exclusively in terms of these metrics. |
| **Trader** | An externally observable market participant (e.g. a Hyperliquid account / prediction-market wallet) that Epigone evaluates and tracks. Not a user of the bot. |
| **User** | A person interacting with Epigone through Telegram. Open to anyone with a Telegram account. |
| **Universe** | The full set of candidate Traders Epigone knows about and scores (initially discovered from open Hyperliquid leaderboard stats; ~40k active accounts as of July 2026). Criteria filter the Universe; Users track Traders from it. |
| **Bot (excluded Trader)** | An account whose statistical profile indicates automated market-making rather than copyable trading skill (e.g. ~100% win rate over hundreds of exits, extreme fill frequency, or PnL from static holdings). Excluded from the Universe during vetting. |
| **Track** | A User's explicit, manual follow of a specific Trader (chosen from screener results or entered directly). Tracking is a stable User→Trader relationship; Criteria never add or remove tracked Traders on their own. |
| **Position Alert** | A realtime Telegram notification pushed to a User when a Trader they track opens, closes, or flips a position. The unit of value of the tracking half of the product. |
