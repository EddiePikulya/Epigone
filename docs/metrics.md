# Metric definitions (issue #8)

Plain-language definitions of the fills-based fine metrics and the Bot
heuristics. The one-line "In plain words" sentences are written to be lifted
verbatim by the criteria builder (V1 spec, user story 8). Source of truth for
the math: `src/epigone/metrics/fine.py` and `src/epigone/metrics/bots.py` —
keep this file in sync with them.

## Where fine metrics come from

Fine metrics are computed from a Trader's **recent fill history** (the info
API serves roughly the last 2,000 fills; S3 bulk backfill for deeper history
is issue #9). Only perp fills count; spot trades and dust conversions are
ignored. The fine pass runs for **coarse-pass survivors** (Traders with a
profitable, active month — positive month PnL and nonzero month volume; a
tunable default gate) **and every tracked Trader**.

A metric can be **unavailable (NULL)** when the fill history can't support it
— a trader with no closed trades has no win rate; one active day can't have a
Sharpe. Unavailable never means zero, and screener surfaces must show
coarse-only Traders as such.

## The closed trade

Most fine metrics are per **closed trade**: all closing fills that share one
closing order, with the trade's PnL being the sum of their `closedPnl`
(before fees). Closing fills are position closes, flips (long→short or
reverse), liquidations, and market settlements. This is the same grouping the
ansem-bullpen vetting used, so the golden wallets reproduce their
independently verified win rates (`tests/test_golden_wallets.py`).

## Fine metrics

### Win rate
- **In plain words:** out of the trades this account closed, the share that
  ended in profit.
- **Definition:** closed trades with PnL > 0 divided by all closed trades.
  Breakeven trades count against the win rate. Unavailable without closed
  trades.

### Average win / average loss
- **In plain words:** the typical size of this account's winning trade versus
  its typical losing trade — big wins with small losses is the profile you
  want to copy.
- **Definition:** mean PnL of winning trades; mean absolute PnL of losing
  trades (reported positive). Breakeven trades join neither. Each side is
  unavailable until it has at least one trade.

### Sharpe
- **In plain words:** how steady the daily profits are — high means smooth
  earning, low means a rollercoaster that happens to end up positive.
- **Definition:** mean ÷ standard deviation of **daily realized PnL** (UTC
  calendar days from first to last perp fill; quiet days count as zero),
  annualized by √365. Unavailable when the window is a single day or the
  daily PnL never varies.

### Max drawdown
- **In plain words:** the deepest hole the account dug from its own peak —
  how much giveback you'd have sat through at worst.
- **Definition:** largest peak-to-trough fall of the cumulative realized-PnL
  curve over the fill window, in USD. Unrealized swings don't move it (fills
  only realize PnL).

### Trade count
- **In plain words:** how many trades the account closed in its recent
  history — more trades, more evidence the other numbers are real.
- **Definition:** number of closed trades (as grouped above) in the fill
  window.

### Average leverage (estimated)
- **In plain words:** roughly how many times the account's own money it puts
  into a typical trade.
- **Definition:** mean over closed trades of peak position value (largest
  `|startPosition| × price` among the trade's closing fills) ÷ the account
  value recorded by the coarse pass. Fills carry no margin data, so this is a
  copyability signal, not the exchange's leverage setting. Unavailable
  without a coarse account value.

### Maker/taker share
- **In plain words:** how often the account waits with resting orders (maker)
  versus paying up to take liquidity (taker) — very high maker share smells
  like a market-making machine.
- **Definition:** share of perp fills that did not cross the book
  (`crossed = false`), by fill count.

## Bot exclusion

A **Bot** (CONTEXT.md) is an account whose statistical profile indicates
automated market-making rather than copyable trading skill. Flagged accounts
keep their rows and metrics in the database but never appear in screener
results; a profile that stops matching the heuristics is unflagged on its
next fine refresh. Thresholds are calibrated against the ansem-bullpen R&D:
every real excluded account is caught, and all 15 vetted wallets clear each
threshold with wide margin (their maxima: 95.6% win rate, ~25 exits/day).

1. **Near-perfect win rate over many exits** — win rate ≥ 98% across ≥ 100
   closed trades. Humans realize losses; market-makers don't.
2. **Extreme exit frequency** — ≥ 200 closed trades per day across the fill
   window (bursts inside a single day are measured against a full day). The
   excluded HFT accounts ran ~440/day.
3. **PnL from static holdings** — ≥ $100k absolute month PnL (coarse pass)
   with ≤ 5 closed trades. The money is made by holding, not trading —
   nothing to copy. Skipped until the Trader has coarse metrics.
