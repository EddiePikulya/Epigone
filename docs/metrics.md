# Metric definitions (issue #8)

Plain-language definitions of the Metric Library and the Bot heuristics. The
one-line "In plain words" sentences are lifted verbatim by the criteria
builder (issue #7) — they live in code in
`src/epigone/metrics/library.py`. Source of truth for the math:
`src/epigone/metrics/fine.py` and `src/epigone/metrics/bots.py` — keep this
file in sync with them.

## Coarse metrics

Coarse metrics come straight from the leaderboard download (issue #26): every
row already carries account value plus per-window pnl/roi/volume, so the whole
Universe is coarse-complete the moment the leaderboard lands — one row per
Trader per timeframe (24h / 7d / 30d / all time), at zero per-account API cost.

### PnL
- **In plain words:** how much money the account made or lost over the
  timeframe, in dollars.

### ROI
- **In plain words:** the account's percentage return over the timeframe —
  how hard the money worked, regardless of account size.
- **Definition:** Hyperliquid's own leaderboard ROI for the window, stored
  verbatim. It is net-deposit-adjusted, so mid-window funding doesn't read as
  return the way a raw pnl-over-starting-stack proxy would.

### Volume
- **In plain words:** how much the account traded over the timeframe, in
  dollars — activity, not profit.

### Account value
- **In plain words:** what the account is worth right now, in dollars.

## Where fine metrics come from

Fine metrics are computed from a Trader's **fill history**. The first refresh
pulls the account's recent fills (the info API serves roughly the last 2,000);
each later refresh **folds in only the fills since the last checkpoint**
(issue #11), so the history accumulates past that 2,000-fill window instead of
being re-truncated on every pull — and a fast-tier refresh costs a few fills'
worth of weight, not a full re-pull. (S3 bulk backfill for pre-first-refresh
history is still issue #9.) Only perp fills count; spot trades and dust
conversions are ignored. The fine pass runs for **coarse-pass survivors**
(Traders with a profitable, active month — positive month PnL and nonzero
month volume; a tunable default gate) **and every tracked Trader**.

A metric can be **unavailable (NULL)** when the fill history can't support it
— a trader with no closed trades has no win rate; one active day can't have a
Sharpe. Unavailable never means zero, and screener surfaces must show
coarse-only Traders as such.

## The trade: a completed round-trip

Most fine metrics are per **trade**, and a trade is a completed **position
round-trip** (issue #58): it opens when a coin's position leaves flat and
completes when the position returns to flat — via a full close, a flip
(long→short or reverse), a liquidation, or a market settlement. Its PnL is
the **net** realized `closedPnl` (before fees) over the position's whole
life. A partial trim realizes money *inside* one trade, never as a trade of
its own — so a wallet cannot look prolific and accurate just by trimming a
single winner many times.

A round-trip only counts when **both** its open and its full close are in the
fill history we hold. A position opened before our history begins is excluded
outright — never given partial credit — and a position still open contributes
nothing yet (its trade completes, with full totals, when it eventually
closes). Long-hold Traders can therefore show few or no trade-quality
metrics until their positions turn over under our watch; that is the honest
reading, not a gap. (The 15 golden wallets are pinned on this basis in
`tests/test_golden_wallets.py`.)

The same exclusion applies when the history we hold has **holes** (issue
#63): every fill carries the position size before it executed, and when the
engine's own walked position disagrees with that beyond float dust,
executions were missed — a fill source the fetch didn't cover, or history
truncated at the API's ~2000-fill cap. The episode is then *demoted*: no
round-trip credit from a walk that skipped executions, though its closes
still bank into total realized PnL. The fill stream itself is the union of
the regular and TWAP-slice endpoints (Hyperliquid serves TWAP executions
only from `userTwapSliceFills`), without which a TWAP-heavy Trader's
position lives would never be walkable in the first place.

**Total realized PnL stays comprehensive**: it sums *all* realized
`closedPnl` in the window, including trims of positions whose opens we never
saw, so it can exceed the sum of the counted round-trips' PnLs by exactly
those unattributable partials. It is banked money — a magnitude sum, not a
per-trade quality signal — so trims cannot game it.

## Fine metrics

### Win rate
- **In plain words:** out of the positions this account opened and fully
  closed, the share that ended in net profit.
- **Definition:** completed round-trips with net PnL > 0 divided by all
  completed round-trips. A trade trimmed in profit but ultimately closed at
  a net loss is a loss. Breakeven trades count against the win rate.
  Unavailable without completed round-trips — never 0 or 100% for a wallet
  that has only trimmed.

### Average win / average loss
- **In plain words (average win):** the typical profit on this account's
  winning trades, in dollars.
- **In plain words (average loss):** the typical damage of this account's
  losing trades, in dollars (a positive number — smaller is better).
- **Definition:** mean net PnL of winning round-trips; mean absolute net PnL
  of losing round-trips (reported positive). Breakeven trades join neither.
  Each side is unavailable until it has at least one trade.

### Sharpe
- **In plain words:** how steady the daily profits are — profit per unit of
  daily wobble; high means smooth earning, low means a rollercoaster that
  happens to end up positive.
- **Definition:** mean ÷ standard deviation of **daily realized PnL** — each
  round-trip's net PnL lands on the UTC calendar day it completed, spanning
  first to last completed trade; quiet days count as zero — annualized by
  √365. Unavailable when the trades span a single day or the daily PnL never
  varies.
- **Using it well:** the value is unbounded and **blind to size** — a wallet
  realizing $10/day like clockwork outscores a whale with normal swings, so
  the universe's most extreme values (hundreds+) are dust-scale bots or
  handfuls of trades, not great traders. The short fill window plus √365
  annualization also inflates every value well past textbook intuitions
  ("2 is world-class" does not apply); calibrate against this universe
  instead — observed 2026-07: **> 3 ≈ steadiest quartile, > 7 ≈ steadiest
  decile**. Use it as a floor together with a PnL (or ROI) floor and a
  Closed-trades floor (≥ 10): each alone finds something degenerate (lottery
  winners, dust bots, churners); together they find big, steady, and proven.

### Max drawdown
- **In plain words:** the deepest hole the account dug from its own peak —
  how much giveback you'd have sat through at worst.
- **Definition:** largest peak-to-trough fall of the cumulative realized-PnL
  curve over the fill window, in USD — the curve steps once per completed
  round-trip, at its close. Unrealized swings don't move it (fills only
  realize PnL). Unlike the other trade metrics it reads 0 (not NULL) with no
  completed round-trips: an empty curve never fell.

### Trade count
- **In plain words:** how many positions the account opened and fully closed
  in its recent history — more trades, more evidence the other numbers are
  real.
- **Definition:** number of completed round-trips (as defined above) in the
  fill window. Partial trims never inflate it: 78 trims of one still-open
  position are 0 trades.

### Avg size (of account), estimated
- **In plain words:** how big a typical position is next to the whole account —
  a sizing signal, not the exchange's leverage dial (the positions view shows
  that separately as e.g. `at 25x`).
- **Definition:** mean over completed round-trips of peak position value
  (largest `|startPosition| × price` among the trade's closing fills) ÷ the
  account value recorded by the coarse pass. Fills carry no margin data, so
  this is a copyability signal, not the exchange's leverage setting.
  Unavailable without a coarse account value.
- **Metric key:** `avg_leverage` (unchanged — stored Criteria keep working).

### Maker/taker share
- **In plain words:** how often the account waits with resting orders (maker)
  versus paying up to take liquidity (taker) — very high maker share smells
  like a market-making machine.
- **Definition:** share of perp fills that did not cross the book
  (`crossed = false`), by fill count.

### Average holding time
- **In plain words:** how long the account typically holds a position before
  closing it — short means scalping, long means swinging.
- **Definition:** mean duration (`close_time − open_time`) of **completed
  round-trips** over the fill window (issue #48) — a trade's holding time is
  the span its position stayed non-flat, so this metric shares the trade
  definition above, including the pre-window exclusion. A position still open
  at window end is excluded (no close time yet), so an in-progress position
  never skews it. Unavailable (NULL) until at least one trade has completed.
  Because a position can straddle an incremental checkpoint — opened in one
  refresh, closed in a later one — the open episode (open-time plus the PnL
  and peak notional banked so far) persists across refreshes, so the trade's
  totals stay correct under the #11 fold. Thresholds are typed naturally
  (`2d`, `12h`, `90m`, `1d 6h`) and displayed the same compact way (`2d 4h`).

## Bot exclusion

A **Bot** (CONTEXT.md) is an account whose statistical profile indicates
automated market-making rather than copyable trading skill. Flagged accounts
keep their rows and metrics in the database but never appear in screener
results; a profile that stops matching the heuristics is unflagged on its
next fine refresh. Thresholds are calibrated against the ansem-bullpen R&D:
every real excluded account is caught, and all 15 vetted wallets clear each
threshold with wide margin (their round-trip maxima: 57 completed trades,
~2.5 trades/day; perfect win rates appear only over samples far below the
min-exits guard).

1. **Near-perfect win rate over many exits** — win rate ≥ 98% across ≥ 100
   completed round-trips. Humans realize losses; market-makers don't.
2. **Extreme exit frequency** — ≥ 200 completed round-trips per day across
   the fill window (bursts inside a single day are measured against a full
   day). The excluded HFT accounts cycled flat ~440 times a day.
3. **PnL from static holdings** — ≥ $100k absolute month PnL (coarse pass)
   with ≤ 50 perp fills in the whole visible history. The money is made by
   holding, not trading — nothing to copy. Judged by fills seen rather than
   completed round-trips (issue #58): a long-hold human whose opens predate
   our fill window shows few round-trips yet thousands of fills, and must
   never be mistaken for a holdings whale. Skipped until the Trader has
   coarse metrics.
