-- Migration 0018: entry/exit VWAPs on round-trips (issue #116).
-- The fine engine now prices each trade: entry VWAP over its
-- position-increasing fills, exit VWAP over its decreasing fills' closing
-- portions (a flip fill splits at the zero crossing, same proration as the
-- PnL attribution). Recorded going forward only — trips folded before this
-- shipped keep NULL prices and the views render them without the `in → out`
-- clause. No backfill is possible (their fills have aged out of the API's
-- ~2000-cap windows) and no wipe is warranted: every already-stored number
-- is still correct, the new columns are strictly additive.
ALTER TABLE fine_trades
    ADD COLUMN entry_vwap NUMERIC,
    ADD COLUMN exit_vwap  NUMERIC;

-- An episode can straddle checkpoints (opened in one batch, closed refreshes
-- later), so the entry/exit weighted sums ride fine_open_episodes exactly
-- like pnl and peak_notional do (#58 fold rigor), and the #63 continuity
-- guard drops them with the episode on demotion. NULL (all four together)
-- marks an episode stored before recording shipped: its opening fills are
-- unknowable, so a trip it completes stays price-less rather than getting a
-- VWAP over only the fills the fold happened to see — which is why the
-- columns default to NULL instead of 0.
ALTER TABLE fine_open_episodes
    ADD COLUMN entry_cost NUMERIC,
    ADD COLUMN entry_size NUMERIC,
    ADD COLUMN exit_cost  NUMERIC,
    ADD COLUMN exit_size  NUMERIC;
