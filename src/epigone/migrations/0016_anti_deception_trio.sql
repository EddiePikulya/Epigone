-- Migration 0016: the anti-deception trio, three trip-derived fine metrics
-- (issue #113). Each catches a different way headline stats lie, validated in
-- live research where they had to be computed by hand to weed out lottery
-- wallets and win-rate illusions:
--
--   median_trade   — median PnL across ALL round-trips (wins and losses
--                    together, so it can be negative). Catches coin-flippers
--                    whose typical trade earns nothing; a positive median over
--                    many trades is nearly unfakeable.
--   profit_factor  — gross winning dollars ÷ gross losing dollars. Catches
--                    win-rate illusions (a 55% win rate that loses money);
--                    below 1 loses money regardless of win rate. NULL when the
--                    wallet has no losses (div-by-zero — "∞", rendered absent).
--   top_trade_share — best single trip's PnL ÷ total trip PnL, a fraction.
--                    Catches lottery records where one moonshot carries
--                    everything. NULL unless total PnL > 0.
--
-- All three reduce from the same completed round-trips behind the other trade
-- metrics (metrics/fine.py reduce_trips), so they recompute in full each
-- refresh and carry no fold accumulator — like win_rate and effective_coins.
--
-- Additive and nullable with no default (docs/metrics.md: NULL is "not
-- computable", never 0), so every existing fine_metrics row simply carries NULL
-- until its next scheduled refresh recomputes it — no data wipe.
ALTER TABLE fine_metrics
    ADD COLUMN median_trade NUMERIC,
    ADD COLUMN profit_factor NUMERIC,
    ADD COLUMN top_trade_share NUMERIC;
