-- Migration 0014: effective coins, a fine metric (issue #95). How many coins a
-- wallet effectively plays — the inverse Herfindahl of its completed
-- round-trips per coin: one coin reads 1.0, a 50/50 pair 2.0, ten coins evenly
-- 10.0. A ceiling (≤ 2) screens for coin specialists, robust to dust probes
-- where a raw top-coin share was not. Reduced from the same per-coin
-- round-trips behind Most played (#80), so it carries no fold accumulator: like
-- win_rate it recomputes in full from the folded trades each refresh, never
-- re-reading raw history.
--
-- NULL when the wallet has no completed round-trips (docs/metrics.md: NULL is
-- "not computable", never 0). Additive and nullable with no default, so every
-- existing fine_metrics row simply carries NULL until its next scheduled
-- refresh recomputes it — no data wipe.
ALTER TABLE fine_metrics
    ADD COLUMN effective_coins NUMERIC;
