-- Migration 0006: average holding time as a fine metric (issue #48). A
-- *position episode* is the span a coin's position is non-flat; the metric is
-- the mean duration of completed episodes over the fill window. Like
-- maker_share, the mean must survive the incremental fold (issue #11) without
-- re-reading full history, so it accumulates as a running sum + count rather
-- than being recoverable from the stored trades.

-- The reduced metric plus its two accumulators. avg_hold_seconds is NULL when
-- no episode has completed (docs/metrics.md: NULL is "not computable", never
-- 0). The accumulators default to 0 so a pre-#48 fine_metrics row folds onto a
-- zero base on its next incremental refresh, exactly like the #11 counters.
ALTER TABLE fine_metrics
    ADD COLUMN avg_hold_seconds  BIGINT,
    ADD COLUMN hold_seconds_sum  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN hold_episode_count INTEGER NOT NULL DEFAULT 0;

-- Episodes open before a checkpoint but closed after it don't have their
-- opening fill in the incremental batch, so the open-time must persist across
-- refreshes: one row per coin a Trader currently holds non-flat, keyed by
-- (address, coin). Rewritten wholesale each refresh (a Trader holds few coins),
-- mirroring how fine_trades carries the rest of the fold state. Read only to
-- rebuild the fold state at refresh time, never on the screener/profile hot path.
CREATE TABLE fine_open_episodes (
    address   TEXT NOT NULL REFERENCES traders (address),
    coin      TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, coin)
);
