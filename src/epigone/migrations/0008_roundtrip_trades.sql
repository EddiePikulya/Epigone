-- Migration 0008: a trade becomes a completed position round-trip (issue #58).
-- The old basis — one "trade" per closing order — let every partial trim of a
-- position count as its own trade, so a wallet trimming one winner looked like
-- many wins. Now a trade spans the position's whole life (flat to flat), its
-- PnL the net realized closedPnl over that life; trims are partial
-- realizations inside one trade.

-- The trade store becomes a round-trip store. Old rows are per-closing-order
-- fragments that cannot be regrouped into round-trips (the opening fills were
-- never persisted), so the table is rebuilt and reseeded from a full re-pull
-- rather than converted. Identity is (address, coin, closed_at, seq): the fill
-- that returned the position to flat, stable across a boundary re-fetch (#11).
-- seq disambiguates same-millisecond completions — a same-block
-- close->reopen->close makes two trades sharing a closed_at, and without the
-- ordinal the primary key would silently keep only one.
DROP TABLE fine_trades;
CREATE TABLE fine_trades (
    address       TEXT NOT NULL REFERENCES traders (address),
    coin          TEXT NOT NULL,
    pnl           NUMERIC NOT NULL,
    peak_notional NUMERIC NOT NULL,
    opened_at     TIMESTAMPTZ NOT NULL,
    closed_at     TIMESTAMPTZ NOT NULL,
    seq           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (address, coin, closed_at, seq)
);

-- A still-open episode accumulates its trims' net PnL and peak notional across
-- refreshes (not just its open-time, #48), so the round-trip's totals are
-- complete when the position finally closes — however many checkpoints later.
ALTER TABLE fine_open_episodes
    ADD COLUMN pnl           NUMERIC NOT NULL DEFAULT 0,
    ADD COLUMN peak_notional NUMERIC NOT NULL DEFAULT 0;

-- Holding time now reduces from the stored round-trips (a trade carries its
-- own duration), so the #48 running accumulators are redundant state.
ALTER TABLE fine_metrics
    DROP COLUMN hold_seconds_sum,
    DROP COLUMN hold_episode_count;

-- Old-basis fine state is wrong-basis, not merely stale: metrics counted trim
-- fragments and open episodes carry no accumulated PnL. Wipe it and reset the
-- checkpoints so the next fine pass reseeds every Trader from a full pull —
-- honest NULLs until then beat trim-inflated numbers (issue #58).
DELETE FROM fine_open_episodes;
DELETE FROM fine_metrics;
UPDATE traders SET fine_checkpoint_at = NULL, fine_refreshed_at = NULL;
