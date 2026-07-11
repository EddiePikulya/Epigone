-- Migration 0005: incremental fine refresh (issue #11). The fine pass used to
-- re-pull a Trader's *full* userFills every refresh (weight 20 + up to ~+100
-- surcharge) and re-derive metrics from scratch, truncated at the ~2000-fill
-- API cap. Persist the derived closed trades and the fill accumulators so a
-- refresh folds in only the fills since the last checkpoint: cheaper fast-tier
-- refreshes, and a fill history that grows past the 2000-fill cap instead of
-- being re-truncated on every pull.

-- One row per realized trade (all closing fills of a single closing order,
-- reduced to what the metrics need). Keyed by (address, order_id) so a
-- boundary re-fetch upserts idempotently. Read only to rebuild the fold state
-- at refresh time — never on the screener/profile hot path, which reads the
-- reduced values in fine_metrics.
CREATE TABLE fine_trades (
    address       TEXT NOT NULL REFERENCES traders (address),
    order_id      BIGINT NOT NULL,
    pnl           NUMERIC NOT NULL,
    peak_notional NUMERIC NOT NULL,
    closed_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, order_id)
);

-- maker_share is a ratio over *all* perp fills (opens included), so it is not
-- recoverable from the stored trades alone — its numerator and denominator
-- accumulate here. Defaulting to 0 lets the first incremental refresh of a
-- pre-#11 fine_metrics row fold onto a zero base; that row's trades are
-- rebuilt once from a full re-pull (its fine_checkpoint_at is still NULL).
ALTER TABLE fine_metrics
    ADD COLUMN maker_fill_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN perp_fill_count  INTEGER NOT NULL DEFAULT 0;

-- The incremental checkpoint: the newest fill (of any kind) already folded.
-- NULL means never refreshed incrementally — the next fine pass does a full
-- pull, seeds the trade store, and stamps this. Later passes fetch only fills
-- strictly after it.
ALTER TABLE traders
    ADD COLUMN fine_checkpoint_at TIMESTAMPTZ;
