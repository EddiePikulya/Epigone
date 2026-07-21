-- Migration 0011: the one-time "first fine data landed" notice queue (issue #83).
-- When a wallet a User follows gets its first-ever fine scan (typically minutes
-- after following, thanks to #82), tell the User once — with a button into the
-- profile view — that the thin "not-yet-scanned" views are now populated.
--
-- This one table is BOTH the dedup store and the delivery queue, keyed uniquely
-- on (user, wallet). The row's *existence* — never a table-state transition —
-- is the single source of "this pair is already handled", which is exactly what
-- makes the notice survive restarts, unfollow+refollow, and the 0008-style fine
-- wipe→reseed without re-notifying: once a pair has a row, follow never
-- re-inserts it (ON CONFLICT DO NOTHING) and the scan-side flip only ever
-- touches 'pending' rows.
--
-- status lifecycle (a pair only ever moves pending → ready, or starts settled):
--   suppressed  the wallet already had fine data when the User followed — the
--               data is simply there when they look, so no notice is ever sent.
--   pending     the User followed before any fine data existed — waiting for
--               the first scan to land.
--   ready       the first fine scan landed while the pair was pending — a notice
--               to deliver; delivered_at/attempts then track delivery exactly as
--               position_alerts does (issue #4).
-- created_at is stamped at follow time from the injected clock (not now()).
CREATE TABLE first_data_notices (
    id               BIGSERIAL PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT NOT NULL REFERENCES traders (address),
    status           TEXT NOT NULL CHECK (status IN ('suppressed', 'pending', 'ready')),
    created_at       TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    attempts         INTEGER NOT NULL DEFAULT 0,
    UNIQUE (user_telegram_id, trader_address)
);

-- The bot's delivery scan: ready-but-undelivered rows, oldest first.
CREATE INDEX first_data_notices_deliverable
    ON first_data_notices (id) WHERE status = 'ready' AND delivered_at IS NULL;

-- The ingest fine pass's pending→ready flip, keyed on the wallet just scanned;
-- partial so a routine refresh of a wallet with no waiting trackers is a no-op.
CREATE INDEX first_data_notices_pending
    ON first_data_notices (trader_address) WHERE status = 'pending';
