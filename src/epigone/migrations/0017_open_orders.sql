-- Migration 0017: resting-order poll state and the Order Alert outbox
-- (issue #115).
--
-- The stream's order-poll pass diffs each tracked wallet's resting-order set
-- (frontendOpenOrders across POSITION_VENUES — per-dex like clearinghouseState,
-- verified live 2026-07-24) against the order ids already seen and alerts only
-- on NEW orders. The first two tables mirror the position poller's pair:
--
--   order_poll_state — one row per polled wallet; its existence is the
--     baseline marker. A wallet's first-ever order poll records ids silently
--     (a ladder that predates observation is not news), and a stream restart
--     diffs against persisted ids instead of re-alerting the same ladder.
--   order_snapshots  — the known resting-order ids, one row per order. Only
--     the id is bookkeeping: an alert renders from the fetch that discovered
--     the order, and a disappearance (cancel or fill) is deliberately silent
--     (fills already alert as position events), so no order detail persists.
--
-- order_alerts is the order-side outbox (ADR-0002: stream and bot meet only in
-- Postgres), one row PER FOLLOWER PER WALLET PER POLL CYCLE — a batch, never
-- one row per order: active makers place orders constantly, and #115's noise
-- rule is one message per wallet per cycle. `orders` holds the batch as a
-- JSONB array (numbers as strings so Decimals round-trip, the criteria.filters
-- precedent); it exists only to be rendered — no SQL ever filters on order
-- fields, and per-follower mute/min-size suppression already happened at queue
-- time (the #10 rule: suppressed content is never stored). The outbox columns
-- (id, user_telegram_id, attempts, delivered_at) satisfy the shared drain's
-- contract (epigone.bot.outbox).

CREATE TABLE order_poll_state (
    trader_address TEXT PRIMARY KEY REFERENCES traders (address),
    baselined_at   TIMESTAMPTZ NOT NULL,
    last_polled_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE order_snapshots (
    trader_address TEXT NOT NULL REFERENCES traders (address),
    order_id       BIGINT NOT NULL,
    first_seen_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trader_address, order_id)
);

CREATE TABLE order_alerts (
    id               BIGSERIAL PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT NOT NULL REFERENCES traders (address),
    orders           JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    attempts         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX order_alerts_undelivered
    ON order_alerts (id) WHERE delivered_at IS NULL;
