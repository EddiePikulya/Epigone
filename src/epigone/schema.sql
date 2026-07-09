-- Epigone schema. Idempotent: applied at process startup.
-- Vocabulary per CONTEXT.md — a User is a person on Telegram, never a Trader.
--
-- CREATE TABLE IF NOT EXISTS never ALTERs an existing table. Tests rebuild
-- their throwaway schema every run (tests/conftest.py), but a deployed
-- database keeps its old shape — before V1 deploys with data worth keeping,
-- it needs a real migration story (issue #16).

CREATE TABLE IF NOT EXISTS users (
    telegram_id   BIGINT PRIMARY KEY,
    username      TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The Universe: candidate Traders seeded from the leaderboard source (issue #5)
-- or pasted directly by a User (issue #3). Addresses are stored lowercased.
-- refresh_tier is NULL until the first coarse pass classifies the Trader;
-- timestamps come from the injected clock, not now().
CREATE TABLE IF NOT EXISTS traders (
    address             TEXT PRIMARY KEY,
    display_name        TEXT,
    refresh_tier        TEXT CHECK (refresh_tier IN ('active', 'dormant')),
    first_seen_at       TIMESTAMPTZ NOT NULL,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    coarse_refreshed_at TIMESTAMPTZ,
    coarse_attempted_at TIMESTAMPTZ
);

-- Scan order: least-recently-attempted first, so Traders whose fetch keeps
-- failing rotate to the back instead of blocking the pass forever.
CREATE INDEX IF NOT EXISTS traders_coarse_attempt_order
    ON traders (coarse_attempted_at ASC NULLS FIRST, address);

-- Coarse Metric Library: one row per Trader per timeframe, from a single
-- portfolio call (spec-defaults two-stage scan, stage 1).
CREATE TABLE IF NOT EXISTS coarse_metrics (
    address       TEXT NOT NULL REFERENCES traders (address),
    time_window   TEXT NOT NULL CHECK (time_window IN ('day', 'week', 'month', 'allTime')),
    pnl           NUMERIC NOT NULL,
    roi           NUMERIC NOT NULL,
    volume        NUMERIC NOT NULL,
    account_value NUMERIC NOT NULL,
    computed_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, time_window)
);

-- A Track is a User's explicit, manual follow of a Trader (CONTEXT.md).
CREATE TABLE IF NOT EXISTS tracks (
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT   NOT NULL REFERENCES traders (address),
    tracked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_telegram_id, trader_address)
);
