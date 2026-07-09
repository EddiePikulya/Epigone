-- Epigone schema. Idempotent: applied at process startup.
-- Vocabulary per CONTEXT.md — a User is a person on Telegram, never a Trader.

CREATE TABLE IF NOT EXISTS users (
    telegram_id   BIGINT PRIMARY KEY,
    username      TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Traders are observed Hyperliquid wallets, never Users. Addresses are stored
-- lowercase; validation/normalization happens at the edge (bot input).
CREATE TABLE IF NOT EXISTS traders (
    address       TEXT PRIMARY KEY CHECK (address = lower(address)),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A Track is a User's explicit, manual follow of a Trader (CONTEXT.md).
CREATE TABLE IF NOT EXISTS tracks (
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT   NOT NULL REFERENCES traders (address),
    tracked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_telegram_id, trader_address)
);
