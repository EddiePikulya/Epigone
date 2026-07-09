-- Epigone schema. Idempotent: applied at process startup.
-- Vocabulary per CONTEXT.md — a User is a person on Telegram, never a Trader.

CREATE TABLE IF NOT EXISTS users (
    telegram_id   BIGINT PRIMARY KEY,
    username      TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
