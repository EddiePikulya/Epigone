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
-- bot_reason marks a Bot (CONTEXT.md): an account whose profile is
-- market-making, not copyable skill. Bots keep their rows and metrics but
-- never reach a screener result (issue #8).
CREATE TABLE IF NOT EXISTS traders (
    address             TEXT PRIMARY KEY,
    display_name        TEXT,
    refresh_tier        TEXT CHECK (refresh_tier IN ('active', 'dormant')),
    first_seen_at       TIMESTAMPTZ NOT NULL,
    last_seen_at        TIMESTAMPTZ NOT NULL,
    coarse_refreshed_at TIMESTAMPTZ,
    coarse_attempted_at TIMESTAMPTZ,
    fine_refreshed_at   TIMESTAMPTZ,
    fine_attempted_at   TIMESTAMPTZ,
    bot_flagged_at      TIMESTAMPTZ,
    bot_reason          TEXT,
    CHECK ((bot_flagged_at IS NULL) = (bot_reason IS NULL))
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

-- Scan order for the fine pass, mirroring the coarse one.
CREATE INDEX IF NOT EXISTS traders_fine_attempt_order
    ON traders (fine_attempted_at ASC NULLS FIRST, address);

-- Fine Metric Library: fills-derived metrics over a Trader's recent fill
-- window (two-stage scan stage 2, issue #8). One row per Trader; a NULL
-- metric means "not computable from this fill history", never zero.
-- Definitions in plain language: docs/metrics.md.
CREATE TABLE IF NOT EXISTS fine_metrics (
    address      TEXT PRIMARY KEY REFERENCES traders (address),
    trade_count  INTEGER NOT NULL,
    win_rate     NUMERIC,
    avg_win      NUMERIC,
    avg_loss     NUMERIC,
    sharpe       NUMERIC,
    max_drawdown NUMERIC NOT NULL,
    avg_leverage NUMERIC,
    maker_share  NUMERIC,
    realized_pnl NUMERIC NOT NULL,
    window_start TIMESTAMPTZ,
    window_end   TIMESTAMPTZ,
    computed_at  TIMESTAMPTZ NOT NULL
);

-- A Track is a User's explicit, manual follow of a Trader (CONTEXT.md).
CREATE TABLE IF NOT EXISTS tracks (
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT   NOT NULL REFERENCES traders (address),
    tracked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_telegram_id, trader_address)
);

-- Stream poller bookkeeping (issue #4). A row means the Trader has a baseline:
-- its open positions have been observed at least once, so later polls diff
-- against the snapshots instead of alerting on pre-existing positions.
CREATE TABLE IF NOT EXISTS position_poll_state (
    trader_address TEXT PRIMARY KEY REFERENCES traders (address),
    baselined_at   TIMESTAMPTZ NOT NULL,
    last_polled_at TIMESTAMPTZ NOT NULL
);

-- Last observed open positions of a tracked Trader, one row per coin — what
-- the next poll diffs against. Persisted so a stream restart never re-alerts
-- on positions it already saw. opened_at is when the poller FIRST observed
-- the position; it survives size/entry changes and feeds holding time.
CREATE TABLE IF NOT EXISTS position_snapshots (
    trader_address TEXT NOT NULL REFERENCES traders (address),
    coin           TEXT NOT NULL,
    side           TEXT NOT NULL CHECK (side IN ('long', 'short')),
    size_usd       NUMERIC NOT NULL,
    leverage       NUMERIC NOT NULL,
    entry_price    NUMERIC NOT NULL,
    unrealized_pnl NUMERIC NOT NULL,
    opened_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trader_address, coin)
);

-- The Position Alert queue, the stream→bot seam (ADR-0002): the stream inserts
-- one row per event per follower in the same transaction as the snapshot diff
-- (each event is detected exactly once, restart-safe); the bot process delivers
-- and stamps delivered_at. side/size/leverage/entry describe the new leg
-- (open, flip); prev_side/realized_pnl/pct_return/opened_at the closed leg
-- (close, flip).
CREATE TABLE IF NOT EXISTS position_alerts (
    id               BIGSERIAL PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT NOT NULL REFERENCES traders (address),
    kind             TEXT NOT NULL CHECK (kind IN ('open', 'close', 'flip')),
    coin             TEXT NOT NULL,
    side             TEXT CHECK (side IN ('long', 'short')),
    size_usd         NUMERIC,
    leverage         NUMERIC,
    entry_price      NUMERIC,
    prev_side        TEXT CHECK (prev_side IN ('long', 'short')),
    realized_pnl     NUMERIC,
    pct_return       NUMERIC,
    opened_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    attempts         INTEGER NOT NULL DEFAULT 0,
    CHECK (kind != 'open' OR side IS NOT NULL),
    CHECK (kind != 'close' OR prev_side IS NOT NULL),
    CHECK (kind != 'flip' OR (side IS NOT NULL AND prev_side IS NOT NULL))
);

-- The bot's delivery scan: undelivered rows only, oldest first.
CREATE INDEX IF NOT EXISTS position_alerts_undelivered
    ON position_alerts (id) WHERE delivered_at IS NULL;
