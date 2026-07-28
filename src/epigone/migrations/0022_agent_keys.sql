-- Migration 0022: the encrypted agent-key keystore (issue #134, ADR-0005).
--
-- Holds Hyperliquid AGENT keys only — trade-only keys a user's master wallet
-- approved via approveAgent. The master key (funds + authority) never touches
-- Epigone code, storage, or docs (ADR-0005's invariant); master_address below
-- is the public account address the agent trades for, never its key.
--
-- Envelope encryption (research §6.5): each key gets its own random DEK; the
-- private key is AES-256-GCM-encrypted under the DEK, and the DEK is wrapped
-- under a KEK that lives in a file OUTSIDE the database (and outside git), so
-- a DB dump or stolen backup alone yields nothing. kek_id names which KEK
-- wrapped the DEK so KEKs can rotate without a flag day. Both blobs carry
-- their GCM nonce prepended and are AAD-bound to (user_id, agent_address) —
-- ciphertext transplanted onto another row fails authentication.
--
-- Lifecycle: at most one ACTIVE key per user (partial unique index); revoking
-- keeps the row as a tombstone because agent addresses must never be reused
-- after deregistration (nonce-replay hazard, ADR-0005) — the global UNIQUE on
-- agent_address enforces that against tombstones forever. expires_at tracks
-- Hyperliquid's ≤180-day agent validity; the health monitor reads it to
-- remind the operator before rotation becomes an outage.
--
-- Addresses are stored lowercased, timestamps come from the injected clock
-- (house conventions). Additive, no wipe.
CREATE TABLE agent_keys (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        BIGINT NOT NULL REFERENCES users (telegram_id),
    master_address TEXT NOT NULL,
    agent_address  TEXT NOT NULL UNIQUE,
    agent_name     TEXT,
    kek_id         TEXT NOT NULL,
    dek_wrapped    BYTEA NOT NULL,
    key_ciphertext BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    revoked_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX agent_keys_one_active_per_user
    ON agent_keys (user_id) WHERE revoked_at IS NULL;
