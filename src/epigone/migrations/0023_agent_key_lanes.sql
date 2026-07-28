-- Migration 0023: agent-key lanes (issue #135, ADR-0005).
--
-- The A3 safety layer adds a second signing process: the watchdog — the
-- PRIMARY dead-man's switch (the scheduleCancel protocol primitive sits
-- behind a $1M cumulative-volume gate, verified live 2026-07-28) — which
-- must be able to cancel the operator's resting orders when the executor
-- is dead. Hyperliquid tracks nonces PER SIGNER and its docs' concurrency
-- advice is one API wallet per trading process, so the watchdog needs its
-- OWN agent key: sharing the executor's would interleave two processes in
-- one nonce lane, and the kill path must not depend on the very lane it
-- exists to clean up after.
--
-- `lane` names the process a key signs for. The one-active-key rule becomes
-- per (user, lane); rotation semantics are unchanged, once per lane.
-- Tombstones and never-reuse-after-deregistration are untouched (the global
-- UNIQUE on agent_address).
--
-- The CHECK pins the known lanes deliberately: a zero-volume account gets
-- exactly 3 agent slots (funded-testnet probe, PR #141), so two lanes plus
-- one rotation-overlap slot is the whole budget — adding a lane is a real
-- decision that belongs in a migration, not a typo'd string.
ALTER TABLE agent_keys ADD COLUMN lane TEXT NOT NULL DEFAULT 'executor';
ALTER TABLE agent_keys ALTER COLUMN lane DROP DEFAULT;
ALTER TABLE agent_keys ADD CONSTRAINT agent_keys_lane_known
    CHECK (lane IN ('executor', 'watchdog'));

DROP INDEX agent_keys_one_active_per_user;
CREATE UNIQUE INDEX agent_keys_one_active_per_lane
    ON agent_keys (user_id, lane) WHERE revoked_at IS NULL;
