-- Sustained rate-limit signal for the health monitor (issue #54). Each row is
-- a RateLimitedError that escaped the gateway's backoff-and-retry (issue #28) —
-- ~30s of 429s for one call, i.e. real limiting, not a lone backoff-absorbed
-- 429 (which never reaches here). Kept off the hot, row-locked rate_budget
-- bucket every spender serializes on, in its own append-only log the monitor
-- counts over a recent window. Events are rare, so the table stays tiny; the
-- recorder prunes stale rows opportunistically.
CREATE TABLE rate_limit_events (
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX rate_limit_events_occurred_at_idx ON rate_limit_events (occurred_at);
