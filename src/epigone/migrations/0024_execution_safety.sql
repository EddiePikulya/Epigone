-- Migration 0024: the A3 execution safety layer (issue #135, ADR-0005).
--
-- Three tables, one per concern:
--
-- process_heartbeats — the liveness seam between the execution processes
-- (ADR-0002: they meet only in Postgres). The executor upserts its row every
-- loop; the watchdog trips when that row goes stale, and beats its own row so
-- the #52 health monitor can tell a live watchdog from a dead one. One row
-- per process name; rows are state, not history (decommissioning a process
-- legitimately deletes its row — see docs/runbooks/halt-and-unwind.md).
--
-- execution_halts — the kill-switch state. At most ONE active halt (the
-- partial unique index over a constant expression): /kill and the watchdog
-- both request halts, the watchdog sweeps resting orders for whichever halt
-- is active, and `swept_at` is stamped only after a fresh enumeration shows
-- an empty book — never on the strength of a cancel call alone, because an
-- ambiguous cancel result may have left live orders (the
-- AmbiguousExecutionError contract). `positions` records the open-position
-- snapshot at sweep time and `unwind_policy` which documented policy was
-- applied to them (halt-and-unwind runbook). Resuming stamps resumed_at —
-- halt rows are history, never deleted.
--
-- execution_audit — the append-only audit trail: every signed exchange
-- action (attempt and outcome as separate rows, `attempt_of` linking outcome
-- to attempt, so a crash between signing and the response still leaves the
-- attempt on record) plus safety-state events (halt, resume, dead-man's
-- switch eligibility/activation). `risk_decision` states what authorized the
-- action — A3 records the safety-layer authorizations; A5's risk policy will
-- write its verdicts here. Append-only is DB-enforced by trigger, not
-- convention: UPDATE and DELETE are refused for every role the app uses.
-- TRUNCATE is deliberately left open — test databases reset by truncation;
-- production hygiene is a role grant question, not this trigger's.
--
-- Timestamps come from the injected clock; addresses are stored lowercased
-- (house conventions). Additive, no wipe.
CREATE TABLE process_heartbeats (
    process   TEXT PRIMARY KEY,
    beaten_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE execution_halts (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    halted_at     TIMESTAMPTZ NOT NULL,
    source        TEXT NOT NULL CHECK (source IN ('kill', 'watchdog')),
    reason        TEXT NOT NULL,
    requested_by  BIGINT,
    swept_at      TIMESTAMPTZ,
    positions     JSONB,
    unwind_policy TEXT,
    resumed_at    TIMESTAMPTZ,
    resumed_by    BIGINT
);

CREATE UNIQUE INDEX execution_halts_one_active
    ON execution_halts ((resumed_at IS NULL)) WHERE resumed_at IS NULL;

CREATE TABLE execution_audit (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL,
    actor          TEXT NOT NULL,
    action         TEXT NOT NULL,
    master_address TEXT,
    signer_address TEXT,
    request        JSONB NOT NULL,
    outcome        TEXT NOT NULL,
    detail         JSONB,
    risk_decision  TEXT NOT NULL,
    attempt_of     BIGINT REFERENCES execution_audit (id)
);

CREATE INDEX execution_audit_occurred_at ON execution_audit (occurred_at);

CREATE FUNCTION execution_audit_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'execution_audit is append-only (issue #135): audit rows are never updated or deleted';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER execution_audit_append_only
    BEFORE UPDATE OR DELETE ON execution_audit
    FOR EACH ROW EXECUTE FUNCTION execution_audit_append_only();
