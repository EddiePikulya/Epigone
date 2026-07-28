-- Migration 0025: watchdog capability verdict (issue #135, PR #143 review).
--
-- A watchdog that beats but whose agent key was deregistered or expired
-- on-chain would read "healthy" in the #52 monitor while every cancel it
-- issued would be rejected — a false sense of safety discovered only when a
-- real halt fails. The watchdog now verifies its own approval on-chain
-- (the public extraAgents readback, no signing, no spend) on a slow cadence
-- and records the verdict HERE, beside its heartbeat, for the monitor to
-- alert on: capable NULL = never checked, TRUE = approved and unexpired,
-- FALSE = impotent (detail says why). This is current state; verdict
-- TRANSITIONS go to execution_audit as events, which is where the history
-- lives. Additive, no wipe.
ALTER TABLE process_heartbeats ADD COLUMN capable BOOLEAN;
ALTER TABLE process_heartbeats ADD COLUMN capability_detail TEXT;
ALTER TABLE process_heartbeats ADD COLUMN capability_checked_at TIMESTAMPTZ;
