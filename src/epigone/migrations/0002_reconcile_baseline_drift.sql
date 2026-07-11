-- Migration 0002: reconcile live-DB drift so the live database becomes identical
-- to a fresh-from-0001 one (issue #37). This is the first real migration on the
-- #16 runner. The live DB predates the runner and was built by hand-applied
-- ALTERs (#10) plus idempotent CREATE TABLE IF NOT EXISTS (which can add columns
-- but never drop them), so it carries drift a fresh 0001 DB never had.
--
-- Every statement is IF EXISTS / drop-then-re-add so this migration NO-OPS (net
-- schema unchanged, only a schema_migrations record) on a fresh DB and CLEANS
-- the live one. It runs exactly once per DB; it is not written to be idempotent
-- beyond that. This file is frozen history once shipped — never edit it.

-- 1. Vestigial `traders` columns + index left over from #26, which dropped them
--    from the schema but could not drop them from the live table (the old
--    apply_schema only ever added). A fresh 0001 DB never had these, so the
--    DROPs are no-ops there. Dropping the columns also drops any index built on
--    them; the explicit DROP INDEX IF EXISTS covers the case where it survives.
ALTER TABLE traders DROP COLUMN IF EXISTS coarse_attempted_at;
ALTER TABLE traders DROP COLUMN IF EXISTS coarse_refreshed_at;
DROP INDEX IF EXISTS traders_coarse_attempt_order;

-- 2. Scale-check constraint name. Same CHECK logic, two names: the live DB has
--    `position_alerts_scale_check` (hand-named for #10), a fresh 0001 DB has
--    `position_alerts_check3` (Postgres auto-named the inline constraint). Since
--    the target is "identical to fresh-from-0001", the canonical name is the
--    fresh one, `position_alerts_check3`. That name is positional — Postgres
--    derived the `3` from the count of anonymous CHECKs on the table in 0001 — so
--    a future migration that reshapes those inline CHECKs must revisit it (the
--    live-vs-fresh convergence test in test_migrations.py pins it for now). Drop
--    whichever name is present and re-add under the canonical name with the
--    identical CHECK from 0001: on a fresh DB this drops and re-adds check3
--    (net-identical, so a no-op); on the live DB it renames scale_check to
--    check3, converging on fresh.
ALTER TABLE position_alerts DROP CONSTRAINT IF EXISTS position_alerts_scale_check;
ALTER TABLE position_alerts DROP CONSTRAINT IF EXISTS position_alerts_check3;
ALTER TABLE position_alerts ADD CONSTRAINT position_alerts_check3 CHECK (
    kind NOT IN ('scale_in', 'scale_out')
    OR (side IS NOT NULL AND size_usd IS NOT NULL AND prev_size_usd IS NOT NULL)
);
