# ADR 0003: Numbered SQL migrations applied at process startup

Date: 2026-07-11
Status: accepted

## Context

Until now the schema shipped as one idempotent `schema.sql` executed by every
process at startup. `CREATE TABLE IF NOT EXISTS` never ALTERs an existing
table, so any schema change left already-created databases stale and the
processes failing at runtime (`UndefinedColumnError`, issue #16). Tests
sidestepped this by dropping and recreating their throwaway schema; production
has no such escape hatch once there is data worth keeping — and there already
is: the live DB gained #10's columns by hand-applied ALTERs.

Two candidate mechanisms: Alembic, or a small runner over numbered SQL files.
Alembic buys autogeneration, downgrade paths, and branching — but drags in
SQLAlchemy (the codebase is plain asyncpg with hand-written SQL) and a second
place where schema truth lives (Python models). At three processes and one
database, that's machinery we'd be feeding, not using.

## Decision

Numbered SQL migration files (`src/epigone/migrations/NNNN_name.sql`) applied
in version order by `epigone.db.migrate()`, which every process (ADR-0002)
calls at startup — replacing `apply_schema`. Applied versions are recorded in
a `schema_migrations` table; the whole run executes as a single transaction
under a Postgres advisory lock, so concurrent process startups serialize and a
failed migration rolls the entire run back, bookkeeping included.

Migration `0001_baseline.sql` is the old `schema.sql` (verbatim apart from
its header comment) and stays idempotent, which is the baseline story for
pre-runner databases: on the live
DB — already at the target shape — it executes as a no-op and simply records
version 1. Later migrations run exactly once and need no idempotency.

Rules of the road:

- A shipped migration file is frozen history. Never edit one to change the
  schema; add the next number. (Dev branches may still edit their own
  unmerged migrations — tests rebuild from scratch precisely so that stale
  bookkeeping in a dev database can't bite.)
- No downgrade files. Rollback at V1 scale is a restored dump or a
  forward-fixing migration.
- Hand-applied production ALTERs are retired as a practice: from now on the
  deploy itself migrates.

## Consequences

- Schema truth lives in exactly one place, as SQL, reviewable in diffs.
- Startup is self-contained: a fresh database, the live database, and a
  half-migrated-then-crashed database all converge on the same schema with no
  operator steps.
- We forgo Alembic's autogeneration and downgrades; writing ALTERs by hand is
  the accepted cost, matching the hand-written-SQL style of the rest of the
  codebase.
- If V1 outgrows this (multiple databases, branched development, Python-level
  data migrations), Alembic can be adopted later by treating the then-current
  schema as its own baseline — the same trick 0001 plays today.
