# Runbook: restore from a pre-deploy dump (issue #160)

## What these dumps are

Every deploy (`scripts/deploy.sh`) takes a compressed `pg_dump` of the live
database *before* any container starts, and keeps the **3 most recent**:

```
~/epigone-dumps/epigone-20260730T120000Z.dump   # UTC, so the names sort chronologically
```

Custom format (`pg_dump --format=custom`) — ~34 MB and ~5s on production's
322 MB, against ~135 MB for plain SQL. The size is a side benefit; the reason
for the format is that it lets you restore **one table** instead of replaying
the whole database, which is what you actually want when a migration mangled
one table and everything else has moved on since.

## What they protect against, and what they don't

The migration runner applies everything in one transaction under an advisory
lock (ADR-0003), so a migration that *errors* rolls back cleanly and takes its
own bookkeeping with it. These dumps are for the case that leaves no trace: a
migration that **succeeded and was wrong** — a delete with a wider predicate
than intended, a column dropped too early, a backfill writing plausible
nonsense.

They are not point-in-time recovery. A dump is the state at the last deploy;
everything the ingest, stream and bot wrote since is not in it. That is why
the single-table path below is almost always the right one — it repairs what
the migration broke without discarding hours of alerts, fills and metrics.

## Before you touch anything

```sh
ssh root@<IP> && cd ~/epigone
ls -lh ~/epigone-dumps                                # which dumps exist
docker compose exec -T postgres pg_restore --list < ~/epigone-dumps/<dump> | head -40
```

`--list` prints the archive's table of contents: the tables it holds, and the
`SCHEMA` entries that tell you what shape the database was in. Pick the dump
whose timestamp is *before* the deploy that caused the problem.

> The dump's own integrity was already checked when it was taken — the deploy
> reads the whole archive back before it will accept it as a dump. `--list`
> alone would not have caught a truncated file: the table of contents lives at
> the head of the archive and reads back fine when the data half is missing.

## Path A — one table is wrong (the usual case)

Puts a single table back to its pre-deploy contents and leaves the rest of the
database, and everything written since, untouched.

```sh
cd ~/epigone
docker compose stop bot ingest stream monitor        # nothing should be writing the table
docker compose exec -T postgres psql -U epigone -d epigone -c 'TRUNCATE <table>;'
docker compose exec -T postgres pg_restore -U epigone -d epigone \
  --data-only --table=<table> < ~/epigone-dumps/<dump>
docker compose up -d
```

`--data-only` is deliberate: the table's *current* definition stays, only the
rows come back. If the bad migration also changed the table's shape, fix the
shape with a forward migration first (ADR-0003 has no downgrades) and only then
restore the rows.

Truncating a table other things reference will fail on its foreign keys rather
than silently cascade — that failure is information, not an obstacle to work
around with `CASCADE`. Stop and think about which tables actually need to move
together.

## Path B — the whole database back to the pre-deploy state

The heavy option. It discards **everything written since the dump**.

```sh
cd ~/epigone
docker compose stop bot ingest stream monitor        # postgres stays up: it holds the data
docker compose exec -T postgres psql -U epigone -d postgres \
  -c 'DROP DATABASE epigone;' -c 'CREATE DATABASE epigone OWNER epigone;'
docker compose exec -T postgres pg_restore -U epigone -d epigone --no-owner \
  < ~/epigone-dumps/<dump>
```

**Now stop.** The database is back at the dump's schema version, and the code
on disk is the code whose migration caused this. Bring the containers up now
and they will re-apply that exact migration at startup, on purpose, within
seconds. Before `docker compose up -d`, one of these must be true:

- the repo is checked out at the commit *before* the bad migration
  (`git checkout <sha>`), or
- a forward-fixing migration is written, reviewed and merged, and the repo is
  pulled to it.

Then:

```sh
docker compose up -d --build
docker compose exec -T postgres psql -U epigone -d epigone \
  -c 'select max(version) from schema_migrations;'
```

## Verifying a restore

Row counts on the tables you care about, and the schema itself:

```sh
docker compose exec -T postgres psql -U epigone -d epigone -c \
"select (select count(*) from traders) traders, (select count(*) from coarse_metrics) coarse,
        (select count(*) from fine_metrics) fine, (select max(version) from schema_migrations) at_migration;"
```

To compare a restored database against another one, fingerprint the schema on
both — identical hashes mean every table, column and type came back:

```sh
docker compose exec -T postgres psql -U epigone -d <db> -tAc \
"select md5(string_agg(table_name||'.'||column_name||':'||data_type, ',' order by table_name, column_name))
 from information_schema.columns where table_schema='public';"
```

## This procedure has been run

Rehearsed against the development database (87 MB, 40,467 traders, at
migration 3) into a scratch database, per the ticket's "an untested restore is
not a backup":

- **Full restore** into a fresh `epigone_restore_check`: 1.4s, exit 0. Row
  counts identical across `traders` / `coarse_metrics` / `fine_metrics`
  (40,467 / 161,864 / 10,262), `schema_migrations` at the same version, and the
  schema fingerprint above matched the source exactly.
- **Single-table restore** (Path A): `coarse_metrics` truncated to 0, then
  `--data-only --table=coarse_metrics` restored all 161,864 rows.
- **A truncated archive is rejected**, so a half-written dump cannot pass
  itself off as a backup: a full read exits 1 (`could not read from input
  file: end of file`) where `pg_restore --list` exits 0.

Worth re-running against a production-sized dump the next time one is copied
down; the shape of the procedure is what was verified here, not the timings.

## Housekeeping

Retention is automatic (3 dumps) but only over the dumps `deploy.sh` writes —
it deletes nothing it cannot positively identify as its own, so the hand-made
dumps that predate this ticket are the operator's to remove. On the server,
after one deploy has run through the new script (so a current dump exists):

```sh
ls -lh ~/epigone_pre_*.sql ~/epigone_deploy.sql
du -ch ~/epigone_pre_*.sql ~/epigone_deploy.sql | tail -1   # ~2.1 GB across 19 files
rm -i ~/epigone_pre_*.sql ~/epigone_deploy.sql
```

Those are the plain-SQL pre-migration dumps for 0004–0023 plus the original
bootstrap dump — the practice this ticket automated. They are superseded by
`~/epigone-dumps`, and at 7% of the disk they are the largest thing on it.
