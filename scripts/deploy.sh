#!/usr/bin/env bash
#
# Epigone deploy (issue #160): dump the database, prune old dumps, then bring
# the stack up.
#
#     cd ~/epigone && git pull && ./scripts/deploy.sh
#
# The dump is not a step beside the deploy, it is the deploy's first act. The
# migration runner's transaction (ADR-0003) protects against a migration that
# *errors*; nothing protects against one that succeeds and is wrong, which is
# what a dump is for. A dump you have to remember is one you eventually don't:
# the server holds pre-migration dumps up to 0023 and production is on 0028.
#
# Order is load-bearing. pg_dump takes locks that let reads and writes through
# but conflict with schema changes, and every container runs migrate() at
# startup — so the dump must *finish* before anything comes up, not race it.
#
# Two things this deliberately does not do:
#
#   * pull. Bash reads a script as it executes it, so a script that git-pulls
#     itself can be re-read from a file that changed underneath it. The pull
#     stays in the operator's hands, one command earlier; everything that must
#     not be skippable is in here.
#   * restart postgres. It is the long-lived container holding the data; only
#     the app containers are rebuilt, and `up -d` leaves an unchanged postgres
#     alone. If a compose change ever does force it to recreate, that happens
#     after the dump has landed.
#
# Knobs, all optional: EPIGONE_DUMP_DIR (default ~/epigone-dumps),
# EPIGONE_DUMP_KEEP (default 3), COMPOSE_PROJECT_NAME (docker's own).

set -euo pipefail

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
DUMP_DIR="${EPIGONE_DUMP_DIR:-$HOME/epigone-dumps}"
KEEP="${EPIGONE_DUMP_KEEP:-3}"
# Matches POSTGRES_USER/POSTGRES_DB in docker-compose.yml.
DB_USER=epigone
DB_NAME=epigone
RETENTION_PY="$REPO_DIR/src/epigone/dump_retention.py"

cd -- "$REPO_DIR"

# Fail closed. If postgres is not up there is nothing to dump, and a deploy that
# proceeds unprotected is the failure this script exists to remove. A first-ever
# install has no database yet and follows docs/deploy.md sections 1-6 instead.
#
# Collected into a variable rather than piped straight into grep: under pipefail,
# `grep -q` exits at its first match and can SIGPIPE the compose process, which
# would read as "postgres is down" on a perfectly healthy server.
running_services="$(docker compose ps --services --filter status=running)"
if ! printf '%s\n' "$running_services" | grep -qx postgres; then
    echo "deploy: the postgres container is not running, so no dump can be taken." >&2
    echo "        Refusing to deploy unprotected. If this is a first install, follow" >&2
    echo "        docs/deploy.md sections 1-6; otherwise start postgres and retry." >&2
    exit 1
fi

echo "==> deploying $(git rev-parse --short HEAD) ($(git log -1 --format=%s))"

mkdir -p -- "$DUMP_DIR"
dump="$DUMP_DIR/$(python3 -- "$RETENTION_PY" name)"
partial="$dump.partial"
# A dump that died halfway must leave nothing behind that a later restore could
# mistake for a backup.
trap 'rm -f -- "$partial"' EXIT

echo "==> dumping $DB_NAME -> $dump"
docker compose exec -T postgres pg_dump -U "$DB_USER" -d "$DB_NAME" --format=custom >"$partial"

# The redirect above creates the file whether pg_dump succeeded or not, and a
# truncated archive is indistinguishable from a good one by size alone. Only an
# archive that reads back end to end becomes a dump: this decompresses every
# data block and throws the SQL away. `pg_restore --list` is the tempting
# cheaper check and it is worthless here — the table of contents sits at the
# head of the file, so it exits 0 on an archive whose data half is missing
# (measured). A full read costs ~0.3s per 6 MB.
docker compose exec -T postgres pg_restore --file=/dev/null <"$partial"
mv -- "$partial" "$dump"
trap - EXIT
echo "==> dump ok ($(du -h -- "$dump" | cut -f1)); restore: docs/runbooks/restore-from-dump.md"

python3 -- "$RETENTION_PY" prune "$DUMP_DIR" --keep "$KEEP"

echo "==> starting containers (migrations run here, after the dump)"
docker compose up -d --build
docker compose ps
