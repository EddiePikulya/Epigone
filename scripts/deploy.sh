#!/usr/bin/env bash
#
# Epigone deploy (issue #160): check what is about to be deployed, dump the
# database, prune old dumps, then bring the stack up.
#
#     cd ~/epigone && git pull && ./scripts/deploy.sh
#
# Nothing ships unchecked and nothing restarts undumped, in that order — the
# guard is first because a deploy that is going to be refused should cost
# nothing, not a dump.
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
#   * pull, or checkout. Bash reads a script as it executes it, so a script that
#     moves its own working tree can be re-read from a file that changed
#     underneath it. The pull stays in the operator's hands, one command earlier
#     — but it is no longer unwitnessed: the guard asserts what is on disk and
#     refuses if the pull was skipped or failed. Everything that must not be
#     skippable is in here; nothing in here mutates the checkout.
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

# Fail closed on *what* is about to be deployed. Printing the commit further
# down is not checking it: HEAD is whatever the operator left on disk, and the
# `git pull` meant to move it ran one command earlier, outside this script,
# where nothing noticed if it never ran or failed and scrolled past. That is the
# expensive mistake precisely because it is the quiet one — the identical commit
# rebuilds, every step reports success, and the operator walks away believing a
# change is live. The inverse costs as much: a commit made here on the server
# ships code no one reviewed.
#
# Assert, never mutate — see the note on pull/checkout above. Each refusal names
# the command that fixes it and stops.

branch="$(git symbolic-ref --quiet --short HEAD || true)"
if [[ -z "$branch" ]]; then
    echo "deploy: HEAD is detached at $(git rev-parse --short HEAD), so there is no" >&2
    echo "        branch here to deploy. Run: git checkout main && git pull" >&2
    exit 1
fi
if [[ "$branch" != main ]]; then
    echo "deploy: this checkout is on '$branch'. Production deploys main, and only" >&2
    echo "        main. Run: git checkout main && git pull" >&2
    exit 1
fi

# Untracked files count as dirty. The image is built from the working tree
# (Dockerfile `COPY src ./src`, and there is no .dockerignore), not from the
# commit, so an unexpected file in src/ ships just as surely as an edited one.
dirty="$(git status --porcelain)"
if [[ -n "$dirty" ]]; then
    echo "deploy: the working tree holds changes that are in no commit:" >&2
    printf '%s\n' "$dirty" | sed 's/^/          /' >&2
    echo "        The build copies these files, so they would ship under a commit hash" >&2
    echo "        that says nothing about them. Get them reviewed and merged, or drop" >&2
    echo "        them here: git stash --include-untracked" >&2
    exit 1
fi

# A fetch writes objects and refs under .git and cannot move the working tree,
# which is what makes it safe inside a running script where a pull is not.
if ! git fetch --quiet origin main; then
    echo "deploy: cannot reach origin, so the commit on disk cannot be confirmed as" >&2
    echo "        the reviewed one. Refusing to deploy code of unproven provenance." >&2
    echo "        Check connectivity and the deploy key (ssh -T github-epigone)." >&2
    exit 1
fi

# FETCH_HEAD, not origin/main: it is exactly what that fetch just brought back,
# independent of how this clone's remote-tracking refs happen to be configured.
head_sha="$(git rev-parse HEAD)"
main_sha="$(git rev-parse FETCH_HEAD)"
if [[ "$head_sha" != "$main_sha" ]]; then
    echo "deploy: HEAD is not origin/main, so this would not deploy what you think." >&2
    echo "          on disk:     $(git log -1 --format='%h %s' "$head_sha")" >&2
    echo "          origin/main: $(git log -1 --format='%h %s' "$main_sha")" >&2
    if git merge-base --is-ancestor "$head_sha" "$main_sha"; then
        echo "        Behind by $(git rev-list --count "$head_sha..$main_sha") commit(s): the pull did not run, or it failed." >&2
        echo "        Run: git pull && ./scripts/deploy.sh" >&2
    elif git merge-base --is-ancestor "$main_sha" "$head_sha"; then
        echo "        $(git rev-list --count "$main_sha..$head_sha") commit(s) exist only here, committed on the server and never" >&2
        echo "        reviewed. Push them through a PR, or: git reset --hard origin/main" >&2
    else
        echo "        The two have diverged: each holds commits the other does not." >&2
        echo "        See what is only here: git log --oneline origin/main..HEAD" >&2
        echo "        Then resolve it deliberately — the fix depends on whether those" >&2
        echo "        commits matter, so there is no one command to suggest." >&2
    fi
    exit 1
fi

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
echo "    verified: on main, clean tree, identical to origin/main"

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
