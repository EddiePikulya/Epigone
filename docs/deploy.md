# Deploy runbook (V1, single VPS)

Deploys Epigone's Docker Compose stack (postgres + bot + ingest + stream +
monitor) onto a single Ubuntu server. Restores the current Universe from a
`pg_dump` so there's no ~8h fine re-fill. Issue #12.

**Roles:** you run these on the **server** (via `ssh root@<IP>`) unless a step is
marked **[on your Mac]**. Paste output back at the `✅ verify` checkpoints.

Sections 1–6 are the **first install**. Every deploy after that is
["Updating later"](#updating-later-after-a-merge-to-main) — `git pull &&
./scripts/deploy.sh`, which dumps the database before it restarts anything.

Placeholders: `<IP>` = server IP, `<MAC-REPO>` = `/Users/ediksymonian/SE/Epigone`.

---

## 0. [on your Mac] Fresh backup + gather secrets

```sh
cd /Users/ediksymonian/SE/Epigone
docker compose -p epigone exec -T postgres pg_dump -U epigone -d epigone > /tmp/epigone_deploy.sql
wc -c /tmp/epigone_deploy.sql   # sanity: tens of MB
```
Note your `.env` values (TELEGRAM_BOT_TOKEN, ADMIN_TELEGRAM_ID) — you'll recreate `.env` on the server (never commit or scp it into git; scp to a path is fine).

## 1. Log in + base hardening

```sh
ssh root@<IP>
apt update && apt -y upgrade
# firewall: allow SSH only. Bot uses outbound long-polling, so no inbound app port.
ufw allow OpenSSH && ufw --force enable
timedatectl set-timezone UTC
```
✅ `ufw status` → only 22/OpenSSH allowed.

## 2. Install Docker + Compose plugin

```sh
curl -fsSL https://get.docker.com | sh
docker compose version   # confirm the plugin is present
systemctl enable docker  # start on boot (usually already enabled)
```
✅ `docker compose version` prints a version.

## 3. Get the code

Private repo → add a **read-only deploy key**:
```sh
ssh-keygen -t ed25519 -f ~/.ssh/epigone_deploy -N ""
cat ~/.ssh/epigone_deploy.pub
```
Add that public key at **GitHub → repo → Settings → Deploy keys → Add** (read-only). Then:
```sh
cat >> ~/.ssh/config <<'EOF'
Host github-epigone
  HostName github.com
  User git
  IdentityFile ~/.ssh/epigone_deploy
EOF
git clone github-epigone:EddiePikulya/Epigone.git ~/epigone
cd ~/epigone
```
✅ `ls` shows the repo (docker-compose.yml, src/, …).

## 4. Server-side secrets

```sh
cd ~/epigone
cat > .env <<'EOF'
TELEGRAM_BOT_TOKEN=<paste token>
ADMIN_TELEGRAM_ID=370818090
# Optional. Coarse Universe re-seed cadence in minutes (issue #50); defaults to
# 60 if unset. It's a single free CDN download that doesn't touch the per-IP
# rate budget, so lowering it (e.g. 30 or 15) only adds DB churn. A non-numeric
# or non-positive value falls back to 60 with a logged warning.
# SEED_INTERVAL_MINUTES=60
#
# Optional. Health-check (issue #52), all with safe defaults — a bad value falls
# back with a logged warning. The monitor reuses the token/admin above (send-only)
# and DMs the admin on problems, recoveries, and a daily heartbeat.
# HEALTHCHECK_INTERVAL_MINUTES=15        # how often the checks run
# HEALTHCHECK_HEARTBEAT_HOUR=9           # UTC hour for the daily "all good" digest
# HEALTHCHECK_REMINDER_HOURS=6           # cadence of reminders while a check stays failing
# HEALTHCHECK_INGEST_STALL_MINUTES=30    # no fine refresh in this window (with traders due) → alert
# HEALTHCHECK_STARVATION_WINDOW_MINUTES=45  # attempts advancing but zero successes this long → alert
# HEALTHCHECK_STARVATION_MIN_DUE=50      # only starve-alert once the due backlog is at least this big
# HEALTHCHECK_COARSE_STALE_MINUTES=      # default = 2× SEED_INTERVAL_MINUTES
# HEALTHCHECK_ALERT_BACKLOG_MINUTES=5    # undelivered Position Alerts older than this → alert
# HEALTHCHECK_DISK_PERCENT=85            # host disk used-% that trips the disk check
EOF
chmod 600 .env
```
`.env` is gitignored — it stays local to the server.

## 5. Restore the Universe

**[on your Mac]** copy the dump up:
```sh
scp /tmp/epigone_deploy.sql root@<IP>:~/epigone_deploy.sql
```
**On the server** — bring up *only* Postgres, then restore into it before starting the app processes:
```sh
cd ~/epigone
docker compose up -d postgres
sleep 8   # let it become healthy
docker compose exec -T postgres psql -U epigone -d epigone < ~/epigone_deploy.sql
```
✅ verify data + migrations landed:
```sh
docker compose exec -T postgres psql -U epigone -d epigone -c \
"select (select count(*) from traders) traders, (select count(*) from fine_metrics) fine, (select max(version) from schema_migrations) at_migration;"
```
Expect ~40k traders, ~10k fine, migration = 3.

## 6. Bring up the whole stack

```sh
docker compose up -d --build   # builds the image + starts bot/ingest/stream/monitor
docker compose ps              # all Up; postgres healthy
```
The app processes call `migrate()` at startup, see v1–v3 already applied (from the restore), and skip. Bot boots gated (ADMIN_TELEGRAM_ID present).

✅ verify:
```sh
docker compose logs bot --tail=5 | grep -i "Run polling"
docker compose logs stream --tail=3
```

## 7. Cut over

- Test in Telegram: the bot on the server now responds (you're the admin). It's the **same bot token**, so **stop the Mac copy first** to avoid two instances polling one token (double responses):
  **[on your Mac]** `docker compose -p epigone stop bot stream ingest monitor` (leave Mac Postgres if you want it as a spare; it's independent). Stop `monitor` too — send-only, so it won't double-poll, but two monitors would double the alerts/heartbeat.
- Reboot test: `reboot` the server, `ssh` back, `docker compose ps` → everything `Up` on its own (restart: unless-stopped + Docker on boot).

## Updating later (after a merge to main)

```sh
cd ~/epigone && git pull && ./scripts/deploy.sh
```

That is the whole update flow — no hand-DDL (the #16/#37 payoff), and no
remembering to back up first (the #160 payoff). `scripts/deploy.sh`:

1. refuses to run if postgres isn't up — there would be nothing to dump, and a
   deploy that proceeds unprotected is the thing it exists to prevent;
2. dumps the database to `~/epigone-dumps/epigone-<UTC>.dump`
   (`--format=custom`, ~34 MB, ~5s) and reads the archive back end to end
   before accepting it. A dump that fails or comes back short stops the deploy
   here, with the old containers still running;
3. deletes all but the **3 most recent** dumps;
4. `docker compose up -d --build`, where `migrate()` applies any new numbered
   migrations at startup.

**The order is the point.** `pg_dump` takes locks that let reads and writes
through but conflict with schema changes, and migrations run at container
startup (ADR-0003) — so the dump has to *finish* before anything comes up, not
run alongside it. Getting a dump and a migration in the same window is how you
get neither.

The `git pull` stays outside the script on purpose: bash reads a script as it
executes it, so a script that pulls itself can be re-read from a file that
changed underneath it mid-run.

Optional knobs, both with safe defaults: `EPIGONE_DUMP_DIR`
(`~/epigone-dumps`), `EPIGONE_DUMP_KEEP` (`3`; a value below 1 is refused
rather than obeyed).

**Restoring one of these dumps: `docs/runbooks/restore-from-dump.md`.** Read it
before you need it — in particular the part about the code on disk re-applying
the migration you just restored away from.

### The dumps from before this existed

Retention only touches files it wrote itself, so the pre-#160 hand-made dumps
(`~/epigone_pre_*.sql`, ~2.1 GB) stay until the operator removes them — the
runbook's "Housekeeping" section has the command.

## Notes
- Postgres is bound to `127.0.0.1` (not internet-exposed); creds are dev-grade but unreachable from outside. Rotating to a strong password is a later hardening.
- Only outbound traffic is needed (Hyperliquid API, Telegram long-poll), so the firewall blocks all inbound except SSH.
