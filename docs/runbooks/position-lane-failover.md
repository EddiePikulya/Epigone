# Runbook: the position lane degraded

**You are here because the monitor said:** *"Position lane DEGRADED …m ago — the
REST poller is producing events: …"*

## What has happened

The websocket lane has stopped being the producer of position events and the
REST poll pass has taken production back, on its own, without anyone
intervening (issue #158, ADR-0009).

**Nothing is down.** Position Alerts, Withdrawal Alerts and copy execution all
continue — from the poller, at its escalated 10s cadence, exactly as they did
before the cutover. That is the entire point of the warm standby: the failover
path is the path that runs every day. There is no emergency action to take.

What you lose while degraded: about 4 seconds of signal latency, the websocket's
finer view of scale-ins, and — because the poller is back at 10s — the share of
the REST weight budget that had been going to ingest, so fine metrics refresh
more slowly.

## Read the reason

```sql
SELECT owner, since, reason, healthy_since FROM lane_authority;
```

The `reason` is the diagnosis and there are only three shapes:

| Reason | What it means |
| --- | --- |
| `websocket heartbeat stale (…s > 45s)` | The lane stopped receiving market data, or the process is gone. The ordinary case. |
| `reconciliation drift: 0x… COIN never arrived on the websocket` | **The important one.** The lane was connected and delivering, and a change still never reached it. This is the failure mode nothing else detects — see below. |
| `websocket authority disabled by configuration` | Somebody set `WS_AUTHORITATIVE=0`. Not an incident; the monitor does not alert on it. |

## If it is a stale heartbeat

Check the lane is alive and what it says:

```sh
docker compose ps ws
docker compose logs --tail=200 ws
```

Common and self-resolving: a reconnect that took longer than 45s (a large poll
set resyncs every wallet over REST first, which is deliberate). Do nothing —
ownership returns automatically after 5 minutes of uninterrupted health, once
the lane has re-read absolute state for every tracked wallet.

If the lane is crash-looping, the logs name the failure. Restarting it is safe
at any time (`docker compose restart ws`); production stays with the poller
until the lane earns it back.

## If it is reconciliation drift

This is the finding the cutover was designed around: a websocket that is
connected, delivering, and silently missing changes. The poller caught it by
comparing its own diff against what the lane produced.

1. **Confirm what was missed.** The reason names the wallet and coin:

   ```sql
   SELECT observed_at, source, authoritative, kind, coin, size_usd
   FROM position_events
   WHERE trader_address = '0x…' AND coin = 'COIN'
   ORDER BY id DESC LIMIT 20;
   ```

   Both lanes write everything they observe, so this shows the two descriptions
   side by side. A `poll` row with no `ws` row near it, on a wallet where the
   lane produced other events either side, is a genuine miss.

2. **Note what this reason has already ruled out.** Two innocent explanations
   are filtered before an incident can be raised, so neither is what you are
   looking at:

   - *the lane was merely slow* — the poller withholds its verdict on a change
     the websocket has not produced and re-polls that wallet on the next tick,
     so drift means the change was still missing ~10s later;
   - *the threshold artefact* — a gradual size change crossing 25% against the
     poller's 60s anchor while never crossing it in any single ~5s push. The
     poller checks the websocket lane's own anchor (`ws_position_snapshots`)
     before doubting anything, and a lane whose memory already matches reality
     owes nothing.

   So a `reconciliation drift` reason means the lane's OWN memory says it still
   owed an event, twice, ten seconds apart. Treat it as real.

3. **Nothing needs restarting.** The escalation already transferred production
   and the event was produced by the poller. The lane will re-read absolute
   state for every wallet before it is trusted again.

4. **If drift repeats on the same wallet**, that wallet's subscription is the
   suspect. Restarting the ws service re-subscribes everything from a clean
   connection. If it repeats across wallets, set `WS_AUTHORITATIVE=0` on the
   `stream` service, redeploy that one service, and open an issue with the rows
   from step 1 — the poller carries production indefinitely at no cost beyond
   latency and budget.

## Turning the cutover off, and back on

```sh
# in .env on the server
WS_AUTHORITATIVE=0
docker compose up -d stream
```

Effective on the next 10s tick, with no probation — undoing a cutover is
immediate by design. The websocket lane keeps running and keeps recording
everything it sees (as non-authoritative rows), so the comparison dataset
carries on growing while it is switched off.

Removing the variable (or setting it to 1) re-enables the cutover; ownership
then returns through the ordinary path — 5 minutes of health plus a full
re-anchor — rather than instantly.

## Verifying the lane is healthy again

```sql
SELECT owner, since, reason FROM lane_authority;                    -- expect 'ws'
SELECT process, beaten_at FROM process_heartbeats WHERE process = 'ws_shadow';
SELECT source, authoritative, count(*) FROM position_events
WHERE observed_at > now() - interval '1 hour' GROUP BY 1, 2;
```

In steady state that last query shows `ws/true` rows and `poll/false` rows: the
websocket producing, the poller shadowing and reconciling. Both lanes writing is
the design working, not a duplicate.
