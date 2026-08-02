# Runbook: agent-key rotation (issue #134, ADR-0005)

Hyperliquid agent keys expire in ≤180 days, so rotation is scheduled work,
not an incident. The health monitor DMs a ⚠️ reminder once the soonest
active-key expiry is inside `HEALTHCHECK_AGENT_KEY_WARN_DAYS` (default 14) and
escalates to 🚨 once a key has actually expired — at which point the keystore
refuses to sign and trading is down. Rotate on the warning, never the alarm.

Rotation is a fresh approval ceremony plus a keystore roll. It cannot be
automated: `approveAgent` is user-signed — master key only — and the master
key never touches Epigone infrastructure (ADR-0005).

Rotate ONE LANE AT A TIME (issue #135): the executor and watchdog lanes each
hold their own key (`--lane` on every keystore command below, default
`executor`), with their own name pairs (`epigone-a`/`-b`,
`epigone-watchdog-a`/`-b`). A zero-volume account has exactly 3 agent slots
(funded probe, PR #141), so the overlap slot only exists for one lane's
rotation at a time.

**After rotating the watchdog lane, RESTART the watchdog service**
(`docker compose restart watchdog`): it loads its signer once at startup, so
until the restart it keeps signing with the old key — and once the old agent
is deregistered (step 4) it is beating-but-impotent. The ~6-hourly on-chain
capability probe will page 🚨 if you forget, but the restart is what fixes
it, and doing it inside the overlap window means zero impotent minutes.

The restart also refreshes the watchdog's **cold-start key cache** (issue
#145): the encrypted local copy it would fall back on if it had to start
while Postgres was unreachable. Until a DB-backed start happens, that cache
still holds the OLD key — so a rotation followed by an outage-time restart
would come up on the pre-rotation key. It reconciles itself (the reconnect
refreshes the cache and logs the mismatch loudly), but the clean sequence is:
rotate → restart → *then* deregister the old agent.

## Steps

1. **Approve the new agent first, under the alternate name.** Run the
   ceremony (docs/runbooks/agent-key-ceremony.md) with the *other* name in
   the `epigone-a`/`epigone-b` pair. Re-approving over the name currently in
   use would deregister the live key instantly (research §3); the alternate
   name keeps the old key valid while the new one is being set up. Stop after
   the UI shows the new key approved — don't import yet.
2. **Roll the keystore.** One active key per user is a hard constraint, so
   revoke-then-import, back to back:

   ```sh
   python -m epigone.keystore revoke --user <operator telegram id>
   python -m epigone.keystore import --user <operator telegram id> \
       --master <master address> --name <the new name> --days <matching the UI expiry>
   ```

   The gap between the two commands is a few seconds in which the keystore
   has no signer; the executor treats that as a transient signing failure and
   retries. Prefer a quiet moment anyway.
3. **Verify** exactly as the ceremony does: `keystore list` (new key active,
   old one revoked) and the `extraAgents` query (new address present, right
   `validUntil`).
4. **Deregister the old agent** on the Hyperliquid API page (master wallet).
   Expiry would prune it eventually; deregistering now closes the window in
   which two keys can trade. The old agent address is never reused — the
   keystore tombstone enforces this permanently (nonce-replay hazard,
   ADR-0005).

## Rehearsal record (2026-07-27)

The keystore-roll leg (steps 2–3) was rehearsed against a scratch database:
`generate` → `revoke` → `import` (fresh in-test key) → `list` showed the old
key as a permanent tombstone and the new one active, and re-importing a
revoked agent address was refused. The on-exchange leg (steps 1 and 4) rides
the approval ceremony, which is blocked headlessly at the testnet deposit
gate (see the ceremony runbook's rehearsal record) — fold it into the
operator's testnet dress rehearsal.

## KEK rotation (rare — suspected KEK exposure, host migration)

The `kek_id` column records which KEK wrapped each key's DEK, and the
keystore refuses to decrypt under a mismatched KEK, so a KEK swap is loud,
never silent corruption. v0 keeps it simple: generate the new KEK file
(`gen-kek` at a fresh path, repoint `KEYSTORE_KEK_FILE`), then run the full
agent rotation above for every active key — new agent keys sealed under the
new KEK, old ones revoked and deregistered. With one operator key in Phase A
that is one ceremony; revisit with a re-wrapping tool if Phase B multiplies
users. If the *agent keys* themselves may have been exposed alongside the
KEK, deregister the old agents on Hyperliquid before anything else.
