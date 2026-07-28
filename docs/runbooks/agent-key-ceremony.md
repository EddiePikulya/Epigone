# Runbook: agent-key approval ceremony (issue #134, ADR-0005)

The operator ceremony that puts a **trade-only Hyperliquid agent key** into
Epigone's encrypted keystore. Read this whole page once before the first run.

**The invariant (ADR-0005, absolute):** the master key — the thing that owns
funds and grants authority — never touches Epigone code, storage, or docs.
Every step below keeps the master key inside the operator's own wallet; the
only secret that ever reaches an Epigone machine is the agent key, which can
trade but can never withdraw, transfer, or grant authority (research §3).

## Prerequisites (once per host)

1. A KEK file, outside git and outside the database:

   ```sh
   export KEYSTORE_KEK_FILE=/etc/epigone/keystore.kek   # any path outside the repo
   python -m epigone.keystore gen-kek
   ```

   Back the KEK up **offline** (password manager or printed hex). Losing it
   orphans every stored key (recoverable by re-running this ceremony); leaking
   it reduces the envelope to the DB's own protection.

   *Why a file KEK rather than the issue's "age/KMS" wording:* the production
   host is a single Hetzner box with no KMS, and an age identity file would be
   exactly this — a secret file on the host — behind an extra dependency. The
   envelope (per-key DEK, KEK outside DB and git) is what §6.5 actually
   requires; the `kek_id` column is the seam for swapping the file for KMS or
   age later without a schema change. §6.5's honest limit applies either way:
   none of these defends a live compromised host — acceptable for agent keys
   only.
2. `DATABASE_URL` pointing at the Epigone database, and a `users` row for the
   account id you'll bind the key to (the operator's Telegram id in Phase A —
   any /start against the bot creates it).
3. When the executor process ships (A4), mount the KEK file **read-only** into
   that container in docker-compose — never bake it into an image or commit it.

## The two ceremony shapes, and which one we use

- **Shape (a) — Epigone generates the keypair**, prints only the agent
  *address* (`python -m epigone.keystore generate`), and the master wallet
  signs `approveAgent` for that address. The SDK's stock `approve_agent()`
  cannot do this (it always generates the key itself); it needs a hand-built
  `approveAgent` action via `sign_agent` — mechanics rehearsed and working,
  see the record below. The catch: *something* must sign with the master key,
  and in Phase A the only signing surfaces are a local script (master key
  exported into a process — exactly the handling pattern behind the incident
  record, research §8) or the Phase B signing page, which does not exist yet.
- **Shape (b) — the Hyperliquid UI generates the agent key** ("API wallet"),
  the master wallet approves it in the browser wallet, and the operator
  imports the printed agent private key into the keystore. The master key
  never leaves the wallet extension. The agent key transits the browser and
  clipboard — **accepted deliberately**: it is a trade-only key with bounded
  blast radius, user-side revocability, and ≤180-day expiry (ADR-0005), and
  the alternative shape exposes the *master* key instead.

**Decision: shape (b) for Phase A.** The strict invariant plus the incident
record make master-key handling the thing to minimize, and (b) is the only
shape where the master key never leaves the operator's wallet today. Shape (a)
becomes the natural upgrade in Phase B, when the signing page lets the master
wallet sign `approveAgent` in situ (ADR-0005 Phase B); the keystore already
supports it (`generate` + the rehearsed external-address approval).

## Testnet rehearsal record (2026-07-27)

`scripts/rehearse_agent_ceremony.py` against `api.hyperliquid-testnet.xyz`,
all keys throwaway and generated in-process:

- **Shape (a) mechanics verified to the deposit gate.** A hand-built
  `approveAgent` for an externally-generated agent address, signed by a
  throwaway master via the SDK's `sign_agent`, is accepted by the exchange up
  to `{"status": "err", "response": "Must deposit before performing actions.
  User: 0xd3b5…eb32"}` — the recovered signer in the error is exactly the
  throwaway master, which proves the action serializes, signs, and recovers
  correctly, and that the exchange does **not** reject an external agent
  address outright. The SDK's stock `approve_agent()` (shape a′) reaches the
  identical gate.
- **The deposit gate is the end of the headless road.** An unfunded account
  cannot submit any action, and the testnet faucet requires a browser wallet
  — the same dependency as shape (b). The funded dress rehearsal is therefore
  an operator step (checklist below), not a CI step.
- **Read-backs behave as documented:** `extraAgents` → `[]`, `userRole` →
  `{"role": "missing"}` for the unfunded throwaway.
- Whether the **UI** accepts externally-generated agent addresses remains
  unverified (issue #134 said don't assume) — irrelevant to shape (b), which
  lets the UI generate; note it before ever reviving a hybrid shape.

### Operator dress rehearsal (testnet, once before the first mainnet ceremony)

With a **throwaway** browser wallet (fine on testnet — never the real master):

1. Fund it: connect at `app.hyperliquid-testnet.xyz`, claim the faucet.
2. Run the ceremony below end-to-end against the testnet UI, importing into a
   scratch database.
3. Re-run `scripts/rehearse_agent_ceremony.py` with the funded throwaway's
   key pasted over `master = Account.create()` if you also want the shape-(a)
   approval to land past the deposit gate — and confirm the `valid_until`
   name-suffix expiry shows up in `extraAgents`.
4. `python -m epigone.keystore list` shows the key; done. Drop the scratch DB.

## The ceremony (shape b)

On the operator's own machine (browser with the master wallet):

1. Open the Hyperliquid app → **More → API** (`app.hyperliquid.xyz/API`).
2. Generate a new API/agent wallet. Name it `epigone-a` (rotation alternates
   `epigone-a`/`epigone-b` — re-approving over an existing name deregisters
   that name's old key instantly, research §3, and rotation wants overlap).
3. Set the longest validity offered (≤180 days) and note the exact expiry.
4. Approve — the master wallet signs `approveAgent` inside the wallet popup.
   This is the only master-key signature in the whole ceremony.
5. Copy the displayed agent **private key**.

On the Epigone host (with `DATABASE_URL` and `KEYSTORE_KEK_FILE` set):

```sh
python -m epigone.keystore import --user <operator telegram id> \
    --master <master account address> --name epigone-a --days <until the noted expiry>
```

**Lanes (issue #135):** each signing *process* gets its own agent key —
Hyperliquid tracks nonces per signer, and the watchdog must not depend on
the executor's key to clean up after the executor. The default lane is
`executor`; the watchdog's key is the same ceremony with `--lane watchdog`
and its own name pair (`epigone-watchdog-a`/`-b`). Mind the slot budget: a
zero-volume account has exactly **3** agent slots (funded probe, PR #141) —
two lanes plus one rotation-overlap slot is all of it, so rotate one lane at
a time.

6. Paste the key at the stdin prompt (never on the command line — argv lands
   in shell history and `ps`). `--days` must match step 3: the stored expiry
   drives the monitor's rotation reminder, and Hyperliquid's clock is the
   truth.
7. Clear the clipboard; close the API page.
8. Verify:
   - `python -m epigone.keystore list` shows the key, active, right expiry;
   - `curl -X POST https://api.hyperliquid.xyz/info -H 'Content-Type:
     application/json' -d '{"type":"extraAgents","user":"<master address>"}'`
     lists the same agent address with the same `validUntil` — the same
     query any user can run to audit our authority from outside our system.

## If anything leaks

Treat a suspected agent-key leak as severe despite being trade-only
(counter-trading extraction, research §3): deregister the agent on the
Hyperliquid API page immediately (master wallet, one click), then
`python -m epigone.keystore revoke --user <id>` and run this ceremony fresh.
Never re-import the old key — the keystore's tombstone will refuse it anyway
(nonce-replay hazard, ADR-0005).
