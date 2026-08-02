"""The watchdog's local, encrypted copy of its own agent key (issue #145).

WHY A SECOND COPY OF A KEY EXISTS AT ALL. The keystore lives in Postgres, so
the watchdog's startup needs Postgres to obtain the key it cancels with. That
made the whole Postgres-outage guarantee (PR #143) conditional on the process
being ALREADY RUNNING: a crash, an OOM, a host reboot, or a deploy *during* an
outage left the account with no cancel path until the database returned — and
those events are CORRELATED with the outage, which is the same argument that
motivated the DB-blind sweep in the first place.

So each successful DB-backed start writes the watchdog lane's key to this
cache, and a start that cannot reach Postgres loads it from here instead
(epigone.safety.coldstart). The custody is deliberately the keystore's, not a
new one:

- SAME KEK. The file is sealed under the same key-encryption key from the same
  file outside the database and outside git (KEYSTORE_KEK_FILE), through the
  keystore's own `seal`/`unseal`. A stolen cache file alone yields nothing —
  exactly the property a stolen DB dump has.
- SAME ENVELOPE. A per-cache random DEK seals the private key; the KEK wraps
  the DEK. Two blobs, same shape as an `agent_keys` row.
- AAD-BOUND TO THE WHOLE HEADER, and under a DISTINCT AAD prefix from the
  keystore's rows: a keystore ciphertext cannot be transplanted into this file
  (nor the reverse), and editing ANY header field — the lane, the master
  address, the expiry — makes the blob fail authentication instead of silently
  redirecting what this key is taken to be.

THE LANE IS PART OF THE INVARIANT, not a parameter. `write()` refuses anything
but the watchdog lane, so the executor's key can never reach this file — the
cold-start path can therefore only ever hold a key whose authority is the
watchdog's. ADR-0005 is untouched: this is a trade-only AGENT key, and master
keys remain absent from Epigone entirely.

WHAT THIS FILE DOES NOT SOLVE. It is a cache, not a source of truth: a
rotation performed while the watchdog is down leaves a stale copy here until
the next DB-backed start refreshes it (the reconnect path refreshes it too,
and says so loudly when the running process is signing with the older key —
docs/runbooks/agent-key-rotation.md). An expired cached key is refused rather
than used: Hyperliquid would reject its signatures anyway.
"""

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from eth_account import Account
from eth_account.signers.local import LocalAccount

from epigone.keystore import WATCHDOG_LANE, AgentKeyRecord, Kek, KeystoreError, seal, unseal

# Bumped only if the header's meaning changes; a file written by another
# version is refused rather than guessed at. It is AAD-bound like every other
# header field, so it cannot be edited down to a weaker reading.
CACHE_VERSION = 1

# The header fields the AAD covers — i.e. everything the loader trusts.
_HEADER_FIELDS = (
    "version",
    "lane",
    "user_id",
    "master_address",
    "agent_address",
    "expires_at",
    "refreshed_at",
)

DEFAULT_CACHE_FILENAME = "watchdog-key-cache.enc"


@dataclass(frozen=True)
class CachedWatchdogKey:
    """What a cold start gets instead of a keystore row: the signer plus the
    account it acts for. `refreshed_at` is how old this copy is — the operator
    surface for "was this cache written before or after the last rotation"."""

    signer: LocalAccount
    master_address: str
    agent_address: str
    expires_at: datetime
    refreshed_at: datetime


class WatchdogKeyCache:
    """The cache file, under the keystore's KEK. Synchronous on purpose: it is
    plain local file I/O on the startup path, and the cold-start decision must
    not depend on anything that can hang the way a database can."""

    def __init__(self, path: Path, kek: Kek) -> None:
        self._path = path
        self._kek = kek

    @property
    def path(self) -> Path:
        return self._path

    def write(self, record: AgentKeyRecord, private_key: bytes, *, now: datetime) -> None:
        """Refresh the cache from a keystore record. Called on EVERY successful
        DB-backed start (and on a cold start's reconnect), so the cached key
        tracks the keystore's active one within one restart.

        Refuses any lane but the watchdog's — the structural half of "the
        cached key is the watchdog lane's key only"."""
        if record.lane != WATCHDOG_LANE:
            raise KeystoreError(
                f"the watchdog key cache holds the {WATCHDOG_LANE} lane ONLY — refusing to "
                f"cache a {record.lane}-lane key (issue #145: a cold-started watchdog must "
                f"never be able to act with the executor's authority)"
            )
        if str(Account.from_key(private_key).address).lower() != record.agent_address:
            raise KeystoreError(
                "refusing to cache a private key that does not match its record's agent address"
            )
        header = {
            "version": CACHE_VERSION,
            "lane": record.lane,
            "user_id": record.user_id,
            "master_address": record.master_address,
            "agent_address": record.agent_address,
            "expires_at": record.expires_at.isoformat(),
            "refreshed_at": now.isoformat(),
        }
        aad = _aad(header)
        dek = os.urandom(32)
        payload = {
            **header,
            "kek_id": self._kek.kek_id,
            "dek_wrapped": seal(self._kek.material, dek, aad).hex(),
            "key_ciphertext": seal(dek, private_key, aad).hex(),
        }
        self._write_atomically(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    def load(self, now: datetime) -> CachedWatchdogKey:
        """The cold-start read. Every refusal is a KeystoreError naming what is
        wrong: the caller's only sane response is to fail fast and say why — a
        watchdog that starts without a usable key would beat its heartbeat
        while being unable to cancel anything, which is worse than not
        starting (the false-safety rule, epigone.safety.main)."""
        try:
            raw = self._path.read_text()
        except OSError as error:
            raise KeystoreError(
                f"no usable watchdog key cache at {self._path}: {error} — a cold start "
                f"during a Postgres outage needs one; it is written by every successful "
                f"DB-backed start"
            ) from error
        try:
            payload = json.loads(raw)
        except ValueError as error:
            raise KeystoreError(
                f"watchdog key cache {self._path} is not valid JSON: {error}"
            ) from error
        if not isinstance(payload, dict):
            raise KeystoreError(f"watchdog key cache {self._path} is not a JSON object")
        header = _header_of(payload, self._path)
        if header["version"] != CACHE_VERSION:
            raise KeystoreError(
                f"watchdog key cache {self._path} is version {header['version']!r}, this "
                f"build reads version {CACHE_VERSION}"
            )
        if header["lane"] != WATCHDOG_LANE:
            raise KeystoreError(
                f"watchdog key cache {self._path} claims lane {header['lane']!r} — only the "
                f"{WATCHDOG_LANE} lane may be cached"
            )
        if payload.get("kek_id") != self._kek.kek_id:
            raise KeystoreError(
                f"watchdog key cache {self._path} was sealed by KEK "
                f"{payload.get('kek_id')!r} but the loaded KEK is {self._kek.kek_id}"
            )
        aad = _aad(header)
        dek = unseal(self._kek.material, _blob(payload, "dek_wrapped", self._path), aad)
        private_key = unseal(dek, _blob(payload, "key_ciphertext", self._path), aad)
        signer: LocalAccount = Account.from_key(private_key)
        agent_address = str(header["agent_address"])
        if str(signer.address).lower() != agent_address:
            raise KeystoreError(
                f"watchdog key cache {self._path} decrypts to a key that is not its stated "
                f"agent address"
            )
        expires_at = _timestamp(header, "expires_at", self._path)
        if expires_at <= now:
            raise KeystoreError(
                f"the cached {WATCHDOG_LANE}-lane agent key expired at "
                f"{expires_at:%Y-%m-%d} — rotate it and restart with Postgres reachable "
                f"(docs/runbooks/agent-key-rotation.md)"
            )
        return CachedWatchdogKey(
            signer=signer,
            master_address=str(header["master_address"]),
            agent_address=agent_address,
            expires_at=expires_at,
            refreshed_at=_timestamp(header, "refreshed_at", self._path),
        )

    def _write_atomically(self, text: str) -> None:
        """Owner-only, and never a half-written file: a torn cache would fail
        authentication at exactly the moment (a cold start mid-outage) there is
        no database to fall back to."""
        self._path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = self._path.with_name(self._path.name + ".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise


def default_cache_path() -> Path:
    """Where the cache lives when WATCHDOG_KEY_CACHE_FILE says nothing. It must
    OUTLIVE the process (a container restart is the very event this exists
    for), so it belongs on a persistent path, never in a tmpfs — the compose
    service mounts a named volume and sets the variable explicitly."""
    return Path.home() / ".epigone" / DEFAULT_CACHE_FILENAME


def _aad(header: dict[str, Any]) -> bytes:
    """The whole header, canonically encoded, under a prefix DISTINCT from the
    keystore's row AAD: no ciphertext can move between the two stores, and no
    header field can be edited without breaking authentication."""
    canonical = json.dumps(header, sort_keys=True, separators=(",", ":"))
    return f"epigone-watchdog-key-cache:{canonical}".encode()


def _header_of(payload: dict[str, Any], path: Path) -> dict[str, Any]:
    missing = [field for field in _HEADER_FIELDS if field not in payload]
    if missing:
        raise KeystoreError(
            f"watchdog key cache {path} is missing {', '.join(missing)}"
        )
    return {field: payload[field] for field in _HEADER_FIELDS}


def _blob(payload: dict[str, Any], field: str, path: Path) -> bytes:
    try:
        return bytes.fromhex(str(payload[field]))
    except (KeyError, ValueError) as error:
        raise KeystoreError(
            f"watchdog key cache {path} has an unreadable {field}: {error}"
        ) from error


def _timestamp(header: dict[str, Any], field: str, path: Path) -> datetime:
    try:
        return datetime.fromisoformat(str(header[field]))
    except ValueError as error:
        raise KeystoreError(
            f"watchdog key cache {path} has an unreadable {field}: {error}"
        ) from error
