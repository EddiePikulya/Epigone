"""Encrypted agent-key keystore (issue #134, ADR-0005) and its operator CLI.

Holds Hyperliquid AGENT keys only — trade-only keys approved by a user's
master wallet via `approveAgent`. The invariant this module exists to keep
(ADR-0005): the master key never touches Epigone code, storage, or docs. The
keystore neither accepts, derives, nor names one; `master_address` throughout
is the public account address an agent trades for.

Envelope encryption (research §6.5): each stored key gets its own random DEK;
the private key is AES-256-GCM-sealed under the DEK and the DEK is wrapped
under a KEK read from a file outside the database and outside git, so a DB
dump or stolen backup alone yields nothing. Both blobs are AAD-bound to
(user_id, agent_address): ciphertext transplanted onto another row fails
authentication instead of redirecting whose signer a key becomes. Plaintext
key bytes exist only in-process, inside `signer()` at signing time — never
logged, never returned by any metadata view. (Python offers no reliable
zeroization; "in-process only" is the honest guarantee, and §6.5 is explicit
that a live compromised host is out of scope for agent keys.)

The seam the A1 execution gateway consumes (issue #133): `signer(user_id)`
yields an `eth_account` LocalAccount — exactly what hyperliquid-python-sdk
signing takes — for the user's active, unexpired key.

The CLI (`python -m epigone.keystore`) is the operator-ceremony surface
(docs/runbooks/agent-key-ceremony.md): generate/import/list/revoke plus KEK
generation. Private keys enter via stdin only — never argv, never env — and
no command ever prints one.
"""

import argparse
import asyncio
import hashlib
import os
import re
import secrets
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from eth_account import Account
from eth_account.signers.local import LocalAccount

from epigone.clock import Clock, SystemClock

# Hyperliquid's hard cap on approveAgent validity (research §3). The keystore
# refuses to record a longer expiry: a row that outlives its on-chain approval
# would silently sign into rejections instead of tripping the rotation reminder.
MAX_AGENT_KEY_DAYS = 180

# Ceremony default (CLI --days): renew comfortably inside the 180-day cap so
# the monitor's near-expiry reminder fires while the old key still works.
DEFAULT_CEREMONY_DAYS = 170

# Signing lanes (issue #135): one agent key per PROCESS per user account —
# Hyperliquid tracks nonces per signer, and the watchdog (the primary
# dead-man's switch) must not depend on the executor's key to clean up after
# the executor. The set is closed on purpose: a zero-volume account has
# exactly 3 agent slots (funded-testnet probe, PR #141) — two lanes plus one
# rotation-overlap slot is the whole budget.
EXECUTOR_LANE = "executor"
WATCHDOG_LANE = "watchdog"
LANES = (EXECUTOR_LANE, WATCHDOG_LANE)

_NONCE_LEN = 12  # AES-GCM standard nonce, prepended to each sealed blob
_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}")


class KeystoreError(Exception):
    """Any keystore refusal: bad KEK, decrypt failure, lifecycle violation."""


# --- KEK: the envelope's outer secret, a file outside the DB and outside git ---


@dataclass(frozen=True)
class Kek:
    """A loaded key-encryption key. `kek_id` (a hash prefix, safe to store and
    print) names which KEK wrapped each row's DEK so KEKs can rotate without a
    flag day; `material` never appears in repr or logs."""

    path: Path
    kek_id: str
    material: bytes = field(repr=False)


def _kek_id(material: bytes) -> str:
    return hashlib.sha256(material).hexdigest()[:12]


def generate_kek(path: Path) -> Kek:
    """Create a fresh 32-byte KEK file (owner-only permissions), refusing to
    clobber an existing one — overwriting a live KEK orphans every DEK it
    wrapped."""
    if path.exists():
        raise KeystoreError(f"KEK file already exists: {path}")
    material = secrets.token_bytes(32)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(material.hex() + "\n")
    return Kek(path=path, kek_id=_kek_id(material), material=material)


def load_kek(path: Path) -> Kek:
    try:
        raw = path.read_text().strip()
    except OSError as error:
        raise KeystoreError(f"cannot read KEK file {path}: {error}") from error
    if not re.fullmatch(r"[0-9a-fA-F]{64}", raw):
        raise KeystoreError(f"KEK file {path} must hold exactly 64 hex characters")
    material = bytes.fromhex(raw)
    return Kek(path=path, kek_id=_kek_id(material), material=material)


# --- envelope primitives ---


def _aad(user_id: int, agent_address: str) -> bytes:
    return f"epigone-agent-key:{user_id}:{agent_address}".encode()


def seal(key: bytes, plaintext: bytes, aad: bytes) -> bytes:
    """One AES-256-GCM envelope leg. Public because the watchdog's local key
    cache (issue #145) must use THIS custody — same KEK, same AAD-binding
    discipline — rather than grow a second, subtly different crypto path."""
    nonce = secrets.token_bytes(_NONCE_LEN)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, aad)


def unseal(key: bytes, blob: bytes, aad: bytes) -> bytes:
    # ValueError covers malformed blobs (e.g. truncated below a valid nonce);
    # every decrypt failure mode must surface as the domain error, and the
    # message must carry no key-derived material.
    try:
        return AESGCM(key).decrypt(blob[:_NONCE_LEN], blob[_NONCE_LEN:], aad)
    except (InvalidTag, ValueError):
        raise KeystoreError(
            "failed to decrypt agent key: sealed blob failed authentication or is malformed"
        ) from None


# --- the keystore ---


@dataclass(frozen=True)
class AgentKeyRecord:
    """One agent key's metadata — everything about a key except the key. The
    only view `list`/`active_record` (and the CLI) ever expose."""

    user_id: int
    lane: str
    master_address: str
    agent_address: str
    agent_name: str | None
    kek_id: str
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None


class AgentKeystore:
    """Agent keys at rest, by user-account id and signing lane (issue #135):
    at most one active key per (user, lane) — a rotation revokes before it
    stores. Revoked rows stay as tombstones so an agent address can never be
    reused after deregistration (nonce-replay hazard, ADR-0005)."""

    def __init__(self, pool: asyncpg.Pool, kek: Kek, clock: Clock) -> None:
        self._pool = pool
        self._kek = kek
        self._clock = clock

    async def store_agent_key(
        self,
        *,
        user_id: int,
        master_address: str,
        private_key: bytes,
        agent_name: str | None,
        expires_at: datetime,
        lane: str = EXECUTOR_LANE,
    ) -> AgentKeyRecord:
        """Encrypt and store an agent private key (ceremony shape where the key
        was created outside Epigone). The plaintext argument is used once to
        seal and derive the agent address, then goes out of scope."""
        master = _validate_address(master_address)
        _validate_lane(lane)
        now = self._clock.now()
        if expires_at <= now:
            raise KeystoreError("expires_at must be in the future")
        if expires_at > now + timedelta(days=MAX_AGENT_KEY_DAYS):
            raise KeystoreError(
                f"expires_at exceeds Hyperliquid's {MAX_AGENT_KEY_DAYS}-day agent validity cap"
            )
        try:
            agent_address = str(Account.from_key(private_key).address).lower()
        except (ValueError, TypeError) as error:
            raise KeystoreError(f"not a valid agent private key: {error}") from None
        aad = _aad(user_id, agent_address)
        dek = secrets.token_bytes(32)
        record = AgentKeyRecord(
            user_id=user_id,
            lane=lane,
            master_address=master,
            agent_address=agent_address,
            agent_name=agent_name,
            kek_id=self._kek.kek_id,
            created_at=now,
            expires_at=expires_at,
            revoked_at=None,
        )
        try:
            await self._pool.execute(
                """
                INSERT INTO agent_keys (user_id, lane, master_address, agent_address,
                                        agent_name, kek_id, dek_wrapped, key_ciphertext,
                                        created_at, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                user_id,
                lane,
                master,
                agent_address,
                agent_name,
                self._kek.kek_id,
                seal(self._kek.material, dek, aad),
                seal(dek, private_key, aad),
                now,
                expires_at,
            )
        except asyncpg.UniqueViolationError as error:
            if error.constraint_name == "agent_keys_one_active_per_lane":
                raise KeystoreError(
                    f"user {user_id} already has an active {lane}-lane agent key — "
                    f"revoke it first"
                ) from None
            raise KeystoreError(
                f"agent address {agent_address} was already stored once — never reuse an "
                f"agent address after deregistration (nonce-replay hazard, ADR-0005)"
            ) from None
        except asyncpg.ForeignKeyViolationError:
            raise KeystoreError(
                f"no user account {user_id} — the user must exist (any /start against "
                f"the bot creates the row) before a key can be bound to them"
            ) from None
        return record

    async def generate_agent_key(
        self,
        *,
        user_id: int,
        master_address: str,
        agent_name: str | None,
        expires_at: datetime,
        lane: str = EXECUTOR_LANE,
    ) -> AgentKeyRecord:
        """Ceremony shape where Epigone creates the keypair: generate in-process
        and store sealed. Only the agent ADDRESS leaves (for the master wallet's
        approveAgent ceremony) — the private key is sealed and dropped."""
        agent = Account.create()
        return await self.store_agent_key(
            user_id=user_id,
            master_address=master_address,
            private_key=bytes(agent.key),
            agent_name=agent_name,
            expires_at=expires_at,
            lane=lane,
        )

    async def signer(self, user_id: int, lane: str = EXECUTOR_LANE) -> LocalAccount:
        """The gateway seam (issue #133): decrypt the user's active agent key
        for `lane` in-process, at signing time, into the LocalAccount the SDK's
        signing helpers take. Refuses expired keys — Hyperliquid would reject
        their signatures anyway, and refusing here turns that into a clear
        error."""
        _validate_lane(lane)
        row = await self._active_row(user_id, lane)
        if row is None:
            raise KeystoreError(f"no active {lane}-lane agent key for user {user_id}")
        if row["expires_at"] <= self._clock.now():
            raise KeystoreError(
                f"{lane}-lane agent key for user {user_id} expired at "
                f"{row['expires_at']:%Y-%m-%d} — rotate it "
                f"(docs/runbooks/agent-key-rotation.md)"
            )
        if row["kek_id"] != self._kek.kek_id:
            raise KeystoreError(
                f"agent key for user {user_id} is wrapped by KEK {row['kek_id']} but the "
                f"loaded KEK is {self._kek.kek_id}"
            )
        aad = _aad(user_id, row["agent_address"])
        dek = unseal(self._kek.material, row["dek_wrapped"], aad)
        private_key = unseal(dek, row["key_ciphertext"], aad)
        account: LocalAccount = Account.from_key(private_key)
        if str(account.address).lower() != row["agent_address"]:
            raise KeystoreError(
                f"decrypted key for user {user_id} does not match its stored agent address"
            )
        return account

    async def revoke(self, user_id: int, lane: str = EXECUTOR_LANE) -> AgentKeyRecord:
        """Tombstone the user's active key on `lane`. The row stays forever: the
        UNIQUE agent_address is what enforces never-reuse-after-deregistration."""
        _validate_lane(lane)
        row = await self._pool.fetchrow(
            """
            UPDATE agent_keys SET revoked_at = $3
            WHERE user_id = $1 AND lane = $2 AND revoked_at IS NULL
            RETURNING user_id, lane, master_address, agent_address, agent_name, kek_id,
                      created_at, expires_at, revoked_at
            """,
            user_id,
            lane,
            self._clock.now(),
        )
        if row is None:
            raise KeystoreError(f"no active {lane}-lane agent key for user {user_id}")
        return _record(row)

    async def active_record(
        self, user_id: int, lane: str = EXECUTOR_LANE
    ) -> AgentKeyRecord | None:
        _validate_lane(lane)
        row = await self._active_row(user_id, lane)
        return None if row is None else _record(row)

    async def list_records(self) -> list[AgentKeyRecord]:
        rows = await self._pool.fetch(
            """
            SELECT user_id, lane, master_address, agent_address, agent_name, kek_id,
                   created_at, expires_at, revoked_at
            FROM agent_keys ORDER BY created_at, agent_address
            """
        )
        return [_record(row) for row in rows]

    async def _active_row(self, user_id: int, lane: str) -> asyncpg.Record | None:
        return await self._pool.fetchrow(
            """
            SELECT user_id, lane, master_address, agent_address, agent_name, kek_id,
                   dek_wrapped, key_ciphertext, created_at, expires_at, revoked_at
            FROM agent_keys WHERE user_id = $1 AND lane = $2 AND revoked_at IS NULL
            """,
            user_id,
            lane,
        )


def _record(row: asyncpg.Record) -> AgentKeyRecord:
    return AgentKeyRecord(
        user_id=row["user_id"],
        lane=row["lane"],
        master_address=row["master_address"],
        agent_address=row["agent_address"],
        agent_name=row["agent_name"],
        kek_id=row["kek_id"],
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        revoked_at=row["revoked_at"],
    )


def _validate_address(raw: str) -> str:
    address = raw.strip().lower()
    if not _ADDRESS_RE.fullmatch(address):
        raise KeystoreError(f"not a Hyperliquid account address: {raw!r}")
    return address


def _validate_lane(lane: str) -> None:
    if lane not in LANES:
        raise KeystoreError(f"unknown signing lane {lane!r} — one of {', '.join(LANES)}")


# --- operator CLI: the ceremony surface (docs/runbooks/agent-key-ceremony.md) ---


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m epigone.keystore",
        description=(
            "Operator ceremony tool for the encrypted AGENT-key keystore (ADR-0005). "
            "Agent keys only — this tool never accepts a master key in any form."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("gen-kek", help="create the KEK file named by KEYSTORE_KEK_FILE")

    def ceremony_args(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument("--user", type=int, required=True, help="Telegram user id")
        cmd.add_argument("--master", required=True, help="master ACCOUNT ADDRESS (never a key)")
        cmd.add_argument("--name", default=None, help="agent name shown on Hyperliquid")
        cmd.add_argument(
            "--days",
            type=int,
            default=DEFAULT_CEREMONY_DAYS,
            help=f"validity in days, must match the approveAgent ceremony "
            f"(default {DEFAULT_CEREMONY_DAYS}, cap {MAX_AGENT_KEY_DAYS})",
        )
        cmd.add_argument(
            "--lane",
            choices=LANES,
            default=EXECUTOR_LANE,
            help=f"which process this key signs for (issue #135; default {EXECUTOR_LANE})",
        )

    generate = sub.add_parser(
        "generate",
        help="generate an agent keypair in-process and store it sealed; prints the "
        "agent ADDRESS for the approval ceremony (the private key never leaves)",
    )
    ceremony_args(generate)

    import_cmd = sub.add_parser(
        "import",
        help="seal an agent private key read from stdin (UI-generated ceremony shape)",
    )
    ceremony_args(import_cmd)

    sub.add_parser("list", help="list stored keys — metadata only, never key material")

    revoke = sub.add_parser("revoke", help="tombstone a user's active key")
    revoke.add_argument("--user", type=int, required=True, help="Telegram user id")
    revoke.add_argument(
        "--lane",
        choices=LANES,
        default=EXECUTOR_LANE,
        help=f"which lane's key to revoke (default {EXECUTOR_LANE})",
    )

    return parser


def _read_private_key_from_stdin() -> bytes:
    """Stdin only — a private key on argv lands in shell history and `ps`."""
    if sys.stdin.isatty():
        print("Paste the agent PRIVATE key (hex, one line), then Enter:", file=sys.stderr)
    raw = sys.stdin.readline().strip().removeprefix("0x")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", raw):
        raise KeystoreError("stdin did not hold a 64-hex-character private key")
    return bytes.fromhex(raw)


def _print_record(record: AgentKeyRecord) -> None:
    status = "revoked" if record.revoked_at else "active"
    name = record.agent_name or "-"
    print(
        f"user {record.user_id}  agent {record.agent_address}  master {record.master_address}\n"
        f"  lane {record.lane}  name {name}  {status}  expires {record.expires_at:%Y-%m-%d}  "
        f"kek {record.kek_id}"
    )


async def _cli(argv: list[str]) -> None:
    args = _build_parser().parse_args(argv)
    if args.command == "gen-kek":
        kek = generate_kek(_kek_path_from_env())
        print(f"KEK {kek.kek_id} written to {kek.path} (mode 600) — back it up "
              f"OFFLINE; it never goes in git or the database")
        return

    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"])
    assert pool is not None
    keystore = AgentKeystore(pool, load_kek(_kek_path_from_env()), SystemClock())
    try:
        if args.command == "generate":
            record = await keystore.generate_agent_key(
                user_id=args.user,
                master_address=args.master,
                agent_name=args.name,
                expires_at=SystemClock().now() + timedelta(days=args.days),
                lane=args.lane,
            )
            print(f"Agent address (approve this via the ceremony): {record.agent_address}")
            _print_record(record)
        elif args.command == "import":
            record = await keystore.store_agent_key(
                user_id=args.user,
                master_address=args.master,
                private_key=_read_private_key_from_stdin(),
                agent_name=args.name,
                expires_at=SystemClock().now() + timedelta(days=args.days),
                lane=args.lane,
            )
            print("Agent key sealed into the keystore.")
            _print_record(record)
        elif args.command == "list":
            records = await keystore.list_records()
            if not records:
                print("No agent keys stored.")
            for record in records:
                _print_record(record)
        elif args.command == "revoke":
            record = await keystore.revoke(args.user, lane=args.lane)
            print(f"Revoked agent {record.agent_address} for user {record.user_id}. "
                  f"Deregister it on Hyperliquid too — and never reuse the address.")
    finally:
        await pool.close()


def _kek_path_from_env() -> Path:
    path = os.environ.get("KEYSTORE_KEK_FILE")
    if not path:
        raise KeystoreError("KEYSTORE_KEK_FILE must name the KEK file (outside git)")
    return Path(path)


if __name__ == "__main__":
    try:
        asyncio.run(_cli(sys.argv[1:]))
    except KeystoreError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
