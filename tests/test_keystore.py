"""The encrypted agent-key keystore (issue #134, ADR-0005).

The seam under test is the one the A1 execution gateway will consume: the
keystore yields an eth_account LocalAccount-compatible signer by user-account
id, decrypting only in-process at signing time. Everything else here defends
the ADR invariants: envelope encryption at rest (no plaintext key bytes in any
stored blob), one active key per user, agent addresses never reused after
revocation, and Hyperliquid's ≤180-day expiry enforced at store and at signing.

All key material is generated in-test (eth_account.Account.create) — nothing
key-shaped ships in the repo (issue #134 safety rules).
"""

from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
import pytest
from eth_account import Account
from eth_account.signers.local import LocalAccount

from epigone.keystore import (
    EXECUTOR_LANE,
    MAX_AGENT_KEY_DAYS,
    WATCHDOG_LANE,
    AgentKeystore,
    KeystoreError,
    generate_kek,
    load_kek,
)
from tests.support.clock import FakeClock

OPERATOR = 424242
OTHER_USER = 434343
MASTER = "0x" + "ab" * 20
OTHER_MASTER = "0x" + "cd" * 20


@pytest.fixture
def kek_path(tmp_path: Path) -> Path:
    return generate_kek(tmp_path / "keystore.kek").path


@pytest.fixture
async def keystore(
    pool: asyncpg.Pool, kek_path: Path, clock: FakeClock
) -> AgentKeystore:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (telegram_id) VALUES ($1), ($2)", OPERATOR, OTHER_USER
        )
    return AgentKeystore(pool, load_kek(kek_path), clock)


def _expiry(clock: FakeClock, days: int = 170) -> datetime:
    return clock.now() + timedelta(days=days)


# --- KEK file handling ---


def test_kek_generate_load_roundtrip(tmp_path: Path) -> None:
    kek = generate_kek(tmp_path / "keystore.kek")
    loaded = load_kek(tmp_path / "keystore.kek")
    assert loaded.kek_id == kek.kek_id
    # Owner-only permissions: the KEK is the envelope's outer secret.
    assert (tmp_path / "keystore.kek").stat().st_mode & 0o777 == 0o600


def test_kek_generate_refuses_to_overwrite(tmp_path: Path) -> None:
    generate_kek(tmp_path / "keystore.kek")
    with pytest.raises(KeystoreError, match="exists"):
        generate_kek(tmp_path / "keystore.kek")


def test_kek_load_rejects_garbage(tmp_path: Path) -> None:
    bad = tmp_path / "bad.kek"
    bad.write_text("not-hex-at-all")
    with pytest.raises(KeystoreError, match="64 hex"):
        load_kek(bad)


def test_kek_never_exposes_bytes_in_repr(tmp_path: Path) -> None:
    kek = generate_kek(tmp_path / "keystore.kek")
    assert kek.material.hex() not in repr(kek)


# --- store → signer roundtrip (the gateway's seam) ---


async def test_stored_key_yields_the_same_signer(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    original = Account.create()
    record = await keystore.store_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        private_key=bytes(original.key),
        agent_name="epigone-op",
        expires_at=_expiry(clock),
    )
    assert record.agent_address == original.address.lower()
    assert record.master_address == MASTER

    signer = await keystore.signer(OPERATOR)
    assert isinstance(signer, LocalAccount)
    assert signer.address == original.address
    digest = b"epigone keystore roundtrip hash!"  # 32 bytes, as signing takes
    assert signer.unsafe_sign_hash(digest) == original.unsafe_sign_hash(digest)


async def test_generated_key_yields_a_working_signer(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    record = await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        agent_name="epigone-op",
        expires_at=_expiry(clock),
    )
    signer = await keystore.signer(OPERATOR)
    assert signer.address.lower() == record.agent_address


async def test_signer_without_a_key_is_a_clear_error(keystore: AgentKeystore) -> None:
    with pytest.raises(KeystoreError, match="no active executor-lane agent key"):
        await keystore.signer(OPERATOR)


# --- signing lanes (issue #135): one key per process, per user ---


async def test_lanes_hold_independent_keys(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    executor = await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name="epigone-a",
        expires_at=_expiry(clock),
    )
    watchdog = await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name="epigone-watchdog-a",
        expires_at=_expiry(clock), lane=WATCHDOG_LANE,
    )
    assert executor.lane == EXECUTOR_LANE
    assert watchdog.lane == WATCHDOG_LANE
    assert executor.agent_address != watchdog.agent_address
    # Each lane resolves to its own signer — separate nonce sets on-chain.
    assert (await keystore.signer(OPERATOR)).address.lower() == executor.agent_address
    assert (
        await keystore.signer(OPERATOR, WATCHDOG_LANE)
    ).address.lower() == watchdog.agent_address


async def test_one_active_key_per_lane(keystore: AgentKeystore, clock: FakeClock) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name="wd-a",
        expires_at=_expiry(clock), lane=WATCHDOG_LANE,
    )
    with pytest.raises(KeystoreError, match="watchdog-lane"):
        await keystore.generate_agent_key(
            user_id=OPERATOR, master_address=MASTER, agent_name="wd-b",
            expires_at=_expiry(clock), lane=WATCHDOG_LANE,
        )


async def test_revoke_is_lane_scoped(keystore: AgentKeystore, clock: FakeClock) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name="epigone-a",
        expires_at=_expiry(clock),
    )
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name="epigone-watchdog-a",
        expires_at=_expiry(clock), lane=WATCHDOG_LANE,
    )
    revoked = await keystore.revoke(OPERATOR, lane=WATCHDOG_LANE)
    assert revoked.lane == WATCHDOG_LANE
    # The executor lane's key is untouched; the watchdog lane is now empty.
    await keystore.signer(OPERATOR)
    with pytest.raises(KeystoreError, match="no active watchdog-lane"):
        await keystore.signer(OPERATOR, WATCHDOG_LANE)


async def test_unknown_lane_is_refused(keystore: AgentKeystore, clock: FakeClock) -> None:
    with pytest.raises(KeystoreError, match="unknown signing lane"):
        await keystore.generate_agent_key(
            user_id=OPERATOR, master_address=MASTER, agent_name="x",
            expires_at=_expiry(clock), lane="exectuor",
        )


# --- encryption at rest ---


async def test_no_plaintext_key_bytes_at_rest(
    keystore: AgentKeystore, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    original = Account.create()
    await keystore.store_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        private_key=bytes(original.key),
        agent_name=None,
        expires_at=_expiry(clock),
    )
    row = await pool.fetchrow("SELECT dek_wrapped, key_ciphertext FROM agent_keys")
    assert row is not None
    assert bytes(original.key) not in row["dek_wrapped"]
    assert bytes(original.key) not in row["key_ciphertext"]


async def test_wrong_kek_refuses_to_decrypt(
    keystore: AgentKeystore, pool: asyncpg.Pool, clock: FakeClock, tmp_path: Path
) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    other_kek = generate_kek(tmp_path / "other.kek")
    other_store = AgentKeystore(pool, other_kek, clock)
    with pytest.raises(KeystoreError, match="KEK"):
        await other_store.signer(OPERATOR)


async def test_tampered_ciphertext_refuses_to_decrypt(
    keystore: AgentKeystore, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    await pool.execute(
        "UPDATE agent_keys SET key_ciphertext = key_ciphertext || '\\x00'::bytea"
    )
    with pytest.raises(KeystoreError, match="decrypt"):
        await keystore.signer(OPERATOR)


async def test_truncated_blob_is_a_keystore_error(
    keystore: AgentKeystore, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """A blob shorter than a valid GCM nonce raises ValueError inside
    `cryptography`, not InvalidTag — it must still surface as the domain
    error, never a raw crypto traceback."""
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    await pool.execute(
        "UPDATE agent_keys SET key_ciphertext = substring(key_ciphertext from 1 for 4)"
    )
    with pytest.raises(KeystoreError, match="decrypt"):
        await keystore.signer(OPERATOR)


async def test_unknown_user_is_a_clear_error(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    with pytest.raises(KeystoreError, match="no user account"):
        await keystore.generate_agent_key(
            user_id=999_999,
            master_address=MASTER,
            agent_name=None,
            expires_at=_expiry(clock),
        )


async def test_ciphertext_is_bound_to_its_row(
    keystore: AgentKeystore, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """AAD binding: blobs transplanted onto another user's row must fail
    authentication, so a row-swapping attacker can't redirect whose signer a
    stored key becomes."""
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    await keystore.generate_agent_key(
        user_id=OTHER_USER,
        master_address=OTHER_MASTER,
        agent_name=None,
        expires_at=_expiry(clock),
    )
    await pool.execute(
        """
        UPDATE agent_keys AS target
        SET dek_wrapped = source.dek_wrapped, key_ciphertext = source.key_ciphertext
        FROM agent_keys AS source
        WHERE target.user_id = $1 AND source.user_id = $2
        """,
        OTHER_USER,
        OPERATOR,
    )
    with pytest.raises(KeystoreError, match="decrypt"):
        await keystore.signer(OTHER_USER)


# --- lifecycle invariants ---


async def test_one_active_key_per_user(keystore: AgentKeystore, clock: FakeClock) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    with pytest.raises(KeystoreError, match="active"):
        await keystore.generate_agent_key(
            user_id=OPERATOR,
            master_address=MASTER,
            agent_name=None,
            expires_at=_expiry(clock),
        )


async def test_revoke_then_store_new_key(keystore: AgentKeystore, clock: FakeClock) -> None:
    first = await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    revoked = await keystore.revoke(OPERATOR)
    assert revoked.agent_address == first.agent_address
    assert revoked.revoked_at == clock.now()
    second = await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    assert second.agent_address != first.agent_address
    signer = await keystore.signer(OPERATOR)
    assert signer.address.lower() == second.agent_address


async def test_agent_addresses_are_never_reused(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    """Nonce-replay hazard (ADR-0005): a revoked tombstone permanently blocks
    re-importing the same agent address."""
    original = Account.create()
    await keystore.store_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        private_key=bytes(original.key),
        agent_name=None,
        expires_at=_expiry(clock),
    )
    await keystore.revoke(OPERATOR)
    with pytest.raises(KeystoreError, match="reuse"):
        await keystore.store_agent_key(
            user_id=OPERATOR,
            master_address=MASTER,
            private_key=bytes(original.key),
            agent_name=None,
            expires_at=_expiry(clock),
        )


async def test_revoke_without_a_key_is_a_clear_error(keystore: AgentKeystore) -> None:
    with pytest.raises(KeystoreError, match="no active executor-lane agent key"):
        await keystore.revoke(OPERATOR)


# --- expiry (Hyperliquid's ≤180-day agent validity) ---


async def test_expired_key_refuses_to_sign(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock, 10)
    )
    clock.advance(11 * 24 * 3600)
    with pytest.raises(KeystoreError, match="expired"):
        await keystore.signer(OPERATOR)


async def test_expiry_beyond_180_days_is_refused(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    with pytest.raises(KeystoreError, match=str(MAX_AGENT_KEY_DAYS)):
        await keystore.generate_agent_key(
            user_id=OPERATOR,
            master_address=MASTER,
            agent_name=None,
            expires_at=_expiry(clock, MAX_AGENT_KEY_DAYS + 1),
        )


async def test_expiry_in_the_past_is_refused(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    with pytest.raises(KeystoreError, match="future"):
        await keystore.generate_agent_key(
            user_id=OPERATOR,
            master_address=MASTER,
            agent_name=None,
            expires_at=clock.now() - timedelta(days=1),
        )


# --- metadata views (no secrets) ---


async def test_active_record_and_listing(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    assert await keystore.active_record(OPERATOR) is None
    stored = await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER,
        agent_name="epigone-op",
        expires_at=_expiry(clock),
    )
    record = await keystore.active_record(OPERATOR)
    assert record == stored
    assert record is not None
    assert record.agent_name == "epigone-op"
    assert record.expires_at == _expiry(clock)

    await keystore.revoke(OPERATOR)
    await keystore.generate_agent_key(
        user_id=OPERATOR, master_address=MASTER, agent_name=None, expires_at=_expiry(clock)
    )
    listed = await keystore.list_records()
    assert len(listed) == 2
    assert sum(1 for r in listed if r.revoked_at is None) == 1


async def test_master_address_is_normalized_and_validated(
    keystore: AgentKeystore, clock: FakeClock
) -> None:
    record = await keystore.generate_agent_key(
        user_id=OPERATOR,
        master_address=MASTER.upper().replace("0X", "0x"),
        agent_name=None,
        expires_at=_expiry(clock),
    )
    assert record.master_address == MASTER
    with pytest.raises(KeystoreError, match="address"):
        await keystore.generate_agent_key(
            user_id=OTHER_USER,
            master_address="not-an-address",
            agent_name=None,
            expires_at=_expiry(clock),
        )
