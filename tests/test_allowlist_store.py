"""Allowlist persistence (issue #33): grants survive as rows, so the invite-only
gate keeps its decisions across restarts. The admin is never stored here."""

import asyncpg

from epigone import allowlist


async def test_unknown_user_is_not_allowed(pool: asyncpg.Pool) -> None:
    assert await allowlist.is_allowed(pool, 555) is False


async def test_granted_user_is_allowed(pool: asyncpg.Pool) -> None:
    await allowlist.grant(pool, 555, granted_by=1)
    assert await allowlist.is_allowed(pool, 555) is True


async def test_grant_is_idempotent(pool: asyncpg.Pool) -> None:
    await allowlist.grant(pool, 555, granted_by=1)
    await allowlist.grant(pool, 555, granted_by=2)  # re-allow keeps first grant

    count = await pool.fetchval("SELECT count(*) FROM allowlist WHERE telegram_id = 555")
    assert count == 1
    assert await pool.fetchval("SELECT granted_by FROM allowlist WHERE telegram_id = 555") == 1


async def test_revoke_removes_access(pool: asyncpg.Pool) -> None:
    await allowlist.grant(pool, 555, granted_by=1)

    assert await allowlist.revoke(pool, 555) is True
    assert await allowlist.is_allowed(pool, 555) is False


async def test_revoke_reports_when_nothing_was_removed(pool: asyncpg.Pool) -> None:
    assert await allowlist.revoke(pool, 999) is False


async def test_list_allowed_is_oldest_grant_first(pool: asyncpg.Pool) -> None:
    await allowlist.grant(pool, 111, granted_by=1)
    await allowlist.grant(pool, 222, granted_by=1)
    await allowlist.grant(pool, 333, granted_by=1)

    assert await allowlist.list_allowed(pool) == [111, 222, 333]


async def test_grant_needs_no_users_row(pool: asyncpg.Pool) -> None:
    """A User can be allowed before they ever /start, so there is no FK to
    users — granting must not require a users row to exist first."""
    await allowlist.grant(pool, 12345, granted_by=1)
    assert await allowlist.is_allowed(pool, 12345) is True
