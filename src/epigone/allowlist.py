"""Invite-only allowlist persistence (issue #33): the set of Telegram Users the
owner has granted access to. The admin (Settings.admin_telegram_id) is always
allowed and is deliberately NOT stored here, so an empty table can never lock
the bot out. The bot's AllowlistGate middleware (epigone.bot.access) calls
is_allowed on every update; /allow, /revoke, /allowed drive the rest."""

import asyncpg


async def is_allowed(
    executor: asyncpg.Pool | asyncpg.Connection, telegram_id: int
) -> bool:
    """Whether this User has been granted access. The admin is handled by the
    gate from config and is not checked here."""
    found = await executor.fetchval("SELECT 1 FROM allowlist WHERE telegram_id = $1", telegram_id)
    return found is not None


async def grant(
    executor: asyncpg.Pool | asyncpg.Connection, telegram_id: int, granted_by: int | None
) -> None:
    """Add a User to the allowlist; idempotent (re-allowing keeps the first
    grant's bookkeeping)."""
    await executor.execute(
        """
        INSERT INTO allowlist (telegram_id, granted_by)
        VALUES ($1, $2) ON CONFLICT (telegram_id) DO NOTHING
        """,
        telegram_id,
        granted_by,
    )


async def revoke(executor: asyncpg.Pool | asyncpg.Connection, telegram_id: int) -> bool:
    """Remove a User from the allowlist. Returns whether a row was actually
    removed, so the caller can tell "revoked" from "wasn't allowed anyway"."""
    result = await executor.execute("DELETE FROM allowlist WHERE telegram_id = $1", telegram_id)
    return bool(result != "DELETE 0")


async def list_allowed(executor: asyncpg.Pool | asyncpg.Connection) -> list[int]:
    """Every allowlisted User, oldest grant first. Excludes the admin (config)."""
    rows = await executor.fetch(
        "SELECT telegram_id FROM allowlist ORDER BY granted_at, telegram_id"
    )
    return [r["telegram_id"] for r in rows]
