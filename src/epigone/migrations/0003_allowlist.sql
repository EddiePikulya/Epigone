-- Migration 0003: the invite-only allowlist (issue #33). Epigone is private —
-- an aiogram middleware (epigone.bot.access) gates every update against this
-- table plus the ADMIN_TELEGRAM_ID env admin. A row means "this Telegram User
-- may talk to the bot"; the admin runs /allow and /revoke to add and remove
-- rows at runtime.
--
-- The admin is deliberately NOT stored here: they are always allowed from
-- config, so an empty (or fully revoked) table can never lock the bot out.
-- No foreign key to users — a User can be granted access before they have ever
-- /started, so before any users row exists. granted_by records which admin ran
-- the /allow (a small audit trail; NULL-safe). granted_at is operational
-- bookkeeping stamped at grant time, like tracks.tracked_at, so it defaults to
-- now() rather than the injected domain clock.
--
-- This file is frozen history once shipped — never edit it for a later schema
-- change; add the next numbered migration instead (src/epigone/db.py).
CREATE TABLE allowlist (
    telegram_id BIGINT PRIMARY KEY,
    granted_by  BIGINT,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
