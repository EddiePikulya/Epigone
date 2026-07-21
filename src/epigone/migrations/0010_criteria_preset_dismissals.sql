-- Migration 0010: per-user "hide this preset for me" state (issue #71).
-- Three curated preset Criteria live in code (epigone.criteria_presets) and
-- appear in every User's list ready to run — existing and new Users alike, no
-- backfill. A User can delete a preset for themselves: that is a hide-for-me,
-- recorded here as one row per (User, preset), never a global delete. A row's
-- absence means "still visible"; presence means "hidden", permanently — a code
-- version that recalibrates a preset's thresholds leaves the dismissal intact,
-- so a User who deleted it stays deleted while everyone else sees the update.
--
-- preset_key is the preset's stable code identity (not its name or thresholds),
-- so recalibration never resurrects a dismissed preset. It is deliberately not
-- a foreign key: presets are defined in code, not rows, and a preset retired in
-- a future version should leave its stale dismissals harmlessly orphaned rather
-- than block the migration. dismissed_at comes from the injected clock.
CREATE TABLE criteria_preset_dismissals (
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    preset_key       TEXT NOT NULL,
    dismissed_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_telegram_id, preset_key)
);
