-- Migration 0015: remember every unfollow so a wallet you already tried and
-- dropped stands out if it surfaces again (issue #99).
--
-- #86 makes unfollowing forget the live name by design — the name rides the
-- tracks row and the unfollow DELETE takes it with it. This log preserves what
-- you called the wallet at the moment you dropped it, so the profile can read
-- "unfollowed 3d ago (as \"avax\")" even though the Track (and its name) is gone.
--
-- PER-USER, exactly one row per (user, wallet): the PRIMARY KEY makes every
-- unfollow an upsert, so re-follow → re-unfollow keeps only the latest unfollow
-- (newest unfollowed_at, newest name). Following again does not erase the row —
-- the surfaces gate on "currently tracked?" so a live Track simply hides the
-- note; dropping the Track again refreshes the timestamp.
--
-- `name` mirrors the tracks.name shape (migration 0012, issue #86): NULL when
-- the wallet was never named, else the 1..32-char nickname it carried at
-- unfollow time.
CREATE TABLE IF NOT EXISTS unfollows (
    user_telegram_id BIGINT      NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT        NOT NULL REFERENCES traders (address),
    unfollowed_at    TIMESTAMPTZ NOT NULL,
    name             TEXT CHECK (name IS NULL OR char_length(name) BETWEEN 1 AND 32),
    PRIMARY KEY (user_telegram_id, trader_address)
);
