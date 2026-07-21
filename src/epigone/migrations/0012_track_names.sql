-- Migration 0012: a User's own nickname for a wallet they track (issue #86).
--
-- A list of 0x94cc…, 0xaf0f…, 0xfc98… is impossible to tell apart, so a User
-- can label each tracked wallet ("scalper-whale", "silver guy"). The name is
-- PER-USER — it lives on the Track relationship, not the shared Trader — so my
-- "silver guy" is never yours, and it never leaks to another tracker of the
-- same wallet. NULL is the default and the cleared state: the wallet then reads
-- as its bare short address, exactly as before this shipped.
--
-- Unfollowing forgets the name for free: the name rides the tracks row, so the
-- unfollow DELETE takes it with it, and a later refollow starts unnamed.
--
-- The bot sanitizes and length-caps a typed name before writing (single line,
-- ≤32 chars); this CHECK is the belt-and-suspenders backstop — a stored name is
-- 1..32 chars, never the empty string (clearing writes NULL, not '').
ALTER TABLE tracks
    ADD COLUMN name TEXT CHECK (name IS NULL OR char_length(name) BETWEEN 1 AND 32);
