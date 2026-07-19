-- Migration 0009: startPosition continuity guard state (issue #63).
-- Hyperliquid serves TWAP slice executions only from userTwapSliceFills, so
-- every fine fold before #63 was TWAP-blind: a stored open episode's walk may
-- have skipped executions. The engine now carries each coin's walked net
-- position across checkpoints and demotes an episode whose next batch
-- disagrees with it (missed executions never earn a round-trip).
--
-- Existing rows get net_position 0 — "never verified". A real continuation
-- always starts non-flat, so 0 matches nothing and every pre-#63 episode
-- demotes on its next incremental. That is deliberate: any wallet may have
-- TWAP fills the old stream never saw, and demotion (the pre-window-open
-- treatment: no round-trip credit, closes still bank realized_pnl) is the
-- honest reading. No checkpoint reset or data wipe is needed: wiping could
-- not recover fills the API has already aged out of its ~2000-cap windows,
-- so accumulated history stays, in-flight episodes demote once, and the
-- guard verifies everything folded from here on.
ALTER TABLE fine_open_episodes
    ADD COLUMN net_position NUMERIC NOT NULL DEFAULT 0;
