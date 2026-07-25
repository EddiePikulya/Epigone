-- Migration 0019: LONG/SHORT direction on round-trips (issue #119).
-- The fine engine now stamps each completed trade with the sign of its
-- episode's net position — LONG or SHORT. An episode is single-direction by
-- construction (a flip splits it at the zero crossing into two trips carrying
-- opposite sides), so the side is the sign of the position the trip held, not
-- an inference from PnL or price direction (a near-zero-PnL trade would make
-- that lie). Recorded going forward only: trips folded before this shipped keep
-- NULL side and the Recent trades line renders them with no LONG/SHORT prefix —
-- the same graceful degradation as the pre-#116 price-less trips. The column is
-- strictly additive, no wipe: every already-stored number is still correct.
ALTER TABLE fine_trades
    ADD COLUMN side TEXT;
