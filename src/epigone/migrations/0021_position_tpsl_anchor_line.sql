-- Migration 0021: the position-attached TP/SL line an open/flip anchor carries
-- (issue #125).
--
-- Wallet views already surface a wallet's protective triggers on their own
-- position lines (a `TP … · SL …` sub-line, matched by coin from the same
-- resting-order fetch the view makes). The open-alert anchor (#91) gains the
-- same line: when the order poller observes the position-TP/SL set for a coin
-- change, it queues a `tpsl` enrichment row and the bot edits the anchor to
-- append/refresh the line — composing with the scale arrows already on it.
--
--   kind = 'tpsl'   a new enrichment kind. Never sends a message of its own;
--                   the bot resolves the coin's live anchor (the same rule
--                   scales use) and edits it in place. A follower with no live
--                   anchor is a silent drop, exactly like a scale after a
--                   floor-suppressed open.
--   tpsl_line       the rendered `TP … · SL …` line. On a `tpsl` row it is the
--                   desired new line (NULL means the set emptied — drop the
--                   line); persisted onto the ANCHOR row so a later scale edit
--                   re-renders the full text (base + arrows + TP/SL line)
--                   without clobbering it, and so an anchor not yet on Telegram
--                   carries the line when it is finally sent.
--
-- Additive: the column is nullable with no default (existing rows and every
-- non-tpsl kind carry NULL), and the kind CHECK is widened, never narrowed.
ALTER TABLE position_alerts DROP CONSTRAINT position_alerts_kind_check;

ALTER TABLE position_alerts
    ADD CONSTRAINT position_alerts_kind_check
    CHECK (kind IN ('open', 'close', 'flip', 'scale_in', 'scale_out', 'tpsl'));

ALTER TABLE position_alerts ADD COLUMN tpsl_line TEXT;
