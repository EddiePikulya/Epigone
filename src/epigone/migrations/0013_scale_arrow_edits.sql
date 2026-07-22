-- Migration 0013: persist the delivered open-alert message id and its running
-- scale-arrow trail (issue #91).
--
-- Alert-noise redesign: adds/trims no longer send a message of their own.
-- Instead the position's original OPEN (or FLIP) alert is edited in place,
-- accumulating one arrow per scale — ⬆️ add, ⬇️ trim — in event order. Two
-- columns on position_alerts carry that state:
--
--   telegram_message_id  the Telegram message id the bot learns from the send
--                        result when it delivers an open/flip anchor. NULL until
--                        delivered, and always NULL on scale rows (which never
--                        send a message of their own). Persisted so scale arrows
--                        keep landing on the right message across bot restarts.
--   scale_arrows         the anchor's accumulated arrow string (e.g. '⬆️⬇️⬆️'),
--                        appended as each scale for that position instance lands.
--                        NULL until the first scale. Lives on the anchor row so
--                        re-opens and flips (each its own anchor) never bleed
--                        arrows across position instances.
--
-- Both nullable with no default — existing rows and non-anchor kinds simply
-- carry NULL, so this is a purely additive change.
ALTER TABLE position_alerts
    ADD COLUMN telegram_message_id BIGINT,
    ADD COLUMN scale_arrows TEXT;
