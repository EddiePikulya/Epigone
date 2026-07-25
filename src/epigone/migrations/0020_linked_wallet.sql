-- Migration 0020: a User's own linked wallet for My-wallet copy alerts (#121).
-- linked_wallet is the address a User declares as *theirs* (paste-style, one per
-- User, stored lowercased like every other address). It is not a Track: the
-- wallet joins the stream poll set only so its positions are snapshotted as the
-- User's holdings reference — never generating tracking alerts. Its sole use is
-- the copy-alert match at delivery time (epigone.bot.alerts): when a wallet the
-- User tracks scales a coin their linked wallet is currently holding, they get a
-- full standalone alert on top of the #91 arrow edit. NULL means no linked
-- wallet, which is the zero-behavior-change default. Additive, no wipe.
ALTER TABLE users
    ADD COLUMN linked_wallet TEXT;
