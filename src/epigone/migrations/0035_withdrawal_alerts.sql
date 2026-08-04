-- Migration 0035: the Withdrawal Alert queue (issue #171, ADR-0007 "Out of A4
-- scope").
--
-- Numbered 0035, not 0033: the copy executor (#136) is in flight in a parallel
-- branch and holds 0033-0034 (operator instruction, 2026-08-04). ADR-0003 has
-- no reservation mechanism, so this is a deliberate hole rather than a rule the
-- runner enforces — checked against `epigone.db.migrate` before taking it: the
-- runner skips by the set of applied versions, never by a high-water mark, so
-- 0033-0034 still apply in order whenever they land, and the hole is permanent
-- but harmless if they never do.
--
-- The 38%-emptied-accounts finding as a push notification rather than a
-- copy-time gate: when a tracked Trader's equity falls between two poll passes
-- by more than that pass's PnL explains, every follower is told. Queued by the
-- stream inside the per-Trader poll transaction, drained by the bot (ADR-0002:
-- the processes meet only in Postgres), exactly like position_alerts.
--
-- ITS OWN TABLE, not a `kind` on position_alerts. That table is about a
-- position: `coin` is NOT NULL and its CHECK constraints encode position shape
-- (an open has a side, a scale has a previous size). A withdrawal has no coin,
-- no side and no notional — it is a fact about the ACCOUNT — so it would arrive
-- there as a row that satisfies the constraints by being empty of everything
-- they describe. order_alerts' precedent (0017): own table, own columns, the
-- shared outbox drain.
--
-- The columns are the alert's whole reason for existing, kept rather than a
-- pre-rendered string, so the wording can change without a migration and the
-- rows stay legible as a record of what was detected:
--
--   amount_usd    the net outflow — the equity drop MINUS the part the pass's
--                 own PnL accounts for (epigone.withdrawals). Never the raw
--                 drop, which would name a drawdown as a withdrawal.
--   prior_equity  what the account was worth at the previous observation; the
--                 denominator of the "% of equity" the message leads with, and
--                 the figure the fraction threshold was judged against.
--   equity_usd    what is left, at this observation.
--   observed_at   when the PREVIOUS observation was taken — the far end of the
--                 window the outflow is attributed to. created_at is the near
--                 end, so the two bracket it; a row therefore says over what
--                 span the money left, which a single instant cannot.
--
-- No FK on `coin`-shaped columns because there are none, and no dedup key: a
-- Trader who empties an account in two 25% steps has withdrawn twice, and both
-- are news. At-most-once comes from the detection itself — the pass that
-- alerts also replaces the equity it compared against (trader_equity is one
-- row per Trader), so the same drop can never be seen a second time — and from
-- delivered_at stamping on the delivery side.
--
-- Deliberately NOT pruned when a wallet leaves the poll set: a queued alert was
-- true when it was detected and still owes delivery (position_alerts' rule).
--
-- Future refinement, noted and NOT built (the ticket's own instruction):
-- Hyperliquid's websocket `ledgerUpdates` channel reports transfers as
-- explicit events. At the WS cutover (#158) it becomes available as a direct
-- detection source, which would replace this inference with an observation —
-- and would see transfers this cannot, e.g. one that lands and leaves inside a
-- single poll interval.
CREATE TABLE withdrawal_alerts (
    id               BIGSERIAL PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL REFERENCES users (telegram_id),
    trader_address   TEXT NOT NULL REFERENCES traders (address),
    amount_usd       NUMERIC NOT NULL,
    prior_equity     NUMERIC NOT NULL,
    equity_usd       NUMERIC NOT NULL,
    observed_at      TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    delivered_at     TIMESTAMPTZ,
    attempts         INTEGER NOT NULL DEFAULT 0,
    -- The renderer divides one by the other to lead with "% of equity", and it
    -- runs in the OTHER process (ADR-0002) — so the invariant that makes that
    -- division safe is written here, where both sides can see it, rather than
    -- living only in the detection that happens to be the sole writer today.
    CHECK (prior_equity > 0),
    CHECK (amount_usd > 0)
);

-- The bot's delivery scan: undelivered rows only, oldest first.
CREATE INDEX withdrawal_alerts_undelivered
    ON withdrawal_alerts (id) WHERE delivered_at IS NULL;
