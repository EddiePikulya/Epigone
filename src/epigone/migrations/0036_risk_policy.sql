-- Migration 0036: the risk policy's own state (issue #137, A5; ADR-0007
-- amendment D-4).
--
-- A4 shipped a hardcoded conservative policy inside the executor. A5 replaces
-- it with a first-class module judged before signing, and the module needs
-- three things this migration provides: the global knobs it re-reads each
-- cycle, the per-sub knobs the operator sets with /copy, and somewhere to put
-- the per-cycle sub-equity observations the deferred daily-loss pause will
-- eventually be calibrated from.
--
-- BASE NOTIONAL BECOMES BASE STAKE (amendment D-4, reopening ADR-0007 decision
-- 2). The column is RENAMED rather than added beside its predecessor because
-- the two cannot coexist honestly: the configured dollars used to fix the
-- POSITION size and now fix the MARGIN behind it, so a row carrying both
-- would be a row where nothing says which number sized the last order. A
-- rename also makes every reader fail loudly at the moment the meaning
-- changed, which is the point — a silently-reinterpreted $200 is a 20x
-- larger position.
--
--   leverage_mode / fixed_leverage — how a copied open picks its leverage.
--   `mirror` takes the Leader's own leverage on that position (the position
--   event already carries it, so mirroring costs no call); `fixed` takes the
--   operator's number. Either answer is then capped by the global backstop and
--   by the asset's own maximum, so these columns set an ASK, never a
--   guarantee.
--
-- copy_episodes.leverage records the leverage the position was actually opened
-- at — the capped answer, after every ceiling applied. Bookkeeping for the
-- trail and the operator's notice, never authority: the exchange's own
-- `leverage` on the live position is what a later reader trusts, exactly as
-- `size_coin` defers to the exchange's size (0033's rule).
--
-- risk_limits — ONE ROW, the global knobs, re-read by the executor every cycle
-- so a /limits change takes effect without a restart (the copy_subs.enabled
-- precedent). A singleton table rather than a key/value store: every knob has
-- its own type and its own CHECK, and a `value TEXT` column would trade all of
-- that for the ability to store a knob nobody wrote code for. The seeded row
-- carries the conservative defaults; the loader ALSO falls back to the same
-- numbers in code if the row is somehow missing, because "no limits row" must
-- never read as "no limits".
--
-- Thresholds are operator-tunable down to 0, which means OFF for the two
-- floors — the floor is a default stance, not a cage (the Liquidity Floor
-- glossary row). The stake caps stay strictly positive: a zero stake cap is
-- not "no cap", it is "copy nothing", which /uncopy already expresses.
--
-- copy_sub_equity — per-cycle equity history per Copy Sub-account. Unlike
-- trader_equity (0032), which keeps only the latest observation because
-- nothing needed the history, this one IS the history: the daily-loss pause is
-- deferred precisely because nobody knows what threshold to set, and a
-- recorded curve is what will set it. The executor already reads each sub's
-- equity every cycle to reconcile it and then discards it, so recording costs
-- no request and no weight.
--
-- KNOWN GAP, recorded not built: nothing prunes this table. At the 5s executor
-- cadence it grows ~17k rows per sub per day, which is small for a
-- single-operator phase-A run and would not be for a long one. The follow-up
-- that ships the daily-loss pause owns the retention window, because the
-- window it needs is the one that decides how far back the pause looks.
--
-- Timestamps come from the injected clock; additive, no wipe.

ALTER TABLE copy_subs RENAME COLUMN base_notional_usd TO base_stake_usd;

ALTER TABLE copy_subs
    -- 'mirror' is the default because it is the mode the Base Stake model was
    -- designed around: the Leader's conviction scales the exposure while the
    -- money at risk stays the operator's constant.
    ADD COLUMN leverage_mode  TEXT NOT NULL DEFAULT 'mirror'
                                CHECK (leverage_mode IN ('mirror', 'fixed')),
    ADD COLUMN fixed_leverage INT CHECK (fixed_leverage > 0),
    ADD CONSTRAINT copy_subs_fixed_leverage_present
        CHECK (leverage_mode <> 'fixed' OR fixed_leverage IS NOT NULL),
    ADD CONSTRAINT copy_subs_mirror_has_no_fixed
        CHECK (leverage_mode <> 'mirror' OR fixed_leverage IS NULL);

ALTER TABLE copy_episodes
    ADD COLUMN leverage NUMERIC CHECK (leverage > 0);

CREATE TABLE risk_limits (
    -- The singleton guard: one row, forever, so a reader never has to ask
    -- which row is the live one.
    id                      SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- The Liquidity Floor's two halves, both in dollars: 24h traded notional
    -- and open-interest notional (openInterest x mark). A coin clears the
    -- floor only by clearing BOTH — a market can be churned by wash volume
    -- with nothing standing behind it, and it can hold stale open interest
    -- nobody is trading. 0 disables that half.
    floor_day_notional_usd  NUMERIC NOT NULL CHECK (floor_day_notional_usd >= 0),
    floor_open_interest_usd NUMERIC NOT NULL CHECK (floor_open_interest_usd >= 0),
    -- Stake caps, in MARGIN dollars — the money at risk, not the position
    -- size. Per (sub, coin) and per sub in aggregate. Each is deliberately
    -- SEVERAL Base Stakes wide: a per-coin cap equal to one stake would admit
    -- the opening order and refuse every scale-in after it, leaving a
    -- half-mirrored position rather than a bounded one. Deliberately NO
    -- cross-sub coordination: see the known gap below.
    max_coin_stake_usd      NUMERIC NOT NULL CHECK (max_coin_stake_usd > 0),
    max_sub_stake_usd       NUMERIC NOT NULL CHECK (max_sub_stake_usd > 0),
    -- The backstop every mirrored leverage is capped by. The Leader's leverage
    -- dial is an attack surface without it: notional, and everything that
    -- scales with notional, is stake x leverage.
    backstop_leverage       INT NOT NULL CHECK (backstop_leverage > 0),
    updated_at              TIMESTAMPTZ,
    -- Who last moved a knob, for the row itself; the full old -> new trail
    -- lives in execution_audit, where every other authorization does.
    updated_by              BIGINT
);

-- KNOWN GAP (carried from the 2026-07-29 wallet research, #137's comment):
-- these caps are per-sub and naive. Two Leaders in two subs holding the same
-- BTC short look independent to this table and are not; a correlation-aware
-- aggregate is the version that would bound TRUE exposure. v0 ships the naive
-- form deliberately — at one operator with a handful of subs the error is
-- bounded by the funded allocations — and this comment is the record of the
-- choice rather than an oversight.

INSERT INTO risk_limits
    (id, floor_day_notional_usd, floor_open_interest_usd,
     max_coin_stake_usd, max_sub_stake_usd, backstop_leverage)
VALUES (1, 100000, 100000, 300, 900, 20);

CREATE TABLE copy_sub_equity (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sub_id        BIGINT NOT NULL REFERENCES copy_subs (id),
    -- The sub's CORE-venue equity, which is where the allocation was funded
    -- and where margin is measured (the executor's own `_sub_state` rule).
    account_value NUMERIC NOT NULL,
    observed_at   TIMESTAMPTZ NOT NULL
);

-- The one query the deferred pause will run: this sub's curve over a window.
CREATE INDEX copy_sub_equity_sub_time ON copy_sub_equity (sub_id, observed_at);
