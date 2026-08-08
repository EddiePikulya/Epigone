-- Migration 0039: the Loss Budget (issue #181, ADR-0007 amendment D-9).
--
-- The operator says, at the moment they commit to a Leader, how much they can
-- afford to lose on that Leader before Epigone stops copying him. Six nullable
-- columns on `copy_subs`, because that is where every other per-Leader term
-- already lives (allocation, Base Stake, leverage, mode, brackets): ADR-0007's
-- split puts a property of ONE Leader's mapping on the mapping and Epigone's
-- global stance in `risk_limits`, and a total loss cap set at /copy is the
-- first kind, not the second. No new `/limits` knob, no percentage.
--
-- ALL NULL AFTER THIS MIGRATION, which is the whole opt-in story: an existing
-- sub gets no budget and behaves exactly as it did yesterday, and a /copy that
-- names no budget writes NULL here.
--
-- WHY SIX COLUMNS AND NOT ONE. The number alone cannot be enforced:
--
--   loss = budget_baseline_usd + deposits since arming − current equity
--
-- so the BASELINE has to be stored (a sub with pre-existing funds — an adopted
-- orphan, a re-copy — is judged only on what happens after the number was set,
-- never on the money that was already there), and the instant it was taken has
-- to be stored beside it (the deposit adjustment is a join against the audit
-- trail's provisioning transfers, and only the ones AFTER the baseline was
-- read may be subtracted — the funding transfer that filled the sub in the
-- first place is already inside the baseline). The two MARKS make the state
-- machine survive a restart, which is the property a deploy mid-wind-down
-- needs: a warned sub does not warn again, and a breached sub does not re-arm
-- itself into copying opens. `budget_spent_usd` is the last measured loss,
-- written by the executor each cycle purely so the bot — which holds no
-- gateway and must not learn to read one — can echo spent-vs-budget in the
-- /copy confirmation and in /copies.
--
-- WHY THE BASELINE IS FILLED IN BY THE EXECUTOR, not by /copy. The equity that
-- baselines a sub is a sum across POSITION_VENUES (each venue collateralises
-- itself), which is an exchange read, and the bot process holds no gateway —
-- the same seam that makes /copy write intent and the execute process create
-- and fund the sub. So /copy writes the NUMBER and leaves the baseline NULL,
-- and the first executor cycle that sees an armed budget with no baseline
-- snapshots it from the state read that cycle already performs. "Armed" is
-- therefore the instant the executor took the baseline, never the instant the
-- operator typed the command, and `budget_armed_at` is that instant precisely
-- so the deposit join cannot double-count the provisioning transfer that
-- landed between the two.
--
-- Additive, no wipe, no backfill.

ALTER TABLE copy_subs
    -- The cap itself: absolute dollars of transfer-adjusted loss. NULL means
    -- no budget, which is the default and the pre-#181 behaviour.
    ADD COLUMN loss_budget_usd     NUMERIC CHECK (loss_budget_usd > 0),
    -- What the sub was worth when the budget was armed, summed across every
    -- venue the executor trades. NULL while a budget is set but not yet
    -- baselined (the window between /copy and the executor's next cycle).
    ADD COLUMN budget_baseline_usd NUMERIC,
    -- When that baseline was read. The deposit adjustment counts only
    -- provisioning transfers recorded strictly AFTER it.
    ADD COLUMN budget_armed_at     TIMESTAMPTZ,
    -- The last loss the executor measured, for the operator's echoes. A
    -- NEGATIVE value is a sub in profit, which is why there is no CHECK on it.
    ADD COLUMN budget_spent_usd    NUMERIC,
    -- The 80% notice fired once per arming rather than every cycle it stays
    -- true — the mark is what makes "once" survive a restart.
    ADD COLUMN budget_warned_at    TIMESTAMPTZ,
    -- Wind-down began: opens, scale-ins and flip open-legs are refused; exits
    -- keep copying. Cleared by an explicit re-issued /copy, never by the
    -- executor — a breach is not something time undoes.
    ADD COLUMN budget_breached_at  TIMESTAMPTZ,
    -- A baseline and the instant it was taken are one fact in two columns.
    ADD CONSTRAINT copy_subs_budget_baseline_paired
        CHECK ((budget_baseline_usd IS NULL) = (budget_armed_at IS NULL)),
    -- No budget, no ledger: `loss off` clears everything it hangs off, so a
    -- disarmed sub cannot keep a stale breach that a later arming inherits.
    ADD CONSTRAINT copy_subs_budget_marks_need_a_budget
        CHECK (
            loss_budget_usd IS NOT NULL
            OR (
                budget_baseline_usd IS NULL
                AND budget_spent_usd IS NULL
                AND budget_warned_at IS NULL
                AND budget_breached_at IS NULL
            )
        ),
    -- Nothing can be warned or breached before there is a baseline to measure
    -- it from.
    ADD CONSTRAINT copy_subs_budget_marks_need_a_baseline
        CHECK (
            budget_armed_at IS NOT NULL
            OR (budget_warned_at IS NULL AND budget_breached_at IS NULL)
        );
