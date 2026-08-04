-- Migration 0032: each Trader's latest covered-venue equity (issue #170).
--
-- The poll pass has fetched this figure on every cycle since #4 and thrown it
-- away: `clearinghouseState` answers with `marginSummary.accountValue` beside
-- the `assetPositions` the diff reads, and the parser dropped it. Keeping it
-- costs no request and no weight — the same call, parsed for everything it
-- carries.
--
-- ONE ROW PER TRADER, overwritten in place. Deliberately not an events table
-- next to position_events: an equity observation is not something a Trader
-- DID, it is what was true when Epigone looked, and it is re-observed every 10
-- seconds whether or not anything changed. Keeping the history would be ~8600
-- rows per Trader per day describing, almost always, nothing happening. The
-- follow-up that alerts on withdrawals (#171, ADR-0007 "Out of A4 scope")
-- needs exactly two figures — the previous observation and the new one — and
-- the previous one is readable for the whole of the pass that replaces it,
-- inside that pass's own transaction. That is the entire requirement, and a
-- single row meets it.
--
-- `account_value` is the SUM across POSITION_VENUES (core + xyz), because each
-- venue collateralises itself: a HIP-3 builder DEX holds its own margin, and
-- neither pool alone is what the Trader has at stake on the venues Epigone
-- watches. Partial sums never land here — the fetch raises unless every venue
-- answered, since a silent venue contributes zero and would read as exactly
-- the withdrawal #171 exists to alert on.
--
-- `observed_at` is when Epigone saw it, not an exchange timestamp: the
-- clearinghouseState `time` field is the venue's clock and the two venues'
-- responses carry two of them. One observation, one instant, Epigone's own —
-- position_events.observed_at's convention.
--
-- Written in the poll pass's existing per-Trader transaction, so equity and
-- snapshots advance together or not at all. An interrupted pass leaves a
-- Trader's equity exactly as consistent with their positions as the snapshots
-- are — the atomicity inherited from #156 rather than reinvented beside it.
--
-- FK to traders like position_snapshots and position_poll_state (0001): every
-- wallet in the poll set is seeded into traders first, tracked or merely
-- linked (#121, handlers.on_mywallet), and this table shares their lifecycle —
-- the poll pass prunes it for wallets that leave the poll set, so a Trader
-- re-followed later re-observes rather than reading an equity from a period
-- nobody was watching.
CREATE TABLE trader_equity (
    trader_address TEXT PRIMARY KEY REFERENCES traders (address),
    account_value  NUMERIC NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL
);
