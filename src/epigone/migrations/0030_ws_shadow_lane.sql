-- Migration 0030: the websocket shadow lane's own memory (issue #157).
--
-- The lane is a SECOND producer of position_events, writing source = 'ws'
-- beside the poller's 'poll' rows so the cutover ticket (#158) can compare two
-- descriptions of the same reality. ADR-0006 built position_events for exactly
-- this and needs no change here: `source` already distinguishes them, and a
-- dual-written (trader, coin) is the comparison working rather than a bug.
--
-- What the lane does need is somewhere to remember what it last saw, and it
-- CANNOT be position_snapshots. That table is the poller's diff memory: the
-- poller reads it, diffs against it, and advances it in one transaction. A
-- second writer advancing those same rows would make the poller diff against
-- state it never observed — the websocket's newer view — and the poller would
-- then miss the change it was about to report. The lanes must therefore be
-- independent in storage and identical only in reasoning, which is why the
-- diff itself moved to epigone/position_diff.py and this table is a
-- deliberate, column-for-column copy of position_snapshots (0001 + 0028).
-- Same columns under the same names is what lets one reader
-- (SnapshotState.from_row) serve both.
--
-- No foreign key to traders, unlike position_snapshots. The poller's rows are
-- written by a pass that has already resolved the wallet into the Universe;
-- the lane subscribes off the poll set and writes from a websocket push, and a
-- shadow lane must never be able to abort — or, worse, block — on a
-- referential race with ingest. Its rows are diagnostic, not authoritative,
-- and nothing joins them to traders. Pruning is by the lane, off the poll set,
-- the same way _prune_untracked works (a wallet nobody watches loses its
-- memory, so a re-follow re-baselines silently instead of alerting on ancient
-- changes).
CREATE TABLE ws_position_snapshots (
    trader_address TEXT NOT NULL,
    coin           TEXT NOT NULL,
    side           TEXT NOT NULL CHECK (side IN ('long', 'short')),
    size_usd       NUMERIC NOT NULL,
    leverage       NUMERIC NOT NULL,
    entry_price    NUMERIC NOT NULL,
    unrealized_pnl NUMERIC NOT NULL,
    size_coin      NUMERIC,
    opened_at      TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trader_address, coin)
);

-- Per-Trader lane bookkeeping. `baselined_at` carries the rule both lanes
-- obey: a Trader's FIRST observation records positions and emits nothing,
-- because a position that predates the first look is not something anyone
-- could have watched open. Its presence — not the snapshot rows, which are
-- legitimately empty for a flat wallet — is what says "this Trader has been
-- observed before", exactly as position_poll_state does for the poller.
--
-- `resynced_at` is the lane's other correctness anchor. A websocket delivers
-- what happens from the moment you subscribe, so a reconnect that resumed
-- streaming without first re-establishing absolute state would silently lose
-- the gap. The lane re-reads each Trader's positions point-in-time before
-- subscribing, and stamps that here; a Trader whose resync has not landed in
-- this connection is not yet trusted to stream.
CREATE TABLE ws_lane_state (
    trader_address TEXT PRIMARY KEY,
    baselined_at   TIMESTAMPTZ NOT NULL,
    resynced_at    TIMESTAMPTZ NOT NULL,
    last_message_at TIMESTAMPTZ
);
