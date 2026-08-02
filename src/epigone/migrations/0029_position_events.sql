-- Migration 0029: the durable position-event seam (issue #156, ADR-0006).
--
-- The poll pass has always decided what a Trader did — open, close, flip,
-- scale in, scale out — and then thrown the decision away, keeping only the
-- per-follower `position_alerts` rows it fanned out to and a snapshot of the
-- NEW state. The event itself, the thing that happened, was never written
-- down. The copy executor (#136) needs it, and it cannot read the alert rows:
-- those are delivery records, suppressed by each Track's mute flag and
-- min_size_usd floor, so a change no follower's floor admits produces ZERO
-- rows and a consumer reading them would silently miss the trade. ADR-0006
-- weighs that alternative, and re-diffing snapshots, in full.
--
-- position_events is written by the poller in the same transaction that
-- advances position_snapshots, so the two can never disagree: an interrupted
-- pass leaves both or neither, and the next pass re-diffs the same change
-- exactly once. That atomicity is the whole exactly-once property, inherited
-- rather than reinvented.
--
-- Column shape mirrors position_alerts, minus every trace of delivery
-- (user_telegram_id, telegram_message_id, scale_arrows, tpsl_line, attempts,
-- delivered_at) and minus the `tpsl` kind, which is an anchor-editing
-- enrichment queued by the ORDER poller (migration 0021) rather than a
-- position event — putting it here would smuggle presentation into the seam.
-- What it adds over the alert:
--
--   size_coin / prev_size_coin  the position in coin units (#155, migration
--                               0028). An executor cannot place an order from
--                               a dollar notional; it needs units, and units
--                               plus notional give the mark price too, which
--                               is why there is no separate mark column to
--                               disagree with them. NULL means "size not
--                               mirrorable" — never a guess.
--   observed_at                 when Epigone saw it; equals the alert row's
--                               created_at for the same change.
--   source                      'poll' today. The WebSocket lane arrives later
--                               as a SECOND PRODUCER writing these same rows,
--                               and this is the column that tells them apart.
--
-- A FLIP is ONE row carrying both legs (prev_side + realized_pnl for the
-- closed one, side + size + entry_price for the new), never a close row
-- followed by an open row. One row makes the pair atomically co-visible and
-- unreorderable for free; splitting it would require guaranteeing both.
--
-- Ordering is total per (trader_address, coin) by id, and deliberately
-- unguaranteed across Traders and coins — every consumer's state is keyed per
-- position, so BTC's order says nothing about ETH's. Same-coin order is what
-- matters: mirroring a scale-in before the open it scales is nonsense. It
-- holds because one producer commits one transaction per Trader per pass; the
-- WS lane inherits the constraint "one producer per (trader, coin) at a time",
-- which falls out of a copy-enabled wallet being streamed OR polled.
--
-- Exclusivity is a property of CONSUMED events, not written ones. A later
-- ticket deliberately runs poll and WS side by side to compare them; a
-- dual-written (trader, coin) is that comparison working, not a bug, and a
-- consumer filters by `source`.
--
-- The CHECK constraints are position_alerts' own, carried over: they encode
-- the diff's shape (an open has a side, a close has a prev_side, a flip has
-- both, a scale has both sizes) and are as true of the event as of the alert.
CREATE TABLE position_events (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trader_address TEXT NOT NULL REFERENCES traders (address),
    coin           TEXT NOT NULL,          -- venue-namespaced, e.g. xyz:META
    kind           TEXT NOT NULL
                     CHECK (kind IN ('open', 'close', 'flip', 'scale_in', 'scale_out')),
    side           TEXT CHECK (side IN ('long', 'short')),      -- new leg
    size_usd       NUMERIC,
    size_coin      NUMERIC,
    prev_size_usd  NUMERIC,
    prev_size_coin NUMERIC,
    leverage       NUMERIC,
    entry_price    NUMERIC,
    prev_side      TEXT CHECK (prev_side IN ('long', 'short')), -- closed leg
    realized_pnl   NUMERIC,
    pct_return     NUMERIC,
    opened_at      TIMESTAMPTZ,
    observed_at    TIMESTAMPTZ NOT NULL,
    source         TEXT NOT NULL DEFAULT 'poll' CHECK (source IN ('poll', 'ws')),
    CHECK (kind != 'open' OR side IS NOT NULL),
    CHECK (kind != 'close' OR prev_side IS NOT NULL),
    CHECK (kind != 'flip' OR (side IS NOT NULL AND prev_side IS NOT NULL)),
    CHECK (
        kind NOT IN ('scale_in', 'scale_out')
        OR (side IS NOT NULL AND size_usd IS NOT NULL AND prev_size_usd IS NOT NULL)
    )
);

-- Per-consumer progress. A consumer's work queue is the events with no claim
-- row bearing its own name; it claims before acting, in the same transaction
-- that records the attempt, so a crash between claim and wire leaves a claimed
-- event with no outcome — the "reconcile me" signal, and the missed copy that
-- ADR-0006 explicitly prefers over a doubled one.
--
-- Claims rather than a per-consumer cursor holding last_event_id, which is
-- cheaper and unsound: identity values are allocated at INSERT and published
-- at COMMIT, so two producers committing concurrently can publish out of id
-- order and a cursor that advanced past the higher id skips the lower one
-- forever. That two-producer future is the entire reason this table exists, so
-- a scheme correct only while there is one producer defeats the decision. A
-- single consumed_at flag on the event row fails the same way in a different
-- dress: it presumes exactly one consumer.
CREATE TABLE position_event_claims (
    event_id   BIGINT NOT NULL REFERENCES position_events (id) ON DELETE CASCADE,
    consumer   TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
