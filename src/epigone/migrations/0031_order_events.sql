-- Migration 0031: the durable order-event seam (issue #168, ADR-0008).
--
-- ADR-0006 §Scope deferred this deliberately: `order_alerts` is one batched
-- row per follower per wallet per cycle (issue #115's noise rule) with the
-- batch stored as rendered JSONB, which is a delivery record and not an event,
-- and the REST weight arithmetic showed order mirroring can never work on a
-- poll cadence. The websocket makes a per-order seam meaningful, so here it is,
-- following that ADR's pattern: own table, own vocabulary, claim table, and
-- nothing consuming the rows yet.
--
-- NOT position_events. That table's `kind` speaks open/close/flip/scale and its
-- CHECK constraints encode position shape (an open has a side, a flip has
-- both). An order lifecycle shares none of it. Keeping them apart is also what
-- makes the #166-review hazard structurally impossible: `outstanding_events`
-- and `claim_event` in epigone/position_events.py do not filter on `source`, so
-- a seam sharing their tables would be silently picked up by the position-event
-- consumer path. No query there names these tables, and none can come to by
-- accident.
--
-- One row is ONE UPDATE TO ONE ORDER, as the wire delivered it. Two columns
-- carry the classification honestly:
--
--   kind    Epigone's vocabulary — what happened to the order.
--   status  the exchange's raw status string, kept verbatim ALONGSIDE the
--           classification. The statuses observed live on 2026-08-03 already
--           include `badAloPxRejected` and `iocCancelRejected` beside the
--           obvious ones, and the `*Canceled` family is open-ended, so an
--           unrecognised status maps to kind 'other' and rides with its raw
--           string intact. The OpenOrder.tpsl precedent: self-describing beats
--           silently wrong. NULL only for a resync-derived row, which no status
--           ever described — hence the CHECK.
--
-- There is deliberately NO 'modified' kind. Hyperliquid's modify is an atomic
-- cancel/replace minting a new oid (verified live, #115) and arrives as exactly
-- that: a `canceled` and an `open`, usually in one frame sharing a
-- statusTimestamp. Collapsing them would need the same size-tolerance matching
-- heuristic stream/orders.py uses to damp ALERT noise — and ADR-0006 is
-- explicit that suppression rules stay in the alert layer and out of the
-- execution seam. `status_at` is stored so a consumer that wants to recognise a
-- modify can pair the halves by instant, without this table having guessed.
--
-- 'gone' is the honest name for a resting order that left the book while the
-- lane was disconnected: absolute state can say it ended, never whether it
-- filled or was cancelled. Naming the ambiguity beats a coin flip between
-- 'filled' and 'canceled', and beats the silence the reconnect criterion
-- forbids.
--
-- The trigger columns are NULLABLE and usually NULL, which is a property of the
-- transport rather than an oversight: WsBasicOrder carries no orderType,
-- isTrigger, triggerPx, reduceOnly or isPositionTpsl, so a STREAMED placement
-- cannot say whether it is a stop, a take-profit or a plain resting limit. The
-- REST resync reads all of it (frontendOpenOrders → OpenOrder) and fills them
-- in. NULL means "not observed", never a default — the size_coin convention of
-- ADR-0006/#155. A consumer mirroring an order treats an unknown type as
-- not-mirrorable rather than assuming a plain limit.
--
-- No foreign key to traders, for ws_position_snapshots' reason (migration
-- 0030): these rows are written by a shadow lane off a websocket push, they are
-- diagnostic rather than authoritative, nothing joins them to the Universe, and
-- a shadow lane must never be able to abort — or block — on a referential race
-- with ingest.
CREATE TABLE order_events (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trader_address   TEXT NOT NULL,
    order_id         BIGINT NOT NULL,        -- the exchange's oid
    coin             TEXT NOT NULL,          -- venue-namespaced, e.g. xyz:CL
    kind             TEXT NOT NULL CHECK (kind IN (
                         'placed', 'filled', 'canceled', 'rejected',
                         'triggered', 'gone', 'other')),
    status           TEXT,                   -- the exchange's own word for it
    origin           TEXT NOT NULL CHECK (origin IN ('stream', 'resync')),
    is_buy           BOOLEAN NOT NULL,
    limit_price      NUMERIC,
    size             NUMERIC,                -- what was left to fill
    original_size    NUMERIC,
    cloid            TEXT,                   -- opaque client order id, verbatim
    order_type       TEXT,                   -- resync-only (see above)
    is_trigger       BOOLEAN,
    trigger_price    NUMERIC,
    reduce_only      BOOLEAN,
    is_position_tpsl BOOLEAN,
    placed_at        TIMESTAMPTZ,            -- the order's own timestamp
    status_at        TIMESTAMPTZ,            -- the exchange's statusTimestamp
    observed_at      TIMESTAMPTZ NOT NULL,   -- when Epigone saw it
    CHECK (origin <> 'stream' OR status IS NOT NULL)
);

-- Per-consumer progress, the ADR-0006 pattern for the ADR-0006 reason: identity
-- values are allocated at INSERT and published at COMMIT, so a cursor holding
-- last_event_id can be advanced past a lower id that has not yet appeared and
-- skip it forever. Claims are order-independent by construction. Ships with no
-- caller, exactly as position_event_claims did — a seam with a write side and
-- no read side is half a seam.
CREATE TABLE order_event_claims (
    event_id   BIGINT NOT NULL REFERENCES order_events (id) ON DELETE CASCADE,
    consumer   TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, consumer)
);

-- The order lane's own diff memory: what it believes is resting right now.
-- Separate from order_snapshots for the reason ws_position_snapshots is
-- separate from position_snapshots (migration 0030) — that table is the REST
-- order poller's diff memory, and a second writer advancing it would make the
-- poller diff against a state it never observed and miss the change it was
-- about to alert on. Independent in storage, identical only in reasoning.
--
-- Richer than order_snapshots because a resync must be able to emit a complete
-- event for an order that has already left the book: by then there is nothing
-- live left to read its price or size from, so memory is the only source. Same
-- argument that puts size_usd on a position CLOSE event.
CREATE TABLE ws_order_state (
    trader_address   TEXT NOT NULL,
    order_id         BIGINT NOT NULL,
    coin             TEXT NOT NULL,
    is_buy           BOOLEAN NOT NULL,
    limit_price      NUMERIC NOT NULL,
    size             NUMERIC NOT NULL,
    original_size    NUMERIC NOT NULL,
    cloid            TEXT,
    order_type       TEXT,
    is_trigger       BOOLEAN,
    trigger_price    NUMERIC,
    reduce_only      BOOLEAN,
    is_position_tpsl BOOLEAN,
    placed_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trader_address, order_id)
);

-- Per-Trader lane bookkeeping, ws_lane_state's shape and rules for one Trader's
-- ORDER feed. `baselined_at` carries the rule every lane in this system obeys:
-- a Trader's first observation records the resting ladder and emits nothing,
-- because orders that predate the first look are not something anyone could
-- have watched being placed. Its presence — not the ws_order_state rows, which
-- are legitimately empty for a Trader resting nothing — is what says "seen
-- before".
--
-- `resynced_at` is the correctness anchor: a websocket delivers only what
-- happens after you subscribe, so a connection that streamed before
-- re-establishing absolute state would lose the gap in silence. `last_message_at`
-- is diagnostic and nothing branches on it.
CREATE TABLE ws_order_lane_state (
    trader_address  TEXT PRIMARY KEY,
    baselined_at    TIMESTAMPTZ NOT NULL,
    resynced_at     TIMESTAMPTZ NOT NULL,
    last_message_at TIMESTAMPTZ
);
