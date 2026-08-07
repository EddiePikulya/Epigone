-- Migration 0037: the websocket-authoritative cutover (issue #158, ADR-0009).
--
-- Two producers of position_events have existed since #157; only one of them
-- was ever consumed, by convention. This migration makes the question "who is
-- authoritative right now?" a fact in the database rather than a convention in
-- a docstring, because after the cutover the answer CHANGES — the websocket
-- owns production while it is healthy, the REST poll pass takes it back when
-- the lane goes bad, and both must agree on the answer at every instant or a
-- copied trade doubles.
--
-- ONE ROW, and it is a lock as much as a value. Both producers read it inside
-- the transaction that writes their events (`SELECT ... FOR SHARE`), and a
-- transfer takes it `FOR UPDATE`. That is what makes "two writers never
-- produce events for one (Trader, coin)" a property of the database rather
-- than a timing argument: a transfer cannot commit while a producer's write
-- transaction is in flight, and a producer that starts after the transfer
-- blocks, then sees the new owner and writes nothing authoritative. The
-- degenerate single-row shape (`id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK
-- (id)`) is the risk_limits convention from 0036, and for the same reason:
-- global state that must never have a second row.
--
--   owner          'ws' or 'poll' — who may write authoritative events NOW.
--   since          when that became true. The handback also measures from it:
--                  the ws lane must have re-established absolute state (its
--                  own `ws_lane_state.resynced_at`) AFTER this instant before
--                  ownership can return, because a lane that streamed through
--                  a degraded window may be diffing against memory the world
--                  moved past.
--   reason         why, in the operator's words — the text the health monitor
--                  puts in front of a human when the lane degrades.
--   healthy_since  the first moment the websocket's heartbeat looked fresh in
--                  the current probation, NULL whenever it does not. Escalate
--                  fast, de-escalate slow: ownership returns only after
--                  sustained health, and any staleness resets this to NULL and
--                  makes the lane serve its probation again.
--
-- Seeded 'poll', because that is what is true the instant this migration runs:
-- the deployed poller owns production and the ws lane is still shadowing. The
-- promotion happens on the first evaluation that finds the lane healthy, which
-- is the same code path a recovery takes — the cutover is not a special case,
-- which is exactly the property the ticket asks for.
CREATE TABLE lane_authority (
    id            BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    owner         TEXT NOT NULL CHECK (owner IN ('poll', 'ws')),
    since         TIMESTAMPTZ NOT NULL,
    reason        TEXT NOT NULL,
    healthy_since TIMESTAMPTZ
);

INSERT INTO lane_authority (owner, since, reason)
VALUES ('poll', now(), 'pre-cutover: the REST poll pass has always owned production');

-- Which rows a consumer may act on. `source` says which TRANSPORT observed the
-- event and stays exactly what it was; `authoritative` says whether the lane
-- that wrote it OWNED production at that instant.
--
-- They are deliberately different questions, and separating them is what keeps
-- the shadow comparison alive through and beyond the cutover. Both lanes write
-- every event they observe, forever; the non-owner's rows land with
-- authoritative = FALSE and nothing consumes them. So the dataset the cutover
-- decision was made from keeps growing after the decision — which matters,
-- because the comparison that justified the cutover ran for four days and its
-- tail behaviour is exactly what a warm standby is defending against.
--
-- This supersedes ADR-0007 decision 4's "the executor filters on source='poll'"
-- (and #158's own checklist item "flip the executor's source to 'ws'"). A
-- source filter cannot survive failover: pinned to 'poll' the executor goes
-- blind the moment the websocket takes over, pinned to 'ws' it goes blind the
-- moment the poller takes it back. `authoritative` is the same guarantee
-- expressed against the thing that actually moves. See ADR-0009.
--
-- The backfill states what was true historically: the poller's rows were the
-- consumed ones, the shadow lane's were not.
--
-- DEFAULT FALSE, and it is a deploy property rather than a code one. New code
-- always passes the value explicitly (`epigone.position_events.record_events`),
-- so the default only ever answers for a writer that has not heard of
-- ownership — and during `docker compose up -d --build` there is one: the
-- containers are replaced one at a time, so a PRE-cutover websocket lane can
-- be writing into an already-migrated database beside a poller that owns
-- production. Had this defaulted TRUE, the executor would have drained both
-- rows for the same change and copied that trade twice, with real money behind
-- it. FALSE means "nobody consumes this", which is the only safe thing an
-- unknowing writer can be taken to have meant.
ALTER TABLE position_events ADD COLUMN authoritative BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE position_events SET authoritative = (source = 'poll');

-- Reconciliation's one look of patience (ADR-0009 §4). The coins on which this
-- poll pass saw a change the websocket had not produced, held over to be judged
-- again rather than escalated on sight.
--
-- The websocket is routinely a few seconds BEHIND the poller even when it is
-- perfectly healthy: it holds entry bursts for a moment by design, and it
-- re-sends state on its own ~5s cadence. So a change the poller sees first is
-- not evidence of a miss — at the standby cadence a meaningful share of all
-- changes fall in that window, and escalating on sight would make the incident
-- routine. An incident that fires every day is one nobody reads.
--
-- The patience is exactly one look, and it costs nothing: the poller withholds
-- the verdict by NOT advancing its snapshot of those coins (the same "when in
-- doubt, do not advance the anchor" move the websocket's coalescing makes), so
-- the next pass re-diffs the identical change against the identical anchor and
-- decides for real. It also stops trusting the standby cadence — a wallet with
-- a pending doubt is re-polled on the next tick — so a genuine miss is produced
-- ~10s late rather than ~60s late.
ALTER TABLE position_poll_state ADD COLUMN reconcile_pending TEXT[];

-- The websocket lane's entry-burst debounce (ADR-0009's coalescing decision).
-- The lane sees INDIVIDUAL FILLS where the 10s poll window coalesced them into
-- one diff — measured on the shadow dataset: three scale-ins inside 2.5s that
-- the poller reported once — so a naive cutover would turn one copied entry
-- into three, each paying fees and each at risk of falling under the exchange
-- minimum. A deferred scale-in is held by NOT advancing this coin's snapshot:
-- the anchor stays where the burst started, so the next observation diffs the
-- WHOLE burst as one event. Nothing is buffered in memory, so nothing is lost
-- if the lane dies mid-burst — the anchor is still on disk and the next
-- observation, or the next resync, re-diffs the same change exactly once.
--
-- `coalescing_since` is when the current deferral started, NULL when no scale
-- is pending. Only ever set for entry-side changes: exits are never delayed.
ALTER TABLE ws_position_snapshots ADD COLUMN coalescing_since TIMESTAMPTZ;
