-- Sub-minute smoothing + consumption metering for the shared rate budget
-- (issue #41). next_send_at is the send gate: each grant reserves a window
-- proportional to its weight so the bucket cannot be emptied in a spike.
-- minute_started_at/minute_spent meter real consumed weight (including
-- post-response surcharges) so actual-vs-limit is observable in logs.
-- Epoch defaults read as "gate open, meter stale" for the pre-#41 row.
ALTER TABLE rate_budget
    ADD COLUMN next_send_at      TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    ADD COLUMN minute_started_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    ADD COLUMN minute_spent      DOUBLE PRECISION NOT NULL DEFAULT 0;
