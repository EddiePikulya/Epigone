-- Migration 0033: the operator copy executor's own state (issue #136, ADR-0007).
--
-- Four tables, and each one exists because ADR-0007 settled a decision that
-- needs somewhere durable to live. Three carry a concern of their own; the
-- fourth (copy_bracket_orders) is copy_episodes' own detail, split out because
-- reconciliation asks "is THIS trigger still on the book" — a question rows
-- answer and an array column turns into string work.
--
-- copy_subs — the Leader→Copy Sub-account mapping (decisions 1, 2, 6, 12).
-- Tracking is not copying: a Track says "tell me what this wallet does", a row
-- here says "mirror it with money", and there is deliberately no path from one
-- to the other except an explicit operator command. `enabled` is read EVERY
-- loop rather than at startup, which is what makes /copy and /uncopy take
-- effect without a restart (the ticket's acceptance criterion) — and a
-- disabled mapping's events simply never enter the backlog, which is why
-- claim-means-handled does not apply to them.
--
-- `sub_address` NULL is the PROVISIONING state, not an error: /copy is issued
-- in the bot process, which holds no signer (ADR-0005 keeps keys in the
-- signing lanes), so the row is written first and the execute process — which
-- does hold the executor lane's agent key — creates and funds the sub on its
-- next loop and fills the address in. That split is ADR-0002's seam applied to
-- a command that has to sign: the two processes meet in Postgres.
--
-- One sub per Leader (decision 1's single-Leader capital model), enforced by
-- the unique index rather than by the command handler. Sub-accounts CANNOT be
-- deleted and the master is capped at 10 of them (finding 10), so /uncopy
-- flips `enabled` off and a later /copy for the same Leader REUSES the row and
-- its sub; nothing here ever deletes.
--
-- copy_episodes — one position's life in a sub, from copied open to whatever
-- ends it (the glossary's Copy Episode). It is the executor's answer to two
-- otherwise-unanswerable questions: "is this leader event about a position we
-- actually hold?" (decision 8's residual — a skipped stale entry leaves later
-- events referring to nothing) and "did this position end because the LEADER
-- exited or because OUR bracket fired?" (decision 6's episode rule g1, which
-- turns the second case into a hard stop on re-entry until the Leader closes
-- and freshly re-opens).
--
-- `size_coin` is bookkeeping, never authority: decision 10's self-damping
-- principle says relative operations apply to the size the EXCHANGE reports,
-- so this column exists to be compared against reality and adopted from it,
-- not to size an order. A divergence updates it; it never overrides.
--
-- copy_notices — the executor's own outbox to the operator's Telegram
-- (decision 11). The audit trail is a record, not a notification channel, and
-- the operator runs this product from a chat window. It is deliberately a
-- SEPARATE queue from position_alerts: ADR-0006's separation holds in both
-- directions — execution never reads alert rows, and copy status is never
-- written onto them — so an alert preference can never change what gets
-- traded, and a copy report can never be suppressed by a mute. Same row shape
-- the shared drain understands (id / user_telegram_id / attempts /
-- delivered_at), so the retry rule is the one in epigone.bot.outbox and not a
-- second copy of it.
--
-- Timestamps come from the injected clock; addresses are stored lowercased
-- (house conventions). Additive, no wipe.

CREATE TABLE copy_subs (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The Operator's Telegram id. Copy is operator-only and hard-gated in TWO
    -- places (decision 12): the command handler refuses everyone else, and the
    -- executor only ever reads mappings owned by the operator id it was
    -- started with. Multi-user copy is Phase B (#127).
    operator_id       BIGINT NOT NULL,
    leader_address    TEXT NOT NULL REFERENCES traders (address),
    -- The createSubAccount name. Spent for good once used: subs cannot be
    -- deleted, so a name is never recycled onto a different sub.
    sub_name          TEXT NOT NULL,
    -- NULL until the execute process has provisioned it (module header).
    sub_address       TEXT,
    -- The funded allocation. It IS the exposure cap, margin-enforced by the
    -- exchange rather than by Epigone bookkeeping (decision 1) — which is why
    -- there is no aggregate-cap column anywhere: a scale the margin cannot
    -- absorb simply rejects.
    allocation_usd    NUMERIC NOT NULL CHECK (allocation_usd > 0),
    -- The fixed dollar size of a copied OPEN (decision 2). The Leader's
    -- absolute size never enters sizing; only their relative changes do.
    base_notional_usd NUMERIC NOT NULL CHECK (base_notional_usd > 0),
    copy_mode         TEXT NOT NULL CHECK (copy_mode IN ('default', 'bracket')),
    -- Bracket percentages, applied at OUR fill price (decision 6). Percent, so
    -- 5 means 5%. Meaningless outside bracket mode, and a bracket sub with
    -- neither is just `default` wearing a different name — both refused.
    take_profit_pct   NUMERIC CHECK (take_profit_pct > 0),
    stop_loss_pct     NUMERIC CHECK (stop_loss_pct > 0 AND stop_loss_pct < 100),
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL,
    provisioned_at    TIMESTAMPTZ,
    CHECK (
        copy_mode <> 'bracket'
        OR take_profit_pct IS NOT NULL
        OR stop_loss_pct IS NOT NULL
    ),
    CHECK (
        copy_mode <> 'default'
        OR (take_profit_pct IS NULL AND stop_loss_pct IS NULL)
    )
);

-- One Copy Sub-account per Leader (decision 1). Multi-Leader subs are
-- deferred, and this index is what makes that a property rather than a habit:
-- re-copying a previously uncopied Leader must reuse its sub, because a second
-- one would burn one of the master's 10 slots forever.
CREATE UNIQUE INDEX copy_subs_one_per_leader ON copy_subs (operator_id, leader_address);
CREATE UNIQUE INDEX copy_subs_name ON copy_subs (operator_id, sub_name);

CREATE TABLE copy_episodes (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sub_id          BIGINT NOT NULL REFERENCES copy_subs (id),
    coin            TEXT NOT NULL,  -- venue-namespaced, e.g. xyz:META
    side            TEXT NOT NULL CHECK (side IN ('long', 'short')),
    opened_at       TIMESTAMPTZ NOT NULL,
    -- The position_events row this episode was opened from, for the trail.
    -- Not a foreign key: events are pruned at 7 days (ADR-0006) and a live
    -- episode must outlive the event that started it.
    opened_event_id BIGINT,
    -- OUR average fill price, not the Leader's entry — the bracket triggers
    -- are anchored to it (decision 6) and the copy's own PnL is measured from
    -- it. Mirroring the Leader's entry price here would be a number we never
    -- paid.
    entry_price     NUMERIC NOT NULL,
    -- Last known held size, adopted from the exchange (module header).
    size_coin       NUMERIC NOT NULL,
    ended_at        TIMESTAMPTZ,
    ended_reason    TEXT,
    CHECK ((ended_at IS NULL) = (ended_reason IS NULL))
);

-- At most one LIVE episode per (sub, coin). A sub hosts exactly one Leader, so
-- a second live episode on the same coin could only mean the executor lost
-- track of the first — which is a bug this index turns into a failed write
-- instead of a doubled position.
CREATE UNIQUE INDEX copy_episodes_one_live_per_coin
    ON copy_episodes (sub_id, coin) WHERE ended_at IS NULL;
CREATE INDEX copy_episodes_sub ON copy_episodes (sub_id);

-- Resting bracket triggers belonging to an episode, by exchange order id.
-- A separate table rather than an array column because the pair is placed and
-- cancelled leg by leg, and because reconciliation asks "is THIS trigger still
-- on the book" — a question rows answer and an array makes into string work.
CREATE TABLE copy_bracket_orders (
    episode_id BIGINT NOT NULL REFERENCES copy_episodes (id) ON DELETE CASCADE,
    order_id   BIGINT NOT NULL,
    tpsl       TEXT NOT NULL CHECK (tpsl IN ('tp', 'sl')),
    placed_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (episode_id, order_id)
);

CREATE TABLE copy_notices (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_telegram_id BIGINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    -- What the operator is being told, so a future filter has something to
    -- filter on. Decision 11 wants FULL verbosity today — events are rare and
    -- the reader is one person acting manually on what they see — so nothing
    -- suppresses anything yet.
    kind             TEXT NOT NULL
                       CHECK (kind IN ('action', 'skip', 'pager', 'provisioning')),
    body             TEXT NOT NULL,
    attempts         INT NOT NULL DEFAULT 0,
    delivered_at     TIMESTAMPTZ
);

CREATE INDEX copy_notices_undelivered
    ON copy_notices (id) WHERE delivered_at IS NULL;
