"""Health checks: gather one liveness snapshot, then evaluate it purely.

The seam the tests drive (issue #52 "Testing Decisions"): `gather_snapshot`
does the impure work — one Postgres round-trip plus a disk read — into a plain
`HealthSnapshot` of raw numbers/timestamps; `evaluate_checks` is a pure function
mapping that snapshot + thresholds to a list of `CheckResult` decisions. Tests
feed synthetic snapshots and assert on which checks fail and what text they
carry, never touching the wall clock or the live server.

Checks split by what's observable (issue #52):
- DB-observable — ingest progress, coarse freshness, alert-delivery backlog,
  rate health, and DB reachability itself; a single connection answers all.
- Host-observable — disk headroom, read via an injected `DiskProbe` so the
  container needs only the host filesystem mounted, not the docker socket.

Rate health (the 429 spike, user story #5) landed as the #52 fast-follow (issue
#54): rather than coupling the monitor to the docker socket to scrape ingest
logs, the ingest/stream passes stamp a `rate_limit_events` row whenever a
`RateLimitedError` escapes the gateway's backoff (epigone.budget), and this
check counts those events over a recent window. That keeps the monitor a pure
DB+Telegram process and distinguishes sustained limiting from the normal
single-429 backoff pacing (issues #28/#41), which never reaches the signal.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import asyncpg

from epigone.bot.alerts import MAX_DELIVERY_ATTEMPTS
from epigone.clock import Clock
from epigone.ingest.fine import count_due_traders
from epigone.lane_authority import DISABLED_REASON, WS_OWNER
from epigone.metrics.library import format_duration

# Machine names, stable across a check's lifetime so the alerting state machine
# (epigone.monitor.alerting) can track each independently.
DATABASE = "database"
INGEST = "ingest"
COARSE = "coarse"
ALERTS = "alerts"
RATE = "rate"
FINE_SUCCESS = "fine_success"
DISK = "disk"
AGENT_KEY = "agent_key"
WATCHDOG = "watchdog"
HALT = "halt"
COPY_PAGER = "copy_pager"
POSITION_LANE = "position_lane"

WARNING = "warning"
CRITICAL = "critical"

# Above the operator's disk trip point a warning suffices; past this the disk is
# nearly full and the alert escalates to critical (🚨). A fixed escalation band,
# not a tunable — the operator tunes *when* to be warned via HEALTHCHECK_DISK_PERCENT.
DISK_CRITICAL_PERCENT = 95

# A watchdog capability verdict older than this is unverified (issue #135):
# the probe runs ~6-hourly with a 5-minute failure retry, so a day of silence
# means the on-chain check has been failing for ~4 cycles straight — long
# enough that a stale capable=TRUE could be masking a deregistration. The
# SAME band is the never-verified grace period from process start (PR #143
# round 2): one ladder, whether the last verdict is old or has never landed.
# Fixed like the disk escalation band, not a tunable: 4× the probe's own
# default cadence, generous against any sane WATCHDOG_CAPABILITY_CHECK_HOURS.
CAPABILITY_VERDICT_STALE = timedelta(hours=24)

# The copy executor's pager cases (issue #136, ADR-0007 decision 11): an
# unfilled close after its retries exhaust, a liquidation, an unclassifiable
# divergence. They are rare and each one wants a human, so ANY of them inside
# this window is CRITICAL — and the window is deliberately long, because "the
# book would not absorb a $200 reduce-only IOC" does not stop mattering an
# hour later. Fixed, not tunable: an operator who wants fewer of these fixes
# the cause, not the threshold.
COPY_PAGER_WINDOW = timedelta(hours=6)

# The audit actions that ARE pager cases. Keyed off the trail rather than off
# `copy_notices` so a delivery failure can never also silence the page: the
# chat message and the monitor alert fail independently.
COPY_PAGER_ACTIONS = (
    "copy_close_unfilled",
    "copy_divergence_unclassifiable",
    "copy_episode_liquidated",
    "copy_unexpected_resting",
)


class DiskProbe(Protocol):
    """Host disk visibility, injected so tests feed a synthetic percentage and
    the container needs only a mounted host path (not the docker socket)."""

    def percent_used(self) -> float | None: ...


@dataclass(frozen=True)
class CheckThresholds:
    """Operator-tunable trip points (issue #52), all with safe defaults in
    epigone.monitor.config."""

    ingest_stall: timedelta
    coarse_stale: timedelta
    alert_backlog: timedelta
    # Sustained rate limiting (issue #54): fail once at least `rate_max_events`
    # escaped-429 events land within `rate_window`. The count threshold is what
    # separates a sustained spike from an isolated backoff-absorbed 429.
    rate_window: timedelta
    rate_max_events: int
    # Fine-pass success starvation (issue #61): fail when the due backlog is at
    # least `starvation_min_due` and no successful fine refresh has landed within
    # `starvation_window` *while attempts keep advancing* — every refresh failing,
    # not a stopped or a caught-up pass.
    starvation_window: timedelta
    starvation_min_due: int
    disk_percent: float
    # Agent-key expiry runway (issue #134): warn once the soonest-expiring
    # active agent key is within this window, so the ≤180-day rotation ceremony
    # (docs/runbooks/agent-key-rotation.md) happens on a reminder, never as an
    # outage response.
    agent_key_warn: timedelta
    # Watchdog liveness (issue #135): the watchdog beats every cycle (~10s);
    # silence past this window means the PRIMARY dead-man's switch is down —
    # the watcher must itself be watched, and this is where.
    watchdog_stale: timedelta


@dataclass(frozen=True)
class HealthSnapshot:
    """One point-in-time reading of every liveness signal. Raw observations
    only — every threshold decision lives in `evaluate_checks`."""

    now: datetime
    db_reachable: bool
    wallet_count: int | None = None
    due_traders: int | None = None
    last_fine_refresh: datetime | None = None
    # Last time *any* fine attempt was stamped (success or failure, issue #61).
    # A recent attempt with a stale refresh is the signature of a pass that is
    # alive and looping but never succeeding.
    last_fine_attempt: datetime | None = None
    fine_refreshed_today: int | None = None
    last_coarse_compute: datetime | None = None
    undelivered_alerts: int | None = None
    oldest_undelivered_alert: datetime | None = None
    # Escaped-429 events (issue #54) within the rate window — the count is taken
    # at gather time so the pure check only compares it to the threshold.
    recent_rate_limits: int | None = None
    disk_percent_used: float | None = None
    # Soonest expires_at over ACTIVE agent keys (issue #134); None means the
    # keystore is empty — a valid Phase A state, not a failure.
    nearest_agent_key_expiry: datetime | None = None
    # Execution-process heartbeats (issue #135). None means the process never
    # ran — a valid pre-A4 (executor) / pre-ceremony (watchdog) state; the
    # watchdog check decides what each combination means.
    executor_beaten_at: datetime | None = None
    watchdog_beaten_at: datetime | None = None
    # The watchdog's on-chain capability verdict (migration 0025, PR #143
    # review): None = never checked, False = beating but IMPOTENT — its agent
    # key can't cancel anything; detail says why. `checked_at` is when the
    # verdict last landed — a verdict the probe hasn't refreshed in a long
    # time is unverified, not trustworthy (an info outage must not let a
    # stale capable=TRUE mask a deregistration forever).
    watchdog_capable: bool | None = None
    watchdog_capability_detail: str | None = None
    watchdog_capability_checked_at: datetime | None = None
    # When the watchdog process last started (migration 0026): the clock a
    # NEVER-verified capability ages against — a probe that has not succeeded
    # once since launch must escalate on the same ladder as a stale verdict,
    # never read as healthy forever (PR #143 round 2).
    watchdog_started_at: datetime | None = None
    # The active execution halt, if any (issue #135) — raw fields off the one
    # active execution_halts row; all None when trading is not halted.
    active_halt_since: datetime | None = None
    active_halt_source: str | None = None
    active_halt_reason: str | None = None
    active_halt_swept_at: datetime | None = None
    # jsonb_array_length of the sweep's position snapshot; None until swept.
    active_halt_positions: int | None = None
    # Copy-executor pager cases in the recent window (issue #136): an unfilled
    # close after retries, or a divergence nothing could classify. Counted from
    # the AUDIT TRAIL, so a copy_notices delivery failure cannot silence the
    # page as well as the message.
    recent_copy_pagers: int | None = None
    latest_copy_pager: str | None = None
    # Who owns position-event production, since when, and why (issue #158,
    # migration 0037). None means no cutover has been recorded — the
    # pre-cutover world, where the poller has always owned it.
    position_lane_owner: str | None = None
    position_lane_since: datetime | None = None
    position_lane_reason: str | None = None


@dataclass(frozen=True)
class CheckResult:
    """One check's verdict. `detail` names the check and the numbers behind it
    (user story #9) so an alert is actionable without re-investigating."""

    name: str
    title: str
    ok: bool
    severity: str
    detail: str


async def gather_snapshot(
    pool: asyncpg.Pool, clock: Clock, disk: DiskProbe, thresholds: CheckThresholds
) -> HealthSnapshot:
    """Read every liveness signal in one pass. A query failure is itself the
    loudest signal (DB down), so the caller catches it and reports it — see
    `db_down`. `now` is stamped from the injected clock, not the wall clock.

    `thresholds.rate_window` scopes the escaped-429 count read here (issue #54),
    the way `delivered_at IS NULL` scopes the alert backlog — a raw observation
    of "how many recently", leaving the threshold decision to `evaluate_checks`."""
    now = clock.now()
    row = await pool.fetchrow(
        """
        SELECT
            (SELECT count(*) FROM traders) AS wallet_count,
            (SELECT max(fine_refreshed_at) FROM traders) AS last_fine_refresh,
            (SELECT max(fine_attempted_at) FROM traders) AS last_fine_attempt,
            (SELECT count(*) FROM traders WHERE fine_refreshed_at >= $1)
                AS fine_refreshed_today,
            (SELECT max(computed_at) FROM coarse_metrics) AS last_coarse_compute,
            (SELECT count(*) FROM position_alerts
                WHERE delivered_at IS NULL AND attempts < $2) AS undelivered_alerts,
            (SELECT min(created_at) FROM position_alerts
                WHERE delivered_at IS NULL AND attempts < $2)
                AS oldest_undelivered_alert,
            (SELECT count(*) FROM rate_limit_events WHERE occurred_at >= $3)
                AS recent_rate_limits,
            (SELECT min(expires_at) FROM agent_keys WHERE revoked_at IS NULL)
                AS nearest_agent_key_expiry,
            (SELECT beaten_at FROM process_heartbeats WHERE process = 'executor')
                AS executor_beaten_at,
            (SELECT beaten_at FROM process_heartbeats WHERE process = 'watchdog')
                AS watchdog_beaten_at,
            (SELECT capable FROM process_heartbeats WHERE process = 'watchdog')
                AS watchdog_capable,
            (SELECT capability_detail FROM process_heartbeats WHERE process = 'watchdog')
                AS watchdog_capability_detail,
            (SELECT capability_checked_at FROM process_heartbeats WHERE process = 'watchdog')
                AS watchdog_capability_checked_at,
            (SELECT started_at FROM process_heartbeats WHERE process = 'watchdog')
                AS watchdog_started_at,
            (SELECT halted_at FROM execution_halts WHERE resumed_at IS NULL)
                AS active_halt_since,
            (SELECT source FROM execution_halts WHERE resumed_at IS NULL)
                AS active_halt_source,
            (SELECT reason FROM execution_halts WHERE resumed_at IS NULL)
                AS active_halt_reason,
            (SELECT swept_at FROM execution_halts WHERE resumed_at IS NULL)
                AS active_halt_swept_at,
            (SELECT jsonb_array_length(positions) FROM execution_halts
                WHERE resumed_at IS NULL) AS active_halt_positions,
            (SELECT count(*) FROM execution_audit
                WHERE occurred_at >= $4 AND action = ANY($5::text[]))
                AS recent_copy_pagers,
            (SELECT max(risk_decision) FROM execution_audit
                WHERE occurred_at >= $4 AND action = ANY($5::text[]))
                AS latest_copy_pager,
            (SELECT owner FROM lane_authority) AS position_lane_owner,
            (SELECT since FROM lane_authority) AS position_lane_since,
            (SELECT reason FROM lane_authority) AS position_lane_reason
        """,
        _start_of_day(now),
        MAX_DELIVERY_ATTEMPTS,
        now - thresholds.rate_window,
        now - COPY_PAGER_WINDOW,
        list(COPY_PAGER_ACTIONS),
    )
    assert row is not None
    return HealthSnapshot(
        now=now,
        db_reachable=True,
        wallet_count=row["wallet_count"],
        due_traders=await count_due_traders(pool, now),
        last_fine_refresh=row["last_fine_refresh"],
        last_fine_attempt=row["last_fine_attempt"],
        fine_refreshed_today=row["fine_refreshed_today"],
        last_coarse_compute=row["last_coarse_compute"],
        undelivered_alerts=row["undelivered_alerts"],
        oldest_undelivered_alert=row["oldest_undelivered_alert"],
        recent_rate_limits=row["recent_rate_limits"],
        disk_percent_used=disk.percent_used(),
        nearest_agent_key_expiry=row["nearest_agent_key_expiry"],
        executor_beaten_at=row["executor_beaten_at"],
        watchdog_beaten_at=row["watchdog_beaten_at"],
        watchdog_capable=row["watchdog_capable"],
        watchdog_capability_detail=row["watchdog_capability_detail"],
        watchdog_capability_checked_at=row["watchdog_capability_checked_at"],
        watchdog_started_at=row["watchdog_started_at"],
        active_halt_since=row["active_halt_since"],
        active_halt_source=row["active_halt_source"],
        active_halt_reason=row["active_halt_reason"],
        active_halt_swept_at=row["active_halt_swept_at"],
        active_halt_positions=row["active_halt_positions"],
        recent_copy_pagers=row["recent_copy_pagers"],
        latest_copy_pager=row["latest_copy_pager"],
        position_lane_owner=row["position_lane_owner"],
        position_lane_since=row["position_lane_since"],
        position_lane_reason=row["position_lane_reason"],
    )


def db_down(now: datetime) -> HealthSnapshot:
    """The snapshot to report when the monitor's own query fails — DB-down is a
    critical signal, and the monitor can still DM (token + admin come from env,
    not the DB)."""
    return HealthSnapshot(now=now, db_reachable=False)


def evaluate_checks(
    snapshot: HealthSnapshot, thresholds: CheckThresholds
) -> list[CheckResult]:
    """Map a snapshot to per-check verdicts. Pure: same snapshot + thresholds
    always yields the same decisions.

    When the DB is unreachable only the database check is meaningful — every
    other signal was read from that same connection — so we report it alone."""
    if not snapshot.db_reachable:
        return [
            CheckResult(
                DATABASE,
                "Database",
                ok=False,
                severity=CRITICAL,
                detail="Database: monitor query failed — Postgres unreachable",
            )
        ]
    return [
        CheckResult(DATABASE, "Database", ok=True, severity=CRITICAL, detail="Database reachable"),
        _ingest_check(snapshot, thresholds.ingest_stall),
        _coarse_check(snapshot, thresholds.coarse_stale),
        _alerts_check(snapshot, thresholds.alert_backlog),
        _rate_check(snapshot, thresholds.rate_max_events),
        _fine_success_check(
            snapshot, thresholds.starvation_window, thresholds.starvation_min_due
        ),
        _disk_check(snapshot, thresholds.disk_percent),
        _agent_key_check(snapshot, thresholds.agent_key_warn),
        _watchdog_check(snapshot, thresholds.watchdog_stale),
        _halt_check(snapshot),
        _copy_pager_check(snapshot),
        _position_lane_check(snapshot),
    ]


def _ingest_check(snapshot: HealthSnapshot, stall: timedelta) -> CheckResult:
    """Stuck iff the fine pass has completed no refresh within the window *and*
    Traders are actually due. The `due > 0` guard is the exact nuance from the
    rescan investigation: a caught-up pass is idle by design, not wedged, and
    must not false-alarm."""
    due = snapshot.due_traders or 0
    age = _age(snapshot.now, snapshot.last_fine_refresh)
    stalled = due > 0 and (age is None or age > stall)
    if stalled:
        refreshed = snapshot.fine_refreshed_today or 0
        since = "never" if snapshot.last_fine_refresh is None else _ago(age)
        return CheckResult(
            INGEST,
            "Ingest",
            ok=False,
            severity=WARNING,
            detail=(
                f"Ingest: no fine refresh in {since} but {due:,} due — "
                f"fine pass may be stuck ({refreshed:,} refreshed today)"
            ),
        )
    return CheckResult(INGEST, "Ingest", ok=True, severity=WARNING, detail="Ingest progressing")


def _coarse_check(snapshot: HealthSnapshot, stale: timedelta) -> CheckResult:
    """The coarse re-seed is an hourly heartbeat (issue #50); metrics older than
    ~2× that interval mean the re-seed likely stopped, staling windowed stats and
    discovery."""
    age = _age(snapshot.now, snapshot.last_coarse_compute)
    if age is None or age > stale:
        since = "never" if snapshot.last_coarse_compute is None else f"{_ago(age)} ago"
        return CheckResult(
            COARSE,
            "Coarse re-seed",
            ok=False,
            severity=WARNING,
            detail=f"Coarse re-seed: metrics last computed {since} — re-seed may be broken",
        )
    return CheckResult(
        COARSE, "Coarse re-seed", ok=True, severity=WARNING, detail="Coarse metrics fresh"
    )


def _alerts_check(snapshot: HealthSnapshot, backlog: timedelta) -> CheckResult:
    """Undelivered Position Alerts older than the window mean the delivery path
    is wedged. Poison rows (attempts ≥ MAX) are already excluded at gather time,
    so a single dead chat never trips this."""
    oldest = snapshot.oldest_undelivered_alert
    age = _age(snapshot.now, oldest)
    if oldest is not None and age is not None and age > backlog:
        count = snapshot.undelivered_alerts or 0
        return CheckResult(
            ALERTS,
            "Alert delivery",
            ok=False,
            severity=WARNING,
            detail=(
                f"Alert delivery: {count:,} undelivered alert(s), oldest {_ago(age)} old — "
                f"delivery path may be wedged"
            ),
        )
    return CheckResult(
        ALERTS, "Alert delivery", ok=True, severity=WARNING, detail="Alert delivery current"
    )


def _rate_check(snapshot: HealthSnapshot, max_events: int) -> CheckResult:
    """Sustained rate limiting (user story #5). Each counted event is a
    RateLimitedError that outlasted the gateway's backoff-and-retry (issue #28)
    — ~30s of 429s on one call — so an isolated backoff-absorbed 429 never
    reaches the signal (user story #2). At or past the threshold over the window
    means limiting is back and likely starving the fine pass or alerts. A None
    count (only on a DB read miss) reads as healthy, never a false alarm."""
    count = snapshot.recent_rate_limits or 0
    if count >= max_events:
        return CheckResult(
            RATE,
            "Rate limiting",
            ok=False,
            severity=WARNING,
            detail=(
                f"Rate limiting: {count:,} sustained rate-limit event(s) in the recent window "
                f"(threshold {max_events:,}) — Hyperliquid is throttling us"
            ),
        )
    return CheckResult(
        RATE, "Rate limiting", ok=True, severity=WARNING, detail="Rate limiting normal"
    )


def _fine_success_check(
    snapshot: HealthSnapshot, window: timedelta, min_due: int
) -> CheckResult:
    """Fine-pass success starvation (issue #61). The 20h outage this closes: the
    `userFills` endpoint 500'd on every call, so each attempt failed-but-stamped
    `fine_attempted_at` while zero refreshes landed — a plain `GatewayError` that
    the 429-only rate check (#54) never sees. The signature is three conditions
    at once, distinguishing "constantly failing" from both healthy and idle:

    - the due backlog is large (≥ `min_due`) — a caught-up pass (small backlog)
      has nothing to succeed at, so it stays quiet like the ingest check does;
    - no successful refresh within `window` — successes landing means healthy;
    - attempts are still advancing within `window` — a *stopped* pass (no recent
      attempt) is the ingest check's province, not this one.
    """
    due = snapshot.due_traders or 0
    success_age = _age(snapshot.now, snapshot.last_fine_refresh)
    attempt_age = _age(snapshot.now, snapshot.last_fine_attempt)
    attempts_advancing = attempt_age is not None and attempt_age <= window
    no_recent_success = success_age is None or success_age > window
    if due >= min_due and attempts_advancing and no_recent_success:
        gap = (
            "no successful refresh ever recorded"
            if snapshot.last_fine_refresh is None
            else f"no successful refresh in {_ago(success_age)}"
        )
        return CheckResult(
            FINE_SUCCESS,
            "Fine success",
            ok=False,
            severity=WARNING,
            detail=(
                f"Fine success: {due:,} due and attempts advancing but {gap} — "
                f"every fine refresh is failing"
            ),
        )
    return CheckResult(
        FINE_SUCCESS, "Fine success", ok=True, severity=WARNING, detail="Fine refreshes landing"
    )


def _disk_check(snapshot: HealthSnapshot, limit: float) -> CheckResult:
    """Disk headroom on the host: backups plus the growing fine_trades table are
    the real risk. A probe that returns None (no host visibility) is treated as
    healthy — a missing reading is not a full disk."""
    used = snapshot.disk_percent_used
    if used is not None and used >= limit:
        return CheckResult(
            DISK,
            "Disk",
            ok=False,
            severity=CRITICAL if used >= DISK_CRITICAL_PERCENT else WARNING,
            detail=f"Disk: {used:.0f}% used (threshold {limit:.0f}%) — free space before it fills",
        )
    return CheckResult(DISK, "Disk", ok=True, severity=WARNING, detail="Disk headroom fine")


def _agent_key_check(snapshot: HealthSnapshot, warn: timedelta) -> CheckResult:
    """Agent-key expiry runway (issue #134). Hyperliquid agent keys expire in
    ≤180 days; rotation is an operator ceremony (a master-wallet re-approval,
    which no automation may do — ADR-0005), so the monitor's job is to make it
    happen early. Quiet while the keystore is empty, a warning inside the warn
    window, and critical once expired — an expired key means the keystore
    refuses to sign and trading is down, not merely at risk."""
    nearest = snapshot.nearest_agent_key_expiry
    if nearest is None:
        return CheckResult(
            AGENT_KEY, "Agent key", ok=True, severity=WARNING, detail="No agent keys stored"
        )
    if nearest <= snapshot.now:
        return CheckResult(
            AGENT_KEY,
            "Agent key",
            ok=False,
            severity=CRITICAL,
            detail=(
                f"Agent key: expired {_ago(snapshot.now - nearest)} ago — trading is down; "
                f"run the rotation runbook (docs/runbooks/agent-key-rotation.md)"
            ),
        )
    if nearest - snapshot.now <= warn:
        return CheckResult(
            AGENT_KEY,
            "Agent key",
            ok=False,
            severity=WARNING,
            detail=(
                f"Agent key: soonest expiry in {_ago(nearest - snapshot.now)} — "
                f"schedule the rotation ceremony (docs/runbooks/agent-key-rotation.md)"
            ),
        )
    return CheckResult(
        AGENT_KEY, "Agent key", ok=True, severity=WARNING, detail="Agent key expiry far off"
    )


def _watchdog_check(snapshot: HealthSnapshot, stale: timedelta) -> CheckResult:
    """Watchdog liveness (issue #135) — the watcher, watched. The watchdog is
    the PRIMARY dead-man's switch (scheduleCancel is volume-gated, PR #141),
    so its death is critical the moment there is anything to guard:

    - watchdog beating recently AND on-chain capable → healthy;
    - watchdog silent past the window → critical: the switch is down;
    - watchdog beating but IMPOTENT (its agent deregistered/expired on-chain,
      the migration-0025 verdict) → critical: heartbeats without the power to
      cancel are the false safety the PR #143 review flagged;
    - watchdog NEVER ran while the executor HAS a heartbeat → critical: an
      executor without its watchdog is exactly the unguarded state A3 forbids;
    - neither ever ran → quiet, like the empty keystore: the pre-deploy state
      of a dev box or a pre-ceremony server is not an incident."""
    beaten = snapshot.watchdog_beaten_at
    if beaten is None:
        if snapshot.executor_beaten_at is not None:
            return CheckResult(
                WATCHDOG,
                "Watchdog",
                ok=False,
                severity=CRITICAL,
                detail=(
                    "Watchdog: never ran, but the executor has a heartbeat — trading "
                    "is UNGUARDED; start the watchdog service (compose profile "
                    "`execution`, issue #135)"
                ),
            )
        return CheckResult(
            WATCHDOG, "Watchdog", ok=True, severity=CRITICAL,
            detail="Watchdog not deployed (no execution processes)",
        )
    age = snapshot.now - beaten
    if age > stale:
        return CheckResult(
            WATCHDOG,
            "Watchdog",
            ok=False,
            severity=CRITICAL,
            detail=(
                f"Watchdog: last heartbeat {_ago(age)} ago (threshold "
                f"{_ago(stale)}) — the dead-man's switch is DOWN; resting orders "
                f"are unguarded (issue #135)"
            ),
        )
    if snapshot.watchdog_capable is False:
        return CheckResult(
            WATCHDOG,
            "Watchdog",
            ok=False,
            severity=CRITICAL,
            detail=(
                f"Watchdog: beating but IMPOTENT — "
                f"{snapshot.watchdog_capability_detail or 'agent not approved on-chain'}; "
                f"re-approve/rotate the watchdog lane and restart the service "
                f"(agent-key-rotation runbook)"
            ),
        )
    # One ladder for "how long has the verdict gone unrefreshed" (PR #143
    # round 2): a verified verdict ages from its last check; a NEVER-verified
    # one ages from process start — because a fresh deploy with a blocked
    # info endpoint may hold a key that was never approved on-chain at all,
    # and NULL-forever must not read as healthy.
    checked = snapshot.watchdog_capability_checked_at
    basis = checked if checked is not None else snapshot.watchdog_started_at
    if basis is not None and snapshot.now - basis > CAPABILITY_VERDICT_STALE:
        gap_detail = (
            f"NEVER verified since the process started {_ago(snapshot.now - basis)} ago "
            f"— the probe has not succeeded once; the agent may not be approved "
            f"on-chain at all"
            if checked is None
            else f"UNVERIFIED for {_ago(snapshot.now - checked)} (probe failing?) — "
            f"the last verdict may be stale"
        )
        return CheckResult(
            WATCHDOG,
            "Watchdog",
            ok=False,
            severity=WARNING,
            detail=(
                f"Watchdog: on-chain capability {gap_detail}; check the watchdog "
                f"logs and the info endpoint"
            ),
        )
    return CheckResult(
        WATCHDOG, "Watchdog", ok=True, severity=CRITICAL, detail="Watchdog beating"
    )


def _copy_pager_check(snapshot: HealthSnapshot) -> CheckResult:
    """The copy executor's PAGER CASES (issue #136, ADR-0007 decisions 5 and
    10). Two shapes qualify and both want a human rather than a retry:

    - a close still UNFILLED after its bounded reduce-only retries — meaning
      the book could not absorb a ~$200 reduce-only IOC inside the slippage
      cap, which is pathological. It has its own audit reason precisely so it
      can page here instead of drowning in generic partial-fill noise;
    - a divergence the reconcile could not classify — where the rule is adopt
      nothing, page, and re-flag until resolved;
    - a liquidated Copy Sub-account;
    - an entry IOC that came back RESTING, which decision 4 says cannot
      happen — so if it does, something is wrong about what this executor
      believes it is sending, and a resting entry is the one order shape the
      halt sweep's timing argument does not cover.

    A liquidation is the third (decision 11 lists all three). Nothing here
    saves the money — it is already gone — but a sub that liquidated is a
    Leader whose copy terms need revisiting, and the operator should not learn
    that from scrolling. Nothing in this check resolves itself, so the
    reminder cadence keeps re-paging while the window holds it."""
    count = snapshot.recent_copy_pagers
    if not count:
        return CheckResult(
            COPY_PAGER, "Copy execution", ok=True, severity=CRITICAL, detail="No copy incidents"
        )
    hours = int(COPY_PAGER_WINDOW.total_seconds() // 3600)
    return CheckResult(
        COPY_PAGER,
        "Copy execution",
        ok=False,
        severity=CRITICAL,
        detail=(
            f"Copy execution: {count} incident(s) in the last {hours}h needing a human — "
            f"latest: {snapshot.latest_copy_pager}"
        ),
    )


def _position_lane_check(snapshot: HealthSnapshot) -> CheckResult:
    """The websocket lane having LOST event production (issue #158, ADR-0009).

    This is the only place a human hears about the failure this cutover was
    designed around. A lane that is connected, delivering, and silently missing
    changes trips no heartbeat and no liveness canary; what catches it is the
    poller's continuous reconciliation, and what the reconciliation does is
    take production back and write WHY onto the authority row. Reading that row
    here turns "drift is an incident" from a docstring into a message.

    A degraded lane is not itself an emergency — the warm standby means alerts
    and copying carry on from the poller, which is the entire point — so this
    is a WARNING rather than a page. What it must not do is pass silently: a
    system quietly running on its fallback is a system with no fallback left.

    Two states are deliberately NOT incidents: the operator having switched the
    cutover off (a decision, not a failure), and no authority recorded at all
    (pre-cutover, where the poller owning production is what has always been
    true). Paging about states someone chose is how a monitor teaches people to
    ignore it."""
    owner = snapshot.position_lane_owner
    reason = snapshot.position_lane_reason
    if owner is None or owner == WS_OWNER:
        return CheckResult(
            POSITION_LANE,
            "Position lane",
            ok=True,
            severity=WARNING,
            detail="Websocket owns position events",
        )
    if reason == DISABLED_REASON:
        return CheckResult(
            POSITION_LANE,
            "Position lane",
            ok=True,
            severity=WARNING,
            detail="Websocket authority switched off by the operator",
        )
    return CheckResult(
        POSITION_LANE,
        "Position lane",
        ok=False,
        severity=WARNING,
        detail=(
            f"Position lane DEGRADED {_ago(_age(snapshot.now, snapshot.position_lane_since))} "
            f"ago — the REST poller is producing events: {reason}. "
            "Alerts and copying continue; ownership returns on its own once the "
            "websocket is healthy again and has re-established state"
        ),
    )


def _halt_check(snapshot: HealthSnapshot) -> CheckResult:
    """An active execution halt (issue #135) is CRITICAL for as long as it
    stands: this is the alert path that pages the operator when the watchdog
    trips (or confirms a /kill took), and the reminder cadence keeps re-paging
    while it is unresolved. The detail carries what the operator needs to act:
    who halted, why, whether the book is actually swept yet, and how many
    positions are being HELD per the unwind policy."""
    since = snapshot.active_halt_since
    if since is None:
        return CheckResult(HALT, "Execution halt", ok=True, severity=CRITICAL,
                           detail="No execution halt")
    sweep = (
        "orders swept"
        if snapshot.active_halt_swept_at is not None
        else "sweep PENDING — orders may still rest"
    )
    positions = (
        f"{snapshot.active_halt_positions} open position(s) HELD"
        if snapshot.active_halt_positions is not None
        else "positions not yet snapshotted"
    )
    return CheckResult(
        HALT,
        "Execution halt",
        ok=False,
        severity=CRITICAL,
        detail=(
            f"Execution HALTED by {snapshot.active_halt_source} {_ago(snapshot.now - since)} "
            f"ago ({snapshot.active_halt_reason}) — {sweep}; {positions} per the unwind "
            f"policy (docs/runbooks/halt-and-unwind.md); /resume lifts it"
        ),
    )


def heartbeat_digest(snapshot: HealthSnapshot) -> str:
    """The daily positive digest (user story #6): the key liveness numbers so
    silence genuinely means healthy and a dead checker is noticeable by its
    missing ping."""
    parts = [f"{_count(snapshot.wallet_count)} wallets"]
    parts.append(f"{_count(snapshot.fine_refreshed_today)} fine-refreshed today")
    # The due backlog (issue #61): a large number here beside a low refreshed-today
    # is the starvation signal in digest form.
    parts.append(f"{_count(snapshot.due_traders)} due")
    coarse_age = _age(snapshot.now, snapshot.last_coarse_compute)
    parts.append(
        "coarse never computed" if coarse_age is None else f"coarse fresh {_ago(coarse_age)} ago"
    )
    parts.append(f"{_count(snapshot.undelivered_alerts)} alerts pending")
    parts.append(f"{_count(snapshot.recent_rate_limits)} rate errors")
    if snapshot.disk_percent_used is not None:
        parts.append(f"disk {snapshot.disk_percent_used:.0f}%")
    # Empty keystore → no clause, like the missing disk reading (issue #134).
    # An expired key must say so — format_duration clamps negatives to "0s",
    # which would read healthy right beside the critical alert.
    if snapshot.nearest_agent_key_expiry is not None:
        runway = snapshot.nearest_agent_key_expiry - snapshot.now
        parts.append(
            "agent key EXPIRED" if runway <= timedelta(0) else f"agent key {_ago(runway)} left"
        )
    # Execution safety (issue #135): clauses only once the processes exist,
    # like the keystore clause above. A standing halt must say so — "healthy"
    # beside a silent halt would be a lie of omission.
    if snapshot.watchdog_beaten_at is not None:
        parts.append(f"watchdog beat {_ago(snapshot.now - snapshot.watchdog_beaten_at)} ago")
    if snapshot.active_halt_since is not None:
        parts.append("execution HALTED")
    return "✅ Epigone healthy · " + " · ".join(parts)


def _age(now: datetime, then: datetime | None) -> timedelta | None:
    return None if then is None else now - then


def _ago(age: timedelta | None) -> str:
    return "unknown" if age is None else format_duration(int(age.total_seconds()))


def _count(value: int | None) -> str:
    return "?" if value is None else f"{value:,}"


def _start_of_day(now: datetime) -> datetime:
    return datetime(now.year, now.month, now.day, tzinfo=UTC)
