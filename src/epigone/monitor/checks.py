"""Health checks: gather one liveness snapshot, then evaluate it purely.

The seam the tests drive (issue #52 "Testing Decisions"): `gather_snapshot`
does the impure work — one Postgres round-trip plus a disk read — into a plain
`HealthSnapshot` of raw numbers/timestamps; `evaluate_checks` is a pure function
mapping that snapshot + thresholds to a list of `CheckResult` decisions. Tests
feed synthetic snapshots and assert on which checks fail and what text they
carry, never touching the wall clock or the live server.

Checks split by what's observable (issue #52):
- DB-observable — ingest progress, coarse freshness, alert-delivery backlog, and
  DB reachability itself; a single connection answers all of them.
- Host-observable — disk headroom, read via an injected `DiskProbe` so the
  container needs only the host filesystem mounted, not the docker socket.

Rate health (429 spike, user story #5) is a deliberate V1 omission: it lives in
the ingest logs, not the DB, and the issue explicitly permits shipping it as a
fast-follow rather than coupling the monitor to the docker socket or widening the
ingest write-path. It is the one gap; the other five checks are complete.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import asyncpg

from epigone.bot.alerts import MAX_DELIVERY_ATTEMPTS
from epigone.clock import Clock
from epigone.ingest.fine import count_due_traders
from epigone.metrics.library import format_duration

# Machine names, stable across a check's lifetime so the alerting state machine
# (epigone.monitor.alerting) can track each independently.
DATABASE = "database"
INGEST = "ingest"
COARSE = "coarse"
ALERTS = "alerts"
DISK = "disk"

WARNING = "warning"
CRITICAL = "critical"

# Above the operator's disk trip point a warning suffices; past this the disk is
# nearly full and the alert escalates to critical (🚨). A fixed escalation band,
# not a tunable — the operator tunes *when* to be warned via HEALTHCHECK_DISK_PERCENT.
DISK_CRITICAL_PERCENT = 95


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
    disk_percent: float


@dataclass(frozen=True)
class HealthSnapshot:
    """One point-in-time reading of every liveness signal. Raw observations
    only — every threshold decision lives in `evaluate_checks`."""

    now: datetime
    db_reachable: bool
    wallet_count: int | None = None
    due_traders: int | None = None
    last_fine_refresh: datetime | None = None
    fine_refreshed_today: int | None = None
    last_coarse_compute: datetime | None = None
    undelivered_alerts: int | None = None
    oldest_undelivered_alert: datetime | None = None
    disk_percent_used: float | None = None


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
    pool: asyncpg.Pool, clock: Clock, disk: DiskProbe
) -> HealthSnapshot:
    """Read every liveness signal in one pass. A query failure is itself the
    loudest signal (DB down), so the caller catches it and reports it — see
    `db_down`. `now` is stamped from the injected clock, not the wall clock."""
    now = clock.now()
    row = await pool.fetchrow(
        """
        SELECT
            (SELECT count(*) FROM traders) AS wallet_count,
            (SELECT max(fine_refreshed_at) FROM traders) AS last_fine_refresh,
            (SELECT count(*) FROM traders WHERE fine_refreshed_at >= $1)
                AS fine_refreshed_today,
            (SELECT max(computed_at) FROM coarse_metrics) AS last_coarse_compute,
            (SELECT count(*) FROM position_alerts
                WHERE delivered_at IS NULL AND attempts < $2) AS undelivered_alerts,
            (SELECT min(created_at) FROM position_alerts
                WHERE delivered_at IS NULL AND attempts < $2)
                AS oldest_undelivered_alert
        """,
        _start_of_day(now),
        MAX_DELIVERY_ATTEMPTS,
    )
    assert row is not None
    return HealthSnapshot(
        now=now,
        db_reachable=True,
        wallet_count=row["wallet_count"],
        due_traders=await count_due_traders(pool, now),
        last_fine_refresh=row["last_fine_refresh"],
        fine_refreshed_today=row["fine_refreshed_today"],
        last_coarse_compute=row["last_coarse_compute"],
        undelivered_alerts=row["undelivered_alerts"],
        oldest_undelivered_alert=row["oldest_undelivered_alert"],
        disk_percent_used=disk.percent_used(),
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
        _disk_check(snapshot, thresholds.disk_percent),
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


def heartbeat_digest(snapshot: HealthSnapshot) -> str:
    """The daily positive digest (user story #6): the key liveness numbers so
    silence genuinely means healthy and a dead checker is noticeable by its
    missing ping."""
    parts = [f"{_count(snapshot.wallet_count)} wallets"]
    parts.append(f"{_count(snapshot.fine_refreshed_today)} fine-refreshed today")
    coarse_age = _age(snapshot.now, snapshot.last_coarse_compute)
    parts.append(
        "coarse never computed" if coarse_age is None else f"coarse fresh {_ago(coarse_age)} ago"
    )
    parts.append(f"{_count(snapshot.undelivered_alerts)} alerts pending")
    if snapshot.disk_percent_used is not None:
        parts.append(f"disk {snapshot.disk_percent_used:.0f}%")
    return "✅ Epigone healthy · " + " · ".join(parts)


def _age(now: datetime, then: datetime | None) -> timedelta | None:
    return None if then is None else now - then


def _ago(age: timedelta | None) -> str:
    return "unknown" if age is None else format_duration(int(age.total_seconds()))


def _count(value: int | None) -> str:
    return "?" if value is None else f"{value:,}"


def _start_of_day(now: datetime) -> datetime:
    return datetime(now.year, now.month, now.day, tzinfo=UTC)
