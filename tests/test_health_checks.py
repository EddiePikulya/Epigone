"""Pure check evaluation (issue #52): synthetic snapshots → decisions.

The check-evaluation seam — no DB, no wall clock. Each test feeds a
HealthSnapshot and asserts which checks fail and what numbers the text carries.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from epigone.monitor.checks import (
    ALERTS,
    COARSE,
    CRITICAL,
    DATABASE,
    DISK,
    FINE_SUCCESS,
    INGEST,
    RATE,
    WARNING,
    CheckResult,
    CheckThresholds,
    HealthSnapshot,
    db_down,
    evaluate_checks,
    heartbeat_digest,
)

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)

THRESHOLDS = CheckThresholds(
    ingest_stall=timedelta(minutes=30),
    coarse_stale=timedelta(minutes=120),
    alert_backlog=timedelta(minutes=5),
    rate_window=timedelta(minutes=15),
    rate_max_events=5,
    disk_percent=85,
    starvation_window=timedelta(minutes=45),
    starvation_min_due=50,
)

HEALTHY = HealthSnapshot(
    now=NOW,
    db_reachable=True,
    wallet_count=41_203,
    due_traders=0,
    last_fine_refresh=NOW - timedelta(minutes=2),
    last_fine_attempt=NOW - timedelta(minutes=1),
    fine_refreshed_today=312,
    last_coarse_compute=NOW - timedelta(minutes=12),
    undelivered_alerts=0,
    oldest_undelivered_alert=None,
    recent_rate_limits=0,
    disk_percent_used=47.0,
)


def _by_name(results: list[CheckResult], name: str) -> CheckResult:
    return next(r for r in results if r.name == name)


def test_a_healthy_system_trips_no_check() -> None:
    results = evaluate_checks(HEALTHY, THRESHOLDS)
    assert all(r.ok for r in results)
    assert {r.name for r in results} == {
        DATABASE,
        INGEST,
        COARSE,
        ALERTS,
        RATE,
        DISK,
        FINE_SUCCESS,
    }


def test_ingest_is_flagged_when_no_refresh_in_window_and_traders_are_due() -> None:
    snapshot = replace(
        HEALTHY, due_traders=600, last_fine_refresh=NOW - timedelta(minutes=45)
    )
    ingest = _by_name(evaluate_checks(snapshot, THRESHOLDS), INGEST)
    assert not ingest.ok
    assert ingest.severity == WARNING
    assert "600" in ingest.detail and "stuck" in ingest.detail


def test_ingest_idle_because_caught_up_does_not_false_alarm() -> None:
    # The exact nuance from the rescan investigation: a caught-up pass is idle by
    # design (0 due), even with a long-stale last refresh — never an alert.
    snapshot = replace(
        HEALTHY, due_traders=0, last_fine_refresh=NOW - timedelta(hours=6)
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), INGEST).ok


def test_ingest_flags_a_never_refreshed_system_with_due_traders() -> None:
    snapshot = replace(HEALTHY, due_traders=10, last_fine_refresh=None)
    ingest = _by_name(evaluate_checks(snapshot, THRESHOLDS), INGEST)
    assert not ingest.ok
    assert "never" in ingest.detail


def test_fine_success_starvation_is_flagged_when_attempts_advance_but_none_succeed() -> None:
    # The #61 outage shape: a large due backlog, the fine pass is actively
    # attempting (recent fine_attempted_at), yet no successful refresh has landed
    # in the window — every refresh is failing (e.g. a 500 storm).
    snapshot = replace(
        HEALTHY,
        due_traders=1_200,
        last_fine_refresh=NOW - timedelta(minutes=90),
        last_fine_attempt=NOW - timedelta(minutes=1),
    )
    fine = _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS)
    assert not fine.ok
    assert fine.severity == WARNING
    assert "1,200" in fine.detail and "failing" in fine.detail


def test_fine_success_flags_a_never_succeeded_pass_that_is_actively_attempting() -> None:
    # No successful refresh has *ever* landed, but attempts are advancing and the
    # backlog is large — starving from the first cycle.
    snapshot = replace(
        HEALTHY,
        due_traders=1_200,
        last_fine_refresh=None,
        last_fine_attempt=NOW - timedelta(minutes=1),
    )
    fine = _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS)
    assert not fine.ok
    assert "ever" in fine.detail


def test_fine_success_is_healthy_when_caught_up_even_without_a_recent_success() -> None:
    # Backlog ≈ 0 → nothing to succeed at; a quiet caught-up pass is idle by
    # design, never starvation (mirrors the ingest caught-up nuance).
    snapshot = replace(
        HEALTHY,
        due_traders=0,
        last_fine_refresh=NOW - timedelta(hours=6),
        last_fine_attempt=NOW - timedelta(minutes=1),
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS).ok


def test_fine_success_is_healthy_when_the_backlog_is_below_the_min_due_floor() -> None:
    # A handful due is normal churn, not a starving pass worth paging on.
    snapshot = replace(
        HEALTHY,
        due_traders=10,
        last_fine_refresh=NOW - timedelta(minutes=90),
        last_fine_attempt=NOW - timedelta(minutes=1),
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS).ok


def test_fine_success_is_healthy_when_a_recent_success_landed() -> None:
    # Successes landing within the window → not starving, even with a big backlog.
    snapshot = replace(
        HEALTHY,
        due_traders=1_200,
        last_fine_refresh=NOW - timedelta(minutes=5),
        last_fine_attempt=NOW - timedelta(minutes=1),
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS).ok


def test_fine_success_defers_to_ingest_when_attempts_are_not_advancing() -> None:
    # A *stopped* pass (last attempt long ago) is the ingest check's province, not
    # this one — the attempts-advancing guard (issue #61).
    snapshot = replace(
        HEALTHY,
        due_traders=1_200,
        last_fine_refresh=NOW - timedelta(minutes=90),
        last_fine_attempt=NOW - timedelta(hours=3),
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS).ok


def test_fine_success_stays_quiet_when_no_attempts_were_ever_recorded() -> None:
    # A brand-new never-run pass (no attempts at all) is not "constantly failing".
    snapshot = replace(
        HEALTHY, due_traders=1_200, last_fine_refresh=None, last_fine_attempt=None
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), FINE_SUCCESS).ok


def test_coarse_is_flagged_when_metrics_are_stale() -> None:
    snapshot = replace(HEALTHY, last_coarse_compute=NOW - timedelta(minutes=200))
    coarse = _by_name(evaluate_checks(snapshot, THRESHOLDS), COARSE)
    assert not coarse.ok
    assert "re-seed" in coarse.detail


def test_alert_backlog_is_flagged_when_undelivered_alerts_age_out() -> None:
    snapshot = replace(
        HEALTHY, undelivered_alerts=4, oldest_undelivered_alert=NOW - timedelta(minutes=10)
    )
    alerts = _by_name(evaluate_checks(snapshot, THRESHOLDS), ALERTS)
    assert not alerts.ok
    assert "4" in alerts.detail


def test_a_fresh_undelivered_alert_within_the_window_is_not_a_backlog() -> None:
    snapshot = replace(
        HEALTHY, undelivered_alerts=1, oldest_undelivered_alert=NOW - timedelta(minutes=2)
    )
    assert _by_name(evaluate_checks(snapshot, THRESHOLDS), ALERTS).ok


def test_a_sustained_rate_limit_spike_is_flagged() -> None:
    # At or past the count threshold within the window (issue #54): real limiting.
    snapshot = replace(HEALTHY, recent_rate_limits=5)
    rate = _by_name(evaluate_checks(snapshot, THRESHOLDS), RATE)
    assert not rate.ok
    assert rate.severity == WARNING
    assert "5" in rate.detail and "throttling" in rate.detail


def test_isolated_rate_limit_events_below_the_threshold_do_not_alarm() -> None:
    # A few escaped-429s under the count are normal pacing, not an outage (user
    # story #2): the check stays healthy.
    assert _by_name(evaluate_checks(replace(HEALTHY, recent_rate_limits=4), THRESHOLDS), RATE).ok


def test_a_missing_rate_reading_is_treated_as_healthy() -> None:
    # None only happens on a read miss — never a false alarm, like the disk probe.
    assert _by_name(evaluate_checks(replace(HEALTHY, recent_rate_limits=None), THRESHOLDS), RATE).ok


def test_disk_is_flagged_at_the_threshold_and_escalates_when_critical() -> None:
    warn = _by_name(evaluate_checks(replace(HEALTHY, disk_percent_used=90.0), THRESHOLDS), DISK)
    assert not warn.ok and warn.severity == WARNING and "90%" in warn.detail

    crit = _by_name(evaluate_checks(replace(HEALTHY, disk_percent_used=97.0), THRESHOLDS), DISK)
    assert not crit.ok and crit.severity == CRITICAL


def test_a_missing_disk_reading_is_treated_as_healthy() -> None:
    # No host visibility is not a full disk: a None probe must not false-alarm.
    assert _by_name(evaluate_checks(replace(HEALTHY, disk_percent_used=None), THRESHOLDS), DISK).ok


def test_db_down_reports_only_the_critical_database_check() -> None:
    results = evaluate_checks(db_down(NOW), THRESHOLDS)
    assert len(results) == 1
    (database,) = results
    assert database.name == DATABASE and not database.ok and database.severity == CRITICAL


def test_heartbeat_digest_carries_the_key_liveness_numbers() -> None:
    digest = heartbeat_digest(HEALTHY)
    assert "41,203 wallets" in digest
    assert "312 fine-refreshed today" in digest
    assert "0 due" in digest
    assert "coarse fresh 12m ago" in digest
    assert "0 rate errors" in digest
    assert "disk 47%" in digest
