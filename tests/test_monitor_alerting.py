"""The alerting state machine (issue #52): verdicts + clock → messages.

Asserts the transition discipline (user story #8): alert once on the way into
failing, an occasional reminder while it stays failing, one "recovered" on the
way out — never a message every cycle.
"""

from datetime import UTC, datetime, timedelta

from epigone.monitor.alerting import Monitor
from epigone.monitor.checks import INGEST, WARNING, CheckResult, HealthSnapshot

NOW = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)


def _monitor() -> Monitor:
    return Monitor(reminder=timedelta(hours=6), heartbeat_hour=9)


def _quiet_monitor() -> Monitor:
    # Heartbeat hour late enough that the transition-focused tests below (which
    # run at noon and advance a few hours) never trip the daily digest.
    return Monitor(reminder=timedelta(hours=6), heartbeat_hour=23)


def _ingest(ok: bool) -> CheckResult:
    return CheckResult(
        INGEST, "Ingest", ok=ok, severity=WARNING,
        detail="Ingest: no fine refresh in 45m but 600 due",
    )


def _snapshot(now: datetime) -> HealthSnapshot:
    return HealthSnapshot(now=now, db_reachable=True)


def test_a_failing_check_alerts_once_then_stays_quiet() -> None:
    monitor = _quiet_monitor()

    first = monitor.evaluate([_ingest(ok=False)], _snapshot(NOW), NOW)
    assert len(first) == 1
    assert first[0].startswith("⚠️") and "600 due" in first[0]

    # Still failing a minute later: no repeat.
    later = NOW + timedelta(minutes=1)
    assert monitor.evaluate([_ingest(ok=False)], _snapshot(later), later) == []


def test_a_still_failing_check_reminds_at_the_reminder_interval() -> None:
    monitor = _quiet_monitor()
    monitor.evaluate([_ingest(ok=False)], _snapshot(NOW), NOW)

    before = NOW + timedelta(hours=5)
    assert monitor.evaluate([_ingest(ok=False)], _snapshot(before), before) == []

    after = NOW + timedelta(hours=6, minutes=1)
    reminder = monitor.evaluate([_ingest(ok=False)], _snapshot(after), after)
    assert len(reminder) == 1 and "Still failing" in reminder[0]


def test_recovery_emits_one_recovered_message() -> None:
    monitor = _quiet_monitor()
    monitor.evaluate([_ingest(ok=False)], _snapshot(NOW), NOW)

    later = NOW + timedelta(minutes=10)
    recovered = monitor.evaluate([_ingest(ok=True)], _snapshot(later), later)
    assert recovered == ["✅ Ingest recovered"]

    # Healthy again next cycle: silent.
    even_later = later + timedelta(minutes=10)
    assert monitor.evaluate([_ingest(ok=True)], _snapshot(even_later), even_later) == []


def test_a_healthy_system_is_silent_until_the_heartbeat_hour() -> None:
    monitor = _monitor()
    before = NOW.replace(hour=8)
    assert monitor.evaluate([_ingest(ok=True)], _snapshot(before), before) == []

    at_hour = NOW.replace(hour=9)
    digest = monitor.evaluate([_ingest(ok=True)], _snapshot(at_hour), at_hour)
    assert len(digest) == 1 and digest[0].startswith("✅ Epigone healthy")


def test_the_heartbeat_fires_once_per_day() -> None:
    monitor = _monitor()
    morning = NOW.replace(hour=9)
    assert len(monitor.evaluate([_ingest(ok=True)], _snapshot(morning), morning)) == 1

    # Later the same day: no second heartbeat.
    noon = NOW.replace(hour=12)
    assert monitor.evaluate([_ingest(ok=True)], _snapshot(noon), noon) == []

    # Next day at the hour: it fires again.
    next_day = (NOW + timedelta(days=1)).replace(hour=9)
    assert len(monitor.evaluate([_ingest(ok=True)], _snapshot(next_day), next_day)) == 1


def test_an_alert_and_the_heartbeat_can_coincide() -> None:
    monitor = _monitor()
    at_hour = NOW.replace(hour=9)
    messages = monitor.evaluate([_ingest(ok=False)], _snapshot(at_hour), at_hour)
    assert len(messages) == 2
    assert any(m.startswith("⚠️") for m in messages)
    assert any(m.startswith("✅ Epigone healthy") for m in messages)
