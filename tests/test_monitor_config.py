"""MonitorConfig.from_env parsing (issue #52): defaults, overrides, and the
safe fallback on a bad value — a misconfiguration must never wedge the checker."""

import logging
from datetime import timedelta

import pytest

from epigone.monitor.config import (
    DEFAULT_DISK_PERCENT,
    DEFAULT_HEARTBEAT_HOUR,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_RATE_MAX_EVENTS,
    DEFAULT_RATE_WINDOW_MINUTES,
    DEFAULT_STARVATION_MIN_DUE,
    DEFAULT_STARVATION_WINDOW_MINUTES,
    MonitorConfig,
)

HEALTHCHECK_VARS = [
    "HEALTHCHECK_INTERVAL_MINUTES",
    "HEALTHCHECK_REMINDER_HOURS",
    "HEALTHCHECK_HEARTBEAT_HOUR",
    "HEALTHCHECK_INGEST_STALL_MINUTES",
    "HEALTHCHECK_COARSE_STALE_MINUTES",
    "HEALTHCHECK_ALERT_BACKLOG_MINUTES",
    "HEALTHCHECK_RATE_WINDOW_MINUTES",
    "HEALTHCHECK_RATE_MAX_EVENTS",
    "HEALTHCHECK_STARVATION_WINDOW_MINUTES",
    "HEALTHCHECK_STARVATION_MIN_DUE",
    "HEALTHCHECK_DISK_PERCENT",
    "HEALTHCHECK_DISK_PATH",
]


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in HEALTHCHECK_VARS:
        monkeypatch.delenv(var, raising=False)


def test_defaults_when_unset() -> None:
    config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.interval == timedelta(minutes=DEFAULT_INTERVAL_MINUTES)
    assert config.heartbeat_hour == DEFAULT_HEARTBEAT_HOUR
    assert config.thresholds.disk_percent == float(DEFAULT_DISK_PERCENT)
    assert config.thresholds.rate_window == timedelta(minutes=DEFAULT_RATE_WINDOW_MINUTES)
    assert config.thresholds.rate_max_events == DEFAULT_RATE_MAX_EVENTS
    assert config.thresholds.starvation_window == timedelta(
        minutes=DEFAULT_STARVATION_WINDOW_MINUTES
    )
    assert config.thresholds.starvation_min_due == DEFAULT_STARVATION_MIN_DUE


def test_coarse_staleness_defaults_to_twice_the_seed_interval() -> None:
    config = MonitorConfig.from_env(seed_interval_minutes=30)
    assert config.thresholds.coarse_stale == timedelta(minutes=60)


def test_valid_overrides_are_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEALTHCHECK_INTERVAL_MINUTES", "5")
    monkeypatch.setenv("HEALTHCHECK_HEARTBEAT_HOUR", "0")
    monkeypatch.setenv("HEALTHCHECK_DISK_PERCENT", "90")
    monkeypatch.setenv("HEALTHCHECK_COARSE_STALE_MINUTES", "45")
    monkeypatch.setenv("HEALTHCHECK_RATE_WINDOW_MINUTES", "30")
    monkeypatch.setenv("HEALTHCHECK_RATE_MAX_EVENTS", "10")
    monkeypatch.setenv("HEALTHCHECK_STARVATION_WINDOW_MINUTES", "60")
    monkeypatch.setenv("HEALTHCHECK_STARVATION_MIN_DUE", "100")
    config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.interval == timedelta(minutes=5)
    assert config.heartbeat_hour == 0  # midnight is valid
    assert config.thresholds.disk_percent == 90.0
    assert config.thresholds.coarse_stale == timedelta(minutes=45)
    assert config.thresholds.rate_window == timedelta(minutes=30)
    assert config.thresholds.rate_max_events == 10
    assert config.thresholds.starvation_window == timedelta(minutes=60)
    assert config.thresholds.starvation_min_due == 100


@pytest.mark.parametrize("bad", ["nonsense", "", "0", "-5"])
def test_invalid_interval_falls_back_to_the_default(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_INTERVAL_MINUTES", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.interval == timedelta(minutes=DEFAULT_INTERVAL_MINUTES)
    assert "HEALTHCHECK_INTERVAL_MINUTES" in caplog.text


@pytest.mark.parametrize("bad", ["nonsense", "24", "-1"])
def test_invalid_heartbeat_hour_falls_back_to_the_default(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_HEARTBEAT_HOUR", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.heartbeat_hour == DEFAULT_HEARTBEAT_HOUR
    assert "HEALTHCHECK_HEARTBEAT_HOUR" in caplog.text


@pytest.mark.parametrize("bad", ["nonsense", "", "0", "-5"])
def test_invalid_rate_thresholds_fall_back_to_defaults(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_RATE_WINDOW_MINUTES", bad)
    monkeypatch.setenv("HEALTHCHECK_RATE_MAX_EVENTS", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.thresholds.rate_window == timedelta(minutes=DEFAULT_RATE_WINDOW_MINUTES)
    assert config.thresholds.rate_max_events == DEFAULT_RATE_MAX_EVENTS
    assert "HEALTHCHECK_RATE_WINDOW_MINUTES" in caplog.text
    assert "HEALTHCHECK_RATE_MAX_EVENTS" in caplog.text


@pytest.mark.parametrize("bad", ["nonsense", "", "0", "-5"])
def test_invalid_starvation_thresholds_fall_back_to_defaults(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_STARVATION_WINDOW_MINUTES", bad)
    monkeypatch.setenv("HEALTHCHECK_STARVATION_MIN_DUE", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.thresholds.starvation_window == timedelta(
        minutes=DEFAULT_STARVATION_WINDOW_MINUTES
    )
    assert config.thresholds.starvation_min_due == DEFAULT_STARVATION_MIN_DUE
    assert "HEALTHCHECK_STARVATION_WINDOW_MINUTES" in caplog.text
    assert "HEALTHCHECK_STARVATION_MIN_DUE" in caplog.text


@pytest.mark.parametrize("bad", ["nonsense", "0", "101", "-3"])
def test_invalid_disk_percent_falls_back_to_the_default(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_DISK_PERCENT", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.thresholds.disk_percent == float(DEFAULT_DISK_PERCENT)
    assert "HEALTHCHECK_DISK_PERCENT" in caplog.text
