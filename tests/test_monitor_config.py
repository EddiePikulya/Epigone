"""MonitorConfig.from_env parsing (issue #52): defaults, overrides, and the
safe fallback on a bad value — a misconfiguration must never wedge the checker."""

import logging
from datetime import timedelta

import pytest

from epigone.monitor.config import (
    DEFAULT_DISK_PERCENT,
    DEFAULT_HEARTBEAT_HOUR,
    DEFAULT_INTERVAL_MINUTES,
    MonitorConfig,
)

HEALTHCHECK_VARS = [
    "HEALTHCHECK_INTERVAL_MINUTES",
    "HEALTHCHECK_REMINDER_HOURS",
    "HEALTHCHECK_HEARTBEAT_HOUR",
    "HEALTHCHECK_INGEST_STALL_MINUTES",
    "HEALTHCHECK_COARSE_STALE_MINUTES",
    "HEALTHCHECK_ALERT_BACKLOG_MINUTES",
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


def test_coarse_staleness_defaults_to_twice_the_seed_interval() -> None:
    config = MonitorConfig.from_env(seed_interval_minutes=30)
    assert config.thresholds.coarse_stale == timedelta(minutes=60)


def test_valid_overrides_are_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEALTHCHECK_INTERVAL_MINUTES", "5")
    monkeypatch.setenv("HEALTHCHECK_HEARTBEAT_HOUR", "0")
    monkeypatch.setenv("HEALTHCHECK_DISK_PERCENT", "90")
    monkeypatch.setenv("HEALTHCHECK_COARSE_STALE_MINUTES", "45")
    config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.interval == timedelta(minutes=5)
    assert config.heartbeat_hour == 0  # midnight is valid
    assert config.thresholds.disk_percent == 90.0
    assert config.thresholds.coarse_stale == timedelta(minutes=45)


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


@pytest.mark.parametrize("bad", ["nonsense", "0", "101", "-3"])
def test_invalid_disk_percent_falls_back_to_the_default(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("HEALTHCHECK_DISK_PERCENT", bad)
    with caplog.at_level(logging.WARNING):
        config = MonitorConfig.from_env(seed_interval_minutes=60)
    assert config.thresholds.disk_percent == float(DEFAULT_DISK_PERCENT)
    assert "HEALTHCHECK_DISK_PERCENT" in caplog.text
