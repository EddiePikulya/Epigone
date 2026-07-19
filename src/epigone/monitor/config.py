"""Monitor process configuration (issue #52).

Reuses the shared secrets (DATABASE_URL, TELEGRAM_BOT_TOKEN, ADMIN_TELEGRAM_ID)
via epigone.config.Settings; everything below is monitor-only, env-tunable, and
falls back to a safe default on a bad value (parsed like SEED_INTERVAL_MINUTES,
issue #50) so a misconfiguration never wedges the checker.
"""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta

from epigone.config import parse_positive_int
from epigone.monitor.checks import CheckThresholds

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_MINUTES = 15
DEFAULT_REMINDER_HOURS = 6
DEFAULT_HEARTBEAT_HOUR = 9  # UTC; the server runs on UTC (deploy.md)
DEFAULT_INGEST_STALL_MINUTES = 30
DEFAULT_ALERT_BACKLOG_MINUTES = 5
# Sustained rate limiting (issue #54): fail when at least this many escaped-429
# events land within the window. Post-#41 the steady state is ~0 escaped 429s,
# so a handful over a quarter-hour is a real regression, not pacing — while a
# lone unlucky call staying under the count keeps quiet (user story #2).
DEFAULT_RATE_WINDOW_MINUTES = 15
DEFAULT_RATE_MAX_EVENTS = 5
# Fine-pass success starvation (issue #61): a due backlog past this floor with
# zero successful refreshes over the window (while attempts keep advancing) is
# real starvation — the 20h 500-storm shape — not the normal handful-due churn.
DEFAULT_STARVATION_WINDOW_MINUTES = 45
DEFAULT_STARVATION_MIN_DUE = 50
DEFAULT_DISK_PERCENT = 85
DEFAULT_DISK_PATH = "/"
# Coarse metrics older than this multiple of the seed interval mean the re-seed
# likely stopped (issue #52). Default = 2× the configured cadence.
COARSE_STALE_SEED_MULTIPLE = 2


@dataclass(frozen=True)
class MonitorConfig:
    interval: timedelta
    reminder: timedelta
    heartbeat_hour: int
    thresholds: CheckThresholds
    disk_path: str

    @classmethod
    def from_env(cls, *, seed_interval_minutes: int) -> "MonitorConfig":
        """Build from HEALTHCHECK_* env vars. `seed_interval_minutes` (the ingest
        cadence, issue #50) sets the default coarse-staleness window at 2× it."""
        coarse_default = seed_interval_minutes * COARSE_STALE_SEED_MULTIPLE
        return cls(
            interval=timedelta(
                minutes=parse_positive_int(
                    os.environ.get("HEALTHCHECK_INTERVAL_MINUTES"),
                    default=DEFAULT_INTERVAL_MINUTES,
                    name="HEALTHCHECK_INTERVAL_MINUTES",
                )
            ),
            reminder=timedelta(
                hours=parse_positive_int(
                    os.environ.get("HEALTHCHECK_REMINDER_HOURS"),
                    default=DEFAULT_REMINDER_HOURS,
                    name="HEALTHCHECK_REMINDER_HOURS",
                )
            ),
            heartbeat_hour=_parse_hour(os.environ.get("HEALTHCHECK_HEARTBEAT_HOUR")),
            thresholds=CheckThresholds(
                ingest_stall=timedelta(
                    minutes=parse_positive_int(
                        os.environ.get("HEALTHCHECK_INGEST_STALL_MINUTES"),
                        default=DEFAULT_INGEST_STALL_MINUTES,
                        name="HEALTHCHECK_INGEST_STALL_MINUTES",
                    )
                ),
                coarse_stale=timedelta(
                    minutes=parse_positive_int(
                        os.environ.get("HEALTHCHECK_COARSE_STALE_MINUTES"),
                        default=coarse_default,
                        name="HEALTHCHECK_COARSE_STALE_MINUTES",
                    )
                ),
                alert_backlog=timedelta(
                    minutes=parse_positive_int(
                        os.environ.get("HEALTHCHECK_ALERT_BACKLOG_MINUTES"),
                        default=DEFAULT_ALERT_BACKLOG_MINUTES,
                        name="HEALTHCHECK_ALERT_BACKLOG_MINUTES",
                    )
                ),
                rate_window=timedelta(
                    minutes=parse_positive_int(
                        os.environ.get("HEALTHCHECK_RATE_WINDOW_MINUTES"),
                        default=DEFAULT_RATE_WINDOW_MINUTES,
                        name="HEALTHCHECK_RATE_WINDOW_MINUTES",
                    )
                ),
                rate_max_events=parse_positive_int(
                    os.environ.get("HEALTHCHECK_RATE_MAX_EVENTS"),
                    default=DEFAULT_RATE_MAX_EVENTS,
                    name="HEALTHCHECK_RATE_MAX_EVENTS",
                ),
                starvation_window=timedelta(
                    minutes=parse_positive_int(
                        os.environ.get("HEALTHCHECK_STARVATION_WINDOW_MINUTES"),
                        default=DEFAULT_STARVATION_WINDOW_MINUTES,
                        name="HEALTHCHECK_STARVATION_WINDOW_MINUTES",
                    )
                ),
                starvation_min_due=parse_positive_int(
                    os.environ.get("HEALTHCHECK_STARVATION_MIN_DUE"),
                    default=DEFAULT_STARVATION_MIN_DUE,
                    name="HEALTHCHECK_STARVATION_MIN_DUE",
                ),
                disk_percent=_parse_disk_percent(os.environ.get("HEALTHCHECK_DISK_PERCENT")),
            ),
            disk_path=os.environ.get("HEALTHCHECK_DISK_PATH") or DEFAULT_DISK_PATH,
        )


def _parse_hour(raw: str | None) -> int:
    """Parse HEALTHCHECK_HEARTBEAT_HOUR (0–23 UTC), falling back to the default
    on anything out of range — 0 is valid (midnight), so this can't reuse the
    positive-int parser."""
    if raw is None:
        return DEFAULT_HEARTBEAT_HOUR
    name = "HEALTHCHECK_HEARTBEAT_HOUR"
    try:
        hour = int(raw)
    except ValueError:
        log.warning("%s=%r is not an integer; using %d", name, raw, DEFAULT_HEARTBEAT_HOUR)
        return DEFAULT_HEARTBEAT_HOUR
    if not 0 <= hour <= 23:
        log.warning("%s=%r is not 0–23; using %d", name, raw, DEFAULT_HEARTBEAT_HOUR)
        return DEFAULT_HEARTBEAT_HOUR
    return hour


def _parse_disk_percent(raw: str | None) -> float:
    """Parse HEALTHCHECK_DISK_PERCENT (a used-% trip point in 1–100), falling
    back to the default on anything out of range."""
    name = "HEALTHCHECK_DISK_PERCENT"
    if raw is None:
        return float(DEFAULT_DISK_PERCENT)
    try:
        percent = float(raw)
    except ValueError:
        log.warning("%s=%r is not a number; using %d", name, raw, DEFAULT_DISK_PERCENT)
        return float(DEFAULT_DISK_PERCENT)
    if not 0 < percent <= 100:
        log.warning("%s=%r is not 1–100; using %d", name, raw, DEFAULT_DISK_PERCENT)
        return float(DEFAULT_DISK_PERCENT)
    return percent
