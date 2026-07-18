"""The alerting state machine: check verdicts + clock → messages to send.

Per-check state is healthy ⇄ failing (issue #52). The operator is told on the
*transition* into failing, once — not every cycle — with at most an occasional
reminder while it stays failing (user story #8); a short "recovered" closes the
loop on the transition back (user story #7). A daily heartbeat fires once at a
configured hour so no-news reliably means healthy (user story #6).

State is in-memory: a monitor restart re-alerting an active problem is
acceptable and even informative (issue #52), and DB-down must not depend on
persisted state to fire.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from epigone.monitor.checks import CRITICAL, CheckResult, HealthSnapshot, heartbeat_digest

log = logging.getLogger(__name__)

_SEVERITY_EMOJI = {CRITICAL: "🚨"}
_WARNING_EMOJI = "⚠️"


@dataclass
class _CheckState:
    failing: bool = False
    last_alerted_at: datetime | None = None


@dataclass
class Monitor:
    """Holds per-check state between cycles and turns each cycle's verdicts into
    the messages to DM the admin. `reminder` throttles repeat alerts on a still-
    failing check; `heartbeat_hour` is the UTC hour the daily digest fires."""

    reminder: timedelta
    heartbeat_hour: int
    _states: dict[str, _CheckState] = field(default_factory=dict)
    _last_heartbeat_date: datetime | None = None

    def evaluate(
        self, results: list[CheckResult], snapshot: HealthSnapshot, now: datetime
    ) -> list[str]:
        """The messages to send this cycle: failing-transitions, throttled
        reminders, recoveries, and — at its hour — the daily heartbeat."""
        messages = [m for r in results if (m := self._transition(r, now)) is not None]
        heartbeat = self._heartbeat(snapshot, now)
        if heartbeat is not None:
            messages.append(heartbeat)
        return messages

    def _transition(self, result: CheckResult, now: datetime) -> str | None:
        state = self._states.setdefault(result.name, _CheckState())
        if not result.ok:
            if not state.failing:
                state.failing = True
                state.last_alerted_at = now
                return f"{_emoji(result.severity)} {result.detail}"
            if state.last_alerted_at is not None and now - state.last_alerted_at >= self.reminder:
                state.last_alerted_at = now
                return f"{_emoji(result.severity)} Still failing — {result.detail}"
            return None
        if state.failing:
            state.failing = False
            state.last_alerted_at = None
            return f"✅ {result.title} recovered"
        return None

    def _heartbeat(self, snapshot: HealthSnapshot, now: datetime) -> str | None:
        """Once per calendar day, at or after the configured hour. A same-day
        restart re-sends it (state is in-memory) — an acceptable, informative
        duplicate rather than a silent gap."""
        last = self._last_heartbeat_date
        already_today = last is not None and last.date() == now.date()
        if already_today or now.hour < self.heartbeat_hour:
            return None
        self._last_heartbeat_date = now
        return heartbeat_digest(snapshot)


def _emoji(severity: str) -> str:
    return _SEVERITY_EMOJI.get(severity, _WARNING_EMOJI)
