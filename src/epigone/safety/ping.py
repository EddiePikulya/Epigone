"""The OUT-OF-BAND page path (issue #213): a dead-man ping to a service that
is not us.

Every alarm Epigone had ran through Epigone. The #52 monitor reads Postgres
to decide the watchdog is dead and DMs the operator from its own container on
the same host — so the single most likely cause of a genuinely dead watchdog,
that host or its database going away, is also the thing that stops anyone
being told. The monitor's DB touches are bounded now (`epigone.monitor.main`,
same issue), which fixes the SILENT-HANG half; it cannot fix the other half,
because a checker inside the failure domain cannot report the domain failing.

So the watchdog also pings OUTWARD, on the inverted logic a dead man's switch
needs: the operator configures a check on an external service
(healthchecks.io and friends), the watchdog says "still here" every
`interval_seconds`, and when the pings STOP the external service pages from
its own infrastructure. Nothing of ours has to be alive for that page to
happen — which is the entire property, and the only one. What it can and
cannot tell the operator is spelled out in docs/runbooks/halt-and-unwind.md;
the short version is that it carries ONE BIT, "the watchdog's cycle loop
reached this call recently", and that bit says nothing about Postgres, the
exchange, or whether the watchdog could actually cancel anything.

THREE PROPERTIES ARE LOAD-BEARING, and each is a rule this module keeps so
its callers do not have to:

- IT CANNOT SLOW THE PATH IT REPORTS ON. `ping()` is deliberately a SYNC
  method: there is no await to inherit a stall, so a caller on the kill path
  cannot be delayed by the network even in principle. It costs one throttle
  comparison and, at most once per interval, an `asyncio.create_task`. The
  request itself is bounded twice over (below), but that is belt and braces
  for the ping's OWN health, not for the caller's — the caller is protected
  by structure rather than by a number, which is the whole reason for the
  odd-looking sync signature on an I/O object.
- IT NO-OPS CLEANLY WHEN UNCONFIGURED. There is no ping URL until the
  operator creates the check and sets `WATCHDOG_DEADMAN_PING_URL`, and the
  watchdog must run identically without one — so "unconfigured" is
  represented by not constructing this at all (`epigone.safety.main` passes
  `alive=None`), never by a disabled instance that might still be reached.
  The startup log says loudly which of the two the process is in, because
  "the out-of-band path is not armed" is exactly the fact an operator can
  otherwise assume away.
- A WEDGED PING CANNOT SILENCE THE NEXT ONE. Only one request is in flight
  at a time (a stalled ping must not stack up one task per pulse against a
  black-holed pinging host), which would be a trap if a stall could last
  forever — the first hung request would suppress every later one and the
  external service would page for a fault in the reporter. Hence the two
  bounds: aiohttp's own total timeout, and an outer `asyncio.wait_for` at a
  multiple of it. This codebase's history is bounds with one more layer
  underneath them (the round 2-5 asyncpg chase in
  `epigone.safety.watchdog`), so the leg whose failure mode is "silence"
  states its own ceiling rather than trusting the library's.

FALSE PAGES ARE THE COST, and they are the right cost. A pinging watchdog
that cannot cancel still looks alive here; a watchdog that is fine while the
pinging host's network path is not looks dead. The first is covered by the
monitor's capability check, the second is why the ping interval is a small
fraction of the external check's grace period. Between "pages when it should
not" and "silent when it should page", a dead-man's switch takes the first
every time.
"""

import asyncio
import contextlib
import logging
import time
from urllib.parse import urlsplit

import aiohttp

log = logging.getLogger(__name__)

# The cadence is NOT defaulted here. It is an operator knob and
# `epigone.safety.config` owns its default (DEFAULT_DEADMAN_PING_SECONDS, 60s)
# beside the URL it goes with; a second 60 in this file would be a source of
# truth that production never reads and a test could silently drift from.
# `interval_seconds` is therefore required at construction.

# What ONE ping may spend. Generously above a healthy round trip to a public
# uptime service and far below the interval, so a slow ping never eats into
# the cadence: a request still in flight when the next pulse comes round is
# skipped, not queued.
PING_TIMEOUT_SECONDS = 5.0

# The outer ceiling, as a multiple of the timeout aiohttp is supposed to
# enforce. Two rather than one so the inner bound is the one that normally
# binds, and the outer only exists for the case this codebase keeps meeting:
# a library bound with one more layer underneath it.
#
# The two are INDISTINGUISHABLE from the outside, and the log line says so
# rather than guessing: since 3.11 `asyncio.TimeoutError` IS the builtin
# `TimeoutError`, which is what aiohttp's total timeout and `asyncio.wait_for`
# both raise. Telling them apart would take a stopwatch for the sake of one
# word in a warning, and the operator's next step ("something on the path to
# the pinging host is not answering") is the same either way.
PING_CEILING_MULTIPLE = 2.0


class ExternalPing:
    """A throttled, fire-and-forget liveness ping to an operator-provided URL.

    One instance per process, sharing the watchdog's `aiohttp.ClientSession`.
    Sharing is deliberate: it is one connector for a process that makes a
    handful of requests, and the ping's host is not the exchange's, so it
    gets its own per-host connection pool rather than queueing behind a
    saturated exchange one.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str,
        *,
        interval_seconds: float,
        timeout_seconds: float = PING_TIMEOUT_SECONDS,
    ) -> None:
        self._session = session
        self._url = url
        self._interval = interval_seconds
        self._timeout = timeout_seconds
        # Monotonic, like every other cadence on this path: a wall clock that
        # steps BACKWARDS (an NTP correction, a VM resuming from a snapshot)
        # would suppress pings for the size of the step, and suppressed pings
        # are how this mechanism pages for a fault that isn't there.
        self._last_ping: float | None = None
        # The one request allowed in flight. Held as a strong reference for
        # its lifetime as well as for the skip check: asyncio keeps only a
        # weak one, so a bare create_task can be garbage-collected mid-flight.
        self._flight: asyncio.Task[None] | None = None

    @property
    def target(self) -> str:
        """The ping's HOST, for logging. Never the full URL: on
        healthchecks.io and every service like it the path IS the credential,
        so a URL in a log line (or in a pasted log line) is a liveness signal
        anyone can forge. The host is what an operator needs to recognise the
        configuration; the secret stays in the environment."""
        return urlsplit(self._url).netloc or "(unparseable)"

    def ping(self) -> None:
        """Say ALIVE, at most once per interval, without awaiting anything.

        Sync on purpose (module docstring): the caller is on the kill path and
        must not be able to inherit this leg's latency even in principle. It
        also never raises — the caller has nothing useful to do about a
        failed ping, and the whole point is that the external service notices
        the absence."""
        now = time.monotonic()
        if self._last_ping is not None and now - self._last_ping < self._interval:
            return
        if self._flight is not None and not self._flight.done():
            # A previous ping is still going. Skip rather than queue: against
            # a black-holed pinging host, one task per pulse would be
            # thousands of them in a long sweep. The bounds in `_send` are
            # what stop this skip from lasting forever.
            return
        # Stamped BEFORE the send, so the cadence is one ping per interval
        # whatever the request costs — the throttle paces attempts, and a
        # slow attempt is already accounted for by the in-flight skip above.
        self._last_ping = now
        self._flight = asyncio.create_task(self._send())

    async def _send(self) -> None:
        """One ping, bounded twice, swallowing everything. Runs as its own
        task, so nothing here is on the caller's timeline."""
        try:
            await asyncio.wait_for(self._get(), self._timeout * PING_CEILING_MULTIPLE)
        except TimeoutError:
            # Either bound may have fired and the type cannot say which
            # (PING_CEILING_MULTIPLE); what matters is that one of them did,
            # so the next pulse is free to try again.
            log.warning(
                "external dead-man ping to %s timed out (bounded at %.1fs, ceiling %.1fs)",
                self.target,
                self._timeout,
                self._timeout * PING_CEILING_MULTIPLE,
            )
        except Exception as exc:
            # Every ping failure shape lands here and is LOGGED, never
            # retried: the next pulse is the retry, and the external service
            # exists precisely to notice if enough of them fail in a row.
            #
            # The exception's TYPE and nothing more — deliberately not
            # `exc_info=True`, and this is the one place the choice is not
            # about noise. Several aiohttp errors put the offending URL in
            # their message (`InvalidUrlClientError` and friends), and the
            # URL is the credential; a traceback here would undo `target`'s
            # whole reason for existing, in the log line most likely to be
            # pasted into an issue. The class name plus the host is enough to
            # act on — refused, DNS, TLS and malformed each name themselves —
            # and `curl` against the value in the environment is the next
            # diagnostic step either way.
            log.warning(
                "external dead-man ping to %s failed: %s", self.target, type(exc).__name__
            )

    async def _get(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with self._session.get(self._url, timeout=timeout) as response:
            if response.status >= 400:
                # A 404 here is the commonest real misconfiguration — a check
                # deleted or a URL mistyped — and it is INVISIBLE from the
                # operator's side, because a service that never sees a ping
                # for a check it doesn't have has nothing to page about. So
                # it has to be loud on ours.
                log.warning(
                    "external dead-man ping to %s answered %d — the check may not exist; "
                    "the operator will NOT be paged if this watchdog dies",
                    self.target,
                    response.status,
                )

    async def aclose(self) -> None:
        """Drop an in-flight ping at shutdown, so a stalled request cannot
        outlive the session it borrows."""
        flight = self._flight
        if flight is None or flight.done():
            return
        flight.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flight
