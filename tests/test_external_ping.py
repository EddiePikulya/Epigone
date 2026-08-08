"""The out-of-band page path (issue #213): the watchdog's dead-man ping.

Against a REAL HTTP server throughout, and against a real STALLED one for the
properties that only exist under a stall — the whole mechanism is a claim
about what happens when the network on the far side misbehaves, and a fake
can only replay a decision its caller already handles (the house argument,
tests/support/hanging.py).

The claims under test, in this file's order: a ping reaches the service; it
is throttled to its interval; it can NEVER cost its caller time (the property
that makes it safe to call from inside a kill sweep); a stalled ping cannot
silence the ones after it; every failure shape is logged rather than raised;
the credential in the URL never reaches a log line; and an unset
`WATCHDOG_DEADMAN_PING_URL` disables the whole thing cleanly rather than
half-arming it.
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Callable

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from epigone.safety.config import WatchdogConfig
from epigone.safety.ping import ExternalPing

# The ping's own bounds, shrunk so a test that must actually wait one out
# costs milliseconds. Production's are epigone.safety.ping's.
TIMEOUT = 0.05
# Generous against the shrunk bounds above, tight against the real ones: any
# assertion that waits this long has caught a hang, not a slow machine.
PATIENCE = 3.0


async def _until(predicate: Callable[[], bool], what: str) -> None:
    """Wait for something a fire-and-forget task will do soon. There is no
    handle to await on purpose — `ping()` returns before its request starts —
    so the tests observe the SERVER, which is what the operator's dead-man
    service does too."""
    deadline = time.monotonic() + PATIENCE
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
async def http_session() -> AsyncGenerator[aiohttp.ClientSession, None]:
    async with aiohttp.ClientSession() as client:
        yield client


async def test_a_ping_reaches_the_external_service(
    http_session: aiohttp.ClientSession,
) -> None:
    hits: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        hits.append(request.path)
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/ping/abc123", handle)
    server = TestServer(app)
    await server.start_server()
    try:
        ping = ExternalPing(http_session, str(server.make_url("/ping/abc123")))
        ping.ping()
        await _until(lambda: hits == ["/ping/abc123"], "the ping to land")
        await ping.aclose()
    finally:
        await server.close()


async def test_pings_are_throttled_to_their_interval(
    http_session: aiohttp.ClientSession,
) -> None:
    """The pulse calls this at every enumeration step — thousands of times in
    a real sweep — so the throttle is what makes it callable that freely."""
    hits: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        hits.append(request.path)
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/ping", handle)
    server = TestServer(app)
    await server.start_server()
    try:
        ping = ExternalPing(
            http_session, str(server.make_url("/ping")), interval_seconds=0.2
        )
        for _ in range(50):
            ping.ping()
        await _until(lambda: len(hits) == 1, "the first ping to land")
        # Fifty calls inside the interval are one ping. Past it, the next call
        # is another — the cadence is a floor on the gap, not a cap on a
        # burst that never happens.
        await asyncio.sleep(0.25)
        assert hits == ["/ping"]
        ping.ping()
        await _until(lambda: len(hits) == 2, "the second ping to land")
        await ping.aclose()
    finally:
        await server.close()


async def test_a_ping_never_costs_its_caller_time(
    http_session: aiohttp.ClientSession,
) -> None:
    """THE load-bearing property (module docstring of epigone.safety.ping).
    The caller is the kill path — in the worst case, a sweep cancelling a real
    book — so this is asserted against a server that never answers at all: not
    "fast", but structurally incapable of waiting, which is why `ping()` is
    sync and has no await to inherit the stall."""
    release = asyncio.Event()

    async def stall(request: web.Request) -> web.Response:
        await release.wait()
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/ping", stall)
    server = TestServer(app)
    await server.start_server()
    try:
        ping = ExternalPing(
            http_session, str(server.make_url("/ping")), timeout_seconds=TIMEOUT
        )
        started = time.monotonic()
        ping.ping()
        elapsed = time.monotonic() - started
        # Milliseconds, not the stall: a caller that yielded here would show
        # the whole request. (Loose enough for a loaded CI box, tight enough
        # that any await on the request would blow it by an order of
        # magnitude.)
        assert elapsed < 0.05
    finally:
        release.set()
        await ping.aclose()
        await server.close()


async def test_a_stalled_ping_cannot_silence_the_ones_after_it(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """Only one request is in flight at a time — otherwise a black-holed
    pinging host collects one task per pulse — and that skip would be a trap
    if a stall could last forever: the first hung request would suppress every
    later one, and the external service would page for a fault in the
    REPORTER. The hard timeout is what closes it."""
    hits: list[str] = []
    release = asyncio.Event()

    async def first_stalls(request: web.Request) -> web.Response:
        hits.append(request.path)
        if len(hits) == 1:
            await release.wait()
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/ping", first_stalls)
    server = TestServer(app)
    await server.start_server()
    try:
        ping = ExternalPing(
            http_session,
            str(server.make_url("/ping")),
            interval_seconds=0.0,  # the throttle is not what is under test
            timeout_seconds=TIMEOUT,
        )
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()
            await _until(lambda: len(hits) == 1, "the stalled ping to start")
            # While it is stalled, further pings are SKIPPED rather than
            # queued — no second request reaches the server.
            ping.ping()
            ping.ping()
            assert len(hits) == 1
            # …and once the stall is cut at the timeout, pinging resumes on
            # its own. This loop is the pulse: it just keeps calling.
            deadline = time.monotonic() + PATIENCE
            while len(hits) < 2 and time.monotonic() < deadline:
                ping.ping()
                await asyncio.sleep(0.01)
        assert len(hits) >= 2, "a cut ping must not silence the next one"
        assert any("timed out" in r.getMessage() for r in caplog.records)
    finally:
        release.set()
        await ping.aclose()
        await server.close()


async def test_a_ping_the_service_rejects_says_nobody_will_be_paged(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """A deleted check or a mistyped URL 404s, and it is INVISIBLE from the
    operator's side — a service that never sees a ping for a check it does not
    have has nothing to page about. So it has to be loud on ours."""

    async def gone(request: web.Request) -> web.Response:
        return web.Response(status=404, text="not found")

    app = web.Application()
    app.router.add_get("/ping", gone)
    server = TestServer(app)
    await server.start_server()
    try:
        ping = ExternalPing(http_session, str(server.make_url("/ping")))
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()
            await _until(
                lambda: any("404" in r.getMessage() for r in caplog.records),
                "the rejected ping to be logged",
            )
        assert any("will NOT be paged" in r.getMessage() for r in caplog.records)
        await ping.aclose()
    finally:
        await server.close()


async def test_a_refused_ping_is_logged_and_never_raises(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """The commonest failure in production is the dullest: no route to the
    pinging host. The caller has nothing useful to do about it — the external
    service noticing the ABSENCE is the entire mechanism — so it is a log
    line, never an exception on the kill path."""
    # A port nothing listens on: bind one, learn its number, close it.
    probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = probe.sockets[0].getsockname()[1]
    probe.close()
    await probe.wait_closed()

    ping = ExternalPing(http_session, f"http://127.0.0.1:{port}/ping")
    with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
        ping.ping()  # must not raise
        await _until(
            lambda: any("failed" in r.getMessage() for r in caplog.records),
            "the refused ping to be logged",
        )
    await ping.aclose()


async def test_the_ping_url_is_a_credential_and_never_reaches_a_log(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """On healthchecks.io and every service like it the PATH is the secret:
    anyone holding it can forge this watchdog's liveness. So logs name the
    host and nothing else — including the log line for a failure, which is
    the one most likely to be pasted somewhere."""
    secret = "f0e1d2c3-b4a5-4697-8899-aabbccddeeff"
    probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = probe.sockets[0].getsockname()[1]
    probe.close()
    await probe.wait_closed()

    ping = ExternalPing(http_session, f"http://127.0.0.1:{port}/{secret}")
    assert secret not in ping.target
    with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
        ping.ping()
        await _until(
            lambda: any("failed" in r.getMessage() for r in caplog.records),
            "the refused ping to be logged",
        )
    assert not any(secret in r.getMessage() for r in caplog.records)
    await ping.aclose()


# --- configuration: unset must be a clean no-op, not a half-armed path ---


def test_an_unset_ping_url_leaves_the_path_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """There is no default and there cannot be one — only the operator can
    create the check. So unset is a supported configuration, and it is
    represented by NOTHING to ping rather than by a disabled pinger."""
    monkeypatch.delenv("WATCHDOG_DEADMAN_PING_URL", raising=False)
    assert WatchdogConfig.from_env().deadman_ping_url is None


def test_a_blank_ping_url_reads_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """`WATCHDOG_DEADMAN_PING_URL=` in a .env file is how an operator writes
    "not yet", and compose passes it through as an empty string."""
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_URL", "   ")
    assert WatchdogConfig.from_env().deadman_ping_url is None


def test_a_malformed_ping_url_degrades_to_no_path_at_all(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The house convention is that a bad value degrades to the safe default,
    and here the safe default is OFF: a URL that cannot be pinged is not a
    page path, and an operator who believes it is stops looking for the fact
    that it isn't."""
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_URL", "hc-ping.com/abc")
    with caplog.at_level(logging.WARNING, logger="epigone.safety.config"):
        assert WatchdogConfig.from_env().deadman_ping_url is None
    assert any("NO out-of-band page path" in r.getMessage() for r in caplog.records)


def test_a_configured_ping_url_and_cadence_survive_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_URL", "https://hc-ping.com/abc123")
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_SECONDS", "30")
    config = WatchdogConfig.from_env()
    assert config.deadman_ping_url == "https://hc-ping.com/abc123"
    assert config.deadman_ping_interval.total_seconds() == 30


def test_a_bad_cadence_falls_back_without_disarming_the_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two knobs degrade in opposite directions on purpose: an unusable
    URL means there is no path, but an unusable INTERVAL still has a perfectly
    good URL behind it, so it takes the default rather than the URL with it."""
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_URL", "https://hc-ping.com/abc123")
    monkeypatch.setenv("WATCHDOG_DEADMAN_PING_SECONDS", "not-a-number")
    config = WatchdogConfig.from_env()
    assert config.deadman_ping_url == "https://hc-ping.com/abc123"
    assert config.deadman_ping_interval.total_seconds() == 60
