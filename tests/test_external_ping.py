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
`WATCHDOG_EXTERNAL_PING_URL` disables the whole thing cleanly rather than
half-arming it.
"""

import asyncio
import logging
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from epigone.safety.config import WatchdogConfig, WatchdogConfigError
from epigone.safety.ping import PING_TIMEOUT_SECONDS, ExternalPing

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


@asynccontextmanager
async def _pinger(
    session: aiohttp.ClientSession,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    *,
    path: str = "/ping",
    interval_seconds: float = 60.0,
    timeout_seconds: float = PING_TIMEOUT_SECONDS,
) -> AsyncGenerator[ExternalPing, None]:
    """A real server answering `path` with `handler`, and a ping aimed at it.

    Owns the teardown of BOTH, in a finally: a test that fails its assertion
    mid-flight must still close the ping, or the in-flight request outlives
    the session and the failure arrives buried in unrelated aiohttp noise."""
    app = web.Application()
    app.router.add_get(path, handler)
    server = TestServer(app)
    await server.start_server()
    ping = ExternalPing(
        session,
        str(server.make_url(path)),
        interval_seconds=interval_seconds,
        timeout_seconds=timeout_seconds,
    )
    try:
        yield ping
    finally:
        await ping.aclose()
        await server.close()


async def _dead_port() -> int:
    """A port nothing listens on: bind one, learn its number, close it."""
    probe = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port: int = probe.sockets[0].getsockname()[1]
    probe.close()
    await probe.wait_closed()
    return port


async def test_a_ping_reaches_the_external_service(
    http_session: aiohttp.ClientSession,
) -> None:
    hits: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        hits.append(request.path)
        return web.Response(text="OK")

    async with _pinger(http_session, handle, path="/ping/abc123") as ping:
        ping.ping()
        await _until(lambda: hits == ["/ping/abc123"], "the ping to land")


async def test_pings_are_throttled_to_their_interval(
    http_session: aiohttp.ClientSession,
) -> None:
    """The pulse calls this at every enumeration step — thousands of times in
    a real sweep — so the throttle is what makes it callable that freely."""
    hits: list[str] = []

    async def handle(request: web.Request) -> web.Response:
        hits.append(request.path)
        return web.Response(text="OK")

    async with _pinger(http_session, handle, interval_seconds=0.2) as ping:
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

    try:
        async with _pinger(http_session, stall, timeout_seconds=TIMEOUT) as ping:
            started = time.monotonic()
            ping.ping()
            elapsed = time.monotonic() - started
            # Milliseconds, not the stall: a caller that yielded here would
            # show the whole request. (Loose enough for a loaded CI box, tight
            # enough that any await on the request would blow it by an order
            # of magnitude.)
            assert elapsed < 0.05
    finally:
        release.set()


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

    try:
        async with _pinger(
            http_session,
            first_stalls,
            interval_seconds=0.0,  # the throttle is not what is under test
            timeout_seconds=TIMEOUT,
        ) as ping:
            with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
                ping.ping()
                await _until(lambda: len(hits) == 1, "the stalled ping to start")
                # While it is stalled, further pings are SKIPPED rather than
                # queued — no second request reaches the server.
                ping.ping()
                ping.ping()
                assert len(hits) == 1
                # …and once the stall is cut at the timeout, pinging resumes
                # on its own. This loop is the pulse: it just keeps calling.
                deadline = time.monotonic() + PATIENCE
                while len(hits) < 2 and time.monotonic() < deadline:
                    ping.ping()
                    await asyncio.sleep(0.01)
            assert len(hits) >= 2, "a cut ping must not silence the next one"
            assert any("timed out" in r.getMessage() for r in caplog.records)
    finally:
        release.set()


async def test_a_ping_the_service_rejects_says_nobody_will_be_paged(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """A deleted check or a mistyped URL 404s, and it is INVISIBLE from the
    operator's side — a service that never sees a ping for a check it does not
    have has nothing to page about. So it has to be loud on ours."""

    async def gone(request: web.Request) -> web.Response:
        return web.Response(status=404, text="not found")

    async with _pinger(http_session, gone) as ping:
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()
            await _until(
                lambda: any("404" in r.getMessage() for r in caplog.records),
                "the rejected ping to be logged",
            )
        assert any("will NOT be paged" in r.getMessage() for r in caplog.records)


async def test_a_refused_ping_is_logged_and_never_raises(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """The commonest failure in production is the dullest: no route to the
    pinging host. The caller has nothing useful to do about it — the external
    service noticing the ABSENCE is the entire mechanism — so it is a log
    line, never an exception on the kill path."""
    port = await _dead_port()
    ping = ExternalPing(http_session, f"http://127.0.0.1:{port}/ping", interval_seconds=60.0)
    try:
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()  # must not raise
            await _until(
                lambda: any("failed" in r.getMessage() for r in caplog.records),
                "the refused ping to be logged",
            )
    finally:
        await ping.aclose()


async def test_the_ping_url_is_a_credential_and_never_reaches_a_log(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """On healthchecks.io and every service like it the PATH is the secret:
    anyone holding it can forge this watchdog's liveness. So logs name the
    host and nothing else — including the log line for a failure, which is
    the one most likely to be pasted somewhere."""
    secret = "f0e1d2c3-b4a5-4697-8899-aabbccddeeff"
    port = await _dead_port()
    ping = ExternalPing(
        http_session, f"http://127.0.0.1:{port}/{secret}", interval_seconds=60.0
    )
    try:
        assert secret not in ping.target
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()
            await _until(
                lambda: any("failed" in r.getMessage() for r in caplog.records),
                "the refused ping to be logged",
            )
        assert not any(secret in r.getMessage() for r in caplog.records)
    finally:
        await ping.aclose()


async def test_an_aiohttp_error_that_quotes_the_url_still_does_not_leak_it(
    http_session: aiohttp.ClientSession, caplog: pytest.LogCaptureFixture
) -> None:
    """The case the obvious implementation gets wrong (found in review).

    A refused connection names only host:port, so logging the traceback LOOKS
    safe. Several aiohttp errors are not like that: `InvalidUrlClientError`
    puts the whole URL — credential path and all — in its message, and the
    config parser only checks the scheme, so a malformed value reaches the
    request and raises exactly that. An `exc_info=True` here would print the
    secret in the log line most likely to be pasted into an issue. The failure
    log therefore carries the exception's TYPE and the host, never the
    exception's text."""
    secret = "f0e1d2c3-b4a5-4697-8899-aabbccddeeff"
    # A scheme-valid URL with no host: passes `_parse_ping_url`, and aiohttp
    # rejects it with an error whose message quotes the URL verbatim.
    ping = ExternalPing(http_session, f"http:///{secret}", interval_seconds=60.0)
    try:
        with caplog.at_level(logging.WARNING, logger="epigone.safety.ping"):
            ping.ping()
            await _until(
                lambda: any("failed" in r.getMessage() for r in caplog.records),
                "the malformed ping to be logged",
            )
        logged = " ".join(r.getMessage() for r in caplog.records)
        assert secret not in logged
        assert "InvalidUrl" in logged  # the type IS reported — diagnosable, not silent
    finally:
        await ping.aclose()


# --- configuration: unset must be a clean no-op, not a half-armed path ---


def test_an_unset_ping_url_leaves_the_path_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """There is no default and there cannot be one — only the operator can
    create the check. So unset is a supported configuration, and it is
    represented by NOTHING to ping rather than by a disabled pinger."""
    monkeypatch.delenv("WATCHDOG_EXTERNAL_PING_URL", raising=False)
    assert WatchdogConfig.from_env().external_ping_url is None


def test_a_blank_ping_url_reads_as_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """`WATCHDOG_EXTERNAL_PING_URL=` in a .env file is how an operator writes
    "not yet", and compose passes it through as an empty string."""
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_URL", "   ")
    assert WatchdogConfig.from_env().external_ping_url is None


def test_a_malformed_ping_url_degrades_to_no_path_at_all_on_testnet(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """On TESTNET the house convention holds: a bad value degrades to the safe
    default, which here is OFF. Nothing is at stake but a testnet book, and a
    watchdog that refused to start would be strictly worse than one running
    without a page path it never had."""
    monkeypatch.delenv("EXECUTOR_ALLOW_MAINNET", raising=False)
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_URL", "hc-ping.com/abc")
    with caplog.at_level(logging.WARNING, logger="epigone.safety.config"):
        assert WatchdogConfig.from_env().external_ping_url is None
    assert any("NO out-of-band page path" in r.getMessage() for r in caplog.records)


def test_a_malformed_ping_url_under_mainnet_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE gate-integrity case (review of PR #216). Degrading here answers a
    fat-fingered secret with a MAINNET watchdog whose only observer is the
    in-domain monitor — precisely the state this issue is a gate against — and
    it does so behind a log line nobody reads twice. The operator plainly
    MEANT to arm the path, so the safe default inverts: refuse, crash-loop
    visibly, and let them fix one line of .env."""
    monkeypatch.setenv("EXECUTOR_ALLOW_MAINNET", "1")
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_URL", "hc-ping.com/abc")
    with pytest.raises(WatchdogConfigError, match="out-of-band"):
        WatchdogConfig.from_env()


def test_an_unset_ping_url_under_mainnet_still_only_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unset is NOT the same as malformed, and is deliberately left as a
    warning in both postures. Malformed means an operator tried and missed;
    unset means they have not armed the path, which is a policy call for them
    to make rather than one this parser should make for them. Pinned as a test
    so the distinction is a decision on the record, not an oversight."""
    monkeypatch.setenv("EXECUTOR_ALLOW_MAINNET", "1")
    monkeypatch.delenv("WATCHDOG_EXTERNAL_PING_URL", raising=False)
    assert WatchdogConfig.from_env().external_ping_url is None


def test_a_configured_ping_url_and_cadence_survive_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_URL", "https://hc-ping.com/abc123")
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_SECONDS", "30")
    config = WatchdogConfig.from_env()
    assert config.external_ping_url == "https://hc-ping.com/abc123"
    assert config.external_ping_interval.total_seconds() == 30


def test_a_bad_cadence_falls_back_without_disarming_the_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two knobs degrade in opposite directions on purpose: an unusable
    URL means there is no path, but an unusable INTERVAL still has a perfectly
    good URL behind it, so it takes the default rather than the URL with it."""
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_URL", "https://hc-ping.com/abc123")
    monkeypatch.setenv("WATCHDOG_EXTERNAL_PING_SECONDS", "not-a-number")
    config = WatchdogConfig.from_env()
    assert config.external_ping_url == "https://hc-ping.com/abc123"
    assert config.external_ping_interval.total_seconds() == 60
