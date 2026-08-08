"""The shared 429 backoff-and-retry vocabulary for both gateway directions
(issue #28 read-side, issue #133 write-side): bounded tries, exponential
equal-jitter windows, Retry-After respected when parseable and bounded always.

Both HTTP gateways retry a 429 in place and only surface a sustained streak
as their RateLimited error. What each direction may REPLAY differs — reads
re-issue freely, writes re-post the identical signed payload (single-use
nonces make that at-most-once) — so the loop lives with each gateway; only
the shared arithmetic lives here.
"""

from collections.abc import Callable

# Bounded 429 retries: 6 tries = up to 5 sleeps (1+2+4+8+16s at full jitter),
# ~30s worst case — long enough to ride out a blip, short enough that a pass
# under sustained limiting still moves on and resumes next cycle.
RATE_LIMIT_MAX_TRIES = 6
RATE_LIMIT_BACKOFF_BASE_SECONDS = 1.0
RATE_LIMIT_BACKOFF_CAP_SECONDS = 30.0

# Retry-After is TRUSTED BUT BOUNDED (issue #204). The header is the server's
# number and we sleep on it, so uncapped it is a remote party's hand on this
# process's wall clock: one `Retry-After: 100000` makes a single read or cancel
# arbitrarily long, and the kill path's liveness argument — every await between
# two sweep pulses bounded well under one dead-man period — is load-bearing on
# it. The cap is deliberately the SAME number as our own backoff's: 30s is
# already the longest this codebase is willing to wait between two tries, and
# a server asking for longer gets our maximum rather than its own. Asking for
# MORE is not ignored, it is answered the way sustained limiting is meant to be
# answered — the tries run out, RateLimitedError surfaces, and the pass moves on
# and resumes next cycle instead of sleeping through the incident.
RETRY_AFTER_CAP_SECONDS = RATE_LIMIT_BACKOFF_CAP_SECONDS


def backoff_delay(attempt: int, rng: Callable[[], float]) -> float:
    """Exponential window with equal jitter: 50-100% of base * 2^attempt."""
    window = min(RATE_LIMIT_BACKOFF_CAP_SECONDS, RATE_LIMIT_BACKOFF_BASE_SECONDS * 2.0**attempt)
    return window * (0.5 + 0.5 * rng())


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After as delta-seconds, clamped into [0, RETRY_AFTER_CAP_SECONDS];
    the HTTP-date form (or garbage) falls back to our own backoff rather than
    trusting a parse of the server's clock.

    Clamped at BOTH ends for the same reason: what comes back is the other
    side's number, and this process sleeps on it (issue #204)."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return min(max(0.0, seconds), RETRY_AFTER_CAP_SECONDS)
