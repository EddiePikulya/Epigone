"""The shared 429 backoff-and-retry vocabulary for both gateway directions
(issue #28 read-side, issue #133 write-side): bounded tries, exponential
equal-jitter windows, Retry-After respected when parseable.

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


def backoff_delay(attempt: int, rng: Callable[[], float]) -> float:
    """Exponential window with equal jitter: 50-100% of base * 2^attempt."""
    window = min(RATE_LIMIT_BACKOFF_CAP_SECONDS, RATE_LIMIT_BACKOFF_BASE_SECONDS * 2.0**attempt)
    return window * (0.5 + 0.5 * rng())


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After as delta-seconds; the HTTP-date form (or garbage) falls back
    to our own backoff rather than trusting a parse of the server's clock."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return max(0.0, seconds)
