import logging
import os
from dataclasses import dataclass

log = logging.getLogger(__name__)

# Coarse Universe re-seed cadence (issue #50). One free CDN download per cycle
# refreshes the whole Universe's windowed coarse stats and discovers new wallets,
# so an hourly heartbeat keeps fine-eligibility responsive within the hour. It
# never touches the per-IP rate budget, so raising the frequency is essentially
# free. Operator-tunable via SEED_INTERVAL_MINUTES; a bad value falls back here.
DEFAULT_SEED_INTERVAL_MINUTES = 60

# How many due Traders one fine-pass cycle processes before returning control to
# the ingest loop (issue #66). The fine pass ran over the *entire* due list, so
# under a big backlog a single pass took hours and the hourly re-seed (#50)
# degraded to once-per-pass. Bounding each pass to a chunk returns control to the
# loop between chunks, so the seed keeps its cadence and the due queue's ordering
# (#65) is re-read every chunk. Sized for ~an hour of work at the observed
# ~450/hr budget-limited rate; operator-tunable via FINE_CHUNK_SIZE. A caught-up
# universe (due count <= chunk) is one full pass, unchanged from before.
DEFAULT_FINE_CHUNK_SIZE = 500

# Order-poll cadence (issue #115): how often the stream diffs tracked wallets'
# resting orders. Resting orders live minutes-to-days, so the cadence buys
# alert latency, and it is also what keeps the heavier endpoint cheap.
#
# The math: one poll costs ORDERS_WEIGHT (20 nominal; ~8 measured, see
# epigone.stream.orders) × 2 covered venues = 40 weight per wallet per cycle,
# so at 100s each tracked wallet adds 24 nominal weight/min (~10 real). Position
# polling adds its own per-wallet weight on top, set by
# epigone.stream.poller.POLL_INTERVAL_SECONDS; the two together against the
# 900/min shared refill are what decide the wallet count at which the stream
# alone claims the whole bucket. That combined saturation figure moves whenever
# either cadence or the covered-venue tuple does, so read it from
# docs/spec-defaults.md (order-poll cadence bullet) rather than from here.
#
# 100s is deliberately past the point where the cadence, not the wallet count,
# sets the pass duration: the pass spaces its wallets by
# stream.orders.ORDER_WALLET_SPACING_SECONDS, so N wallets take ~7N−5 seconds
# and anything over ~15 runs longer than one cycle. That is a soft edge, not a
# cliff — the loop sleeps `max(0, interval − elapsed)` (epigone.stream.main), so
# an over-long pass simply starts the next one immediately. What it costs is the
# duty-cycle argument below: order polling then holds the #41 send gate ~29% of
# the time continuously rather than in bursts, and its own spend ceilings out at
# ~40/7 ≈ 5.7 weight/s (~343/min nominal) no matter how many wallets are
# tracked. Watch order-alert latency, not the budget, when raising the cap.
#
# Position polls always win regardless: order spends carry the ingest-style
# stream reserve (epigone.stream.main), so a mis-tuned interval degrades to
# slower order alerts, never to starved Position Alerts. The reserve guards
# tokens; the #41 send gate (FCFS) is guarded separately, by that same spacing.
# Operator-tunable via ORDER_POLL_INTERVAL_SECONDS; a bad value falls back here.
DEFAULT_ORDER_POLL_INTERVAL_SECONDS = 100


@dataclass(frozen=True)
class Settings:
    """Config shared by every process. Only the bot needs the Telegram token
    and admin id — ingest/stream run without either (ADR-0002: independent
    processes)."""

    database_url: str
    telegram_bot_token: str | None
    # The invite-only owner (issue #33): always allowed and the only one who can
    # /allow, /revoke, /allowed. None means no admin is configured, so the bot
    # has no owner and the allowlist can only be seeded out-of-band.
    admin_telegram_id: int | None
    # How often the ingest loop re-seeds the Universe from the leaderboard
    # (issue #50). Only the ingest process reads it.
    seed_interval_minutes: int
    # How many due Traders each fine-pass cycle processes before returning to the
    # loop (issue #66). Only the ingest process reads it.
    fine_chunk_size: int
    # How often the stream diffs tracked wallets' resting orders (issue #115).
    # Only the stream process reads it.
    order_poll_interval_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        admin_id = os.environ.get("ADMIN_TELEGRAM_ID")
        return cls(
            database_url=os.environ["DATABASE_URL"],
            telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN"),
            admin_telegram_id=int(admin_id) if admin_id else None,
            seed_interval_minutes=_parse_seed_interval_minutes(
                os.environ.get("SEED_INTERVAL_MINUTES")
            ),
            fine_chunk_size=_parse_fine_chunk_size(os.environ.get("FINE_CHUNK_SIZE")),
            order_poll_interval_seconds=_parse_order_poll_interval_seconds(
                os.environ.get("ORDER_POLL_INTERVAL_SECONDS")
            ),
        )

    def require_bot_token(self) -> str:
        if not self.telegram_bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required for the bot process")
        return self.telegram_bot_token

    def require_admin_telegram_id(self) -> int:
        # The bot is invite-only (issue #33): without an owner an empty allowlist
        # would lock everyone out, so the bot process refuses to start without
        # one. ingest/stream don't gate updates and never call this.
        if self.admin_telegram_id is None:
            raise RuntimeError("ADMIN_TELEGRAM_ID is required for the bot process")
        return self.admin_telegram_id


def parse_positive_int(raw: str | None, *, default: int, name: str) -> int:
    """Parse a positive-int env var, falling back to `default` (with a logged
    warning naming the var) on anything non-numeric or non-positive. The house
    convention for operator-tunable knobs (issues #50, #52): a misconfiguration
    must degrade to the safe default, never wedge or hammer a process."""
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s=%r is not an integer; using %d", name, raw, default)
        return default
    if value <= 0:
        log.warning("%s=%r is not positive; using %d", name, raw, default)
        return default
    return value


def _parse_seed_interval_minutes(raw: str | None) -> int:
    return parse_positive_int(
        raw, default=DEFAULT_SEED_INTERVAL_MINUTES, name="SEED_INTERVAL_MINUTES"
    )


def _parse_fine_chunk_size(raw: str | None) -> int:
    return parse_positive_int(raw, default=DEFAULT_FINE_CHUNK_SIZE, name="FINE_CHUNK_SIZE")


def _parse_order_poll_interval_seconds(raw: str | None) -> int:
    return parse_positive_int(
        raw, default=DEFAULT_ORDER_POLL_INTERVAL_SECONDS, name="ORDER_POLL_INTERVAL_SECONDS"
    )
