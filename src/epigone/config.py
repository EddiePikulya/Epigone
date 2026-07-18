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
