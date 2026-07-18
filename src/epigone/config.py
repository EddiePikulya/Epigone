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


def _parse_seed_interval_minutes(raw: str | None) -> int:
    """Parse SEED_INTERVAL_MINUTES, falling back to the 60-min default (with a
    logged warning) on anything non-numeric or non-positive — a misconfiguration
    must never wedge or hammer ingestion (issue #50)."""
    if raw is None:
        return DEFAULT_SEED_INTERVAL_MINUTES
    try:
        minutes = int(raw)
    except ValueError:
        log.warning(
            "SEED_INTERVAL_MINUTES=%r is not an integer; using %d",
            raw,
            DEFAULT_SEED_INTERVAL_MINUTES,
        )
        return DEFAULT_SEED_INTERVAL_MINUTES
    if minutes <= 0:
        log.warning(
            "SEED_INTERVAL_MINUTES=%r is not positive; using %d",
            raw,
            DEFAULT_SEED_INTERVAL_MINUTES,
        )
        return DEFAULT_SEED_INTERVAL_MINUTES
    return minutes
