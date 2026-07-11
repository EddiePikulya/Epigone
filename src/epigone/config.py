import os
from dataclasses import dataclass


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

    @classmethod
    def from_env(cls) -> "Settings":
        admin_id = os.environ.get("ADMIN_TELEGRAM_ID")
        return cls(
            database_url=os.environ["DATABASE_URL"],
            telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN"),
            admin_telegram_id=int(admin_id) if admin_id else None,
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
