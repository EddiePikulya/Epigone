import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Config shared by every process. Only the bot needs the Telegram token —
    ingest/stream run without one (ADR-0002: independent processes)."""

    database_url: str
    telegram_bot_token: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ["DATABASE_URL"],
            telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN"),
        )

    def require_bot_token(self) -> str:
        if not self.telegram_bot_token:
            raise RuntimeError("TELEGRAM_BOT_TOKEN is required for the bot process")
        return self.telegram_bot_token
