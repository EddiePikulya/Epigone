"""Settings.from_env parsing and the bot-process guards (issues #33)."""

import pytest

from epigone.config import Settings


def _settings(admin_telegram_id: int | None) -> Settings:
    return Settings(
        database_url="postgresql://x",
        telegram_bot_token="token",
        admin_telegram_id=admin_telegram_id,
    )


def test_from_env_parses_admin_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("ADMIN_TELEGRAM_ID", "370818090")
    assert Settings.from_env().admin_telegram_id == 370818090


def test_from_env_admin_id_is_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.delenv("ADMIN_TELEGRAM_ID", raising=False)
    assert Settings.from_env().admin_telegram_id is None


def test_require_admin_returns_the_owner() -> None:
    assert _settings(370818090).require_admin_telegram_id() == 370818090


def test_require_admin_refuses_to_start_without_an_owner() -> None:
    # The invite-only bot must never boot without an owner: an empty allowlist
    # would otherwise lock everyone out.
    with pytest.raises(RuntimeError, match="ADMIN_TELEGRAM_ID"):
        _settings(None).require_admin_telegram_id()
