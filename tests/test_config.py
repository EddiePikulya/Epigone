"""Settings.from_env parsing and the bot-process guards (issues #33, #50)."""

import logging

import pytest

from epigone.config import DEFAULT_SEED_INTERVAL_MINUTES, Settings


def _settings(admin_telegram_id: int | None) -> Settings:
    return Settings(
        database_url="postgresql://x",
        telegram_bot_token="token",
        admin_telegram_id=admin_telegram_id,
        seed_interval_minutes=DEFAULT_SEED_INTERVAL_MINUTES,
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


def test_seed_interval_defaults_to_60_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.delenv("SEED_INTERVAL_MINUTES", raising=False)
    assert Settings.from_env().seed_interval_minutes == 60


def test_seed_interval_honours_a_valid_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("SEED_INTERVAL_MINUTES", "15")
    assert Settings.from_env().seed_interval_minutes == 15


@pytest.mark.parametrize("bad", ["nonsense", "", "0", "-5"])
def test_seed_interval_falls_back_to_60_on_invalid(
    bad: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A non-numeric or non-positive value must never wedge or hammer ingestion:
    # fall back to the safe default and say so (issue #50).
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("SEED_INTERVAL_MINUTES", bad)
    with caplog.at_level(logging.WARNING):
        assert Settings.from_env().seed_interval_minutes == 60
    assert "SEED_INTERVAL_MINUTES" in caplog.text
