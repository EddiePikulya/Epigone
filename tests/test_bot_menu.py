"""The Telegram command menu (bot/menu.py): the public menu every User sees vs
the admin's, which alone carries the invite-only allowlist controls (#33).

Asserts on the outgoing setMyCommands calls the fake transport records — what
Telegram would actually publish — not on internals.
"""

from aiogram.methods import SetMyCommands
from aiogram.types import BotCommandScopeChat, BotCommandScopeDefault

from epigone.bot.menu import set_bot_commands
from tests.support.telegram import RecordingSession, make_bot

ADMIN_ONLY = {"allow", "revoke", "allowed"}


def _menus(session: RecordingSession) -> list[SetMyCommands]:
    return [m for m in session.requests if isinstance(m, SetMyCommands)]


async def test_public_menu_excludes_admin_commands() -> None:
    session = RecordingSession()
    await set_bot_commands(make_bot(session), admin_telegram_id=555)

    default = [m for m in _menus(session) if isinstance(m.scope, BotCommandScopeDefault)]
    assert len(default) == 1
    names = {c.command for c in default[0].commands}
    assert "screener" in names  # the everyday commands are there
    assert names.isdisjoint(ADMIN_ONLY)  # but not the owner controls


async def test_admin_menu_is_scoped_to_the_admin_chat_and_adds_controls() -> None:
    session = RecordingSession()
    await set_bot_commands(make_bot(session), admin_telegram_id=555)

    scoped = [m for m in _menus(session) if isinstance(m.scope, BotCommandScopeChat)]
    assert len(scoped) == 1
    assert scoped[0].scope.chat_id == 555  # only the owner's chat, no one else's
    names = {c.command for c in scoped[0].commands}
    assert ADMIN_ONLY <= names  # owner sees the allowlist controls
    assert "screener" in names  # plus everything a normal User sees


async def test_without_an_admin_only_the_public_menu_is_published() -> None:
    session = RecordingSession()
    await set_bot_commands(make_bot(session), admin_telegram_id=None)

    menus = _menus(session)
    assert len(menus) == 1
    assert isinstance(menus[0].scope, BotCommandScopeDefault)
