"""The Telegram seam: a fake transport for aiogram.

Tests assert on outgoing Bot API calls (what the User would see) and feed
incoming updates — no network, no real Telegram.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any, cast

from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.methods import GetMe, SendMessage, TelegramMethod
from aiogram.methods.base import TelegramType
from aiogram.types import Chat, Message, Update
from aiogram.types import User as TgUser


class RecordingSession(BaseSession):
    """Records every outgoing Bot API call and answers with canned results."""

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[TelegramMethod[Any]] = []
        self._message_id = 0

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[TelegramType],
        timeout: int | None = None,
    ) -> TelegramType:
        self.requests.append(method)
        if isinstance(method, SendMessage):
            self._message_id += 1
            message = Message(
                message_id=self._message_id,
                date=datetime.now(UTC),
                chat=Chat(id=int(method.chat_id), type="private"),
                text=method.text,
            )
            return cast(TelegramType, message)
        if isinstance(method, GetMe):
            bot_user = TgUser(id=1, is_bot=True, first_name="Epigone", username="epigone_bot")
            return cast(TelegramType, bot_user)
        raise AssertionError(f"Fake transport has no canned reply for {type(method).__name__}")

    async def stream_content(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        chunk_size: int = 65536,
        raise_for_status: bool = True,
    ) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError("Fake transport does not stream content")
        yield b""  # pragma: no cover

    async def close(self) -> None:
        pass

    def sent_messages(self) -> list[SendMessage]:
        return [m for m in self.requests if isinstance(m, SendMessage)]


def make_bot(session: RecordingSession) -> Bot:
    return Bot(token="42:TEST-TOKEN", session=session)


_update_id = 0


async def feed_text(
    dp: Dispatcher,
    bot: Bot,
    text: str,
    *,
    user_id: int,
    username: str | None = None,
    first_name: str = "Test",
) -> None:
    """Deliver a private text message from a User to the bot, as Telegram would."""
    global _update_id
    _update_id += 1
    update = Update(
        update_id=_update_id,
        message=Message(
            message_id=_update_id,
            date=datetime.now(UTC),
            chat=Chat(id=user_id, type="private"),
            from_user=TgUser(
                id=user_id, is_bot=False, first_name=first_name, username=username
            ),
            text=text,
        ),
    )
    # Re-validate with bot context so nested objects (message.answer etc.) are bound.
    bound = Update.model_validate(update.model_dump(), context={"bot": bot})
    await dp.feed_update(bot, bound)
