"""Gate hardening (issue #40): the invite-only gate must fail *closed*.

#33 identified the sender only from `message` and `callback_query`; every other
update type resolved to no user and was passed *through* to routing (fail-open).
Harmless while the bot registered only message/callback handlers, but a latent
trap: a future handler on another update type would be ungated. These tests pin
the hardened behaviour — an update whose sender can't be resolved is dropped,
and every from_user-bearing update type is gated — so that trap can't reopen.
"""

from datetime import UTC, datetime

import asyncpg
import pytest
from aiogram import Bot, Dispatcher
from aiogram.types import (
    CallbackQuery,
    Chat,
    InlineQuery,
    Message,
    PollAnswer,
    PreCheckoutQuery,
    Update,
)
from aiogram.types import User as TgUser

from epigone.bot.access import _update_user, install_allowlist_gate
from epigone.bot.handlers import build_router
from epigone.gateway.fake import FakeHyperliquidGateway
from tests.support.clock import FakeClock

ADMIN_ID = 370818090
STRANGER_ID = 999

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _tg_user(user_id: int) -> TgUser:
    return TgUser(id=user_id, is_bot=False, first_name="Test")


def _msg(user_id: int, *, text: str = "hi") -> Message:
    return Message(
        message_id=1,
        date=_NOW,
        chat=Chat(id=user_id, type="private"),
        from_user=_tg_user(user_id),
        text=text,
    )


# One update per from_user-bearing variant the bot doesn't special-case today.
# _update_user must resolve the sender for each, so gating them is automatic.
def _updates_with_sender(user_id: int) -> dict[str, Update]:
    u = _tg_user(user_id)
    return {
        "message": Update(update_id=1, message=_msg(user_id)),
        "edited_message": Update(update_id=1, edited_message=_msg(user_id)),
        "callback_query": Update(
            update_id=1,
            callback_query=CallbackQuery(
                id="1", from_user=u, chat_instance="ci", data="x", message=_msg(user_id)
            ),
        ),
        "inline_query": Update(
            update_id=1, inline_query=InlineQuery(id="1", from_user=u, query="q", offset="")
        ),
        "pre_checkout_query": Update(
            update_id=1,
            pre_checkout_query=PreCheckoutQuery(
                id="1", from_user=u, currency="USD", total_amount=1, invoice_payload="p"
            ),
        ),
        # Reactions and poll answers name the sender `user`, not `from_user` —
        # the gate must resolve that spelling too.
        "poll_answer": Update(
            update_id=1,
            poll_answer=PollAnswer(
                poll_id="1", user=u, option_ids=[0], option_persistent_ids=[]
            ),
        ),
    }


@pytest.mark.parametrize("event_type", list(_updates_with_sender(STRANGER_ID)))
def test_update_user_resolves_sender_for_every_from_user_type(event_type: str) -> None:
    update = _updates_with_sender(STRANGER_ID)[event_type]
    user = _update_user(update)
    assert user is not None, f"{event_type} carries a from_user the gate must see"
    assert user.id == STRANGER_ID


def test_update_user_is_none_when_no_sender() -> None:
    # A channel post carries no from_user — nothing to gate on.
    channel_post = Update(
        update_id=1,
        channel_post=Message(message_id=1, date=_NOW, chat=Chat(id=-100, type="channel"), text="x"),
    )
    assert _update_user(channel_post) is None


def _make_gated_dp(
    pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock
) -> Dispatcher:
    dp = Dispatcher()
    dp["pool"] = pool
    dp["gateway"] = gateway
    dp["clock"] = clock
    dp["admin_telegram_id"] = ADMIN_ID
    dp["drafts"] = {}
    dp["min_size_pending"] = {}
    install_allowlist_gate(dp)
    dp.include_router(build_router())
    return dp


@pytest.fixture
def gated_dp(pool: asyncpg.Pool, gateway: FakeHyperliquidGateway, clock: FakeClock) -> Dispatcher:
    return _make_gated_dp(pool, gateway, clock)


async def test_unresolvable_update_is_dropped_not_routed(
    gated_dp: Dispatcher, bot: Bot
) -> None:
    """Fail closed: an update with no resolvable sender never reaches a handler,
    even if one is registered for its type."""
    reached: list[str] = []

    async def spy(post: Message) -> None:  # pragma: no cover - must never run
        reached.append("routed")

    gated_dp.channel_post.register(spy)

    update = Update(
        update_id=1,
        channel_post=Message(message_id=1, date=_NOW, chat=Chat(id=-100, type="channel"), text="x"),
    )
    bound = Update.model_validate(update.model_dump(), context={"bot": bot})
    await gated_dp.feed_update(bot, bound)

    assert reached == []


async def test_new_handler_type_is_gated_for_stranger_and_admin(
    gated_dp: Dispatcher, bot: Bot
) -> None:
    """The regression pin. Register a handler on an update type the gate doesn't
    special-case (edited_message) and confirm the gate still decides who reaches
    it: a stranger is blocked, the admin gets through. If a future change makes
    the gate fail open again, the stranger reaches the handler and this fails; if
    it stops resolving new senders, the admin can't reach it and this fails."""
    reached: list[int] = []

    async def spy(edited: Message) -> None:
        assert edited.from_user is not None
        reached.append(edited.from_user.id)

    gated_dp.edited_message.register(spy)

    async def feed_edit(user_id: int) -> None:
        update = Update(update_id=user_id, edited_message=_msg(user_id))
        bound = Update.model_validate(update.model_dump(), context={"bot": bot})
        await gated_dp.feed_update(bot, bound)

    await feed_edit(STRANGER_ID)
    assert reached == []  # gate blocked the stranger

    await feed_edit(ADMIN_ID)
    assert reached == [ADMIN_ID]  # gate let the admin through
