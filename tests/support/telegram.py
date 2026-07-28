"""The Telegram seam: a fake transport for aiogram.

Tests assert on outgoing Bot API calls (what the User would see) and feed
incoming updates — no network, no real Telegram.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any, cast

from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.methods import (
    AnswerCallbackQuery,
    DeleteMessage,
    EditMessageText,
    GetMe,
    SendMessage,
    SetMyCommands,
    TelegramMethod,
)
from aiogram.methods.base import TelegramType
from aiogram.types import CallbackQuery, Chat, InlineKeyboardMarkup, Message, Update
from aiogram.types import User as TgUser

from epigone.bot.delete import DELETE_CALLBACK


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
        if isinstance(
            method, AnswerCallbackQuery | DeleteMessage | EditMessageText | SetMyCommands
        ):
            return cast(TelegramType, True)
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

    def edited_messages(self) -> list[EditMessageText]:
        return [m for m in self.requests if isinstance(m, EditMessageText)]

    def callback_answers(self) -> list[AnswerCallbackQuery]:
        return [m for m in self.requests if isinstance(m, AnswerCallbackQuery)]

    def deleted_messages(self) -> list[DeleteMessage]:
        return [m for m in self.requests if isinstance(m, DeleteMessage)]


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
            from_user=TgUser(id=user_id, is_bot=False, first_name=first_name, username=username),
            text=text,
        ),
    )
    await _deliver(dp, bot, update)


async def feed_callback(
    dp: Dispatcher,
    bot: Bot,
    data: str,
    *,
    user_id: int,
    message_id: int = 1,
    username: str | None = None,
) -> None:
    """Deliver an inline-button tap from a User to the bot, as Telegram would."""
    global _update_id
    _update_id += 1
    update = Update(
        update_id=_update_id,
        callback_query=CallbackQuery(
            id=str(_update_id),
            from_user=TgUser(id=user_id, is_bot=False, first_name="Test", username=username),
            chat_instance="test-chat-instance",
            data=data,
            message=Message(
                message_id=message_id,
                date=datetime.now(UTC),
                chat=Chat(id=user_id, type="private"),
                text="tracked list",
            ),
        ),
    )
    await _deliver(dp, bot, update)


async def follow_wallet(
    dp: Dispatcher,
    bot: Bot,
    address: str,
    *,
    user_id: int,
    username: str | None = None,
) -> None:
    """Follow a wallet the way the UI now does — the profile's ➕ Follow tap
    (pfollow:) — for tests that need a tracked wallet as an arrange step. Pasting
    an address only opens its profile now (#111); the deliberate Follow tap is what
    writes the Track (and fires the #82/#83 follow chain), so arrange goes through
    that same callback rather than a paste."""
    await feed_callback(dp, bot, f"pfollow:{address}", user_id=user_id, username=username)


async def _deliver(dp: Dispatcher, bot: Bot, update: Update) -> None:
    # Re-validate with bot context so nested objects (message.answer etc.) are bound.
    bound = Update.model_validate(update.model_dump(), context={"bot": bot})
    await dp.feed_update(bot, bound)


# --- the 🗑 delete-button structural guard (issues #73, #130) ---
#
# Every informational/terminal message the bot sends or edits must carry the
# one-tap 🗑 delete row (epigone.bot.delete.with_delete_button) — an edit path
# that rebuilds a keyboard without it silently drops the button (the #130 repro:
# the positions-view unfollow refresh). The `session` conftest fixture runs
# `assert_delete_buttons` after every test, so EVERY test that drives the bot
# doubles as a delete-button regression test — no per-flow hand-written check.
#
# The exemptions (armed interactive-flow prompts, and the invite-only gate
# refusal — see is_delete_button_exempt for the why of each) live here, once, so
# adding a new one is a visible diff rather than a scattered per-site decision.


def _delete_row_present(markup: InlineKeyboardMarkup | None) -> bool:
    """Does this keyboard end in the bare 🗑 delete row? None (no keyboard at
    all) is the repro's shape — a dropped button — so it never counts."""
    if markup is None:
        return False
    rows = [[b.callback_data or "" for b in row] for row in markup.inline_keyboard]
    return bool(rows) and rows[-1] == [DELETE_CALLBACK]


def is_delete_button_exempt(text: str) -> bool:
    """True for the messages deliberately sent without the 🗑 delete row (issue
    #73). Everything else the bot sends/edits must carry it.

    Two exempt categories, both matched against the message text:

    1. Interactive-flow prompts — the criteria builder, the min-size prompt, and
       the rename prompt each live in a self-replacing message the User is
       mid-flow in, so a delete tap would only strand the draft.
    2. The invite-only gate refusal (bot/access.py) — its recipient is not
       allowlisted, so a delete callback would just bounce off the same gate;
       a button there would be dead. The admin allowlist *replies* are not
       exempt: the admin is allowed, so they carry the row.

    The flow constants are matched exactly where the bot exposes them, plus a
    handful of distinctive prefixes/markers for prompts whose text is assembled
    inline. Keep this the single source of truth — a new exemption belongs here,
    next to the others, and nowhere else."""
    from epigone.bot import access, controls, criteria, names, operator

    # Fully-static messages, keyed off the real constant so a wording change in
    # the bot moves the exemption with it (no stale literal to drift).
    exact = {
        # Invite-only gate refusal (bot/access.py) — un-allowlisted recipient.
        access.REFUSAL_TEXT,
        # Criteria builder (bot/criteria.py) — pickers and typed-input prompts.
        criteria.HOME_EMPTY_TEXT,
        criteria.FILTER_PICKER_TEXT,
        criteria.SORT_PICKER_TEXT,
        criteria.TIMEFRAME_TEXT,
        criteria.NAME_PROMPT_TEXT,
        criteria.NAME_TOO_LONG_TEXT,
        criteria.FOCUS_TICKER_PROMPT_TEXT,
        criteria.FOCUS_TICKER_UNREADABLE_TEXT,
        # Rename flow (bot/names.py) — reprompts on a still-armed prompt. The
        # terminal cancel/saved/cleared confirmations are NOT exempt: the flow is
        # over, so they carry the row like any other terminal message.
        names.NAME_EMPTY_TEXT,
        names.NAME_TOO_LONG_TEXT,
        # Min-size flow (bot/controls.py) — global prompt and the still-armed
        # reprompt. The terminal cancel is not exempt (it carries the row).
        controls.GLOBAL_MIN_PROMPT_TEXT,
        controls.MIN_SIZE_UNREADABLE_TEXT,
    }
    if text in exact:
        return True
    # Assembled messages (a name/label/threshold is interpolated in), so matched
    # by a distinctive substring rather than the whole string. Kept sharp enough
    # not to collide with any terminal message that must carry the delete row —
    # e.g. the global-min *confirmation* ("⚙️ Global minimum set to …") is not
    # matched by the prompt marker below.
    markers = (
        # Criteria builder screens and inline prompts (bot/criteria.py).
        criteria.HOME_HEADER,  # the saved-criteria home
        "🛠 Criteria builder",  # _render_builder
        "✔ Added ",  # on_builder_text, filter added → builder redraw
        "💾 Saved ",  # on_builder_text, criteria saved → home redraw
        "Keep traders where",  # numeric filter operator picker
        "Which market should this wallet specialize in?",  # focus-market picker
        "Send the threshold as a number",  # numeric threshold prompt
        "Which end of the ranking comes first?",  # sort-direction picker
        "You already have a criteria called",  # duplicate-name reprompt
        "I couldn't read that as a number",  # threshold reprompt
        "No analyzed wallet has traded",  # unseen-ticker reprompt
        # Min-size flow (bot/controls.py) — per-track prompt (address interpolated).
        "💵 Minimum position size for",
        # Rename flow (bot/names.py) — the armed prompt only. Its terminal
        # confirmations (saved / name-cleared / no-longer-tracking) carry the row.
        "✏️ Rename ",  # rename prompt (named wallet)
        "✏️ Name ",  # rename prompt (unnamed wallet)
        # Resume-confirm prompt (bot/operator.py, #135) — mid-flow, carries its
        # own confirm/cancel keyboard, optionally with the sweep-pending line
        # appended (hence a marker, not an exact match). The terminal
        # resumed/cancelled/killed replies are NOT exempt.
        operator.RESUME_PROMPT_TEXT.splitlines()[0],  # "Resume trading?"
    )
    return any(marker in text for marker in markers)


def assert_delete_buttons(session: "RecordingSession") -> None:
    """Fail if any non-exempt message the session sent or edited lacks the 🗑
    delete row (issues #73, #130). Walks both fresh sends and in-place edits —
    the edit paths are where the button silently regresses."""
    offenders: list[str] = []
    for method in session.requests:
        if isinstance(method, SendMessage):
            text, markup = method.text, method.reply_markup
        elif isinstance(method, EditMessageText):
            text, markup = method.text, method.reply_markup
        else:
            continue
        body = text or ""
        if is_delete_button_exempt(body):
            continue
        if not _delete_row_present(markup if isinstance(markup, InlineKeyboardMarkup) else None):
            verb = "sent" if isinstance(method, SendMessage) else "edited"
            preview = body.splitlines()[0] if body else "<no text>"
            offenders.append(f"{verb}: {preview!r}")
    if offenders:
        joined = "\n  ".join(offenders)
        raise AssertionError(
            "message(s) missing the 🗑 delete row (issue #73/#130) — route the "
            "markup through epigone.bot.delete.with_delete_button, or add the text "
            "to is_delete_button_exempt if it is a genuine interactive-flow prompt:"
            f"\n  {joined}"
        )
