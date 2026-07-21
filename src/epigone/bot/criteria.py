"""The Criteria builder (issue #7): a guided, button-driven flow where a User
assembles filters (metric → operator → threshold), a timeframe and a sort into
a Criteria, runs it, and saves it under a name.

Drafts live in memory per User (losing one to a restart costs a few taps);
saved Criteria live in the criteria table and survive restarts. Free-text is
needed twice — a filter's threshold and the saved name — so the draft records
what it is waiting for and build_router routes matching messages here, ahead
of the wallet-paste handler; commands always cut through. ADR-0002 sketched
these dialogs on aiogram's FSM; with buttons carrying the state transitions
and only those two typed inputs, a per-User Draft in dispatcher data gives
the same UX with less machinery, so the FSM stays unused.

Callback vocabulary (all criteria callbacks start with "c"): cnew/chome/cmenu
navigate, cfadd→cfm:→cfo: adds a filter, cfdel: removes one, cwin→cw: sets the
timeframe, csort→csm:→csd: the sort, crun:{d|id|p<key>}:{offset} runs a draft, a
saved Criteria, or a starter preset, csave/cedit:/cdel: manage saved ones,
cdelp:<key> hides a preset for the User (issue #71), cfw: follows a result.
Callback payloads are client-forgeable, so every id is scoped to the tapping
User and every metric/operator/window/preset key is looked up, never trusted.
"""

from dataclasses import dataclass, field
from decimal import Decimal

import asyncpg
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from epigone.bot.format import short_address, signed_pct, signed_usd
from epigone.bot.handlers import (
    SCREENER_PAGE_SIZE,
    SCREENER_PENDING_LABEL,
    follow_toast,
    track_address,
    tracked_set,
    upsert_user,
)
from epigone.clock import Clock
from epigone.criteria_presets import PRESETS, PRESETS_BY_KEY
from epigone.criteria_store import (
    SavedCriteria,
    delete_criteria,
    dismiss_preset,
    get_criteria,
    hidden_preset_keys,
    list_criteria,
    save_criteria,
    update_criteria,
)
from epigone.gateway import Window
from epigone.metrics.library import METRICS, format_value, parse_threshold
from epigone.screener import (
    Criteria,
    Filter,
    Op,
    ScreenerRow,
    run_criteria,
    strictest_filter,
)

MAX_NAME_LENGTH = 40

WINDOW_LABELS = {
    Window.DAY: "24h",
    Window.WEEK: "7d",
    Window.MONTH: "30d",
    Window.ALL_TIME: "all time",
}
WINDOW_BUTTON_LABELS = {
    Window.DAY: "24 hours",
    Window.WEEK: "7 days",
    Window.MONTH: "30 days",
    Window.ALL_TIME: "All time",
}

HOME_HEADER = "🎯 Your saved criteria:"

# Marks a starter preset in the list so it reads apart from a User's own saved
# criteria (issue #71). The "p" ref prefix namespaces preset run/follow refs
# away from the integer ids of saved Criteria.
PRESET_MARKER = "⭐"
PRESET_REF_PREFIX = "p"

HOME_EMPTY_TEXT = (
    "You haven't saved any criteria yet.\n\n"
    "A criteria is your own definition of the best trader: filters over the "
    "metrics, a timeframe, and a sort. Build one, run it, save it under a name."
)

FILTER_PICKER_TEXT = (
    "Filter on which metric?\n\n"
    "PnL, ROI, volume and account value cover every scanned trader. The "
    "fills-based stats (win rate and below) keep only fully-analyzed traders."
)

SORT_PICKER_TEXT = "Sort by which metric?"

TIMEFRAME_TEXT = (
    "Timeframe for PnL, ROI and volume.\n\n"
    "Fills-based stats (win rate, Sharpe…) always cover the trader's recent "
    "fill history, whatever the timeframe."
)

NAME_PROMPT_TEXT = "What should this criteria be called? Send a short name — e.g. Steady scalpers."
NAME_TOO_LONG_TEXT = (
    f"That name is too long — {MAX_NAME_LENGTH} characters max. Send a shorter one."
)

EMPTY_UNIVERSE_TEXT = (
    "No traders to rank yet — the universe is still being scanned. Run it again later."
)

DRAFT_EXPIRED_TOAST = "That draft expired — start a fresh one."
CRITERIA_GONE_TOAST = "That criteria is gone — it may have been deleted."


@dataclass
class Draft:
    """A Criteria being built, plus what the flow is waiting for. editing_id
    is set when the draft was loaded from a saved Criteria — saving then
    updates in place and keeps the name."""

    filters: list[Filter] = field(default_factory=list)
    time_window: Window = Window.MONTH
    sort_key: str = "roi"
    sort_desc: bool = True
    editing_id: int | None = None
    editing_name: str | None = None
    pending_metric: str | None = None  # a filter awaiting its threshold…
    pending_op: Op | None = None  # …compared with this operator
    awaiting_name: bool = False

    def to_criteria(self) -> Criteria:
        return Criteria(
            filters=tuple(self.filters),
            time_window=self.time_window,
            sort_key=self.sort_key,
            sort_desc=self.sort_desc,
        )

    def clear_pending(self) -> None:
        self.pending_metric = None
        self.pending_op = None
        self.awaiting_name = False


def draft_from(saved: SavedCriteria) -> Draft:
    return Draft(
        filters=list(saved.criteria.filters),
        time_window=saved.criteria.time_window,
        sort_key=saved.criteria.sort_key,
        sort_desc=saved.criteria.sort_desc,
        editing_id=saved.id,
        editing_name=saved.name,
    )


Drafts = dict[int, Draft]


def register(router: Router) -> None:
    """All criteria handlers. Called by build_router before the wallet-paste
    handler so pending threshold/name input is consumed here first."""
    router.message.register(cmd_criteria, Command("criteria"))
    router.message.register(on_builder_text, awaiting_builder_input)
    router.callback_query.register(on_new, F.data == "cnew")
    router.callback_query.register(on_home, F.data == "chome")
    router.callback_query.register(on_menu, F.data == "cmenu")
    router.callback_query.register(on_add_filter, F.data == "cfadd")
    router.callback_query.register(on_filter_metric, F.data.startswith("cfm:"))
    router.callback_query.register(on_filter_op, F.data.startswith("cfo:"))
    router.callback_query.register(on_filter_delete, F.data.startswith("cfdel:"))
    router.callback_query.register(on_window_menu, F.data == "cwin")
    router.callback_query.register(on_window_set, F.data.startswith("cw:"))
    router.callback_query.register(on_sort_menu, F.data == "csort")
    router.callback_query.register(on_sort_metric, F.data.startswith("csm:"))
    router.callback_query.register(on_sort_direction, F.data.startswith("csd:"))
    router.callback_query.register(on_run, F.data.startswith("crun:"))
    router.callback_query.register(on_save, F.data == "csave")
    router.callback_query.register(on_edit, F.data.startswith("cedit:"))
    router.callback_query.register(on_delete_preset, F.data.startswith("cdelp:"))
    router.callback_query.register(on_delete, F.data.startswith("cdel:"))
    router.callback_query.register(on_follow, F.data.startswith("cfw:"))


def awaiting_builder_input(message: Message, drafts: Drafts) -> bool:
    """True when this User's draft is waiting for typed input. Commands are
    never swallowed — /help mid-prompt still answers, the prompt stays live."""
    if message.from_user is None or message.text is None or message.text.startswith("/"):
        return False
    draft = drafts.get(message.from_user.id)
    return draft is not None and (draft.pending_op is not None or draft.awaiting_name)


async def cmd_criteria(message: Message, pool: asyncpg.Pool, drafts: Drafts) -> None:
    user = message.from_user
    if user is None:
        return
    await upsert_user(pool, user.id, user.username)
    draft = drafts.get(user.id)
    if draft is not None:
        draft.clear_pending()  # a fresh /criteria shouldn't leave a stale prompt armed
    text, markup = await _render_home(pool, user.id)
    await message.answer(text, reply_markup=markup)


async def on_new(callback: CallbackQuery, drafts: Drafts) -> None:
    draft = Draft()
    drafts[callback.from_user.id] = draft
    await _show(callback, *_render_builder(draft))


async def on_home(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    draft = drafts.get(callback.from_user.id)
    if draft is not None:
        draft.clear_pending()
    await _show(callback, *await _render_home(pool, callback.from_user.id))


async def on_menu(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    """Back to the builder — also the cancel for a pending threshold/name."""
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    draft.clear_pending()
    await _show(callback, *_render_builder(draft))


async def on_add_filter(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    if drafts.get(callback.from_user.id) is None:
        await _expired(callback, pool)
        return
    await _show(callback, FILTER_PICKER_TEXT, _metric_picker("cfm:"))


async def on_filter_metric(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    spec = METRICS.get((callback.data or "").removeprefix("cfm:"))
    if spec is None:
        await callback.answer("Unknown metric.")
        return
    if drafts.get(callback.from_user.id) is None:
        await _expired(callback, pool)
        return
    keyboard = [
        [
            InlineKeyboardButton(
                text=f"at least ({Op.GTE.symbol})", callback_data=f"cfo:{spec.key}:gte"
            ),
            InlineKeyboardButton(
                text=f"at most ({Op.LTE.symbol})", callback_data=f"cfo:{spec.key}:lte"
            ),
        ],
        [InlineKeyboardButton(text="◀ Back", callback_data="cfadd")],
    ]
    text = f"{spec.label} — {spec.explanation}\n\nKeep traders where {spec.label} is…"
    await _show(callback, text, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_filter_op(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    key, _, op_raw = (callback.data or "").removeprefix("cfo:").partition(":")
    spec = METRICS.get(key)
    try:
        op = Op(op_raw)
    except ValueError:
        op = None
    if spec is None or op is None:
        await callback.answer("Unknown filter.")
        return
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    draft.awaiting_name = False
    draft.pending_metric = spec.key
    draft.pending_op = op
    text = (
        f"{spec.label} {op.symbol} …?\n\n"
        f"{spec.label} — {spec.explanation}\n\n"
        f"Send the threshold as a number — e.g. {spec.example}."
    )
    keyboard = [[InlineKeyboardButton(text="◀ Cancel", callback_data="cmenu")]]
    await _show(callback, text, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_filter_delete(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    """Remove a filter (cfdel:index:metric). Old builder messages keep their
    keyboards, so the metric key rides along as an identity check — a stale
    index alone could delete a different filter after the list shifted."""
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    index_raw, _, metric = (callback.data or "").removeprefix("cfdel:").partition(":")
    index = _parse_int(index_raw)
    if (
        index is not None
        and 0 <= index < len(draft.filters)
        and draft.filters[index].metric == metric
    ):
        removed = draft.filters.pop(index)
        await _show(callback, *_render_builder(draft), toast=f"Removed {_describe(removed)}")
    else:
        await _show(callback, *_render_builder(draft), toast="That button is out of date.")


async def on_window_menu(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    if drafts.get(callback.from_user.id) is None:
        await _expired(callback, pool)
        return
    keyboard = [
        [
            InlineKeyboardButton(text=WINDOW_BUTTON_LABELS[w], callback_data=f"cw:{w.value}")
            for w in (Window.DAY, Window.WEEK)
        ],
        [
            InlineKeyboardButton(text=WINDOW_BUTTON_LABELS[w], callback_data=f"cw:{w.value}")
            for w in (Window.MONTH, Window.ALL_TIME)
        ],
        [InlineKeyboardButton(text="◀ Back", callback_data="cmenu")],
    ]
    await _show(callback, TIMEFRAME_TEXT, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_window_set(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    try:
        window = Window((callback.data or "").removeprefix("cw:"))
    except ValueError:
        await callback.answer("Unknown timeframe.")
        return
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    draft.time_window = window
    await _show(callback, *_render_builder(draft))


async def on_sort_menu(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    if drafts.get(callback.from_user.id) is None:
        await _expired(callback, pool)
        return
    await _show(callback, SORT_PICKER_TEXT, _metric_picker("csm:"))


async def on_sort_metric(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    spec = METRICS.get((callback.data or "").removeprefix("csm:"))
    if spec is None:
        await callback.answer("Unknown metric.")
        return
    if drafts.get(callback.from_user.id) is None:
        await _expired(callback, pool)
        return
    keyboard = [
        [
            InlineKeyboardButton(text="⬇ Highest first", callback_data=f"csd:{spec.key}:d"),
            InlineKeyboardButton(text="⬆ Lowest first", callback_data=f"csd:{spec.key}:a"),
        ],
        [InlineKeyboardButton(text="◀ Back", callback_data="csort")],
    ]
    text = f"{spec.label} — {spec.explanation}\n\nWhich end of the ranking comes first?"
    await _show(callback, text, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_sort_direction(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    key, _, direction = (callback.data or "").removeprefix("csd:").partition(":")
    spec = METRICS.get(key)
    if spec is None or direction not in ("d", "a"):
        await callback.answer("Unknown sort.")
        return
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    draft.sort_key = spec.key
    draft.sort_desc = direction == "d"
    await _show(callback, *_render_builder(draft))


async def on_run(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    """Run a draft (crun:d:offset) or a saved Criteria (crun:id:offset).
    A pure database read, like every screener surface."""
    ref, _, offset_raw = (callback.data or "").removeprefix("crun:").partition(":")
    await _show_results(callback, pool, drafts, ref=ref, offset=_parse_int(offset_raw) or 0)


async def on_save(
    callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock, drafts: Drafts
) -> None:
    """Save the draft: editing an existing Criteria updates it in place and
    keeps its name; a new draft asks for a name first."""
    draft = drafts.get(callback.from_user.id)
    if draft is None:
        await _expired(callback, pool)
        return
    if draft.editing_id is not None:
        updated = await update_criteria(
            pool, callback.from_user.id, draft.editing_id, draft.to_criteria(), clock.now()
        )
        if not updated:  # deleted meanwhile, e.g. from another chat with the bot
            drafts.pop(callback.from_user.id)
            await _expired(callback, pool, toast=CRITERIA_GONE_TOAST)
            return
        name = draft.editing_name
        drafts.pop(callback.from_user.id)
        await _show(
            callback, *await _render_home(pool, callback.from_user.id), toast=f"Saved ‘{name}’"
        )
        return
    draft.awaiting_name = True
    keyboard = [[InlineKeyboardButton(text="◀ Cancel", callback_data="cmenu")]]
    await _show(callback, NAME_PROMPT_TEXT, InlineKeyboardMarkup(inline_keyboard=keyboard))


async def on_edit(callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts) -> None:
    saved_id = _parse_int((callback.data or "").removeprefix("cedit:"))
    saved = (
        await get_criteria(pool, callback.from_user.id, saved_id) if saved_id is not None else None
    )
    if saved is None:
        await _expired(callback, pool, toast=CRITERIA_GONE_TOAST)
        return
    draft = draft_from(saved)
    drafts[callback.from_user.id] = draft
    await _show(callback, *_render_builder(draft))


async def on_delete(callback: CallbackQuery, pool: asyncpg.Pool) -> None:
    saved_id = _parse_int((callback.data or "").removeprefix("cdel:"))
    name = (
        await delete_criteria(pool, callback.from_user.id, saved_id)
        if saved_id is not None
        else None
    )
    toast = f"Deleted ‘{name}’" if name is not None else "Already gone."
    await _show(callback, *await _render_home(pool, callback.from_user.id), toast=toast)


async def on_delete_preset(callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock) -> None:
    """Delete a starter preset — a hide-for-me: it leaves this User's list only,
    permanently (issue #71). Other Users are untouched, and a later version that
    recalibrates the preset's thresholds keeps it hidden for this User."""
    key = (callback.data or "").removeprefix("cdelp:")
    preset = PRESETS_BY_KEY.get(key)
    if preset is None:
        await _show(
            callback, *await _render_home(pool, callback.from_user.id), toast="Already gone."
        )
        return
    await dismiss_preset(pool, callback.from_user.id, key, clock.now())
    await _show(
        callback,
        *await _render_home(pool, callback.from_user.id),
        toast=f"Removed ‘{preset.name}’ from your list",
    )


async def on_follow(
    callback: CallbackQuery, pool: asyncpg.Pool, clock: Clock, drafts: Drafts
) -> None:
    """Follow straight from a results row (cfw:ref:offset:address), then
    re-render the page so the row flips to Following. Same Track seam as the
    screener and the paste path — it feeds the alert poller for free."""
    ref, _, rest = (callback.data or "").removeprefix("cfw:").partition(":")
    offset_raw, _, address = rest.partition(":")
    async with pool.acquire() as conn, conn.transaction():
        outcome = await track_address(
            conn, callback.from_user.id, callback.from_user.username, address, clock.now()
        )
    await _show_results(
        callback,
        pool,
        drafts,
        ref=ref,
        offset=_parse_int(offset_raw) or 0,
        toast=follow_toast(outcome, address),
    )


async def on_builder_text(
    message: Message, pool: asyncpg.Pool, clock: Clock, drafts: Drafts
) -> None:
    """The two typed inputs of the flow: a filter's threshold, or the name a
    draft is saved under. Everything else in the builder is buttons."""
    user = message.from_user
    if user is None or message.text is None:
        return
    draft = drafts.get(user.id)
    if draft is None:
        return
    if draft.awaiting_name:
        name = message.text.strip()
        if not name:
            await message.answer(NAME_PROMPT_TEXT)
            return
        if len(name) > MAX_NAME_LENGTH:
            await message.answer(NAME_TOO_LONG_TEXT)
            return
        # Reusing a name must never silently destroy a saved Criteria — the
        # restart-survival promise (#7) would be defeatable by an innocent typo.
        # A visible starter preset's name is taken too, so the list never shows
        # two identical names; deleting that preset frees the name (issue #71).
        if name in await _taken_names(pool, user.id):
            await message.answer(
                f"You already have a criteria called ‘{name}’ — send a different "
                "name, or delete the old one from /criteria first."
            )
            return
        await save_criteria(pool, user.id, name, draft.to_criteria(), clock.now())
        drafts.pop(user.id)
        home_text, markup = await _render_home(pool, user.id)
        await message.answer(
            f"💾 Saved ‘{name}’ — run it any time from /criteria.\n\n{home_text}",
            reply_markup=markup,
        )
        return
    if draft.pending_metric is None or draft.pending_op is None:
        return  # unreachable while awaiting_builder_input gates registration
    spec = METRICS[draft.pending_metric]
    threshold = parse_threshold(spec, message.text)
    if threshold is None:
        await message.answer(f"I couldn't read that as a number. Send e.g. {spec.example}.")
        return
    added = Filter(metric=spec.key, op=draft.pending_op, threshold=threshold)
    draft.filters.append(added)
    draft.clear_pending()
    text, markup = _render_builder(draft)
    await message.answer(f"✔ Added {_describe(added)}.\n\n{text}", reply_markup=markup)


# --- rendering ---


async def _render_home(pool: asyncpg.Pool, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    saved = await list_criteria(pool, user_id)
    hidden = await hidden_preset_keys(pool, user_id)
    presets = [p for p in PRESETS if p.key not in hidden]
    keyboard: list[list[InlineKeyboardButton]] = []
    if not saved and not presets:
        # Every user starts with the three presets, so this is only reached once
        # a user has deleted them all and saved nothing of their own.
        text = HOME_EMPTY_TEXT
    else:
        lines = [HOME_HEADER, ""]
        # A User's own saved Criteria first — what they came back for — then the
        # curated starters below, always available, each visibly marked.
        for s in saved:
            lines.append(f"• {s.name} — {_summarize(s.criteria)}")
            keyboard.append(
                [
                    InlineKeyboardButton(text=f"▶ {s.name}", callback_data=f"crun:{s.id}:0"),
                    InlineKeyboardButton(text="✏️ Edit", callback_data=f"cedit:{s.id}"),
                    InlineKeyboardButton(text="🗑 Delete", callback_data=f"cdel:{s.id}"),
                ]
            )
        for p in presets:
            lines.append(f"{PRESET_MARKER} {p.name} — {_summarize(p.criteria)}  (starter)")
            keyboard.append(
                [
                    InlineKeyboardButton(
                        text=f"▶ {p.name}",
                        callback_data=f"crun:{PRESET_REF_PREFIX}{p.key}:0",
                    ),
                    InlineKeyboardButton(text="🗑 Delete", callback_data=f"cdelp:{p.key}"),
                ]
            )
        text = "\n".join(lines)
    keyboard.append([InlineKeyboardButton(text="➕ New criteria", callback_data="cnew")])
    return text, InlineKeyboardMarkup(inline_keyboard=keyboard)


def _summarize(criteria: Criteria) -> str:
    count = len(criteria.filters)
    noun = "filter" if count == 1 else "filters"
    return (
        f"{count} {noun} · {WINDOW_LABELS[criteria.time_window]} · "
        f"sort: {METRICS[criteria.sort_key].label}"
    )


async def _taken_names(pool: asyncpg.Pool, user_id: int) -> set[str]:
    """The names already showing in this User's list — their own saved Criteria
    plus any starter presets they haven't deleted. A new name must avoid all of
    them so the list never carries two identical names (issue #71)."""
    hidden = await hidden_preset_keys(pool, user_id)
    names = {p.name for p in PRESETS if p.key not in hidden}
    names.update(s.name for s in await list_criteria(pool, user_id))
    return names


def _render_builder(draft: Draft) -> tuple[str, InlineKeyboardMarkup]:
    title = f"editing ‘{draft.editing_name}’" if draft.editing_name is not None else "new criteria"
    lines = [f"🛠 Criteria builder — {title}", ""]
    if draft.filters:
        lines.append("Filters:")
        lines.extend(f"{i}. {_describe(f)}" for i, f in enumerate(draft.filters, start=1))
    else:
        lines.append("No filters yet — every scanned trader qualifies.")
    direction = "highest first" if draft.sort_desc else "lowest first"
    lines.append("")
    lines.append(
        f"Timeframe: {WINDOW_LABELS[draft.time_window]} · "
        f"Sort: {METRICS[draft.sort_key].label}, {direction}"
    )
    keyboard = [
        [InlineKeyboardButton(text="➕ Add filter", callback_data="cfadd")],
        [
            InlineKeyboardButton(text="🕒 Timeframe", callback_data="cwin"),
            InlineKeyboardButton(text="↕ Sort", callback_data="csort"),
        ],
    ]
    keyboard.extend(
        [
            InlineKeyboardButton(
                text=f"✖ Remove {_describe(f)}", callback_data=f"cfdel:{i}:{f.metric}"
            )
        ]
        for i, f in enumerate(draft.filters)
    )
    keyboard.append(
        [
            InlineKeyboardButton(text="▶ Run", callback_data="crun:d:0"),
            InlineKeyboardButton(text="💾 Save", callback_data="csave"),
        ]
    )
    keyboard.append([InlineKeyboardButton(text="◀ Your criteria", callback_data="chome")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard)


def _metric_picker(prefix: str) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for spec in METRICS.values():
        row.append(InlineKeyboardButton(text=spec.label, callback_data=f"{prefix}{spec.key}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="◀ Back", callback_data="cmenu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _render_results(
    pool: asyncpg.Pool,
    user_id: int,
    criteria: Criteria,
    *,
    ref: str,
    offset: int,
    title: str,
    back: InlineKeyboardButton,
) -> tuple[str, InlineKeyboardMarkup]:
    # One extra row tells us whether a next page exists without a second query.
    rows = await run_criteria(pool, criteria, limit=SCREENER_PAGE_SIZE + 1, offset=offset)
    has_next = len(rows) > SCREENER_PAGE_SIZE
    rows = rows[:SCREENER_PAGE_SIZE]
    if not rows and offset > 0:  # a stale Next button after the results shrank
        return await _render_results(
            pool, user_id, criteria, ref=ref, offset=0, title=title, back=back
        )
    if not rows:
        return await _render_zero_results(pool, criteria), InlineKeyboardMarkup(
            inline_keyboard=[[back]]
        )

    direction = "highest first" if criteria.sort_desc else "lowest first"
    lines = [
        f"🎯 {title} — {WINDOW_LABELS[criteria.time_window]} · "
        f"{METRICS[criteria.sort_key].label}, {direction}",
        "",
    ]
    tracked = await tracked_set(pool, user_id, [r.address for r in rows])
    keyboard: list[list[InlineKeyboardButton]] = []
    for rank, row in enumerate(rows, start=offset + 1):
        lines.append(f"{rank}. {row.display_name or short_address(row.address)}")
        lines.append(f"    {_stats_line(row, criteria)}")
        followed = row.address in tracked
        keyboard.append(
            [
                InlineKeyboardButton(
                    text=f"📊 {short_address(row.address)}", callback_data=f"profile:{row.address}"
                ),
                InlineKeyboardButton(
                    text="✓ Following" if followed else "➕ Follow",
                    callback_data=f"cfw:{ref}:{offset}:{row.address}",
                ),
            ]
        )
    nav: list[InlineKeyboardButton] = []
    if offset > 0:
        prev_offset = max(0, offset - SCREENER_PAGE_SIZE)
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"crun:{ref}:{prev_offset}"))
    if has_next:
        nav.append(
            InlineKeyboardButton(
                text="Next ▶", callback_data=f"crun:{ref}:{offset + SCREENER_PAGE_SIZE}"
            )
        )
    if nav:
        keyboard.append(nav)
    keyboard.append([back])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _render_zero_results(pool: asyncpg.Pool, criteria: Criteria) -> str:
    """Issue #7 acceptance: a zero-result run names the strictest filter so
    the User knows what to loosen."""
    strictness = await strictest_filter(pool, criteria)
    if strictness is None:  # no filters, so the Universe itself is empty
        return EMPTY_UNIVERSE_TEXT
    if strictness.solo_matches == 0:
        clears = "no scanned trader clears it even on its own"
    else:
        count = strictness.solo_matches
        noun = "trader clears" if count == 1 else "traders clear"
        clears = f"only {count} {noun} it on its own"
    return (
        "😶 No traders match this criteria.\n\n"
        f"The strictest filter is {_describe(strictness.filter)} — {clears}. "
        "Loosen it and run again."
    )


def _stats_line(row: ScreenerRow, criteria: Criteria) -> str:
    """Key stats per result row, reading like the screener's: ROI and PnL
    always, win rate where the fine pass has run (else the pending marker),
    plus the sort metric when it isn't already shown."""
    parts = [f"ROI {signed_pct(row.roi)}", f"PnL {signed_usd(row.pnl)}"]
    if row.win_rate is not None:
        parts.append(f"{row.win_rate:.0%} win")
    elif not row.fine_available:
        parts.append(SCREENER_PENDING_LABEL)
    if criteria.sort_key not in ("roi", "pnl", "win_rate"):
        spec = METRICS[criteria.sort_key]
        value = getattr(row, spec.key)
        if value is not None:
            parts.append(f"{spec.label} {format_value(spec, Decimal(value))}")
    return " · ".join(parts)


def _describe(f: Filter) -> str:
    spec = METRICS[f.metric]
    return f"{spec.label} {f.op.symbol} {format_value(spec, f.threshold)}"


# --- plumbing ---


async def _show_results(
    callback: CallbackQuery,
    pool: asyncpg.Pool,
    drafts: Drafts,
    *,
    ref: str,
    offset: int,
    toast: str | None = None,
) -> None:
    """The shared tail of every results tap (run, page, follow): resolve the
    ref, render that page in place."""
    resolved = await _resolve(callback, pool, drafts, ref, toast=toast)
    if resolved is None:
        return
    criteria, title, back = resolved
    text, markup = await _render_results(
        pool,
        callback.from_user.id,
        criteria,
        ref=ref,
        offset=max(0, offset),
        title=title,
        back=back,
    )
    await _show(callback, text, markup, toast=toast)


async def _resolve(
    callback: CallbackQuery, pool: asyncpg.Pool, drafts: Drafts, ref: str, toast: str | None = None
) -> tuple[Criteria, str, InlineKeyboardButton] | None:
    """A results ref → (criteria, title, back button). "d" is this User's
    draft; anything else a saved Criteria id, scoped to the User. On a dead
    ref the caller is already answered and back at home."""
    if ref == "d":
        draft = drafts.get(callback.from_user.id)
        if draft is None:
            await _expired(callback, pool, toast=DRAFT_EXPIRED_TOAST if toast is None else toast)
            return None
        back = InlineKeyboardButton(text="🛠 Back to builder", callback_data="cmenu")
        return draft.to_criteria(), draft.editing_name or "Your draft", back
    if ref.startswith(PRESET_REF_PREFIX):
        preset = PRESETS_BY_KEY.get(ref.removeprefix(PRESET_REF_PREFIX))
        if preset is None:  # a preset retired since this keyboard was drawn
            await _expired(callback, pool, toast=toast or CRITERIA_GONE_TOAST)
            return None
        back = InlineKeyboardButton(text="◀ Your criteria", callback_data="chome")
        return preset.criteria, preset.name, back
    saved_id = _parse_int(ref)
    saved = (
        await get_criteria(pool, callback.from_user.id, saved_id) if saved_id is not None else None
    )
    if saved is None:
        await _expired(callback, pool, toast=toast or CRITERIA_GONE_TOAST)
        return None
    back = InlineKeyboardButton(text="◀ Your criteria", callback_data="chome")
    return saved.criteria, saved.name, back


async def _expired(
    callback: CallbackQuery, pool: asyncpg.Pool, toast: str = DRAFT_EXPIRED_TOAST
) -> None:
    """A callback for state that no longer exists lands the User back home."""
    await _show(callback, *await _render_home(pool, callback.from_user.id), toast=toast)


async def _show(
    callback: CallbackQuery,
    text: str,
    markup: InlineKeyboardMarkup,
    toast: str | None = None,
) -> None:
    """The flow lives in one message, edited in place on every tap."""
    if isinstance(callback.message, Message):
        await callback.message.edit_text(text, reply_markup=markup)
    await callback.answer(toast)


def _parse_int(raw: str) -> int | None:
    try:
        return int(raw)
    except ValueError:
        return None
