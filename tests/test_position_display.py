"""Issue #35: the position display shows real margin (money at risk),
return-on-margin, and holding time — not just leveraged notional.

Pure-function coverage of the shared renderer (`_render_positions`), the age
formatter (`open_age`), and the `Position` margin/return fallbacks; the DB-backed
age lookup and the live call sites are exercised in test_track_wallet.py.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from epigone.bot.format import fills_open_age, held_for, open_age, usd_compact
from epigone.bot.handlers import (
    FOLLOW_FOR_AGE_HINT,
    _render_most_played,
    _render_positions,
    _render_recent_activity,
)
from epigone.gateway import Position, Side

WHALE = "0xaf0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
NOW = datetime(2026, 7, 11, 12, 0, tzinfo=UTC)

# Ansem's real example from the ticket: 40x BTC long, ~$96 put up, +$344 (+357%).
BTC_LONG = Position(
    coin="BTC",
    side=Side.LONG,
    size_usd=Decimal("3853"),
    leverage=Decimal("40"),
    entry_price=Decimal("38530"),
    unrealized_pnl=Decimal("344"),
    margin_used=Decimal("96"),
    return_on_equity=Decimal("3.57"),
)


# --- margin / return-on-margin on Position ----------------------------------


def test_margin_prefers_the_exact_api_value() -> None:
    assert BTC_LONG.margin == Decimal("96")  # marginUsed, not notional/leverage
    assert BTC_LONG.return_on_margin == Decimal("3.57")


def test_margin_falls_back_to_notional_over_leverage() -> None:
    bare = Position(
        coin="ETH",
        side=Side.SHORT,
        size_usd=Decimal("2000"),
        leverage=Decimal("20"),
        entry_price=Decimal("1677.9"),
        unrealized_pnl=Decimal("-50"),
    )
    assert bare.margin == Decimal("100")  # 2000 / 20
    assert bare.return_on_margin == Decimal("-0.5")  # -50 / 100


# --- open_age formatting -----------------------------------------------------


def test_open_age_reads_plainly_for_a_position_seen_opening() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)  # 2d 4h before NOW
    assert open_age(opened, NOW, baselined=False) == "open 2d 4h"


def test_open_age_marks_a_baselined_position_as_at_least() -> None:
    # A position already open at baseline only knows time-since-tracking (#4),
    # so it must never read as a precise open age.
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    assert open_age(opened, NOW, baselined=True) == "open ≥2d 4h"


def test_held_for_still_reads_a_closed_span() -> None:
    opened = datetime(2026, 7, 11, 8, 40, tzinfo=UTC)
    assert held_for(opened, NOW) == "3h 20m"


# --- fills-derived open age (issue #78) -------------------------------------
#
# For an untracked wallet the poller never observed, the fine store's open
# episode (#63) supplies the age instead — read as an approximation (`~`) and,
# when the fills scan is stale, hedged like the activity line (#72).


def test_fills_open_age_reads_as_an_approximation() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)  # 2d 4h before NOW
    assert fills_open_age(opened, NOW, stale=False) == "open ~2d 4h"


def test_fills_open_age_hedges_staleness_as_of_last_scan() -> None:
    # Knowledge only as fresh as the last fills scan — if that scan is old the
    # wallet may have changed the position since, so it must not read as live.
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    assert fills_open_age(opened, NOW, stale=True) == "open ~2d 4h (as of last scan)"


# --- the shared position renderer -------------------------------------------


def test_render_shows_margin_notional_and_return_on_margin() -> None:
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert "$3,853 notional" in text  # the leveraged size, still shown
    assert "$96 margin" in text  # the real money at risk (issue #35)
    assert "40x" in text
    assert "+$344 (+357%)" in text  # uPnL made legible by return-on-margin


def test_render_shows_a_precise_age_for_a_freshly_opened_position() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={"BTC": (opened, False)}, now=NOW)
    assert "open 2d 4h" in text


def test_render_marks_a_baselined_positions_age_as_at_least() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={"BTC": (opened, True)}, now=NOW)
    assert "open ≥2d 4h" in text


def test_render_omits_age_when_no_snapshot_exists() -> None:
    # An untracked wallet's profile has no poller snapshot, so there's no honest
    # age to show — the line simply carries none rather than inventing one.
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert "open " not in text


# --- fills-derived age in the renderer (issue #78) --------------------------
#
# When no poller snapshot exists, a matching open episode (coin + direction)
# supplies the age; a contradicting, demoted, or missing episode still shows
# none. The `fills` map is coin → (opened_at, net_position, fills_seen_at).

FILLS_OPENED = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)  # 2d 4h before NOW
FRESH_SCAN = NOW - timedelta(hours=2)


def test_render_shows_a_fills_derived_age_when_the_episode_matches() -> None:
    # BTC_LONG has no snapshot but a long open episode (net_position > 0) — its
    # age comes from the fills, read as an approximation.
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"BTC": (FILLS_OPENED, Decimal("0.5"), FRESH_SCAN)},
    )
    assert "open ~2d 4h" in text


def test_render_hedges_a_fills_derived_age_when_the_scan_is_stale() -> None:
    stale_scan = NOW - timedelta(days=3)
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"BTC": (FILLS_OPENED, Decimal("0.5"), stale_scan)},
    )
    assert "open ~2d 4h (as of last scan)" in text


def test_render_omits_age_when_the_episode_direction_contradicts_the_position() -> None:
    # The live position is long but the fills snapshot is short — the wallet
    # flipped since the last refresh, so the episode is not this position.
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"BTC": (FILLS_OPENED, Decimal("-0.5"), FRESH_SCAN)},
    )
    assert "open " not in text


def test_render_omits_age_for_a_demoted_episode() -> None:
    # net_position 0 is the pre-#63 "never verified" default: it matches no live
    # direction, so it never lends an age.
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"BTC": (FILLS_OPENED, Decimal("0"), FRESH_SCAN)},
    )
    assert "open " not in text


def test_render_omits_age_when_no_episode_covers_the_position() -> None:
    # A fills map that has other coins but not this one still shows no age.
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"ETH": (FILLS_OPENED, Decimal("-1"), FRESH_SCAN)},
    )
    assert "open " not in text


def test_render_prefers_the_poller_snapshot_over_the_fills_episode() -> None:
    # A tracked wallet keeps its precise poller age even when a fills episode
    # also exists — the snapshot is the fresher source.
    snap_opened = datetime(2026, 7, 11, 8, 0, tzinfo=UTC)  # 4h before NOW
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={"BTC": (snap_opened, False)},
        now=NOW,
        fills={"BTC": (FILLS_OPENED, Decimal("0.5"), FRESH_SCAN)},
    )
    assert "open 4h" in text
    assert "~" not in text


# --- follow nudge for ageless untracked positions (issue #78) ---------------
#
# On an untracked profile (offer_follow=True), a position we can't date gets no
# invented age; instead one nudge under the block explains the gap and points to
# following. A follower (offer_follow=False) never sees it.


def test_render_offers_follow_when_an_untracked_position_is_ageless() -> None:
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW, offer_follow=True)
    assert FOLLOW_FOR_AGE_HINT in text
    assert "age unknown" not in text  # no per-line text — one nudge only


def test_render_shows_no_follow_nudge_for_a_follower() -> None:
    # offer_follow defaults to False (the tracked positions view): an ageless
    # position simply shows no age, exactly as before.
    text, _ = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert FOLLOW_FOR_AGE_HINT not in text


def test_render_omits_the_nudge_when_every_position_is_dated() -> None:
    # An untracked profile where each position already has an age (here a fills
    # episode) needs no nudge — nothing is missing.
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG],
        ages={},
        now=NOW,
        fills={"BTC": (opened, Decimal("0.5"), NOW - timedelta(hours=2))},
        offer_follow=True,
    )
    assert "open ~2d 4h" in text
    assert FOLLOW_FOR_AGE_HINT not in text


def test_render_offers_follow_when_only_some_positions_are_dated() -> None:
    # BTC dated from a fills episode, SOL ageless → the nudge still appears once.
    sol = Position(
        coin="SOL",
        side=Side.LONG,
        size_usd=Decimal("2000"),
        leverage=Decimal("10"),
        entry_price=Decimal("73"),
        unrealized_pnl=Decimal("10"),
    )
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text, _ = _render_positions(
        WHALE,
        [BTC_LONG, sol],
        ages={},
        now=NOW,
        fills={"BTC": (opened, Decimal("0.5"), NOW - timedelta(hours=2))},
        offer_follow=True,
    )
    assert "open ~2d 4h" in text  # BTC dated
    assert text.count(FOLLOW_FOR_AGE_HINT) == 1  # one nudge, not per-line


def test_render_derives_return_on_margin_without_the_api_field() -> None:
    bare = Position(
        coin="ETH",
        side=Side.SHORT,
        size_usd=Decimal("2000"),
        leverage=Decimal("20"),
        entry_price=Decimal("1677.9"),
        unrealized_pnl=Decimal("-50"),
    )
    text, _ = _render_positions(WHALE, [bare], ages={}, now=NOW)
    assert "$100 margin" in text
    assert "-$50 (-50%)" in text


def test_render_handles_no_open_positions() -> None:
    text, _ = _render_positions(WHALE, [], ages={}, now=NOW)
    assert "no open positions" in text


# --- full address, tap-to-copy header (issue #93) ---------------------------
#
# The header shows the whole address (not the short form) with a `code` entity
# over exactly the address span, so Telegram offers tap-to-copy. Offsets are
# UTF-16 code units, so an emoji in the nickname must be accounted for.


def _entity_span(text: str, entity: object) -> str:
    """The substring a MessageEntity covers, decoded via UTF-16 code units —
    exactly how Telegram interprets offset/length."""
    units = text.encode("utf-16-le")
    start = entity.offset * 2  # type: ignore[attr-defined]
    end = (entity.offset + entity.length) * 2  # type: ignore[attr-defined]
    return units[start:end].decode("utf-16-le")


def test_header_shows_full_address_with_a_code_entity_unnamed() -> None:
    text, entities = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert text.startswith(f"{WHALE} — current positions:")
    (entity,) = entities
    assert entity.type == "code"
    assert _entity_span(text, entity) == WHALE  # covers exactly the address


def test_header_shows_full_address_with_a_code_entity_named() -> None:
    text, entities = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW, name="Avax")
    assert text.startswith(f"Avax ({WHALE}) — current positions:")
    (entity,) = entities
    assert entity.type == "code"
    assert _entity_span(text, entity) == WHALE


def test_header_code_entity_survives_an_emoji_nickname() -> None:
    # A name with an emoji (a UTF-16 surrogate pair) must not shift the entity
    # off the address — the offset is measured in UTF-16 code units.
    text, entities = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW, name="🐳 Whale")
    assert text.startswith(f"🐳 Whale ({WHALE}) — current positions:")
    (entity,) = entities
    assert _entity_span(text, entity) == WHALE


def test_no_positions_header_also_carries_the_copy_entity() -> None:
    text, entities = _render_positions(WHALE, [], ages={}, now=NOW, name="🐳 Whale")
    assert text.startswith(f"🐳 Whale ({WHALE}) has no open positions")
    (entity,) = entities
    assert _entity_span(text, entity) == WHALE


# --- recent-activity line (issue #72) ---------------------------------------
#
# Last-trade recency comes from the fine store's newest folded *perp* fill
# (window_end); the PnL/ROI ride alongside from the coarse leaderboard, labeled
# by the window the toggle selects (#104: week/month/all-time). ROI is stored as
# a fraction (0.12 == 12%).


def test_activity_labels_performance_by_the_selected_window() -> None:
    two_hours_ago = NOW - timedelta(hours=2)  # fresh scan, precise recency
    pnl, roi, account = Decimal("48000"), Decimal("0.12"), Decimal("1100000")
    # The label names the coarse window (#104): the default view is all-time; the
    # 7d/30d toggles read week/month. Account value is the denominator all of
    # them read against (#85) and is window-independent.
    assert _render_recent_activity(two_hours_ago, two_hours_ago, pnl, roi, account, NOW) == (
        "Last trade: 2h ago · all-time PnL +$48,000 (ROI +12%) · account $1.1M"
    )
    assert (
        _render_recent_activity(two_hours_ago, two_hours_ago, pnl, roi, account, NOW, "week")
        == "Last trade: 2h ago · week PnL +$48,000 (ROI +12%) · account $1.1M"
    )
    assert (
        _render_recent_activity(two_hours_ago, two_hours_ago, pnl, roi, account, NOW, "month")
        == "Last trade: 2h ago · month PnL +$48,000 (ROI +12%) · account $1.1M"
    )


def test_activity_marks_last_trade_as_of_last_scan_when_fills_knowledge_lags() -> None:
    # Our newest-fill knowledge is only as fresh as the last fine refresh; a scan
    # older than a day can't imply a live "last trade" time, so it hedges rather
    # than reading with false precision (same spirit as the ≥ open-age marker).
    three_days_ago = NOW - timedelta(days=3)
    line = _render_recent_activity(three_days_ago, three_days_ago, None, None, None, NOW)
    assert line == "Last trade: 3d ago (as of last scan)"


def test_activity_shows_only_recency_when_no_coarse_metrics() -> None:
    two_hours_ago = NOW - timedelta(hours=2)
    line = _render_recent_activity(two_hours_ago, two_hours_ago, None, None, None, NOW)
    assert line == "Last trade: 2h ago"


def test_activity_shows_account_value_even_without_window_pnl() -> None:
    # Account value rides on the coarse row independently of PnL/ROI, so it can
    # appear as the sole coarse addition to an otherwise recency-only line.
    two_hours_ago = NOW - timedelta(hours=2)
    line = _render_recent_activity(
        two_hours_ago, two_hours_ago, None, None, Decimal("50000"), NOW
    )
    assert line == "Last trade: 2h ago · account $50k"


def test_activity_says_no_fills_seen_but_still_shows_coarse_performance() -> None:
    # Coarse leaderboard data exists even for a wallet with no captured fills, so
    # the performance line must not depend on fine availability.
    line = _render_recent_activity(
        None, None, Decimal("3000000"), Decimal("0.21"), Decimal("13400000"), NOW, "all-time"
    )
    assert line == (
        "No recent trading activity seen · all-time PnL +$3,000,000 (ROI +21%) · account $13.4M"
    )


def test_activity_says_no_fills_seen_when_nothing_is_known() -> None:
    assert (
        _render_recent_activity(None, None, None, None, None, NOW)
        == "No recent trading activity seen"
    )


def test_usd_compact_abbreviates_by_magnitude() -> None:
    assert usd_compact(Decimal("1100000")) == "$1.1M"
    assert usd_compact(Decimal("13400000")) == "$13.4M"
    assert usd_compact(Decimal("50000")) == "$50k"
    assert usd_compact(Decimal("940")) == "$940"


# --- most-played tickers (#80) ----------------------------------------------
#
# Ranking is by completed round-trip count per coin over the fill window, with a
# currently-open episode adding to its coin's weight (a wallet parked in one big
# short has few trips but that coin is plainly its coin). Top 3, dex-prefixed
# builder-DEX coins rendered as the bare ticker, and no line at all when the fine
# store has nothing to rank.


def test_most_played_ranks_by_round_trip_count_and_takes_the_top_three() -> None:
    line = _render_most_played(
        [
            ("SOL", 9, False),
            ("BTC", 5, False),
            ("ETH", 3, False),
            ("DOGE", 1, False),
        ]
    )
    # 18 trips over 9/5/3/1 → inverse HHI 324/116 ≈ 2.8 effective coins (#95).
    assert line == "Most played: SOL · BTC · ETH (~2.8 coins)"


def test_most_played_counts_open_exposure_toward_its_coin() -> None:
    # A wallet sitting in one big BTC short for weeks has few completed BTC trips,
    # but the open position makes BTC plainly its coin — the open episode counts.
    line = _render_most_played(
        [
            ("ETH", 2, False),
            ("BTC", 1, True),
            ("SOL", 1, False),
        ]
    )
    # The open BTC ranks the coin, but the effective-coins annotation is trips
    # only (4 trips over 2/1/1 → 16/6 ≈ 2.7), blind to the open episode.
    assert line == "Most played: BTC · ETH · SOL (~2.7 coins)"


def test_most_played_reads_a_fifty_fifty_pair_as_two_effective_coins() -> None:
    line = _render_most_played([("SOL", 5, False), ("ETH", 5, False)])
    assert line == "Most played: ETH · SOL (~2 coins)"  # equal weight ties break on coin name


def test_most_played_includes_a_coin_that_is_only_open() -> None:
    # No completed trips yet, but a live episode is exposure worth surfacing —
    # and with zero trips the effective-coins annotation is dropped.
    line = _render_most_played([("BTC", 0, True)])
    assert line == "Most played: BTC"


def test_most_played_renders_dex_prefixed_coins_cleanly() -> None:
    line = _render_most_played([("xyz:SP500", 4, False), ("BTC", 2, False)])
    assert line == "Most played: SP500 · BTC (~1.8 coins)"


def test_most_played_is_omitted_when_there_is_nothing_to_rank() -> None:
    assert _render_most_played([]) is None


# --- the track-record header's trade-span label ------------------------------


def test_trades_span_label_reads_days_under_two_months() -> None:
    from datetime import UTC, datetime, timedelta

    from epigone.bot.handlers import _trades_span_label

    now = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    assert _trades_span_label(now - timedelta(days=10), now) == "trades from the last 10 days"
    assert _trades_span_label(now - timedelta(hours=3), now) == "trades from the last 1 day"


def test_trades_span_label_reads_months_beyond_two() -> None:
    from datetime import UTC, datetime, timedelta

    from epigone.bot.handlers import _trades_span_label

    now = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    assert _trades_span_label(now - timedelta(days=250), now) == "trades from the last ~8 months"


def test_trades_span_label_falls_back_without_trips() -> None:
    from datetime import UTC, datetime

    from epigone.bot.handlers import _trades_span_label

    now = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    assert _trades_span_label(None, now) == "from recent fills"
