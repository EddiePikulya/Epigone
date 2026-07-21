"""Issue #35: the position display shows real margin (money at risk),
return-on-margin, and holding time — not just leveraged notional.

Pure-function coverage of the shared renderer (`_render_positions`), the age
formatter (`open_age`), and the `Position` margin/return fallbacks; the DB-backed
age lookup and the live call sites are exercised in test_track_wallet.py.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from epigone.bot.format import held_for, open_age
from epigone.bot.handlers import _render_positions, _render_recent_activity
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


# --- the shared position renderer -------------------------------------------


def test_render_shows_margin_notional_and_return_on_margin() -> None:
    text = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert "$3,853 notional" in text  # the leveraged size, still shown
    assert "$96 margin" in text  # the real money at risk (issue #35)
    assert "40x" in text
    assert "+$344 (+357%)" in text  # uPnL made legible by return-on-margin


def test_render_shows_a_precise_age_for_a_freshly_opened_position() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text = _render_positions(WHALE, [BTC_LONG], ages={"BTC": (opened, False)}, now=NOW)
    assert "open 2d 4h" in text


def test_render_marks_a_baselined_positions_age_as_at_least() -> None:
    opened = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    text = _render_positions(WHALE, [BTC_LONG], ages={"BTC": (opened, True)}, now=NOW)
    assert "open ≥2d 4h" in text


def test_render_omits_age_when_no_snapshot_exists() -> None:
    # An untracked wallet's profile has no poller snapshot, so there's no honest
    # age to show — the line simply carries none rather than inventing one.
    text = _render_positions(WHALE, [BTC_LONG], ages={}, now=NOW)
    assert "open " not in text


def test_render_derives_return_on_margin_without_the_api_field() -> None:
    bare = Position(
        coin="ETH",
        side=Side.SHORT,
        size_usd=Decimal("2000"),
        leverage=Decimal("20"),
        entry_price=Decimal("1677.9"),
        unrealized_pnl=Decimal("-50"),
    )
    text = _render_positions(WHALE, [bare], ages={}, now=NOW)
    assert "$100 margin" in text
    assert "-$50 (-50%)" in text


def test_render_handles_no_open_positions() -> None:
    assert "no open positions" in _render_positions(WHALE, [], ages={}, now=NOW)


# --- recent-activity line (issue #72) ---------------------------------------
#
# Last-trade recency comes from the fine store's newest folded *perp* fill
# (window_end); the month PnL/ROI ride alongside from the coarse leaderboard.
# ROI is stored as a fraction (0.12 == 12%).


def test_activity_shows_last_trade_and_month_performance() -> None:
    two_hours_ago = NOW - timedelta(hours=2)  # fresh scan, precise recency
    line = _render_recent_activity(
        two_hours_ago, two_hours_ago, Decimal("48000"), Decimal("0.12"), NOW
    )
    assert line == "Last trade: 2h ago · month PnL +$48,000 (ROI +12%)"


def test_activity_marks_last_trade_as_of_last_scan_when_fills_knowledge_lags() -> None:
    # Our newest-fill knowledge is only as fresh as the last fine refresh; a scan
    # older than a day can't imply a live "last trade" time, so it hedges rather
    # than reading with false precision (same spirit as the ≥ open-age marker).
    three_days_ago = NOW - timedelta(days=3)
    line = _render_recent_activity(three_days_ago, three_days_ago, None, None, NOW)
    assert line == "Last trade: 3d ago (as of last scan)"


def test_activity_shows_only_recency_when_no_coarse_metrics() -> None:
    two_hours_ago = NOW - timedelta(hours=2)
    line = _render_recent_activity(two_hours_ago, two_hours_ago, None, None, NOW)
    assert line == "Last trade: 2h ago"


def test_activity_says_no_fills_seen_but_still_shows_coarse_performance() -> None:
    # Coarse leaderboard data exists even for a wallet with no captured fills, so
    # the performance line must not depend on fine availability.
    line = _render_recent_activity(None, None, Decimal("3000000"), Decimal("0.21"), NOW)
    assert line == "No recent trading activity seen · month PnL +$3,000,000 (ROI +21%)"


def test_activity_says_no_fills_seen_when_nothing_is_known() -> None:
    assert _render_recent_activity(None, None, None, None, NOW) == "No recent trading activity seen"
