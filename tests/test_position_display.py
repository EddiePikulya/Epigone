"""Issue #35: the position display shows real margin (money at risk),
return-on-margin, and holding time — not just leveraged notional.

Pure-function coverage of the shared renderer (`_render_positions`), the age
formatter (`open_age`), and the `Position` margin/return fallbacks; the DB-backed
age lookup and the live call sites are exercised in test_track_wallet.py.
"""

from datetime import UTC, datetime
from decimal import Decimal

from epigone.bot.format import held_for, open_age
from epigone.bot.handlers import _render_positions
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
