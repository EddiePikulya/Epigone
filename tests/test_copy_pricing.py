"""ADR-0007's sizing and pricing rules as arithmetic (issue #136).

Pure functions, so these tests pin the DECISIONS — fixed Base Notional,
relative mirroring against actual held size, bounded slippage, venue
precision — without a database or a wire anywhere near them.
"""

from decimal import Decimal

import pytest

from epigone.execute.pricing import (
    UnpriceableError,
    ioc_limit_price,
    open_size,
    relative_size,
    round_price,
    round_size,
    scale_fraction,
    trigger_price,
)


def test_an_open_is_base_notional_at_the_mark_whatever_the_leader_holds() -> None:
    # Decision 2: the Leader's absolute size NEVER determines copy size. Two
    # leaders with wildly different books produce the identical copy.
    assert open_size(Decimal("200"), Decimal("2000"), 4) == Decimal("0.1")
    assert open_size(Decimal("200"), Decimal("63500"), 5) == Decimal("0.00314")


def test_an_open_rounds_size_down_so_it_never_asks_for_margin_it_lacks() -> None:
    # 200 / 63500 = 0.003149606…; at 5 dp that truncates rather than rounds up.
    assert open_size(Decimal("200"), Decimal("63500"), 5) == Decimal("0.00314")
    # And at a coarse precision, down is the only safe direction.
    assert open_size(Decimal("200"), Decimal("63500"), 3) == Decimal("0.003")


def test_a_size_too_small_to_express_is_an_error_not_a_zero_order() -> None:
    with pytest.raises(UnpriceableError):
        open_size(Decimal("200"), Decimal("63500"), 1)  # 0.0031… → 0.0


def test_a_scale_supplies_only_the_fraction_and_it_comes_from_coin_units() -> None:
    # A 50% scale-in and a 30% trim, as the Leader's before/after sizes.
    assert scale_fraction(Decimal("2"), Decimal("3")) == Decimal("0.5")
    assert scale_fraction(Decimal("10"), Decimal("7")) == Decimal("0.3")


def test_a_scale_without_coin_units_is_not_mirrorable_rather_than_guessed() -> None:
    # ADR-0006 forbids inventing units from the notional, and a USD ratio
    # would be exactly that: the two notionals are marked at DIFFERENT
    # observations, so their ratio mixes the size change with the price move.
    with pytest.raises(UnpriceableError):
        scale_fraction(None, Decimal("3"))
    with pytest.raises(UnpriceableError):
        scale_fraction(Decimal("2"), None)


def test_a_relative_size_applies_to_what_we_actually_hold() -> None:
    # Decision 10's self-damping principle: 30% of the REAL position. If a
    # partial fill left us with 0.8 rather than the 1.0 we expected, the trim
    # is 0.24, not 0.30 — divergences converge instead of compounding.
    assert relative_size(Decimal("0.8"), Decimal("0.3"), 4) == Decimal("0.24")
    assert relative_size(Decimal("1.0"), Decimal("0.3"), 4) == Decimal("0.3")


def test_an_ioc_buy_is_capped_above_the_mark_and_a_sell_below_it() -> None:
    # Decision 4: aggressive enough to cross, never past the 1% cap.
    assert ioc_limit_price(Decimal("2000"), is_buy=True, sz_decimals=4) == Decimal("2020.0")
    assert ioc_limit_price(Decimal("2000"), is_buy=False, sz_decimals=4) == Decimal("1980.0")


def test_the_slippage_cap_is_a_ceiling_rounding_can_never_exceed() -> None:
    # 63500 * 1.01 = 64135; a buy's bound rounds DOWN, a sell's UP, so the
    # rounded price is always at least as tight as the configured cap.
    mark = Decimal("63500")
    buy = ioc_limit_price(mark, is_buy=True, sz_decimals=5)
    sell = ioc_limit_price(mark, is_buy=False, sz_decimals=5)
    assert buy <= mark * Decimal("1.01")
    assert sell >= mark * Decimal("0.99")
    # …and still aggressive enough to cross the mark in the right direction.
    assert buy > mark > sell


def test_prices_obey_five_significant_figures_and_the_decimal_budget() -> None:
    from decimal import ROUND_DOWN

    # 5 significant figures binds on a mid-priced coin…
    assert round_price(Decimal("2020.0499"), 4, rounding=ROUND_DOWN) == Decimal("2020")
    # …and on a cheap one whose size precision leaves room to spare
    # (szDecimals 0 → a 6-decimal budget, so the figure rule is what bites).
    assert round_price(Decimal("1.23456789"), 0, rounding=ROUND_DOWN) == Decimal("1.2345")
    # …while the 6 − szDecimals decimal budget binds when it is the tighter
    # of the two (szDecimals 4 → 2 decimals, coarser than 5 figures).
    assert round_price(Decimal("1.23456789"), 4, rounding=ROUND_DOWN) == Decimal("1.23")
    # …and a large integer price keeps every digit it needs.
    assert round_price(Decimal("123456.7"), 5, rounding=ROUND_DOWN) == Decimal("123456")


def test_sub_dollar_prices_keep_five_figures_from_the_first_real_digit() -> None:
    from decimal import ROUND_DOWN

    assert round_price(Decimal("0.00123456"), 0, rounding=ROUND_DOWN) == Decimal("0.001234")


def test_a_bracket_anchors_to_our_fill_and_mirrors_for_a_short() -> None:
    # Decision 6: the percentages are applied at OUR fill price — a long takes
    # profit above and stops below; a short is the mirror image.
    entry = Decimal("2000")
    assert trigger_price(
        entry, pct=Decimal("10"), is_long=True, take_profit=True, sz_decimals=4
    ) == Decimal("2200")
    assert trigger_price(
        entry, pct=Decimal("5"), is_long=True, take_profit=False, sz_decimals=4
    ) == Decimal("1900")
    assert trigger_price(
        entry, pct=Decimal("10"), is_long=False, take_profit=True, sz_decimals=4
    ) == Decimal("1800")
    assert trigger_price(
        entry, pct=Decimal("5"), is_long=False, take_profit=False, sz_decimals=4
    ) == Decimal("2100")


def test_round_size_refuses_a_precision_the_wire_cannot_carry() -> None:
    with pytest.raises(UnpriceableError):
        round_size(Decimal("1"), 9)
