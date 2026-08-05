"""ADR-0007's sizing and pricing rules as arithmetic (issues #136, #137).

Pure functions, so these tests pin the DECISIONS — Base Stake × mirrored
leverage, relative mirroring against actual held size, bounded slippage, venue
precision — without a database or a wire anywhere near them.

The sizing half was rewritten by amendment D-4 (#137): the configured dollars
used to fix the POSITION and now fix the MARGIN behind it. These tests are
where that change is nailed down, because it is a change no type system can
catch — the old and new calls differ by one argument and by a factor of the
leverage.
"""

from decimal import Decimal

import pytest

from epigone.execute.policy import UnpriceableStakeError, open_position_notional
from epigone.execute.pricing import (
    UnpriceableError,
    clamped_size,
    ioc_limit_price,
    open_size,
    relative_size,
    round_price,
    round_size,
    scale_fraction,
    trigger_price,
)


def test_a_position_is_the_stake_times_the_leverage() -> None:
    # Amendment D-4's whole sentence, as arithmetic: $100 of margin behind a
    # 10x leader is a $1,000 position, and the $100 is what can be lost.
    assert open_position_notional(Decimal("100"), Decimal("10")) == Decimal("1000")
    # 1x is the degenerate case where stake and position coincide — which is
    # exactly what Base Notional used to mean, so the old model is the new
    # model with the leverage dial at its floor.
    assert open_position_notional(Decimal("200"), Decimal("1")) == Decimal("200")


def test_a_stake_or_leverage_that_is_not_positive_is_an_error_not_a_zero() -> None:
    with pytest.raises(UnpriceableStakeError):
        open_position_notional(Decimal("0"), Decimal("10"))
    with pytest.raises(UnpriceableStakeError):
        open_position_notional(Decimal("100"), Decimal("0"))


def test_a_full_grant_sends_the_asked_size_untouched() -> None:
    # A clamp must be the ONLY thing that can change what was sized, so a full
    # grant does not re-derive the size by a second route that could disagree.
    assert clamped_size(
        Decimal("0.5"), asked_stake=Decimal("100"), granted_stake=Decimal("100"), sz_decimals=4
    ) == Decimal("0.5")


def test_a_clamped_grant_scales_the_size_by_the_same_ratio_and_rounds_down() -> None:
    # $100 asked, $40 given → 40% of the size, re-rounded DOWN at the asset's
    # precision so rounding can never put a clamp back over the cap.
    assert clamped_size(
        Decimal("0.5"), asked_stake=Decimal("100"), granted_stake=Decimal("40"), sz_decimals=4
    ) == Decimal("0.2")
    assert clamped_size(
        Decimal("0.00314"), asked_stake=Decimal("200"), granted_stake=Decimal("70"), sz_decimals=5
    ) == Decimal("0.00109")  # 0.001099 truncated, never rounded up


def test_a_clamp_that_rounds_to_nothing_is_an_error_not_a_zero_order() -> None:
    with pytest.raises(UnpriceableError):
        clamped_size(
            Decimal("0.01"), asked_stake=Decimal("100"), granted_stake=Decimal("1"), sz_decimals=2
        )


def test_an_open_sizes_stake_times_leverage_whatever_the_leader_holds() -> None:
    # Decision 2 as amended: the Leader's absolute size still NEVER determines
    # copy size — only their LEVERAGE does. $200 of stake at 1x is the old
    # $200 position…
    assert open_size(Decimal("200"), Decimal("1"), Decimal("2000"), 4) == Decimal("0.1")
    # …and the same stake at 10x is ten times the coin units, on the same mark.
    assert open_size(Decimal("200"), Decimal("10"), Decimal("2000"), 4) == Decimal("1")
    assert open_size(Decimal("100"), Decimal("10"), Decimal("63500"), 5) == Decimal("0.01574")


def test_an_open_rounds_size_down_so_it_never_asks_for_margin_it_lacks() -> None:
    # 200 / 63500 = 0.003149606…; at 5 dp that truncates rather than rounds up.
    assert open_size(Decimal("200"), Decimal("1"), Decimal("63500"), 5) == Decimal("0.00314")
    # And at a coarse precision, down is the only safe direction.
    assert open_size(Decimal("200"), Decimal("1"), Decimal("63500"), 3) == Decimal("0.003")


def test_a_size_too_small_to_express_is_an_error_not_a_zero_order() -> None:
    with pytest.raises(UnpriceableError):
        open_size(Decimal("200"), Decimal("1"), Decimal("63500"), 1)  # 0.0031… → 0.0


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
