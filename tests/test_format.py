"""Pure formatting helpers (epigone.bot.format). No DB or transport — just the
tricky rounding boundaries the seam tests only exercise incidentally."""

from decimal import Decimal

import pytest

from epigone.bot.format import usd_size


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        ("6900", "$6.9k"),  # the #123 spec example: k-tenth kept
        ("127600", "$127.6k"),
        ("20000", "$20k"),  # a round k drops its trailing .0
        ("6000", "$6k"),
        ("1100000", "$1.1M"),
        ("2000000", "$2M"),  # a round M drops its trailing .0 too
        ("940", "$940"),  # sub-thousand reads in full
        ("0", "$0"),
    ],
)
def test_usd_size_keeps_one_km_decimal_and_trims_round_values(
    amount: str, expected: str
) -> None:
    assert usd_size(Decimal(amount)) == expected
