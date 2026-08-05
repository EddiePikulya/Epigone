"""How Epigone writes a number for a human to read (issue #185).

Postgres hands a NUMERIC back as a Decimal that keeps the column's own idea of
where the digits sit, not the one a reader has: `100000` decodes to
`Decimal('1.0E+5')`, whose `str()` is `1.0E+5`. That is the same number, and it
is not the same sentence — a risk limit that reads `$1.0E+5` in a chat message
is a limit the operator has to decode before they can trust it (observed live
on the first `/limits` after A5).

So every number that leaves the system towards a person goes through here.
Deliberately a formatting rule and nothing else: it does not round, quantize or
abbreviate — `format_usd`-style shortening is a per-surface decision (see
`bot/format.py`), while "never in scientific notation" is not a preference any
surface should get to hold differently.
"""

from decimal import Decimal

__all__ = ["fixed_point"]


def fixed_point(value: Decimal | int) -> str:
    """`value` in fixed-point, never in scientific notation.

    Every digit the value carries is kept, trailing zeros included: a Decimal's
    exponent records the precision it was written with, and `250.00` came from
    somewhere that meant two decimal places. Only the *exponent form* is the
    bug, so only the exponent form is removed."""
    return format(value, "f") if isinstance(value, Decimal) else str(value)
