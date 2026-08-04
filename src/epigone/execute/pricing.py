"""Turning ADR-0007's sizing rules into orders the exchange will accept.

Three separate jobs, all pure functions so the executor's tests can pin the
arithmetic without a database or a wire.

**Sizing (decision 2).** A copied OPEN is Base Notional at the current mark,
in coin units — the Leader's absolute size never enters it. A scale is
RELATIVE: a 50% scale-in grows the copy 50%, a 30% trim sells 30% — and,
crucially, of what we ACTUALLY HOLD (decision 10's self-damping principle),
never of a bookkept expectation. The Leader's own event supplies only the
FRACTION, computed from their before/after sizes.

**Precision.** Hyperliquid refuses a size with more decimals than the asset's
`szDecimals`, and a perp price with more than 5 significant figures or more
than 6 − szDecimals decimals. Both roundings go the CONSERVATIVE way: sizes
round DOWN (a rounded-up entry would ask for margin the allocation may not
have; a rounded-up trim would sell more than the Leader did) and IOC limit
prices round toward the LESS aggressive side, so the slippage cap is a
ceiling that is never quietly exceeded. At a 5-significant-figure tick the
rounding is ~0.001% of the price against a 1% cap, so it cannot meaningfully
cost a fill.

**The IOC price (decision 4).** A market order is expressed the way the
protocol expresses it: tif=IOC at a price aggressive enough to cross,
bounded by the slippage cap. Nothing entry-shaped ever rests, which is what
makes the "the Leader closes while our entry rests" edge dissolve.
"""

from decimal import ROUND_DOWN, ROUND_UP, Decimal

from epigone.execute.policy import SLIPPAGE_CAP

# The wire's own precision floor (epigone.gateway.execution.decimal_to_wire).
MAX_WIRE_DECIMALS = 8
# Perp price rule: at most 5 significant figures, and at most 6 − szDecimals
# decimal places. Both bounds apply; the tighter one wins.
PRICE_SIGNIFICANT_FIGURES = 5
PERP_PRICE_DECIMAL_BUDGET = 6


class UnpriceableError(ValueError):
    """A size or price could not be expressed on the wire — a non-positive
    mark, or a size that rounds to zero at the asset's precision. Raised
    rather than returned as a sentinel: every caller must decide to SKIP and
    say why, and none of them may quietly send a zero."""


def round_size(size_coin: Decimal, sz_decimals: int) -> Decimal:
    """A coin-unit size at the venue's precision, rounded DOWN.

    Down for both directions and for the same reason each time: an entry
    rounded up asks for margin the allocation may not have, and an exit
    rounded up sells more than the Leader did. The residue an exit leaves is
    covered by decision 5's bounded retry and by the next reconcile."""
    if sz_decimals < 0 or sz_decimals > MAX_WIRE_DECIMALS:
        raise UnpriceableError(f"szDecimals {sz_decimals} is outside the wire's precision")
    quantum = Decimal(1).scaleb(-sz_decimals)
    rounded = size_coin.quantize(quantum, rounding=ROUND_DOWN)
    if rounded <= 0:
        raise UnpriceableError(
            f"size {size_coin} rounds to zero at {sz_decimals} decimal places — "
            f"too small to express on this asset"
        )
    return rounded


def open_size(notional_usd: Decimal, mark: Decimal, sz_decimals: int) -> Decimal:
    """Base Notional at the mark, in coin units (decision 2). The Leader's own
    size is not an input — that is the whole point of fixed sizing: the
    5%-of-their-account vs 40%-of-ours conflict dissolves by construction, and
    the model is robust to the stale leader-equity data that made 38% of
    screened wallets look alive after they had emptied out."""
    if mark <= 0:
        raise UnpriceableError(f"mark price {mark} is not positive")
    return round_size(notional_usd / mark, sz_decimals)


def scale_fraction(prev_size: Decimal | None, new_size: Decimal | None) -> Decimal:
    """How much a Leader's scale changed their position, as a fraction of what
    they held before: +0.5 for a 50% scale-in, 0.3 for a 30% trim (unsigned —
    the caller already knows the direction from the event kind).

    Coin units ONLY. ADR-0006 records NULL coin units as "size not mirrorable"
    and forbids inventing them from the notional, and a USD ratio here would
    be exactly that invention: the two notionals are marked at DIFFERENT
    observations, so their ratio conflates the Leader's size change with the
    price move between polls. A caller handed None must skip and say so."""
    if prev_size is None or new_size is None:
        raise UnpriceableError("the event carries no coin-unit sizes — not mirrorable")
    if prev_size <= 0:
        raise UnpriceableError(f"previous size {prev_size} is not positive")
    return abs(new_size - prev_size) / prev_size


def relative_size(held_coin: Decimal, fraction: Decimal, sz_decimals: int) -> Decimal:
    """A fraction of what we ACTUALLY hold, at the venue's precision.

    `held_coin` comes from the exchange, never from bookkeeping — that is
    decision 10's self-damping principle, and it is why a divergence converges
    instead of compounding."""
    if held_coin <= 0:
        raise UnpriceableError(f"held size {held_coin} is not positive")
    return round_size(held_coin * fraction, sz_decimals)


def round_price(price: Decimal, sz_decimals: int, *, rounding: str) -> Decimal:
    """A perp price at the venue's rules: ≤5 significant figures AND ≤
    6 − szDecimals decimal places, whichever binds tighter.

    INTEGER prices are the exchange's own carve-out from the figure rule — a
    $123,456 mark has seven significant figures and is legal, while $123,456.7
    is not. So a price whose integer part already spends the budget rounds to
    a whole number rather than keeping a decimal the venue would refuse."""
    decimals = max(PERP_PRICE_DECIMAL_BUDGET - sz_decimals, 0)
    integer_digits = len(str(int(abs(price)))) if abs(price) >= 1 else 0
    if integer_digits >= PRICE_SIGNIFICANT_FIGURES:
        allowed = 0  # integers are always expressible; a decimal here is not
    elif integer_digits > 0:
        allowed = PRICE_SIGNIFICANT_FIGURES - integer_digits
    else:
        # Sub-$1: five figures counted from the first non-zero digit, e.g.
        # 0.0012345. `adjusted()` is that digit's exponent (-3 for 0.0012…).
        allowed = PRICE_SIGNIFICANT_FIGURES - 1 - price.adjusted()
    capped = price.quantize(
        Decimal(1).scaleb(-max(min(decimals, allowed), 0)), rounding=rounding
    )
    if capped <= 0:
        raise UnpriceableError(f"price {price} rounds to zero at this asset's precision")
    return capped


def ioc_limit_price(
    mark: Decimal, *, is_buy: bool, sz_decimals: int, cap: Decimal = SLIPPAGE_CAP
) -> Decimal:
    """The bounded-slippage IOC price (decision 4): mark ± the cap, aggressive
    enough to cross, and never worse than the cap.

    Rounding goes toward the LESS aggressive side — a buy's ceiling rounds
    down, a sell's floor rounds up — so the cap is a ceiling the wire can
    never quietly exceed. That direction can in principle cost a fill; at a
    5-significant-figure tick it moves the bound by ~0.001% against a 1% cap,
    which is why the trade is worth making in this direction and not the
    other."""
    if mark <= 0:
        raise UnpriceableError(f"mark price {mark} is not positive")
    bound = mark * (Decimal(1) + cap) if is_buy else mark * (Decimal(1) - cap)
    return round_price(bound, sz_decimals, rounding=ROUND_DOWN if is_buy else ROUND_UP)


def trigger_price(
    entry: Decimal, *, pct: Decimal, is_long: bool, take_profit: bool, sz_decimals: int
) -> Decimal:
    """A bracket leg's arming price, anchored to OUR fill (decision 6).

    A long takes profit above entry and stops below; a short is the mirror.
    Rounding is toward the SAFER side of each leg — a stop rounds toward entry
    (triggering marginally early) and a take-profit rounds toward entry too
    (banking marginally early) — because both errors are small and neither
    silently widens the risk the operator configured."""
    if entry <= 0:
        raise UnpriceableError(f"entry price {entry} is not positive")
    move = entry * pct / Decimal(100)
    above = take_profit == is_long
    price = entry + move if above else entry - move
    return round_price(price, sz_decimals, rounding=ROUND_DOWN if above else ROUND_UP)
