"""Unit tests for the ExecutionGateway seam's types, taxonomy, nonces, and
fake (issue #133) — pure in-memory, no network, no key material beyond
ephemeral in-test values.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from epigone.gateway.execution import (
    ActionRejectedError,
    CancelOk,
    CancelSpec,
    CloidCancelSpec,
    Grouping,
    ModifySpec,
    NonceSource,
    OrderRejected,
    OrderResting,
    OrderSpec,
    RejectReason,
    TpSl,
    Trigger,
    classify_reject,
    decimal_to_wire,
    exchange_action_weight,
    timestamp_ms,
)
from epigone.gateway.execution_fake import FakeExecutionGateway
from tests.support.clock import FakeClock

CLOID = "0x" + "ab" * 16


def order(**overrides: object) -> OrderSpec:
    defaults: dict[str, object] = {
        "asset": 4,
        "is_buy": True,
        "size": Decimal("0.5"),
        "limit_price": Decimal("1800"),
    }
    defaults.update(overrides)
    return OrderSpec(**defaults)  # type: ignore[arg-type]


# --- wire encoding -----------------------------------------------------------


def test_decimal_to_wire_normalizes_like_the_sdk() -> None:
    # The SDK's float_to_wire normal form: no trailing zeros, no exponent.
    assert decimal_to_wire(Decimal("1.2300")) == "1.23"
    assert decimal_to_wire(Decimal("100")) == "100"
    assert decimal_to_wire(Decimal("1E+2")) == "100"
    assert decimal_to_wire(Decimal("0.00000001")) == "0.00000001"
    assert decimal_to_wire(Decimal("0")) == "0"
    assert decimal_to_wire(Decimal("0.000")) == "0"


def test_decimal_to_wire_rejects_more_than_8_decimals() -> None:
    with pytest.raises(ValueError):
        decimal_to_wire(Decimal("0.000000001"))


def test_decimal_to_wire_rejects_non_finite() -> None:
    with pytest.raises(ValueError):
        decimal_to_wire(Decimal("NaN"))
    with pytest.raises(ValueError):
        decimal_to_wire(Decimal("Infinity"))


# --- spec validation ---------------------------------------------------------


def test_order_spec_rejects_non_positive_size_and_price() -> None:
    with pytest.raises(ValueError):
        order(size=Decimal("0"))
    with pytest.raises(ValueError):
        order(size=Decimal("-1"))
    with pytest.raises(ValueError):
        order(limit_price=Decimal("0"))


def test_order_spec_rejects_a_malformed_cloid() -> None:
    with pytest.raises(ValueError):
        order(cloid="not-a-cloid")
    with pytest.raises(ValueError):
        order(cloid="0x" + "AB" * 16)  # uppercase hex is not the wire form
    with pytest.raises(ValueError):
        order(cloid="0x" + "ab" * 15)  # too short
    order(cloid=CLOID)  # the wire form passes


def test_cloid_cancel_spec_rejects_a_malformed_cloid() -> None:
    with pytest.raises(ValueError):
        CloidCancelSpec(asset=4, cloid="77")
    CloidCancelSpec(asset=4, cloid=CLOID)


def test_trigger_rejects_a_non_positive_price() -> None:
    with pytest.raises(ValueError):
        Trigger(trigger_price=Decimal("0"), is_market=True, tpsl=TpSl.STOP_LOSS)


# --- reject taxonomy ---------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        # The GitBook error-responses table's documented reject classes
        # (research §2), verbatim where the docs quote them.
        ("Order must have minimum value of $10.", RejectReason.MIN_NOTIONAL),
        ("Insufficient margin to place order. asset=4", RejectReason.INSUFFICIENT_MARGIN),
        (
            "Post only order would have immediately matched, bbo was 1800.1@1800.2. asset=4",
            RejectReason.POST_ONLY_CROSS,
        ),
        (
            "Order could not immediately match against any resting orders. asset=4",
            RejectReason.NO_IMMEDIATE_MATCH,
        ),
        ("Invalid nonce", RejectReason.INVALID_NONCE),
        ("User or API Wallet 0xf5d8a3bd12aef7a8ca4d3f4a02db4a71ff3c9e21 does not exist.",
         RejectReason.UNAUTHORIZED_SIGNER),
        ("Price must be divisible by tick size. asset=4", RejectReason.TICK_PRICE),
        (
            "Order price cannot be more than 80% away from the reference price",
            RejectReason.PRICE_BAND,
        ),
        ("Reduce only order would increase position. asset=4", RejectReason.REDUCE_ONLY_VIOLATION),
        (
            "Order was never placed, already canceled, or filled. asset=4",
            RejectReason.MISSING_ORDER,
        ),
        ("Invalid TP/SL price. asset=4", RejectReason.BAD_TRIGGER_PRICE),
        ("Order would exceed the open interest cap.", RejectReason.OPEN_INTEREST_CAP),
        ("something the exchange never said before", RejectReason.UNKNOWN),
    ],
)
def test_classify_reject_maps_documented_strings(message: str, reason: RejectReason) -> None:
    assert classify_reject(message) is reason


def test_action_rejected_error_carries_the_classified_reason_and_raw_message() -> None:
    error = ActionRejectedError("Invalid nonce")
    assert error.reason is RejectReason.INVALID_NONCE
    assert error.message == "Invalid nonce"


# --- nonces ------------------------------------------------------------------


def test_nonces_follow_the_clock_in_milliseconds() -> None:
    clock = FakeClock(datetime(2026, 7, 27, 12, 0, tzinfo=UTC))
    nonces = NonceSource(clock)
    assert nonces.next() == timestamp_ms(clock.now())


def test_same_millisecond_nonces_still_strictly_increase() -> None:
    # The docs' atomic-counter advice (research §2): concurrent calls inside
    # one ms must not collide — a reused nonce is a rejected action.
    clock = FakeClock()
    nonces = NonceSource(clock)
    values = [nonces.next() for _ in range(5)]
    assert values == sorted(set(values))


def test_a_backwards_clock_step_never_reissues_a_nonce() -> None:
    clock = FakeClock()
    nonces = NonceSource(clock)
    first = nonces.next()
    clock.advance(-3600)
    assert nonces.next() > first


# --- weight ------------------------------------------------------------------


def test_exchange_action_weight_is_1_plus_batch_over_40() -> None:
    # Research §5: an exchange action weighs 1 + floor(batch_length / 40).
    assert exchange_action_weight(1) == 1
    assert exchange_action_weight(39) == 1
    assert exchange_action_weight(40) == 2
    assert exchange_action_weight(120) == 4


# --- the fake ----------------------------------------------------------------


async def test_fake_records_actions_in_submission_order() -> None:
    fake = FakeExecutionGateway()
    await fake.place_orders([order()])
    await fake.cancel_orders([CancelSpec(asset=4, oid=77)])
    await fake.schedule_cancel(None)
    assert [method for method, _ in fake.actions] == [
        "place_orders",
        "cancel_orders",
        "schedule_cancel",
    ]


async def test_fake_orders_rest_under_fresh_oids_by_default() -> None:
    fake = FakeExecutionGateway()
    first = await fake.place_orders([order(), order(is_buy=False)])
    second = await fake.place_orders([order(cloid=CLOID)])
    assert first == [OrderResting(oid=1), OrderResting(oid=2)]
    assert second == [OrderResting(oid=3, cloid=CLOID)]


async def test_fake_serves_configured_results_then_defaults() -> None:
    fake = FakeExecutionGateway()
    rejected = OrderRejected(
        reason=RejectReason.MIN_NOTIONAL, message="Order must have minimum value of $10."
    )
    fake.place_results.append([rejected])
    assert await fake.place_orders([order()]) == [rejected]
    assert await fake.place_orders([order()]) == [OrderResting(oid=1)]


async def test_fake_raises_a_configured_error_on_the_next_call() -> None:
    fake = FakeExecutionGateway()
    fake.errors.append(ActionRejectedError("Invalid nonce"))
    with pytest.raises(ActionRejectedError):
        await fake.cancel_orders([CancelSpec(asset=4, oid=77)])
    # The failed call is still recorded — the exchange saw it before rejecting.
    assert len(fake.actions) == 1
    assert await fake.cancel_orders([CancelSpec(asset=4, oid=78)]) == [CancelOk()]


async def test_fake_records_grouping_and_modify_payloads() -> None:
    fake = FakeExecutionGateway()
    tp = order(
        is_buy=False,
        trigger=Trigger(trigger_price=Decimal("2000"), is_market=True, tpsl=TpSl.TAKE_PROFIT),
    )
    await fake.place_orders([order(), tp], grouping=Grouping.NORMAL_TPSL)
    await fake.modify_orders([ModifySpec(oid=5, order=order())])
    method, payload = fake.actions[0]
    assert method == "place_orders"
    assert payload == ([order(), tp], Grouping.NORMAL_TPSL, None)
    assert fake.actions[1] == ("modify_orders", [ModifySpec(oid=5, order=order())])
