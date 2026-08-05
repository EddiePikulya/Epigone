"""Replayed-response tests for HttpExecutionGateway (issue #133).

A local HTTP server stands in for POST /exchange, so the real gateway code
runs a full sign → post → parse cycle. The load-bearing assertion is
SIGNATURE RECOVERY: every payload the server receives is re-hashed with the
SDK's own recover helper and must recover to the signer's address — proving
our wire construction (key order included, which the msgpack signature
covers) matches the SDK's byte-for-byte. Keys here are ephemeral in-test
constants for a local server; nothing ever touches a real network.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from eth_account import Account
from hyperliquid.utils.signing import recover_agent_or_user_from_l1_action

import epigone.gateway.execution_http as execution_http
from epigone.gateway.execution import (
    ActionRejectedError,
    AmbiguousExecutionError,
    BuilderFee,
    CancelOk,
    CancelRejected,
    CancelSpec,
    CloidCancelSpec,
    ExecutionError,
    ExecutionRateLimitedError,
    Grouping,
    MainnetNotEnabledError,
    MasterKeySignerError,
    ModifySpec,
    OrderFilled,
    OrderRejected,
    OrderResting,
    OrderSpec,
    RejectReason,
    TpSl,
    Trigger,
)
from epigone.gateway.execution_http import (
    MAINNET_EXCHANGE_URL,
    RATE_LIMIT_MAX_TRIES,
    HttpExecutionGateway,
)
from tests.support.clock import FakeClock

# Ephemeral test-only key material: a fixed private key for the agent signer
# (deterministic signatures make failures reproducible) and an unrelated
# address as the traded master. Never funded, never near a real endpoint.
AGENT = Account.from_key("0x" + "ab" * 32)
MASTER = "0x1234567890AbcdEF1234567890aBcdef12345678"

CLOID = "0x" + "cd" * 16

OK_DEFAULT = {"status": "ok", "response": {"type": "default"}}


def ok_statuses(*statuses: Any) -> dict[str, Any]:
    return {"status": "ok", "response": {"type": "order", "data": {"statuses": list(statuses)}}}


class RecordingBudget:
    def __init__(self) -> None:
        self.spends: list[int] = []

    async def spend(self, weight: int) -> None:
        self.spends.append(weight)

    async def settle(self, weight: int) -> None:  # pragma: no cover - never billed
        raise AssertionError("exchange responses carry no settled surcharge")


class Harness:
    def __init__(
        self,
        gateway: HttpExecutionGateway,
        received: list[Any],
        budget: RecordingBudget,
        clock: FakeClock,
    ) -> None:
        self.gateway = gateway
        self.received = received
        self.budget = budget
        self.clock = clock

    def recovered_signer(self, payload: Any) -> str:
        """Recover the signing address from a received payload exactly as the
        exchange does — from the JSON it was handed, not from anything we
        kept client-side."""
        recovered = recover_agent_or_user_from_l1_action(
            payload["action"],
            payload["signature"],
            payload["vaultAddress"],
            payload["nonce"],
            payload["expiresAfter"],
            False,  # the test server is not MAINNET_EXCHANGE_URL
        )
        assert isinstance(recovered, str)
        return recovered


@pytest.fixture
async def replaying(request: pytest.FixtureRequest) -> AsyncGenerator[Any, None]:
    """A factory for gateways whose exchange URL points at a local server
    replaying a queue of responses (each entry a JSON body, or an int for a
    429 status)."""
    exits: list[Any] = []

    async def build(
        *responses: Any,
        retry_after: str | None = None,
        handler_sleep: float = 0.0,
    ) -> Harness:
        received: list[Any] = []
        queue = list(responses)

        async def exchange(request: web.Request) -> web.Response:
            received.append(await request.json())
            if handler_sleep:
                await asyncio.sleep(handler_sleep)
            body = queue.pop(0) if queue else OK_DEFAULT
            if body == 429:
                headers = {"Retry-After": retry_after} if retry_after is not None else {}
                return web.Response(status=429, headers=headers)
            return web.json_response(body)

        app = web.Application()
        app.router.add_post("/exchange", exchange)
        server = TestServer(app)
        await server.start_server()
        session = aiohttp.ClientSession()
        clock = FakeClock(datetime(2026, 7, 27, 12, 0, tzinfo=UTC))
        budget = RecordingBudget()
        gateway = HttpExecutionGateway(
            session,
            clock,
            budget,
            signer=AGENT,
            master_address=MASTER,
            exchange_url=str(server.make_url("/exchange")),
            rng=lambda: 1.0,
        )
        exits.append((session, server))
        return Harness(gateway, received, budget, clock)

    yield build
    for session, server in exits:
        await session.close()
        await server.close()


# --- agent-key-only, by construction -----------------------------------------


def test_a_signer_that_is_the_master_is_refused_at_construction() -> None:
    # The ADR-0005 invariant made structural: the key that owns the traded
    # account can never sign on this path, checksum-case notwithstanding.
    with pytest.raises(MasterKeySignerError):
        HttpExecutionGateway(
            None,  # type: ignore[arg-type]  # refused before any use
            FakeClock(),
            RecordingBudget(),
            signer=AGENT,
            master_address=AGENT.address.upper().replace("0X", "0x"),
        )


def test_a_mainnet_url_is_refused_until_a5_passes_the_capability() -> None:
    # The A5 gate (PR #140 review): "testnet-only pre-A5" is construction,
    # not convention — nothing in the codebase passes allow_mainnet=True.
    with pytest.raises(MainnetNotEnabledError):
        HttpExecutionGateway(
            None,  # type: ignore[arg-type]  # refused before any use
            FakeClock(),
            RecordingBudget(),
            signer=AGENT,
            master_address=MASTER,
            exchange_url=MAINNET_EXCHANGE_URL,
        )
    # The capability itself works — A5 is the only intended caller.
    HttpExecutionGateway(
        None,  # type: ignore[arg-type]  # construction only
        FakeClock(),
        RecordingBudget(),
        signer=AGENT,
        master_address=MASTER,
        exchange_url=MAINNET_EXCHANGE_URL,
        allow_mainnet=True,
    )


# --- wire shapes and signatures ----------------------------------------------


async def test_an_order_payload_signs_and_recovers_to_the_agent(replaying: Any) -> None:
    h = await replaying(ok_statuses({"resting": {"oid": 77}}))
    await h.gateway.place_orders(
        [OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))]
    )
    (payload,) = h.received
    assert payload["action"] == {
        "type": "order",
        "orders": [
            {"a": 4, "b": True, "p": "1800", "s": "0.5", "r": False, "t": {"limit": {"tif": "Gtc"}}}
        ],
        "grouping": "na",
    }
    assert payload["vaultAddress"] is None
    assert payload["expiresAfter"] is None
    assert h.recovered_signer(payload) == AGENT.address


async def test_a_sub_account_order_carries_the_vault_in_body_and_signature(
    replaying: Any,
) -> None:
    """The copy lane's central wire fact (issue #136, ADR-0007 decision 1):
    an order for a Copy Sub-account names the sub in `vaultAddress`, and that
    field is COVERED BY the signature — recovery only yields the agent when
    the verifier is handed the same vault the signer hashed. The address is
    lowercased on the way out (finding 2)."""
    sub = "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
    h = await replaying(ok_statuses({"filled": {"oid": 9, "totalSz": "0.5", "avgPx": "1800"}}))
    await h.gateway.place_orders(
        [OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))],
        vault_address=sub,
    )
    (payload,) = h.received
    assert payload["vaultAddress"] == sub.lower()
    assert h.recovered_signer(payload) == AGENT.address
    # And the negative that proves the signature really covers it: verify the
    # same payload as if it had been sent for the MASTER and the recovered
    # address is a stranger, not the agent.
    assert (
        recover_agent_or_user_from_l1_action(
            payload["action"],
            payload["signature"],
            None,
            payload["nonce"],
            payload["expiresAfter"],
            False,
        )
        != AGENT.address
    )


async def test_a_sub_account_cancel_carries_the_vault_too(replaying: Any) -> None:
    # The sweep's half of the pair: A4 is the first thing that can place on a
    # sub, so /kill has to be able to cancel on one (ADR-0007 decision 1).
    sub = "0xabcdef0123456789abcdef0123456789abcdef01"
    h = await replaying(
        {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}
    )
    await h.gateway.cancel_orders([CancelSpec(asset=4, oid=77)], vault_address=sub)
    (payload,) = h.received
    assert payload["vaultAddress"] == sub
    assert h.recovered_signer(payload) == AGENT.address


async def test_sub_account_provisioning_signs_the_documented_wire(replaying: Any) -> None:
    """createSubAccount answers the new address in `data` (finding 3), and
    subAccountTransfer carries a LOWERCASE subAccountUser with micro-USD
    (finding 6) — both signed by the agent on the master's own lane, no vault
    flag, because provisioning acts as the master."""
    h = await replaying(
        {
            "status": "ok",
            "response": {"type": "createSubAccount", "data": "0xB5836370" + "aa" * 16},
        },
        OK_DEFAULT,
    )
    address = await h.gateway.create_sub_account("epicopy-1")
    await h.gateway.sub_account_transfer(address, is_deposit=True, usd_micro=200_000_000)
    created, funded = h.received
    assert created["action"] == {"type": "createSubAccount", "name": "epicopy-1"}
    assert created["vaultAddress"] is None
    assert address == ("0xB5836370" + "aa" * 16).lower()
    assert funded["action"] == {
        "type": "subAccountTransfer",
        "subAccountUser": address,
        "isDeposit": True,
        "usd": 200_000_000,
    }
    assert h.recovered_signer(created) == AGENT.address
    assert h.recovered_signer(funded) == AGENT.address


async def test_renaming_a_sub_account_signs_the_probed_wire(replaying: Any) -> None:
    """`subAccountModify` renames an existing sub (finding 11, probed
    2026-08-05). The SDK has no method for it, so the key order and the
    lowercased subAccountUser are this gateway's to get right — the recovered
    signer is what proves it did."""
    sub = "0xB583637E" + "aa" * 16
    h = await replaying(OK_DEFAULT)

    await h.gateway.rename_sub_account(sub, "epicopy-leader")

    (renamed,) = h.received
    assert renamed["action"] == {
        "type": "subAccountModify",
        "subAccountUser": sub.lower(),
        "name": "epicopy-leader",
    }
    assert renamed["vaultAddress"] is None
    assert h.recovered_signer(renamed) == AGENT.address


async def test_a_create_sub_account_without_an_address_is_ambiguous(replaying: Any) -> None:
    # A sub may exist under a name that can never be freed (finding 10 caps
    # the master at 10), so an unreadable ack must not read as "nothing
    # happened" — a blind retry would spend another slot.
    h = await replaying({"status": "ok", "response": {"type": "createSubAccount"}})
    with pytest.raises(AmbiguousExecutionError):
        await h.gateway.create_sub_account("epicopy-1")


async def test_a_tpsl_leg_with_cloid_and_builder_rides_the_documented_wire(replaying: Any) -> None:
    h = await replaying(ok_statuses({"resting": {"oid": 1}}, {"resting": {"oid": 2}}))
    entry = OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))
    stop = OrderSpec(
        asset=4,
        is_buy=False,
        size=Decimal("0.5"),
        limit_price=Decimal("1650"),
        reduce_only=True,
        cloid=CLOID,
        trigger=Trigger(trigger_price=Decimal("1700"), is_market=True, tpsl=TpSl.STOP_LOSS),
    )
    await h.gateway.place_orders(
        [entry, stop],
        grouping=Grouping.NORMAL_TPSL,
        builder=BuilderFee(address="0xAA" + "00" * 19, fee_tenth_bp=10),
    )
    (payload,) = h.received
    assert payload["action"]["grouping"] == "normalTpsl"
    assert payload["action"]["builder"] == {"b": "0xaa" + "00" * 19, "f": 10}
    assert payload["action"]["orders"][1] == {
        "a": 4,
        "b": False,
        "p": "1650",
        "s": "0.5",
        "r": True,
        "t": {"trigger": {"isMarket": True, "triggerPx": "1700", "tpsl": "sl"}},
        "c": CLOID,
    }
    assert h.recovered_signer(payload) == AGENT.address


async def test_cancels_modify_leverage_and_schedule_cancel_sign_and_recover(
    replaying: Any,
) -> None:
    h = await replaying(
        {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}},
        {"status": "ok", "response": {"type": "cancelByCloid", "data": {"statuses": ["success"]}}},
        ok_statuses({"resting": {"oid": 8}}),
        OK_DEFAULT,
        OK_DEFAULT,
        OK_DEFAULT,
    )
    await h.gateway.cancel_orders([CancelSpec(asset=4, oid=77)])
    await h.gateway.cancel_orders_by_cloid([CloidCancelSpec(asset=4, cloid=CLOID)])
    await h.gateway.modify_orders(
        [
            ModifySpec(
                oid=8,
                order=OrderSpec(
                    asset=4, is_buy=True, size=Decimal("0.6"), limit_price=Decimal("1790")
                ),
            )
        ]
    )
    await h.gateway.update_leverage(4, 20, is_cross=False)
    await h.gateway.schedule_cancel(datetime(2026, 7, 27, 12, 5, tzinfo=UTC))
    await h.gateway.schedule_cancel(None)

    actions = [payload["action"] for payload in h.received]
    assert actions[0] == {"type": "cancel", "cancels": [{"a": 4, "o": 77}]}
    assert actions[1] == {"type": "cancelByCloid", "cancels": [{"asset": 4, "cloid": CLOID}]}
    assert actions[2] == {
        "type": "batchModify",
        "modifies": [
            {
                "oid": 8,
                "order": {
                    "a": 4,
                    "b": True,
                    "p": "1790",
                    "s": "0.6",
                    "r": False,
                    "t": {"limit": {"tif": "Gtc"}},
                },
            }
        ],
    }
    assert actions[3] == {"type": "updateLeverage", "asset": 4, "isCross": False, "leverage": 20}
    assert actions[4] == {
        "type": "scheduleCancel",
        "time": int(datetime(2026, 7, 27, 12, 5, tzinfo=UTC).timestamp() * 1000),
    }
    # None REMOVES the schedule: the wire carries no time key at all.
    assert actions[5] == {"type": "scheduleCancel"}
    for payload in h.received:
        assert h.recovered_signer(payload) == AGENT.address


async def test_nonces_strictly_increase_across_submissions(replaying: Any) -> None:
    h = await replaying(OK_DEFAULT, OK_DEFAULT, OK_DEFAULT)
    for _ in range(3):
        await h.gateway.schedule_cancel(None)
    nonces = [payload["nonce"] for payload in h.received]
    assert nonces == sorted(set(nonces))


# --- typed results ------------------------------------------------------------


async def test_order_statuses_parse_to_typed_results(replaying: Any) -> None:
    h = await replaying(
        ok_statuses(
            {"resting": {"oid": 77}},
            {"filled": {"totalSz": "0.5", "avgPx": "1799.5", "oid": 78}},
            {"error": "Order must have minimum value of $10."},
        )
    )
    results = await h.gateway.place_orders(
        [
            OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))
            for _ in range(3)
        ]
    )
    assert results == [
        OrderResting(oid=77),
        OrderFilled(oid=78, total_size=Decimal("0.5"), avg_price=Decimal("1799.5")),
        OrderRejected(
            reason=RejectReason.MIN_NOTIONAL, message="Order must have minimum value of $10."
        ),
    ]


async def test_cancel_statuses_parse_to_typed_results(replaying: Any) -> None:
    message = "Order was never placed, already canceled, or filled. asset=4"
    h = await replaying(
        {
            "status": "ok",
            "response": {"type": "cancel", "data": {"statuses": ["success", {"error": message}]}},
        }
    )
    results = await h.gateway.cancel_orders(
        [CancelSpec(asset=4, oid=77), CancelSpec(asset=4, oid=78)]
    )
    assert results == [
        CancelOk(),
        CancelRejected(reason=RejectReason.MISSING_ORDER, message=message),
    ]


async def test_a_status_count_mismatch_fails_loudly(replaying: Any) -> None:
    # Zipping misaligned statuses to orders would attribute verdicts to the
    # wrong legs — worse than failing.
    h = await replaying(ok_statuses({"resting": {"oid": 77}}))
    with pytest.raises(ExecutionError):
        await h.gateway.place_orders(
            [
                OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))
                for _ in range(2)
            ]
        )


async def test_a_whole_action_rejection_raises_with_the_classified_reason(
    replaying: Any,
) -> None:
    # Research §2: pre-validation failures return ONE error for the whole
    # batch — nothing executed.
    h = await replaying({"status": "err", "response": "Invalid nonce"})
    with pytest.raises(ActionRejectedError) as excinfo:
        await h.gateway.schedule_cancel(None)
    assert excinfo.value.reason is RejectReason.INVALID_NONCE
    assert excinfo.value.message == "Invalid nonce"


async def test_an_unexpected_response_shape_raises_execution_error(replaying: Any) -> None:
    h = await replaying({"weird": True})
    with pytest.raises(ExecutionError):
        await h.gateway.schedule_cancel(None)


# --- budget ------------------------------------------------------------------


async def test_the_execution_lane_is_billed_the_documented_action_weight(replaying: Any) -> None:
    h = await replaying(
        ok_statuses(*({"resting": {"oid": i}} for i in range(40))),
        OK_DEFAULT,
    )
    spec = OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))
    await h.gateway.place_orders([spec] * 40)  # 1 + 40 // 40
    await h.gateway.schedule_cancel(None)
    assert h.budget.spends == [2, 1]


# --- failure modes -----------------------------------------------------------


async def test_a_429_retry_reposts_the_identical_signed_payload(replaying: Any) -> None:
    # THE write-safety property: the retry must not re-sign under a fresh
    # nonce — the single-use nonce is what makes the replay at-most-once.
    h = await replaying(429, OK_DEFAULT)
    await h.gateway.schedule_cancel(None)
    assert len(h.received) == 2
    assert h.received[0] == h.received[1]
    assert h.clock.slept  # backed off between tries


async def test_a_429_streak_escapes_as_rate_limited(replaying: Any) -> None:
    h = await replaying(*([429] * RATE_LIMIT_MAX_TRIES), retry_after="0.1")
    with pytest.raises(ExecutionRateLimitedError):
        await h.gateway.schedule_cancel(None)
    assert len(h.received) == RATE_LIMIT_MAX_TRIES


async def test_a_timeout_raises_the_typed_ambiguous_error_without_retry(
    replaying: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A timeout means the action may have executed — it must surface as the
    # typed ambiguous error and must NOT be replayed blindly by the gateway.
    monkeypatch.setattr(execution_http, "REQUEST_TIMEOUT", aiohttp.ClientTimeout(total=0.05))
    h = await replaying(OK_DEFAULT, handler_sleep=0.5)
    with pytest.raises(AmbiguousExecutionError):
        await h.gateway.schedule_cancel(None)
    assert len(h.received) == 1


async def test_an_invalid_nonce_after_a_429_retry_is_ambiguous_not_rejected(
    replaying: Any,
) -> None:
    # The silent-live-order hazard (PR #140 review): if the 429'd attempt
    # WAS processed (429-means-not-processed is unverified), the same-nonce
    # retry answers "Invalid nonce" while the order is live. That sequence
    # must surface as reconcile-required, never as a clean rejection.
    h = await replaying(429, {"status": "err", "response": "Invalid nonce"})
    with pytest.raises(AmbiguousExecutionError):
        await h.gateway.schedule_cancel(None)
    assert len(h.received) == 2


async def test_an_invalid_nonce_without_any_retry_stays_a_clean_rejection(
    replaying: Any,
) -> None:
    # No 429 happened, so nothing earlier could have executed under this
    # nonce: the reject is unambiguous and keeps its classified reason.
    h = await replaying({"status": "err", "response": "Invalid nonce"})
    with pytest.raises(ActionRejectedError) as excinfo:
        await h.gateway.schedule_cancel(None)
    assert not isinstance(excinfo.value, AmbiguousExecutionError)
    assert excinfo.value.reason is RejectReason.INVALID_NONCE


async def test_a_non_nonce_rejection_after_a_429_retry_stays_a_clean_rejection(
    replaying: Any,
) -> None:
    # Ambiguity is specific to the invalid-nonce signature a processed-then-
    # retried action would leave; an ordinary reject after a 429 is still a
    # reject (the margin check failing twice says nothing executed).
    h = await replaying(429, {"status": "err", "response": "Insufficient margin to place order."})
    with pytest.raises(ActionRejectedError) as excinfo:
        await h.gateway.schedule_cancel(None)
    assert excinfo.value.reason is RejectReason.INSUFFICIENT_MARGIN


async def test_a_never_established_connection_is_the_one_unambiguous_failure(
    replaying: Any,
) -> None:
    # Nothing was ever sent, so nothing can have executed: plain
    # ExecutionError, NOT the ambiguous subclass — the only transport
    # failure allowed to say so.
    h = await replaying()
    h.gateway._session = aiohttp.ClientSession()  # type: ignore[attr-defined]
    try:
        h.gateway._exchange_url = "http://127.0.0.1:1/exchange"  # type: ignore[attr-defined]
        with pytest.raises(ExecutionError) as excinfo:
            await h.gateway.schedule_cancel(None)
        assert not isinstance(excinfo.value, AmbiguousExecutionError)
    finally:
        await h.gateway._session.close()  # type: ignore[attr-defined]


class _Flaky429ThenConnectFailSession:
    """First post answers 429; the second raises as if the connection never
    established — the sequence where even a connect failure is ambiguous,
    because the 429'd attempt may have been processed."""

    def __init__(self) -> None:
        self.calls = 0

    def post(self, url: str, *, json: Any, timeout: Any) -> Any:
        self.calls += 1
        if self.calls > 1:
            # str(ClientConnectorError) reads host/port/ssl off the
            # connection key, so give it a minimal stand-in.
            key = SimpleNamespace(host="127.0.0.1", port=1, ssl=None)
            raise aiohttp.ClientConnectorError(
                key,  # type: ignore[arg-type]
                OSError("connection refused"),
            )

        class _Response:
            status = 429
            headers: dict[str, str] = {}

            async def __aenter__(self) -> "_Response":
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

        return _Response()


async def test_a_connect_failure_after_a_429_attempt_is_ambiguous() -> None:
    session = _Flaky429ThenConnectFailSession()
    gateway = HttpExecutionGateway(
        session,  # type: ignore[arg-type]
        FakeClock(),
        RecordingBudget(),
        signer=AGENT,
        master_address=MASTER,
        exchange_url="http://127.0.0.1:1/exchange",
        rng=lambda: 1.0,
    )
    with pytest.raises(AmbiguousExecutionError):
        await gateway.schedule_cancel(None)
    assert session.calls == 2


async def test_an_unreadable_ok_response_is_ambiguous(replaying: Any) -> None:
    # HTTP 200 means the exchange processed SOMETHING; a body we can't map
    # must never read as "nothing happened".
    h = await replaying({"status": "ok", "response": {"type": "order", "data": {"statuses": [42]}}})
    with pytest.raises(AmbiguousExecutionError):
        await h.gateway.place_orders(
            [OrderSpec(asset=4, is_buy=True, size=Decimal("0.5"), limit_price=Decimal("1800"))]
        )
