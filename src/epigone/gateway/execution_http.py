"""The production ExecutionGateway: signed L1 actions against POST /exchange.

Signing rides the official hyperliquid-python-sdk helpers (sign_l1_action —
ADR-0005), but the wire dicts are built HERE, key-for-key in the SDK's own
insertion order: the signature covers keccak(msgpack(action)‖nonce‖vault
flag), and the server re-canonicalizes the JSON it receives in that same
order, so a reordered key means a signature that recovers to a stranger. The
unit tests pin this by recovering the signer from every recorded payload with
the SDK's own recover helper.

429s back off and retry like the read gateway (issue #28) — but here the
retry re-posts the SAME signed payload: nonces are single-use per signer, so
a replay can never execute twice, making the retry safe by construction. A
persistent streak escapes as ExecutionRateLimitedError. Timeouts and other
transport failures raise ExecutionError WITHOUT retry — a lost response is
ambiguous (the action may have executed) and only the caller, via the read
gateway, can reconcile that.

Base URL is constructor-injected and testnet-first: Phase A1 points at
TESTNET_EXCHANGE_URL; the signature's phantom-agent `source` field ("a"
mainnet / "b" testnet) derives from the URL exactly as the SDK derives it,
so a testnet gateway cannot produce a mainnet-valid signature.
"""

import logging
import random
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp
from hyperliquid.utils.signing import sign_l1_action

from epigone.budget import Budget
from epigone.clock import Clock
from epigone.gateway.backoff import RATE_LIMIT_MAX_TRIES, backoff_delay, parse_retry_after
from epigone.gateway.execution import (
    ActionRejectedError,
    BuilderFee,
    CancelOk,
    CancelRejected,
    CancelResult,
    CancelSpec,
    CloidCancelSpec,
    ExecutionError,
    ExecutionRateLimitedError,
    Grouping,
    MasterKeySignerError,
    ModifySpec,
    NonceSource,
    OrderFilled,
    OrderRejected,
    OrderResting,
    OrderResult,
    OrderSpec,
    Signer,
    classify_reject,
    decimal_to_wire,
    exchange_action_weight,
    timestamp_ms,
)

log = logging.getLogger(__name__)

MAINNET_EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"
TESTNET_EXCHANGE_URL = "https://api.hyperliquid-testnet.xyz/exchange"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)


class HttpExecutionGateway:
    """One instance = one signer = one traded master account = one nonce lane
    (the module contract in epigone.gateway.execution).

    `master_address` is the account whose orders this lane trades. It never
    rides the wire in Phase A (the exchange resolves the acting master from
    the agent signature itself); it exists for the gateway-side layer of the
    ADR-0005 invariant: construction REFUSES a signer whose address equals
    the master's, so this-account's-own-master-key can never sign here.
    Defense in depth, not the whole defense — see the layered statement in
    epigone.gateway.execution's module docstring."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        clock: Clock,
        budget: Budget,
        *,
        signer: Signer,
        master_address: str,
        exchange_url: str = TESTNET_EXCHANGE_URL,
        rng: Callable[[], float] = random.random,
    ) -> None:
        if signer.address.lower() == master_address.lower():
            raise MasterKeySignerError(
                f"signer {signer.address} IS the master account: the master key "
                "never signs on Epigone's execution path (ADR-0005) — approve an "
                "agent key and pass that instead"
            )
        self._session = session
        self._clock = clock
        self._budget = budget
        self._signer = signer
        self._master_address = master_address.lower()
        self._exchange_url = exchange_url
        self._is_mainnet = exchange_url == MAINNET_EXCHANGE_URL
        self._rng = rng
        self._nonces = NonceSource(clock)

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
    ) -> list[OrderResult]:
        action: dict[str, Any] = {
            "type": "order",
            "orders": [_order_wire(order) for order in orders],
            "grouping": grouping.value,
        }
        if builder is not None:
            action["builder"] = {"b": builder.address.lower(), "f": builder.fee_tenth_bp}
        data = await self._submit(action, batch_len=len(orders))
        return parse_order_statuses(data, expected=len(orders))

    async def cancel_orders(self, cancels: list[CancelSpec]) -> list[CancelResult]:
        action = {
            "type": "cancel",
            "cancels": [{"a": cancel.asset, "o": cancel.oid} for cancel in cancels],
        }
        data = await self._submit(action, batch_len=len(cancels))
        return parse_cancel_statuses(data, expected=len(cancels))

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        action = {
            "type": "cancelByCloid",
            "cancels": [{"asset": cancel.asset, "cloid": cancel.cloid} for cancel in cancels],
        }
        data = await self._submit(action, batch_len=len(cancels))
        return parse_cancel_statuses(data, expected=len(cancels))

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        action = {
            "type": "batchModify",
            "modifies": [
                {"oid": modify.oid, "order": _order_wire(modify.order)} for modify in modifies
            ],
        }
        data = await self._submit(action, batch_len=len(modifies))
        return parse_order_statuses(data, expected=len(modifies))

    async def update_leverage(self, asset: int, leverage: int, *, is_cross: bool = True) -> None:
        action = {
            "type": "updateLeverage",
            "asset": asset,
            "isCross": is_cross,
            "leverage": leverage,
        }
        await self._submit(action, batch_len=1)

    async def schedule_cancel(self, at: datetime | None) -> None:
        action: dict[str, Any] = {"type": "scheduleCancel"}
        if at is not None:
            action["time"] = timestamp_ms(at)
        await self._submit(action, batch_len=1)

    async def _submit(self, action: dict[str, Any], *, batch_len: int) -> Any:
        """Spend the execution lane's weight, sign once, post (with same-
        payload 429 retry), and unwrap the response envelope to its `data`.
        Raises ActionRejectedError on {"status": "err"}."""
        await self._budget.spend(exchange_action_weight(batch_len))
        nonce = self._nonces.next()
        signature = sign_l1_action(self._signer, action, None, nonce, None, self._is_mainnet)
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": None,
            "expiresAfter": None,
        }
        body = await self._post(payload)
        return _unwrap(body)

    async def _post(self, payload: dict[str, Any]) -> Any:
        for attempt in range(RATE_LIMIT_MAX_TRIES):
            try:
                async with self._session.post(
                    self._exchange_url, json=payload, timeout=REQUEST_TIMEOUT
                ) as response:
                    if response.status != 429:
                        response.raise_for_status()
                        return await response.json()
                    delay = parse_retry_after(response.headers.get("Retry-After"))
                    if delay is None:
                        delay = backoff_delay(attempt, self._rng)
            except TimeoutError as exc:
                # Ambiguous by nature: the action may have executed and the
                # response died in flight. No retry — the caller reconciles
                # through the read gateway (module docstring).
                raise ExecutionError(
                    f"exchange request timed out (AMBIGUOUS — the action may have "
                    f"executed; reconcile before re-issuing): {exc!r}"
                ) from exc
            except aiohttp.ClientError as exc:
                raise ExecutionError(f"exchange request failed: {exc}") from exc
            if attempt + 1 < RATE_LIMIT_MAX_TRIES:
                log.warning(
                    "429 from %s: backing off %.1fs (try %d)",
                    self._exchange_url,
                    delay,
                    attempt + 1,
                )
                await self._clock.sleep(delay)
        raise ExecutionRateLimitedError(
            f"still 429 from {self._exchange_url} after {RATE_LIMIT_MAX_TRIES} tries"
        )

def _order_wire(order: OrderSpec) -> dict[str, Any]:
    """One order as the exchange's wire dict — the SDK's OrderWire key order
    exactly (a, b, p, s, r, t, c): the action is signed over its msgpack, so
    key order is part of the signature."""
    type_wire: dict[str, Any]
    if order.trigger is not None:
        type_wire = {
            "trigger": {
                "isMarket": order.trigger.is_market,
                "triggerPx": decimal_to_wire(order.trigger.trigger_price),
                "tpsl": order.trigger.tpsl.value,
            }
        }
    else:
        type_wire = {"limit": {"tif": order.tif.value}}
    wire: dict[str, Any] = {
        "a": order.asset,
        "b": order.is_buy,
        "p": decimal_to_wire(order.limit_price),
        "s": decimal_to_wire(order.size),
        "r": order.reduce_only,
        "t": type_wire,
    }
    if order.cloid is not None:
        wire["c"] = order.cloid
    return wire


def _unwrap(body: Any) -> Any:
    """The response envelope: {"status": "ok", "response": {"type": ...,
    "data": ...}} or {"status": "err", "response": "<prose>"} (research §2).
    Returns the inner data (None for ack-only types like "default")."""
    try:
        status = body["status"]
        response = body["response"]
    except (KeyError, TypeError) as exc:
        raise ExecutionError(f"unexpected exchange response shape: {body!r}") from exc
    if status == "err":
        raise ActionRejectedError(str(response))
    if status != "ok":
        raise ExecutionError(f"unexpected exchange response status: {body!r}")
    if isinstance(response, dict):
        return response.get("data")
    raise ExecutionError(f"unexpected exchange response shape: {body!r}")


def parse_order_statuses(data: Any, *, expected: int) -> list[OrderResult]:
    """Map an order/batchModify response's per-order statuses (research §2:
    resting{oid} / filled{totalSz, avgPx, oid} / error{...}) to typed
    results. A count mismatch fails loudly — silently zipping misaligned
    statuses to orders would attribute verdicts to the wrong legs."""
    try:
        statuses = data["statuses"]
        results: list[OrderResult] = []
        for status in statuses:
            if "resting" in status:
                resting = status["resting"]
                results.append(OrderResting(oid=int(resting["oid"]), cloid=resting.get("cloid")))
            elif "filled" in status:
                filled = status["filled"]
                results.append(
                    OrderFilled(
                        oid=int(filled["oid"]),
                        total_size=Decimal(filled["totalSz"]),
                        avg_price=Decimal(filled["avgPx"]),
                        cloid=filled.get("cloid"),
                    )
                )
            elif "error" in status:
                message = str(status["error"])
                results.append(OrderRejected(reason=classify_reject(message), message=message))
            else:
                raise ValueError(f"unknown order status {status!r}")
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise ExecutionError(f"unexpected order response shape: {data!r}") from exc
    if len(results) != expected:
        raise ExecutionError(f"expected {expected} order statuses, got {len(results)}: {data!r}")
    return results


def parse_cancel_statuses(data: Any, *, expected: int) -> list[CancelResult]:
    """Map a cancel/cancelByCloid response's statuses ("success" or
    {"error": ...}) to typed results, with the same count check as orders."""
    try:
        statuses = data["statuses"]
        results: list[CancelResult] = []
        for status in statuses:
            if status == "success":
                results.append(CancelOk())
            elif isinstance(status, dict) and "error" in status:
                message = str(status["error"])
                results.append(CancelRejected(reason=classify_reject(message), message=message))
            else:
                raise ValueError(f"unknown cancel status {status!r}")
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionError(f"unexpected cancel response shape: {data!r}") from exc
    if len(results) != expected:
        raise ExecutionError(f"expected {expected} cancel statuses, got {len(results)}: {data!r}")
    return results
