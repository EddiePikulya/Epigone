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
a replay can never execute TWICE (chain-enforced). Whether a 429'd attempt
was ever processed at all is a separate, unverified question (the one
citation point: ExecutionRateLimitedError's docstring); its consequence
here is that an invalid-nonce reject on a RETRIED submission — the exact
signature a processed-then-retried action would leave — surfaces as
AmbiguousExecutionError, never as a clean rejection. Timeouts and post-send
transport failures are ambiguous without retry; _post's docstring carries
the full failure split. In every ambiguous case only the caller, via the
read gateway, can reconcile.

Base URL is constructor-injected and testnet-only by construction: Phase
A1–A4 points at TESTNET_EXCHANGE_URL, and a MAINNET URL is refused at
construction (MainnetNotEnabledError) unless the A5 safety layer passes its
explicit allow_mainnet capability. The signature's phantom-agent `source`
field ("a" mainnet / "b" testnet) derives from the URL exactly as the SDK
derives it, so a testnet gateway cannot produce a mainnet-valid signature
either.
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
from epigone.gateway.backoff import (
    RATE_LIMIT_MAX_TRIES,
    backoff_delay,
    parse_retry_after,
    retry_fits_budget,
)
from epigone.gateway.execution import (
    ActionRejectedError,
    AmbiguousExecutionError,
    BuilderFee,
    CancelOk,
    CancelRejected,
    CancelResult,
    CancelSpec,
    CloidCancelSpec,
    ExecutionError,
    ExecutionRateLimitedError,
    Grouping,
    MainnetNotEnabledError,
    MasterKeySignerError,
    ModifySpec,
    NonceSource,
    OrderFilled,
    OrderRejected,
    OrderResting,
    OrderResult,
    OrderSpec,
    RejectReason,
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
        allow_mainnet: bool = False,
        rng: Callable[[], float] = random.random,
    ) -> None:
        # The A5 gate (PR #140 review): mainnet is unreachable BY
        # CONSTRUCTION until the safety layer exists. Nothing in the
        # codebase passes allow_mainnet=True; A5 (risk policy v0 — caps,
        # allowlist, kill switch) is the slice that gets to wire it. The
        # guard shares _is_mainnet's predicate on purpose: any URL variant
        # that slipped past a different check would still sign with the
        # testnet phantom-agent source and be rejected by mainnet.
        is_mainnet = exchange_url == MAINNET_EXCHANGE_URL
        if is_mainnet and not allow_mainnet:
            raise MainnetNotEnabledError(
                "mainnet execution is gated behind the A5 safety layer "
                "(ADR-0005): pass allow_mainnet=True only from A5's wiring, "
                "never before the risk policy exists"
            )
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
        self._is_mainnet = is_mainnet
        self._rng = rng
        self._nonces = NonceSource(clock)

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
        vault_address: str | None = None,
    ) -> list[OrderResult]:
        action: dict[str, Any] = {
            "type": "order",
            "orders": [_order_wire(order) for order in orders],
            "grouping": grouping.value,
        }
        if builder is not None:
            action["builder"] = {"b": builder.address.lower(), "f": builder.fee_tenth_bp}
        data = await self._submit(
            action, batch_len=len(orders), vault_address=vault_address
        )
        return parse_order_statuses(data, expected=len(orders))

    async def cancel_orders(
        self, cancels: list[CancelSpec], *, vault_address: str | None = None
    ) -> list[CancelResult]:
        action = {
            "type": "cancel",
            "cancels": [{"a": cancel.asset, "o": cancel.oid} for cancel in cancels],
        }
        data = await self._submit(
            action, batch_len=len(cancels), vault_address=vault_address
        )
        return parse_cancel_statuses(data, expected=len(cancels))

    async def create_sub_account(self, name: str) -> str:
        """SubAccountProvisioning: mint a sub-account of the master. The
        response `data` IS the new address (finding 3); it is returned
        lowercased like every other address in the system."""
        data = await self._submit({"type": "createSubAccount", "name": name}, batch_len=1)
        if not isinstance(data, str):
            raise AmbiguousExecutionError(
                f"createSubAccount({name!r}) answered {data!r} instead of an address: "
                "the sub-account MAY exist — read `subAccounts` before retrying, or a "
                "retry spends another of the master's 10 slots (finding 10)"
            )
        return data.lower()

    async def rename_sub_account(self, sub_address: str, name: str) -> None:
        """SubAccountProvisioning: rename an existing sub (finding 11). The
        SDK has no method for `subAccountModify`, so the wire dict is built
        here in the exchange's own key order like every other action, and the
        address is lowercased for the same reason subAccountTransfer's is."""
        await self._submit(
            {
                "type": "subAccountModify",
                "subAccountUser": sub_address.lower(),
                "name": name,
            },
            batch_len=1,
        )

    async def sub_account_transfer(
        self, sub_address: str, *, is_deposit: bool, usd_micro: int
    ) -> None:
        """SubAccountProvisioning: fund (or defund) a sub-account. The address
        rides INSIDE the action, so it must be lowercase or signature recovery
        yields a stranger (finding 2) — the one gotcha that costs a whole
        debugging session if it is left to the caller."""
        await self._submit(
            {
                "type": "subAccountTransfer",
                "subAccountUser": sub_address.lower(),
                "isDeposit": is_deposit,
                "usd": usd_micro,
            },
            batch_len=1,
        )

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

    async def update_leverage(
        self,
        asset: int,
        leverage: int,
        *,
        is_cross: bool = True,
        vault_address: str | None = None,
    ) -> None:
        action = {
            "type": "updateLeverage",
            "asset": asset,
            "isCross": is_cross,
            "leverage": leverage,
        }
        await self._submit(action, batch_len=1, vault_address=vault_address)

    async def schedule_cancel(self, at: datetime | None) -> None:
        action: dict[str, Any] = {"type": "scheduleCancel"}
        if at is not None:
            action["time"] = timestamp_ms(at)
        await self._submit(action, batch_len=1)

    async def _submit(
        self, action: dict[str, Any], *, batch_len: int, vault_address: str | None = None
    ) -> Any:
        """Spend the execution lane's weight, sign once, post (with same-
        payload 429 retry), and unwrap the response envelope to its `data`.
        Raises ActionRejectedError on {"status": "err"} — EXCEPT an
        invalid-nonce reject on a RETRIED submission, which is exactly what
        a processed-then-retried action would answer (the unverified 429
        assumption, ExecutionRateLimitedError's docstring) and so raises
        AmbiguousExecutionError, never a clean rejection.

        `vault_address` (a sub-account, for the copy lane) is covered BY THE
        SIGNATURE as well as carried in the payload: the SDK hashes the vault
        flag alongside the action and nonce, so passing it to sign_l1_action
        and omitting it from the body — or the reverse — produces a signature
        that recovers to a stranger. Lowercased here, once, for the same
        canonicalization reason (finding 2). The nonce lane is unchanged:
        nonces are per SIGNER, and one signer trades the master and every sub
        of it (finding 6)."""
        await self._budget.spend(exchange_action_weight(batch_len))
        nonce = self._nonces.next()
        vault = None if vault_address is None else vault_address.lower()
        signature = sign_l1_action(self._signer, action, vault, nonce, None, self._is_mainnet)
        payload = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": vault,
            "expiresAfter": None,
        }
        body, retried = await self._post(payload)
        try:
            return _unwrap(body)
        except ActionRejectedError as exc:
            if retried and exc.reason is RejectReason.INVALID_NONCE:
                raise AmbiguousExecutionError(
                    "invalid-nonce reject after a 429 retry: the 429'd attempt "
                    "may have executed under this nonce — reconcile via the "
                    f"read gateway before re-issuing (exchange said: {exc.message!r})"
                ) from exc
            raise

    async def _post(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        """POST the signed payload, retrying 429s with the IDENTICAL body
        (single-use nonces make the replay at-most-once). Returns (json body,
        whether any retry happened) — the retry flag is what lets _submit
        treat a subsequent invalid-nonce reject as ambiguous.

        The failure split (the ExecutionError hierarchy's question — could
        anything have reached the exchange?): only a connection that never
        established, on a submission with no prior 429'd attempt, raises
        plain ExecutionError. Everything else — timeout, post-send transport
        error, HTTP error status, unparseable 200 body — may follow a
        request the exchange received, and any failure after a 429'd attempt
        inherits that attempt's possibly-processed status (the unverified
        429 assumption, see ExecutionRateLimitedError) — all ambiguous.

        The 429 loop is bounded in TRIES and in WALL CLOCK (issue #204). The
        clock bound is what makes a CANCEL POST safe for the watchdog's kill
        path to await: a cancel riding six slow 429s used to be minutes long,
        which is long enough to straddle the moment the dead-man's push falls
        due and let the last-resort net discharge behind a working watchdog.
        It has to bound ITSELF rather than be cancelled from outside —
        cancelling a write mid-flight would leave the audit trail's attempt
        row with no outcome, on the one path whose evidence matters most."""
        started = self._clock.now()
        tries = 0
        for attempt in range(RATE_LIMIT_MAX_TRIES):
            tries = attempt + 1
            try:
                async with self._session.post(
                    self._exchange_url, json=payload, timeout=REQUEST_TIMEOUT
                ) as response:
                    if response.status != 429:
                        response.raise_for_status()
                        return await response.json(), attempt > 0
                    delay = parse_retry_after(response.headers.get("Retry-After"))
                    if delay is None:
                        delay = backoff_delay(attempt, self._rng)
            except TimeoutError as exc:
                # The response died in flight; the action may have executed.
                # No retry — the caller reconciles (module docstring).
                raise AmbiguousExecutionError(
                    f"exchange request timed out — the action may have executed; "
                    f"reconcile before re-issuing: {exc!r}"
                ) from exc
            except aiohttp.ClientConnectorError as exc:
                if attempt > 0:
                    raise AmbiguousExecutionError(
                        f"connection failed after a 429'd attempt — that attempt "
                        f"may have been processed; reconcile before re-issuing: {exc}"
                    ) from exc
                # The connection never established and nothing was ever sent:
                # the one transport failure that is honestly unambiguous.
                raise ExecutionError(f"exchange connection failed: {exc}") from exc
            except aiohttp.ClientError as exc:
                raise AmbiguousExecutionError(
                    f"exchange request failed after send — the action may have "
                    f"executed; reconcile before re-issuing: {exc}"
                ) from exc
            if attempt + 1 >= RATE_LIMIT_MAX_TRIES or not retry_fits_budget(
                started, self._clock.now(), delay
            ):
                break
            log.warning(
                "429 from %s: backing off %.1fs (try %d)",
                self._exchange_url,
                delay,
                attempt + 1,
            )
            await self._clock.sleep(delay)
        raise ExecutionRateLimitedError(
            f"still 429 from {self._exchange_url} after {tries} tries in "
            f"{(self._clock.now() - started).total_seconds():.0f}s "
            "(whether any 429'd attempt was processed is unverified — reconcile "
            "before re-issuing non-idempotent work)"
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
    Returns the inner data (None for ack-only types like "default").

    A body this cannot read arrived on an HTTP 200 — the exchange processed
    SOMETHING we failed to interpret — so shape failures here (and in the
    status parsers below) are AmbiguousExecutionError: reconcile, don't
    assume nothing happened."""
    try:
        status = body["status"]
        response = body["response"]
    except (KeyError, TypeError) as exc:
        raise AmbiguousExecutionError(f"unexpected exchange response shape: {body!r}") from exc
    if status == "err":
        raise ActionRejectedError(str(response))
    if status != "ok":
        raise AmbiguousExecutionError(f"unexpected exchange response status: {body!r}")
    if isinstance(response, dict):
        return response.get("data")
    raise AmbiguousExecutionError(f"unexpected exchange response shape: {body!r}")


def parse_order_statuses(data: Any, *, expected: int) -> list[OrderResult]:
    """Map an order/batchModify response's per-order statuses (research §2:
    resting{oid} / filled{totalSz, avgPx, oid} / error{...}) to typed
    results. A count mismatch fails loudly — silently zipping misaligned
    statuses to orders would attribute verdicts to the wrong legs. Failures
    are ambiguous (the _unwrap rule): the action DID execute, we just can't
    read what it did."""
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
        raise AmbiguousExecutionError(f"unexpected order response shape: {data!r}") from exc
    if len(results) != expected:
        raise AmbiguousExecutionError(
            f"expected {expected} order statuses, got {len(results)}: {data!r}"
        )
    return results


def parse_cancel_statuses(data: Any, *, expected: int) -> list[CancelResult]:
    """Map a cancel/cancelByCloid response's statuses ("success" or
    {"error": ...}) to typed results, with the same count check — and the
    same ambiguity rule — as orders."""
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
        raise AmbiguousExecutionError(f"unexpected cancel response shape: {data!r}") from exc
    if len(results) != expected:
        raise AmbiguousExecutionError(
            f"expected {expected} cancel statuses, got {len(results)}: {data!r}"
        )
    return results
