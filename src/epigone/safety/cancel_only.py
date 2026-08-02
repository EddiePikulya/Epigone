"""Cancel-only authority for the safety lane (issue #145).

The watchdog's job is subtractive: cancel resting orders, push the
protocol-native dead-man's switch. It has never had a reason to PLACE an
order — but "has no reason to" is a code-reading claim, and issue #145 needs
a structural one, because the cold-start path now signs with a key loaded
from a local file instead of the keystore. A watchdog that came up blind,
without a database, without halt state, and without the executor's view of
the world, must not be one refactor away from being able to open a position.

So the watchdog's gateway is wrapped here BEFORE the audit wrapper, and the
order-placing half of the ExecutionGateway surface raises instead of
signing. The refusal is an ExecutionError — nothing reached the exchange —
so a caller that somehow tried it gets the trail's `error` row and the loud
message, not a silently-dropped action.

Kept: `cancel_orders`, `cancel_orders_by_cloid` (the sweep), and
`schedule_cancel` (deadman.py's upgrade path — also purely subtractive).
Refused: `place_orders`, `modify_orders` (a modify is a placement in
disguise: it replaces a resting order with a new one) and `update_leverage`
(a risk-posture change, not a cancel).

This is a lane restriction, not a security boundary against a compromised
process — an attacker with the process also has the signer. What it buys is
the guarantee ADR-0005's layering asks for: the blind cold-start path's
authority is legible from its construction, and any future code that tries
to widen it fails loudly and in tests rather than at 3am on the wire.
"""

import logging
from datetime import datetime

from epigone.gateway.execution import (
    BuilderFee,
    CancelResult,
    CancelSpec,
    CloidCancelSpec,
    ExecutionError,
    ExecutionGateway,
    Grouping,
    ModifySpec,
    OrderResult,
    OrderSpec,
)

log = logging.getLogger(__name__)


class OrderPlacementForbiddenError(ExecutionError):
    """The safety lane tried to place, modify, or re-lever. Nothing was
    signed and nothing reached the exchange: the wrapper refuses before the
    inner gateway is called at all."""


class CancelOnlyExecutionGateway:
    """An ExecutionGateway with the additive half removed. Wrap the real
    gateway with this and no code path through it can open risk — including
    the cold-start path, whose signer came from the local key cache."""

    def __init__(self, inner: ExecutionGateway) -> None:
        self._inner = inner

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
    ) -> list[OrderResult]:
        raise self._refuse("place_orders", len(orders))

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        raise self._refuse("modify_orders", len(modifies))

    async def update_leverage(self, asset: int, leverage: int, *, is_cross: bool = True) -> None:
        raise self._refuse("update_leverage", 1)

    async def cancel_orders(self, cancels: list[CancelSpec]) -> list[CancelResult]:
        return await self._inner.cancel_orders(cancels)

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        return await self._inner.cancel_orders_by_cloid(cancels)

    async def schedule_cancel(self, at: datetime | None) -> None:
        await self._inner.schedule_cancel(at)

    def _refuse(self, action: str, count: int) -> OrderPlacementForbiddenError:
        message = (
            f"the safety lane is CANCEL-ONLY: refusing {action} ({count} item(s)) — "
            f"the watchdog cancels and schedules cancels, it never opens risk "
            f"(epigone.safety.cancel_only, issue #145)"
        )
        log.error(message)
        return OrderPlacementForbiddenError(message)
