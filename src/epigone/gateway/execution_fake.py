"""In-memory ExecutionGateway for tests: record actions, no network, no keys.

The action-order/nonce contract this fake mirrors (the read fake's
convention, epigone.gateway.fake): the real gateway submits actions in call
order, each under a fresh strictly-increasing per-signer nonce, one signer
per gateway instance. The fake's `actions` list records every call in that
same order — a test asserting on `actions` is asserting the sequence the
exchange would have seen, nonce order included. There is nothing to sign
here: the fake stands in AFTER the signer seam, so tests of executor logic
never touch key material at all.
"""

from datetime import datetime

from epigone.gateway.execution import (
    BuilderFee,
    CancelOk,
    CancelResult,
    CancelSpec,
    CloidCancelSpec,
    Grouping,
    ModifySpec,
    OrderResting,
    OrderResult,
    OrderSpec,
)


class FakeExecutionGateway:
    """Configure results per call via the `..._results` queues (each pop-left
    per call; an empty queue answers the benign default — every order rests
    under a fresh oid, every cancel succeeds, acks ack). Configure failures
    by appending an Exception to `errors`: the NEXT call raises it, whatever
    the method — matching the real gateway, where any action can hit any
    transport/reject failure."""

    def __init__(self) -> None:
        # Every call in submission order: (method name, payload tuple).
        self.actions: list[tuple[str, object]] = []
        self.errors: list[Exception] = []
        self.place_results: list[list[OrderResult]] = []
        self.cancel_results: list[list[CancelResult]] = []
        self.modify_results: list[list[OrderResult]] = []
        # Sub-account addresses create_sub_account hands out, in order; empty
        # falls back to a generated one (issue #136).
        self.sub_addresses: list[str] = []
        self._next_oid = 1
        self._next_sub = 0

    def _record(self, method: str, payload: object) -> None:
        self.actions.append((method, payload))
        if self.errors:
            raise self.errors.pop(0)

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
        vault_address: str | None = None,
    ) -> list[OrderResult]:
        self._record("place_orders", (list(orders), grouping, builder, vault_address))
        if self.place_results:
            return self.place_results.pop(0)
        results: list[OrderResult] = []
        for order in orders:
            results.append(OrderResting(oid=self._next_oid, cloid=order.cloid))
            self._next_oid += 1
        return results

    async def cancel_orders(
        self, cancels: list[CancelSpec], *, vault_address: str | None = None
    ) -> list[CancelResult]:
        self._record("cancel_orders", (list(cancels), vault_address))
        if self.cancel_results:
            return self.cancel_results.pop(0)
        return [CancelOk() for _ in cancels]

    async def create_sub_account(self, name: str) -> str:
        """SubAccountProvisioning, faked: a deterministic address per name, so
        a test can assert WHICH sub a later order was placed on."""
        self._record("create_sub_account", name)
        if self.sub_addresses:
            return self.sub_addresses.pop(0)
        self._next_sub += 1
        return f"0x{self._next_sub:040x}"

    async def rename_sub_account(self, sub_address: str, name: str) -> None:
        self._record("rename_sub_account", (sub_address.lower(), name))

    async def sub_account_transfer(
        self, sub_address: str, *, is_deposit: bool, usd_micro: int
    ) -> None:
        self._record("sub_account_transfer", (sub_address.lower(), is_deposit, usd_micro))

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        self._record("cancel_orders_by_cloid", list(cancels))
        if self.cancel_results:
            return self.cancel_results.pop(0)
        return [CancelOk() for _ in cancels]

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        self._record("modify_orders", list(modifies))
        if self.modify_results:
            return self.modify_results.pop(0)
        results: list[OrderResult] = []
        for modify in modifies:
            results.append(OrderResting(oid=modify.oid, cloid=modify.order.cloid))
        return results

    async def update_leverage(self, asset: int, leverage: int, *, is_cross: bool = True) -> None:
        self._record("update_leverage", (asset, leverage, is_cross))

    async def schedule_cancel(self, at: datetime | None) -> None:
        self._record("schedule_cancel", at)
