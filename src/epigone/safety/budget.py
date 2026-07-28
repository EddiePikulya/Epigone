"""The safety lane's rate budget: shared when Postgres answers, in-process
when it doesn't (issue #135, PR #143 round 2).

SharedWeightBudget takes `SELECT … FOR UPDATE` on the rate_budget row for
every spend — including the one HttpExecutionGateway makes immediately
before signing a cancel. That made the kill path hard-depend on Postgres at
the exact moment a correlated infrastructure outage is the likeliest reason
the executor is dead. This wrapper closes that hole for the SAFETY lane
only: try the shared bucket; if the database cannot grant, log loudly,
degrade to an in-process bucket, and PROCEED — rate-limit risk during an
incident is strictly less bad than not cancelling. (The lock-wedged case —
a holder dead mid-transaction — surfaces as an exception at all only
because the safety pool bounds lock waits: epigone.safety.db.)

The executor's ORDER lane must never use this: overspending to place orders
is the opposite trade (evidence and pacing before we spend money; action
before we stop losing it — the audit module's asymmetry, applied to pacing).

Degraded-mode honesty (round 3 item 6): the fallback matches the shared
REFILL rate (900/min) but NOT the shared design's burst discipline — it
starts full at 900 capacity versus the shared bucket's 240 burst cap, and
has no 20/s send gate, both of which exist because unsmoothed spikes 429
(issue #41). So a degraded incident can briefly burst harder than the
healthy system ever would. Accepted deliberately, not equivalence-claimed:
a sweep cycle is ~40–260 weight, the window lasts only while Postgres is
down, and the alternative is a kill switch queued behind a dead lock row.
It is also uncoordinated with other processes during flapping — same
acceptance, same reasoning.
"""

import logging

from epigone.budget import SHARED_WEIGHT_PER_MINUTE, Budget, WeightBudget
from epigone.clock import Clock

log = logging.getLogger(__name__)


class FallbackBudget:
    """Budget for the safety lane: `primary` (the shared Postgres bucket)
    first, an in-process WeightBudget when the primary's storage fails."""

    def __init__(self, primary: Budget, clock: Clock) -> None:
        self._primary = primary
        self._fallback = WeightBudget(SHARED_WEIGHT_PER_MINUTE, clock)

    async def spend(self, weight: int) -> None:
        try:
            await self._primary.spend(weight)
        except Exception:
            log.exception(
                "shared rate budget unavailable — degrading to in-process pacing "
                "(safety lane, weight %d): the protective action proceeds",
                weight,
            )
            await self._fallback.spend(weight)

    async def settle(self, weight: int) -> None:
        try:
            await self._primary.settle(weight)
        except Exception:
            log.exception(
                "shared rate budget unavailable for settle (safety lane, weight %d)", weight
            )
            await self._fallback.settle(weight)
