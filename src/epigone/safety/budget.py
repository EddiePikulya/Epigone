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
from epigone.safety.db import SAFETY_DB_TIMEOUT_SECONDS

log = logging.getLogger(__name__)

# A hard real-time ceiling on each DATABASE ATTEMPT of the shared bucket:
# its spend/settle run inside `conn.transaction()`, the one shape the pool's
# timeouts cannot bound (asyncpg's transaction exit awaits an untimed
# cancel_waiter — round 5). The safety lane constructs its
# SharedWeightBudget with `attempt_ceiling=` this value (main.py), which
# ceilings the transaction attempt WITHOUT touching legitimate pacing waits
# (a token-deficit sleep may honestly exceed any DB ceiling and must not
# degrade the lane to the fallback). The gateway's pre-sign spend and the
# watchdog's read spends all pass through that one seam. Safe composition:
# Pool.release is shielded, so a cancelled attempt's connection is still
# terminated within the acquire budget.
PRIMARY_ATTEMPT_CEILING_SECONDS = 4 * SAFETY_DB_TIMEOUT_SECONDS


class FallbackBudget:
    """Budget for the safety lane: `primary` (the shared Postgres bucket)
    first, an in-process WeightBudget when the primary's storage fails —
    and the in-process bucket ALONE while an incident is declared.

    `incident_mode` is the round-5 structural rule: rounds 2–4 each bounded
    one more hanging leg of the shared bucket's Postgres path (command
    timeout, then release, then rollback) and each time the same bug
    reappeared one layer deeper — asyncpg's transaction exit awaits an
    UNTIMED cancel_waiter before any per-op timeout arms, so a partition
    striking mid-transaction still hangs past every bound. So during a
    declared incident (a DB-blind window or a trip — the watchdog flips
    this flag) the shared bucket is not attempted AT ALL: it is a
    coordination optimisation for normal operation, and during an incident
    the only thing this lane does is cancel — correctness beats
    coordination. Postgres-free by construction, not by exception handling:
    nothing left to hang."""

    def __init__(self, primary: Budget, clock: Clock) -> None:
        self._primary = primary
        self._fallback = WeightBudget(SHARED_WEIGHT_PER_MINUTE, clock)
        self.incident_mode = False

    async def spend(self, weight: int) -> None:
        if self.incident_mode:
            await self._fallback.spend(weight)
            return
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
        if self.incident_mode:
            await self._fallback.settle(weight)
            return
        try:
            await self._primary.settle(weight)
        except Exception:
            log.exception(
                "shared rate budget unavailable for settle (safety lane, weight %d)", weight
            )
            await self._fallback.settle(weight)
