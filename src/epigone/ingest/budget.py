"""Weight budgeter: keeps ingest inside its share of Hyperliquid's 1200 weight/min
per-IP limit (~1/3 per the V1 spec rate-budget decision; stream owns the rest)."""

from epigone.clock import Clock

INGEST_WEIGHT_PER_MINUTE = 400
PORTFOLIO_WEIGHT = 20
FILLS_WEIGHT = 20


class WeightBudget:
    """Token bucket: starts full (one burst), refills continuously at the configured rate."""

    def __init__(self, weight_per_minute: int, clock: Clock) -> None:
        self._capacity = float(weight_per_minute)
        self._rate_per_second = weight_per_minute / 60.0
        self._clock = clock
        self._available = self._capacity
        self._last_refill = clock.now()

    async def spend(self, weight: int) -> None:
        """Block (via the injected clock) until `weight` fits in the budget, then take it."""
        if weight > self._capacity:
            raise ValueError(f"weight {weight} exceeds per-minute capacity {self._capacity}")
        while True:
            self._refill()
            if self._available >= weight:
                self._available -= weight
                return
            deficit = weight - self._available
            await self._clock.sleep(deficit / self._rate_per_second)

    def _refill(self) -> None:
        now = self._clock.now()
        elapsed = (now - self._last_refill).total_seconds()
        self._last_refill = now
        self._available = min(self._capacity, self._available + elapsed * self._rate_per_second)
