"""Execute-process configuration (issue #136).

Shared secrets (DATABASE_URL, ADMIN_TELEGRAM_ID, KEYSTORE_KEK_FILE) ride
epigone.config.Settings and the keystore's own env; everything below is
executor-only, env-tunable, and falls back to a safe default on a bad value
(the watchdog-config convention) so a misconfiguration never wedges the loop.

The exchange URL defaults to TESTNET and mainnet stays refused BY
CONSTRUCTION regardless: HttpExecutionGateway raises MainnetNotEnabledError
until A5's wiring passes allow_mainnet=True, which nothing here does — the
ticket's live gate, unchanged. The info URL is derived from the exchange URL
so the executor can never read one network's book while trading on the other,
which is also why a malformed EXECUTOR_EXCHANGE_URL degrades BOTH urls to the
testnet default rather than one side of the pair.
"""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta

from epigone.config import parse_positive_int
from epigone.gateway.execution_http import TESTNET_EXCHANGE_URL
from epigone.gateway.http import TESTNET_INFO_URL

log = logging.getLogger(__name__)

# The copy loop's cadence. The signal itself is 10–20s old by construction
# (the poller's 10s cadence plus the position pass), so a faster executor buys
# nothing; what the interval must stay well under is the watchdog's 60s
# executor-stall threshold, since every cycle beats the heartbeat that
# threshold reads. Five seconds is comfortably inside it and adds ~2.5s of
# expected latency to a copy.
DEFAULT_INTERVAL_SECONDS = 5


@dataclass(frozen=True)
class ExecutorConfig:
    interval: timedelta
    exchange_url: str
    info_url: str

    @classmethod
    def from_env(cls) -> "ExecutorConfig":
        exchange_url = _parse_exchange_url(os.environ.get("EXECUTOR_EXCHANGE_URL"))
        return cls(
            interval=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("EXECUTOR_INTERVAL_SECONDS"),
                    default=DEFAULT_INTERVAL_SECONDS,
                    name="EXECUTOR_INTERVAL_SECONDS",
                )
            ),
            exchange_url=exchange_url,
            info_url=_info_url_for(exchange_url),
        )


def _parse_exchange_url(raw: str | None) -> str:
    if not raw:
        return TESTNET_EXCHANGE_URL
    if raw.endswith("/exchange"):
        return raw
    log.warning(
        "EXECUTOR_EXCHANGE_URL %r is not /exchange-shaped; using the testnet default", raw
    )
    return TESTNET_EXCHANGE_URL


def _info_url_for(exchange_url: str) -> str:
    if exchange_url.endswith("/exchange"):
        return exchange_url.removesuffix("/exchange") + "/info"
    return TESTNET_INFO_URL  # unreachable after _parse_exchange_url; belt and braces
