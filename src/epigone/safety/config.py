"""Watchdog process configuration (issue #135).

Shared secrets (DATABASE_URL, ADMIN_TELEGRAM_ID, KEYSTORE_KEK_FILE) ride
epigone.config.Settings and the keystore's own env; everything below is
watchdog-only, env-tunable, and falls back to a safe default on a bad value
(the monitor-config convention) so a misconfiguration never wedges the
switch.

The exchange URL defaults to TESTNET and mainnet stays refused by
construction regardless: HttpExecutionGateway raises MainnetNotEnabledError
until A5's wiring passes allow_mainnet=True, which nothing here does. The
info URL is derived from the exchange URL so the watchdog can never read one
network's book while cancelling on the other — which is also why a malformed
WATCHDOG_EXCHANGE_URL degrades BOTH urls to the testnet default, never just
one side of the pair.
"""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta

from epigone.config import parse_positive_int
from epigone.gateway.execution_http import TESTNET_EXCHANGE_URL
from epigone.gateway.http import TESTNET_INFO_URL

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS = 10
# The executor (A4+) will beat every loop, i.e. every few seconds; a minute
# of silence is a dead or wedged process, not a slow cycle. /kill latency is
# bounded by the INTERVAL, not this: an operator halt sweeps next cycle.
DEFAULT_EXECUTOR_STALE_SECONDS = 60
# DB-blind trip threshold (PR #143 rounds 2–3, a deliberate call): if
# Postgres cannot answer the liveness question CONTINUOUSLY for this long —
# a failure streak; any successful read resets it — the watchdog cancels
# resting orders rather than sit blind: the outage that blinds it is the
# likeliest cause of a dead executor (correlated failures), cancelling is
# cheap and recoverable, and nothing here closes positions or spends. 3× the
# executor-stall default. Once tripped, the only thing between the incident
# and the wire is the venue enumeration's HTTP legs (30s/request, the read
# gateway's own bound) — the incident path performs ZERO Postgres work
# before the cancel (round 5), so there is no database term left in that
# leg. Worst case from outage onset to the trip: up to one poll interval
# for the streak to open, + this threshold, + up to one more poll interval
# plus one waiting cycle's ceilinged DB blocks (each bounded by
# watchdog.DB_BLOCK_CEILING_SECONDS; the trip quantizes to cycle starts) —
# hang-shaped outages included — for an ALREADY-RUNNING watchdog
# (cold-start during an outage has no cancel path; see the runbook).
DEFAULT_DB_BLIND_SECONDS = 3 * DEFAULT_EXECUTOR_STALE_SECONDS
# scheduleCancel horizon (the upgrade path, deadman.py): long enough that a
# deploy restart (seconds) or one slow cycle can't spuriously fire it, short
# enough that total host death strands resting orders for minutes, not hours.
DEFAULT_DEADMAN_HORIZON_SECONDS = 300
# Eligibility re-probe cadence while volume-gated: cumulative volume moves at
# trading speed, so hours, not cycles. Four probes a day keeps the audit
# trail legible.
DEFAULT_DEADMAN_REPROBE_HOURS = 6
# On-chain capability check cadence (PR #143 review): agent approvals change
# at ceremony speed (rotations, revocations), so a few checks a day bounds
# the beating-but-impotent window to hours while costing almost nothing.
DEFAULT_CAPABILITY_CHECK_HOURS = 6


@dataclass(frozen=True)
class WatchdogConfig:
    interval: timedelta
    executor_stale: timedelta
    db_blind_after: timedelta
    deadman_horizon: timedelta
    deadman_reprobe: timedelta
    capability_interval: timedelta
    exchange_url: str
    info_url: str

    @classmethod
    def from_env(cls) -> "WatchdogConfig":
        exchange_url = _parse_exchange_url(os.environ.get("WATCHDOG_EXCHANGE_URL"))
        return cls(
            interval=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_INTERVAL_SECONDS"),
                    default=DEFAULT_INTERVAL_SECONDS,
                    name="WATCHDOG_INTERVAL_SECONDS",
                )
            ),
            executor_stale=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_EXECUTOR_STALE_SECONDS"),
                    default=DEFAULT_EXECUTOR_STALE_SECONDS,
                    name="WATCHDOG_EXECUTOR_STALE_SECONDS",
                )
            ),
            db_blind_after=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_DB_BLIND_SECONDS"),
                    default=DEFAULT_DB_BLIND_SECONDS,
                    name="WATCHDOG_DB_BLIND_SECONDS",
                )
            ),
            deadman_horizon=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_DEADMAN_HORIZON_SECONDS"),
                    default=DEFAULT_DEADMAN_HORIZON_SECONDS,
                    name="WATCHDOG_DEADMAN_HORIZON_SECONDS",
                )
            ),
            deadman_reprobe=timedelta(
                hours=parse_positive_int(
                    os.environ.get("WATCHDOG_DEADMAN_REPROBE_HOURS"),
                    default=DEFAULT_DEADMAN_REPROBE_HOURS,
                    name="WATCHDOG_DEADMAN_REPROBE_HOURS",
                )
            ),
            capability_interval=timedelta(
                hours=parse_positive_int(
                    os.environ.get("WATCHDOG_CAPABILITY_CHECK_HOURS"),
                    default=DEFAULT_CAPABILITY_CHECK_HOURS,
                    name="WATCHDOG_CAPABILITY_CHECK_HOURS",
                )
            ),
            exchange_url=exchange_url,
            info_url=_info_url_for(exchange_url),
        )


def _parse_exchange_url(raw: str | None) -> str:
    """A /exchange-shaped URL or the testnet default. A malformed value must
    not survive into EITHER url of the pair (module docstring): reading one
    network's book while cancelling on another would make the sweep's
    verify-by-enumeration a lie, so the degrade replaces the whole pair."""
    if not raw:
        return TESTNET_EXCHANGE_URL
    if raw.endswith("/exchange"):
        return raw
    log.warning(
        "WATCHDOG_EXCHANGE_URL %r is not /exchange-shaped; using the testnet default", raw
    )
    return TESTNET_EXCHANGE_URL


def _info_url_for(exchange_url: str) -> str:
    """The /info endpoint on the SAME host as the (already validated)
    exchange URL."""
    if exchange_url.endswith("/exchange"):
        return exchange_url.removesuffix("/exchange") + "/info"
    return TESTNET_INFO_URL  # unreachable after _parse_exchange_url; belt and braces
