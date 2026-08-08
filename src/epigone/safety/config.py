"""Watchdog process configuration (issue #135).

Shared secrets (DATABASE_URL, ADMIN_TELEGRAM_ID, KEYSTORE_KEK_FILE) ride
epigone.config.Settings and the keystore's own env; everything below is
watchdog-only, env-tunable, and falls back to a safe default on a bad value
(the monitor-config convention) so a misconfiguration never wedges the
switch.

`WATCHDOG_KEY_CACHE_FILE` is the one knob here that is not a duration: where
the cold-start key cache lives (issue #145). It must sit on storage that
OUTLIVES the container — the whole point is surviving a restart during a
Postgres outage — so the compose service points it at a named volume; the
default is a path under the operator's home for local runs.

`WATCHDOG_DEADMAN_PING_URL` is the other one, and it is the only knob here
that is a SECRET (issue #213): the operator creates a check on an external
dead-man service and pastes its ping URL, whose path is the credential. It
has no default and cannot have one — nobody but the operator can create that
check — so unset is a legitimate, fully supported configuration in which the
watchdog runs exactly as before and the out-of-band page path simply does not
exist. `epigone.safety.main` says which of the two the process is in at
startup, loudly, because that is the assumption an operator is most likely to
make wrongly. Setting it is a MAINNET GATE, not a testnet one.

The exchange URL defaults to TESTNET. The info URL is derived from the exchange
URL so the watchdog can never read one network's book while cancelling on the
other — which is also why a malformed WATCHDOG_EXCHANGE_URL degrades BOTH urls
to the testnet default, never just one side of the pair.

MAINNET RIDES THE EXECUTOR'S OWN FLAG, deliberately: `EXECUTOR_ALLOW_MAINNET`
opens the gate here too (issue #137). The watchdog is the executor's dead-man's
switch and /kill's only sweeping hand, so a live executor beside a
testnet-refused watchdog is the one configuration nobody should be able to
reach by setting a single variable — the halt would record, the sweep would
raise at gateway construction, and resting orders would stay on a real book.
One flag for the pair means the dangerous asymmetry cannot be typed. The URL
is still separately set per process (WATCHDOG_EXCHANGE_URL), so the flag alone
changes nothing here either.
"""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from epigone.config import parse_allow_mainnet, parse_positive_int
from epigone.gateway.execution_http import TESTNET_EXCHANGE_URL
from epigone.gateway.http import TESTNET_INFO_URL
from epigone.safety.keycache import default_cache_path

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
# hang-shaped outages included. A process that STARTS into the outage has its
# own, equally bounded path: the cold-start grace below, then its first cycle.
DEFAULT_DB_BLIND_SECONDS = 3 * DEFAULT_EXECUTOR_STALE_SECONDS
# How long startup keeps probing an unreachable Postgres before COLD-STARTING
# BLIND (issue #145): loading the watchdog lane's key from the encrypted local
# cache and declaring the incident at launch. Deliberately the same span as
# the blind threshold, so one rule covers both shapes — Postgres unreachable
# CONTINUOUSLY for this long is an incident, however the process got there.
# Zero would cancel the whole book because a deploy landed during a five-second
# blip; the probe returns the instant the database answers, so a healthy
# restart pays nothing for the window.
DEFAULT_COLDSTART_GRACE_SECONDS = DEFAULT_DB_BLIND_SECONDS
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
# How often the external dead-man service is told this process is alive
# (issue #213). A minute is a small fraction of any sane grace period on the
# operator's check, so a handful of dropped pings is absorbed while a dead
# watchdog trips the check on the operator's own schedule rather than a
# multiple of ours. Tunable mostly for the free tiers of ping services that
# meter by request count.
DEFAULT_DEADMAN_PING_SECONDS = 60


@dataclass(frozen=True)
class WatchdogConfig:
    interval: timedelta
    executor_stale: timedelta
    db_blind_after: timedelta
    coldstart_grace: timedelta
    deadman_horizon: timedelta
    deadman_reprobe: timedelta
    capability_interval: timedelta
    exchange_url: str
    info_url: str
    allow_mainnet: bool
    key_cache_path: Path
    # None = no out-of-band page path configured (module docstring). The
    # interval is still parsed and carried, so setting the URL later is the
    # only change an operator has to make.
    deadman_ping_url: str | None
    deadman_ping_interval: timedelta

    @classmethod
    def from_env(cls) -> "WatchdogConfig":
        exchange_url = _parse_exchange_url(os.environ.get("WATCHDOG_EXCHANGE_URL"))
        cache_file = os.environ.get("WATCHDOG_KEY_CACHE_FILE")
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
            coldstart_grace=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_COLDSTART_GRACE_SECONDS"),
                    default=DEFAULT_COLDSTART_GRACE_SECONDS,
                    name="WATCHDOG_COLDSTART_GRACE_SECONDS",
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
            # The executor's variable by name, on purpose (module docstring):
            # the sweep must be able to reach whatever book the executor can
            # trade on, and one flag is what makes that impossible to get half
            # right.
            allow_mainnet=parse_allow_mainnet(os.environ.get("EXECUTOR_ALLOW_MAINNET")),
            key_cache_path=Path(cache_file) if cache_file else default_cache_path(),
            deadman_ping_url=_parse_ping_url(os.environ.get("WATCHDOG_DEADMAN_PING_URL")),
            deadman_ping_interval=timedelta(
                seconds=parse_positive_int(
                    os.environ.get("WATCHDOG_DEADMAN_PING_SECONDS"),
                    default=DEFAULT_DEADMAN_PING_SECONDS,
                    name="WATCHDOG_DEADMAN_PING_SECONDS",
                )
            ),
        )


def _parse_ping_url(raw: str | None) -> str | None:
    """The external dead-man ping URL, or None for "not configured".

    The house convention for a bad value is a safe default, and here the safe
    default is OFF: a URL we cannot ping is not a page path, and pretending it
    is would be worse than admitting there is none — an operator who believes
    the out-of-band path is armed stops looking for the fact that it isn't.
    So a malformed value degrades to unconfigured, and the startup log the
    unconfigured case already prints is what carries it.

    Scheme-checked and nothing more. The path is opaque on purpose (it is the
    credential), the host is the operator's choice of service, and a URL that
    is well-formed but wrong still announces itself the first time it is
    pinged — `ExternalPing` logs a 4xx as "the operator will NOT be paged"."""
    if not raw or not raw.strip():
        return None
    url = raw.strip()
    if not url.startswith(("http://", "https://")):
        log.warning(
            "WATCHDOG_DEADMAN_PING_URL is not an http(s) URL; the watchdog will run "
            "with NO out-of-band page path"
        )
        return None
    if url.startswith("http://"):
        # Accepted, not refused — a self-hosted checker on a private network is
        # a legitimate setup, and refusing here would trade a working page path
        # for a purity point. But the path is the credential, so plaintext puts
        # it on the wire for anyone between here and there, and the operator
        # should hear that once at startup rather than never.
        log.warning(
            "WATCHDOG_DEADMAN_PING_URL is plain http; the ping URL's path is a "
            "credential and will travel in clear — prefer https"
        )
    return url


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
