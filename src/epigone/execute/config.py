"""Execute-process configuration (issue #136).

Shared secrets (DATABASE_URL, ADMIN_TELEGRAM_ID, KEYSTORE_KEK_FILE) ride
epigone.config.Settings and the keystore's own env; everything below is
executor-only, env-tunable, and falls back to a safe default on a bad value
(the watchdog-config convention) so a misconfiguration never wedges the loop.

The exchange URL defaults to TESTNET. The info URL is derived from the exchange
URL so the executor can never read one network's book while trading on the
other, which is also why a malformed EXECUTOR_EXCHANGE_URL degrades BOTH urls
to the testnet default rather than one side of the pair.

THE ONE-NETWORK DOCTRINE, RE-SCOPED (issue #184). "Everything the executor
reads comes from the book it trades" held until a deployment mirrored MAINNET
Leaders onto the TESTNET book, and then it said the wrong thing about one read:
the Leader's liveness equity. Two kinds of read, and only the first is about
the book:

- ORDERS AND ORDER-ADJACENT READS — marks, market stats, asset specs, sub
  state, fills — are single-network BY CONSTRUCTION and still derive from the
  exchange URL. Nothing here is configurable, and nothing here may be: a size
  priced on one network and sent to another is the failure the derivation
  exists to make untypeable.
- THE LEADER'S ACCOUNT is a SIGNAL-NETWORK entity. Decision 7's question — "is
  this still the trader whose stats earned the copy?" — is about the account
  tracking observed, so its liveness read is pinned to EXECUTOR_SIGNAL_INFO_URL
  (default: the tracking product's MAINNET info endpoint, the same one the
  pollers use) rather than derived. This is the ONE documented exception, and
  the gateway that serves it is read-only and has exactly one caller.

On a mainnet deployment the two urls coincide and the distinction is invisible,
which is precisely why nothing caught it until the shakedown (ADR-0007
amendment D-6).

MAINNET TAKES TWO DELIBERATE ACTS (issue #137's live gate). Pointing
EXECUTOR_EXCHANGE_URL at mainnet is not enough: `HttpExecutionGateway` refuses
a mainnet URL at construction unless it is ALSO handed the explicit capability,
and `EXECUTOR_ALLOW_MAINNET` is the only thing in the codebase that hands it
over. Two switches rather than one because a URL is the kind of thing that gets
copied from a runbook, and because "did someone mean to go live" should be
answerable by grepping for one env var. Neither switch decides anything about
FUNDING — the account still has to hold real money — so going live remains a
manual operator act in three parts: the flag, the URL, and the deposit.

A5 SHIPPING THE GATE IS NOT A5 OPENING IT. The default stays False, compose
sets neither, and every test that has ever asserted "testnet by construction"
still passes — what changed is that there is now a documented way to say
otherwise, instead of a code path that could not be reached at all.
"""

import logging
import os
from dataclasses import dataclass
from datetime import timedelta

from epigone.config import parse_allow_mainnet, parse_positive_int
from epigone.gateway.execution_http import MAINNET_EXCHANGE_URL, TESTNET_EXCHANGE_URL
from epigone.gateway.http import INFO_URL, TESTNET_INFO_URL

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
    signal_info_url: str
    allow_mainnet: bool

    @classmethod
    def from_env(cls) -> "ExecutorConfig":
        exchange_url = _parse_exchange_url(os.environ.get("EXECUTOR_EXCHANGE_URL"))
        allow_mainnet = parse_allow_mainnet(os.environ.get("EXECUTOR_ALLOW_MAINNET"))
        if allow_mainnet and exchange_url != MAINNET_EXCHANGE_URL:
            # Not an error — the flag is harmless without the URL — but it is
            # the shape of a half-finished go-live, and an operator who set one
            # switch and not the other should hear it from the log rather than
            # from a copy that never landed on the book they were watching.
            log.warning(
                "EXECUTOR_ALLOW_MAINNET is set but EXECUTOR_EXCHANGE_URL is %s — "
                "still trading TESTNET",
                exchange_url,
            )
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
            signal_info_url=_parse_signal_info_url(os.environ.get("EXECUTOR_SIGNAL_INFO_URL")),
            allow_mainnet=allow_mainnet,
        )

    @property
    def is_mainnet(self) -> bool:
        """Whether this configuration actually trades real money — BOTH
        switches, which is the only question worth asking anywhere else."""
        return self.exchange_url == MAINNET_EXCHANGE_URL and self.allow_mainnet


def _parse_exchange_url(raw: str | None) -> str:
    if not raw:
        return TESTNET_EXCHANGE_URL
    if raw.endswith("/exchange"):
        return raw
    log.warning(
        "EXECUTOR_EXCHANGE_URL %r is not /exchange-shaped; using the testnet default", raw
    )
    return TESTNET_EXCHANGE_URL


def _parse_signal_info_url(raw: str | None) -> str:
    """The SIGNAL network's info endpoint (issue #184) — read-only, and the one
    read that is deliberately NOT derived from the exchange url.

    Defaults to the tracking product's MAINNET endpoint because that is where
    the pollers observed the Leader, and the Leader's account is a fact about
    the network the signal came from, not about the book this process trades.
    On a mainnet deployment the two coincide and nothing here is observable."""
    if not raw:
        return INFO_URL
    if raw.endswith("/info"):
        return raw
    log.warning(
        "EXECUTOR_SIGNAL_INFO_URL %r is not /info-shaped; using the mainnet default", raw
    )
    return INFO_URL


def _info_url_for(exchange_url: str) -> str:
    if exchange_url.endswith("/exchange"):
        return exchange_url.removesuffix("/exchange") + "/info"
    return TESTNET_INFO_URL  # unreachable after _parse_exchange_url; belt and braces
