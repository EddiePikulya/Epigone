"""TESTNET sub-account CAP probe (issue #136, ADR-0007 decision 1) — run explicitly:

    uv run python scripts/testnet_subaccount_cap_probe.py [max_attempts]

Never part of the unit suite; the suite must stay network-free. Targets
api.hyperliquid-testnet.xyz ONLY, throwaway keys from
~/.epigone/testnet-keys.json (worktree .testnet-keys.json fallback).

The one question: HOW MANY sub-accounts can one master hold? ADR-0007's
capital model gives every copy-enabled Leader a dedicated Copy Sub-account,
so the cap is the hard ceiling on concurrent Leaders — the ADR says "probe
before implementation" for exactly that reason.

Method: create sub-accounts named `capprobe_NNN` one at a time past the
already-crossed $100k volume gate, reading the `subAccounts` listing back
after each (the readback is the observable, never the ack), until one is
REFUSED or `max_attempts` is reached. A refusal prints the exchange's own
prose, which is what a cap looks like on this venue (cf. the agent-cap
refusal "Too many extra agents for cumulative volume traded. Current limit
is 3", finding 1 in epigone.gateway.execution).

Sub-accounts cannot be deleted, so this probe is one-way on the throwaway
master. It is safe: an unfunded sub holds nothing and costs nothing.

Findings land in the ExecutionGateway protocol docstring (the #63
empirical-contract convention) — this transcript is the evidence.
"""

import json
import sys
import time
from pathlib import Path
from typing import Any

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import TESTNET_API_URL

DURABLE_KEYS_PATH = Path.home() / ".epigone" / "testnet-keys.json"
WORKTREE_KEYS_PATH = Path(__file__).resolve().parent.parent / ".testnet-keys.json"

NAME_PREFIX = "capprobe"
# A bound, not a belief: if the venue's real cap is far above this the probe
# reports "no cap observed below N", which is still the answer A4 needs (the
# operator will never run 40 Leaders in phase A).
DEFAULT_MAX_ATTEMPTS = 40


def load_keys() -> dict[str, Any]:
    for path in (DURABLE_KEYS_PATH, WORKTREE_KEYS_PATH):
        if path.exists():
            return json.loads(path.read_text())
    sys.exit(f"no testnet keys at {DURABLE_KEYS_PATH} or {WORKTREE_KEYS_PATH}")


def sub_names(info: Info, master_addr: str) -> list[str]:
    subs = info.post("/info", {"type": "subAccounts", "user": master_addr}) or []
    return [sub["name"] for sub in subs]


def main() -> None:
    assert "testnet" in TESTNET_API_URL, "this probe is testnet-ONLY"
    max_attempts = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MAX_ATTEMPTS
    print(f"Hyperliquid TESTNET sub-account CAP probe — target {TESTNET_API_URL}")
    keys = load_keys()
    master = Account.from_key(keys["master"])
    master_addr = master.address.lower()
    print(f"throwaway master: {master_addr}")

    info = Info(TESTNET_API_URL, skip_ws=True)
    rate = info.post("/info", {"type": "userRateLimit", "user": master_addr})
    print(f"cumVlm: {rate['cumVlm']} (createSubAccount gate is $100k)")

    existing = sub_names(info, master_addr)
    print(f"sub-accounts already held: {len(existing)} {existing}")

    as_master = Exchange(master, TESTNET_API_URL)
    held = len(existing)
    for attempt in range(max_attempts):
        name = f"{NAME_PREFIX}_{attempt:03d}"
        if name in existing:
            print(f"  {name}: already exists (prior run); skipping")
            continue
        try:
            result: Any = as_master.create_sub_account(name)
        except Exception as exc:  # noqa: BLE001 - a harness records, never crashes
            result = exc
        print(f"  createSubAccount({name!r}) -> {result!r}", flush=True)
        time.sleep(2)
        names = sub_names(info, master_addr)
        if name not in names:
            print(
                f"\nCAP OBSERVED: {name} was refused; the master holds {len(names)} "
                f"sub-account(s): {names}"
            )
            return
        held = len(names)
        print(f"    held now: {held}")

    print(
        f"\nNO CAP OBSERVED below {max_attempts} creations — the master holds {held} "
        f"sub-account(s). The A4 ceiling is therefore not the venue's."
    )


if __name__ == "__main__":
    main()
