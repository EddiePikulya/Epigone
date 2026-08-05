"""TESTNET sub-account RENAME probe (issue #178) — run explicitly:

    uv run python scripts/testnet_subaccount_rename_probe.py [sub_name]

Never part of the unit suite; the suite must stay network-free. Targets
api.hyperliquid-testnet.xyz ONLY, throwaway keys from
~/.epigone/testnet-keys.json (worktree .testnet-keys.json fallback).

The one question issue #178 asks: DOES HYPERLIQUID OFFER A SUB-ACCOUNT RENAME
ACTION? Adoption re-uses an orphaned sub whose name was spent by whatever
created it (`capprobe_003`), while the mapping wants it named for its Leader.
If a rename exists, adoption uses it; if not, the mismatch is cosmetic and the
ticket says move on — so this probe decides which sentence the code carries,
and nothing more.

Method: submit the candidate action names against a sub the master already
holds, printing the exchange's own answer for each. An UNKNOWN action type
comes back as a deserialization error that ENUMERATES the valid variants,
which is the answer either way — the readback of the `subAccounts` listing's
`name` field is the observable, never the ack.

Sub-accounts cannot be deleted, so this probe never creates one: it renames a
sub the throwaway master already holds and prints the before/after listing.

Findings land in the SubAccountProvisioning protocol docstring (the #63
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
from hyperliquid.utils.signing import get_timestamp_ms, sign_l1_action

DURABLE_KEYS_PATH = Path.home() / ".epigone" / "testnet-keys.json"
WORKTREE_KEYS_PATH = Path(__file__).resolve().parent.parent / ".testnet-keys.json"

RENAMED_SUFFIX = "_renamed"


def load_keys() -> dict[str, Any]:
    for path in (DURABLE_KEYS_PATH, WORKTREE_KEYS_PATH):
        if path.exists():
            return json.loads(path.read_text())
    sys.exit(f"no testnet keys at {DURABLE_KEYS_PATH} or {WORKTREE_KEYS_PATH}")


def listing(info: Info, master_addr: str) -> list[dict[str, Any]]:
    return info.post("/info", {"type": "subAccounts", "user": master_addr}) or []


def submit(exchange: Exchange, action: dict[str, Any]) -> Any:
    """One raw L1 action, signed as the master (the shape every probe here
    uses when the SDK has no method for the action under test)."""
    timestamp = get_timestamp_ms()
    signature = sign_l1_action(exchange.wallet, action, None, timestamp, None, False)
    return exchange._post_action(action, signature, timestamp)


def attempt(label: str, call: Any) -> None:
    """Run one probe call and print its outcome. The ack is never the
    observable here — the listing readback is — so nothing is returned."""
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 - a harness records, never crashes
        print(f"  {label} -> {exc!r}", flush=True)
        return
    print(f"  {label} -> {result!r}", flush=True)


def main() -> None:
    assert "testnet" in TESTNET_API_URL, "this probe is testnet-ONLY"
    print(f"Hyperliquid TESTNET sub-account RENAME probe — target {TESTNET_API_URL}")
    keys = load_keys()
    master = Account.from_key(keys["master"])
    master_addr = master.address.lower()
    print(f"throwaway master: {master_addr}")

    info = Info(TESTNET_API_URL, skip_ws=True)
    subs = listing(info, master_addr)
    print(f"sub-accounts held: {[sub['name'] for sub in subs]}")
    if not subs:
        sys.exit("the throwaway master holds no sub-accounts — nothing to rename")

    wanted = sys.argv[1] if len(sys.argv) > 1 else None
    target = next(
        (sub for sub in subs if wanted is None or sub["name"] == wanted), None
    )
    if target is None:
        sys.exit(f"no sub named {wanted!r}; held: {[sub['name'] for sub in subs]}")
    sub_addr = str(target["subAccountUser"]).lower()
    old_name = target["name"]
    new_name = old_name + RENAMED_SUFFIX
    print(f"renaming {old_name!r} ({sub_addr}) -> {new_name!r}")

    # Candidate spellings, most-likely first. An unknown `type` answers with a
    # deserialization error naming every variant the exchange DOES accept,
    # which is the finding whichever way it lands.
    candidates: list[dict[str, Any]] = [
        {"type": "subAccountModify", "subAccountUser": sub_addr, "name": new_name},
        {"type": "modifySubAccount", "subAccountUser": sub_addr, "name": new_name},
        {"type": "renameSubAccount", "subAccountUser": sub_addr, "name": new_name},
    ]
    as_master = Exchange(master, TESTNET_API_URL)
    for action in candidates:
        attempt(str(action["type"]), lambda action=action: submit(as_master, action))
        time.sleep(1)
        names = [sub["name"] for sub in listing(info, master_addr)]
        if new_name in names:
            print(f"\nRENAME WORKS via {action['type']!r}: listing now {names}")
            return
    print(
        f"\nNO RENAME OBSERVED — the listing still reads "
        f"{[sub['name'] for sub in listing(info, master_addr)]}. An adopted sub "
        "keeps the name whatever minted it spent; the mismatch is cosmetic."
    )


if __name__ == "__main__":
    main()
