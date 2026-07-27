"""TESTNET rehearsal of the agent-approval ceremony shapes (issue #134).

Probes the mechanics behind docs/runbooks/agent-key-ceremony.md against the
Hyperliquid TESTNET, with throwaway keys generated in-process — nothing here
ever touches mainnet, real funds, or a real master key, and no key material is
persisted anywhere. The script hard-refuses any URL that is not the testnet.

What it probes:
  (a)  approveAgent for an EXTERNALLY-generated agent address (the
       Epigone-generates-the-key ceremony shape), signed by a throwaway
       testnet master via the SDK's own sign_agent helper — the SDK's stock
       `approve_agent()` cannot do this (it always generates the key itself),
       so this is the exact code path a real shape-(a) ceremony would need.
  (a') the SDK's stock `approve_agent()` for comparison.
  (ex) the `valid_until` agent-name suffix that encodes an expiry.
  (rb) the public read-back (`extraAgents`, `userRole`) a user would use to
       verify and revoke our authority from outside our system.

Run:  .venv/bin/python scripts/rehearse_agent_ceremony.py
Paste the transcript into the runbook's rehearsal record.
"""

import json
import sys
import time

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import TESTNET_API_URL
from hyperliquid.utils.signing import get_timestamp_ms, sign_agent

from epigone.keystore import DEFAULT_CEREMONY_DAYS

BASE_URL = TESTNET_API_URL


def main() -> None:
    if "testnet" not in BASE_URL:
        sys.exit("refusing to run: this rehearsal is testnet-only")
    print(f"# Agent-ceremony rehearsal against {BASE_URL}")
    print("# All keys below are throwaway, generated in-process for this run.\n")

    master = Account.create()
    print(f"throwaway testnet master: {master.address}")

    exchange = Exchange(master, BASE_URL)
    info = Info(BASE_URL, skip_ws=True)

    # --- (a) externally-generated agent address, approveAgent built by hand ---
    external_agent = Account.create()
    print(f"\n[a] approveAgent for externally-generated agent {external_agent.address}")
    timestamp = get_timestamp_ms()
    valid_until_ms = timestamp + DEFAULT_CEREMONY_DAYS * 24 * 3600 * 1000
    action = {
        "type": "approveAgent",
        "agentAddress": external_agent.address,
        "agentName": f"epigone-rehearsal valid_until {valid_until_ms}",
        "nonce": timestamp,
    }
    signature = sign_agent(master, action, is_mainnet=False)
    result = exchange._post_action(action, signature, timestamp)
    print(f"    response: {json.dumps(result)}")

    # --- (a') the SDK's stock approve_agent, which generates the key itself ---
    print("\n[a'] SDK stock approve_agent (SDK generates the agent key)")
    result, agent_key = exchange.approve_agent(name="epigone-rehearsal-sdk")
    agent_address = Account.from_key(agent_key).address
    print(f"    generated agent address: {agent_address} (key discarded)")
    print(f"    response: {json.dumps(result)}")

    # --- (rb) public read-back: what the user can verify outside our system ---
    time.sleep(2)
    print(f"\n[rb] extraAgents({master.address})")
    try:
        agents = info.post("/info", {"type": "extraAgents", "user": master.address})
        print(f"    {json.dumps(agents)}")
    except Exception as error:  # a probe, not production code
        print(f"    query failed: {error}")
    print(f"\n[rb] userRole({master.address})")
    try:
        role = info.post("/info", {"type": "userRole", "user": master.address})
        print(f"    {json.dumps(role)}")
    except Exception as error:
        print(f"    query failed: {error}")

    print(
        "\n# Shape (b) — UI-generated agent key — cannot be rehearsed headlessly:\n"
        "# it requires app.hyperliquid-testnet.xyz + a browser wallet. Checklist in\n"
        "# docs/runbooks/agent-key-ceremony.md."
    )


if __name__ == "__main__":
    main()
