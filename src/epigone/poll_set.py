"""The poll set: every distinct wallet whose positions Epigone watches.

One definition, read by everything that watches a wallet — the REST poll pass
(issue #4), the websocket lanes that subscribe to exactly this set (#157, #168),
and the cutover's ownership decision, which cannot hand production to a
transport that does not cover every wallet the other one does (#158).

It lives in its own module for that last reason. While there was one watcher
the definition could sit inside it; with three, a copy of the UNION in each is
three chances for the lanes to watch different subjects — and a difference in
COVERAGE would read as a difference in transports, which is exactly the mistake
the shadow comparison exists to avoid making.
"""

import asyncpg

# The poll set, negated: a wallet neither tracked by anyone nor linked as some
# User's own (#121). The positive form is `fetch_poll_set`'s UNION; this is the
# one place the two must agree, so widening the poll set means editing both.
OFF_THE_POLL_SET = """
    trader_address NOT IN (SELECT trader_address FROM tracks)
    AND trader_address NOT IN
        (SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL)
"""


async def fetch_poll_set(conn: asyncpg.Pool | asyncpg.Connection) -> list[str]:
    """Every distinct wallet whose positions Epigone watches, sorted.

    Tracked wallets UNION Users' own linked wallets (#121). A linked wallet is
    polled purely so its positions are snapshotted as the User's holdings
    reference — the diff still runs, but the alert fan-out reaches only `tracks`
    followers, so a wallet nobody tracks produces zero alerts. UNION dedups the
    both-roles case, so it costs one poll either way.

    Budget delta: each distinct wallet costs POSITIONS_WEIGHT per POSITION_VENUE
    — 2 venues × weight 2 = 4/pass — whether it is tracked, linked, or both.
    Linked wallets are one-per-User and only the followers' own, so in practice
    they add a handful of wallets, not a multiplier.

    Takes a pool or a connection: the ownership decision (#158) reads it inside
    the transaction that may transfer production on the strength of it."""
    rows = await conn.fetch(
        """
        SELECT trader_address FROM tracks
        UNION
        SELECT linked_wallet FROM users WHERE linked_wallet IS NOT NULL
        ORDER BY 1
        """
    )
    return [row["trader_address"] for row in rows]


async def leaders_first(
    conn: asyncpg.Pool | asyncpg.Connection, addresses: list[str]
) -> list[str]:
    """The poll set with copy-enabled Leaders at the front (issue #158).

    Every consumer of the poll set has a scarce resource to spend on it and the
    same answer about who gets it first: the wallets that move money.

    - The REST pass is paced by the shared weight budget, so a set too large
      for its cadence stretches at the TAIL — and a Leader must never be in the
      tail. Ordering IS the prioritisation the degraded mode needs; nothing
      else is required.
    - The websocket lanes can hold 15 unique users per IP (ADR-0008) and take a
      prefix of the poll set when it is larger. An alphabetical prefix decides
      by leading hex digit, which is the one criterion with no meaning at all
      (#158's 2026-08-04 comment: "selection needs to be deliberate, not
      alphabetical").

    Applied in every mode rather than only degraded ones: the ordering is
    harmless when nothing is scarce, and a rule that only runs during incidents
    is a rule nobody has tested. Ties break alphabetically, so the order is
    stable — a lane re-reading the set does not churn its subscriptions."""
    leaders = {
        row["leader_address"]
        for row in await conn.fetch(
            "SELECT DISTINCT leader_address FROM copy_subs WHERE enabled"
        )
    }
    return sorted(addresses, key=lambda address: (address not in leaders, address))
