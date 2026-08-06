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
