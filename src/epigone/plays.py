"""The most-played ranking (issue #80), defined once: per (address, coin), a
coin's weight is its completed round-trips plus a point for a currently-open
episode, ties broken on the coin name for a stable order.

Both consumers read this fragment — the profile's "Most played" line
(epigone.bot.handlers) scoped to one wallet, and the focus-market ticker
filter (#108) ranking the whole Universe inside the screener query — so the
ranking can never drift between the two surfaces.
"""

# Universe-wide ranked plays: one row per (address, coin) with the completed
# round-trip count, whether an episode is open right now, and the coin's rank
# within its wallet. Callers wrap it in a subquery and filter; a WHERE on
# `address` pushes down through the window (it partitions on address), so the
# single-wallet read stays on the primary-key index.
RANKED_PLAYS_SQL = """
    SELECT address, coin,
           count(*) FILTER (WHERE src = 'trade')::int AS trips,
           bool_or(src = 'open') AS is_open,
           row_number() OVER (
               PARTITION BY address
               ORDER BY count(*) FILTER (WHERE src = 'trade')
                        + CASE WHEN bool_or(src = 'open') THEN 1 ELSE 0 END DESC,
                        coin
           )::int AS play_rank
    FROM (
        SELECT address, coin, 'trade' AS src FROM fine_trades
        UNION ALL
        SELECT address, coin, 'open' AS src FROM fine_open_episodes
    ) plays
    GROUP BY address, coin
"""
