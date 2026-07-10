"""Ingest's share of the Hyperliquid weight budget (~1/3 per the V1 spec
rate-budget decision; stream owns the rest — epigone.stream.poller).

Coarse metrics come free with the leaderboard download (issue #26), so the
whole budget funds the fine pass's per-Trader userFills calls."""

INGEST_WEIGHT_PER_MINUTE = 400
FILLS_WEIGHT = 20
