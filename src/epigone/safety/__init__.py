"""The A3 execution safety layer (issue #135, ADR-0005).

The pieces that must exist BEFORE the first real order, settled by the
funded-testnet probe (PR #141):

- `audit` — the append-only execution audit trail: every signed exchange
  action leaves an attempt row before the wire and an outcome row after;
  safety-state changes (halt, resume, dead-man's-switch eligibility) land as
  event rows in the same table.
- `heartbeat` — the process-liveness seam in Postgres (ADR-0002): the
  executor beats; the watchdog watches; the watchdog beats too so the #52
  monitor can watch the watcher.
- `halt` — the kill-switch state: /kill and the watchdog both request halts;
  at most one is active; resuming is an explicit operator confirmation.
- `deadman` — the scheduleCancel UPGRADE PATH. The protocol primitive is
  gated behind $1M cumulative traded volume (verified live 2026-07-28:
  "Cannot set scheduled cancel time until enough volume traded. Required:
  $1000000. Traded: $0."), so it is implemented, eligibility-probed, and
  inactive-but-ready — never depended on.
- `watchdog` — the PRIMARY dead-man's switch: an independent process with
  its own agent key (keystore lane, issue #135) that cancel-alls and halts
  when the executor's heartbeat goes stale, precisely because no protocol
  primitive will do it for an under-$1M account.

Neither mechanism closes POSITIONS — both only kill resting orders. What
happens to open positions on a halt is the documented unwind policy
(docs/runbooks/halt-and-unwind.md), applied by the watchdog at sweep time.
"""
