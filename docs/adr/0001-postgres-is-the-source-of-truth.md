# 0001: Postgres is the source of truth for tickets

**Status:** Accepted

## Context
Several strategies keep state elsewhere: Redis holds a stock counter, and the
queue strategy holds requests in process memory. If two stores disagree about
who owns a ticket, one of them has to win.

## Decision
A ticket exists only if it has a row in Postgres `tickets`. Every other store
is a gate or an optimisation in front of it. Redis may say "yes" and Postgres
may still say "no", for example when the idempotency key already exists. When
that happens, the Redis reservation is released.

## Consequences
- Every invariant can be checked with one SQL query (`store.Stats`), and the
  benchmark checks them after every run.
- The Redis strategy has to compensate when the Postgres write fails. A crash
  between the two steps can lose a unit of stock: the event undersells, it
  never oversells. Fixing that needs reservations with a TTL plus a
  reconciliation job, which is out of scope for now.
