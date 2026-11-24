# 0004: No foreign key from `tickets` to the hot `events` row

**Status:** Accepted

## Context
The first version had `tickets.event_id REFERENCES events(id)`. The benchmark
was much slower than expected, so I sampled `pg_stat_activity` during a run:

```
wait_event_type | wait_event            | count
LWLock          | MultiXactMemberSLRU   | 54     <- 54 of 64 connections
LWLock          | MultiXactMemberBuffer | 5
```

Inserting into a table with a foreign key makes Postgres take a
`FOR KEY SHARE` lock on the referenced row, so that the parent cannot be
deleted mid-transaction. When many transactions hold that shared lock on the
**same** row, Postgres records the set of holders as a *MultiXact*. Every new
holder creates a new MultiXact that copies the existing members. On one hot
row with 64 concurrent writers, maintaining those structures became the
bottleneck.

## Decision
Drop the foreign key. Check that the event exists in the application, which
every strategy already does as part of reading or updating the event row.

## Consequences
Measured on the reference run (1,000 seats, 10,000 buyers), throughput
with → without the FK:

| Strategy | With FK | Without FK |
|---|--:|--:|
| `atomic` | 674 req/s | 1,395 req/s (**2.1×**) |
| `optimistic` | 264 req/s | 616 req/s (2.3×) |
| `pessimistic` | 424 req/s | 592 req/s (1.4×) |

- We give up referential integrity enforced by the database. Events are never
  deleted in this domain. If they were, use a soft delete or a periodic
  integrity check.
- Reproduce with `go run ./cmd/bench -with-fk`.
- `pessimistic` uses `FOR NO KEY UPDATE` rather than `FOR UPDATE`, so it stays
  deadlock-free even if someone puts the FK back. `FOR UPDATE` conflicts with
  the FK's `FOR KEY SHARE`.
