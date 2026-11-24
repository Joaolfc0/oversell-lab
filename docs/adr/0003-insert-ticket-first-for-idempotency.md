# 0003: Insert the ticket first to get idempotency

**Status:** Accepted

## Context
Clients retry. Users double-click. Load balancers replay requests after a
timeout. A purchase request has to be safe to repeat: the same
`Idempotency-Key` must never create two tickets, and a replay should get the
original ticket back.

The obvious approach, "SELECT by key, then INSERT if missing", is a
check-then-act race of its own.

## Decision
- A `UNIQUE (event_id, idempotency_key)` index on `tickets`.
- Inside the purchase transaction, the first statement is
  `INSERT ... ON CONFLICT DO NOTHING RETURNING id`, before capacity is touched.
  - If it inserts, continue. If the event turns out to be sold out, the
    transaction rolls back and the row goes with it.
  - If it conflicts, this is a replay: look up and return the existing
    ticket.
- When two requests with the same key arrive at the same moment, Postgres
  makes the second `INSERT` wait on the first transaction's index entry. If
  the first commits, the second sees a conflict. If it rolls back, the second
  goes ahead. No application lock is needed.

## Consequences
- A replay is answered without touching the hot `events` row.
- Requests on the sold-out path do an `INSERT` that is then rolled back. That
  is wasted work, and the price of handling replays correctly. Strategies
  that check capacity first (naive, optimistic) instead look up the key on
  the sold-out path, so that a replay after sell-out still returns the
  ticket.
- Lock ordering is always "ticket key, then event row", so these two locks
  can never form a deadlock cycle.
