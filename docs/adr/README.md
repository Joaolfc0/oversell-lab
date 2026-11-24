# Architecture Decision Records

Short notes on decisions that are not obvious from the code: the context, what
was decided, and what it costs. Format based on
[Michael Nygard's template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

| # | Decision | Status |
|---|---|---|
| [0001](0001-postgres-is-the-source-of-truth.md) | Postgres is the source of truth for tickets | Accepted |
| [0002](0002-no-check-constraint-on-sold.md) | No `CHECK (sold <= capacity)` in this lab | Accepted (lab only) |
| [0003](0003-insert-ticket-first-for-idempotency.md) | Insert the ticket first to get idempotency | Accepted |
| [0004](0004-no-foreign-key-on-hot-row.md) | No foreign key from `tickets` to the hot `events` row | Accepted |
