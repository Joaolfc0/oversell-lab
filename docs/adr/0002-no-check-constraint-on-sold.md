# 0002: No `CHECK (sold <= capacity)` in this lab

**Status:** Accepted (lab only; do the opposite in production)

## Context
A `CHECK (sold <= capacity)` constraint would make Postgres reject any write
that oversells, whatever the application code does. It is cheap, and it is
the right last line of defence for real inventory.

## Decision
Leave it out, deliberately. With the constraint in place, the naive strategy
would fail with errors instead of overselling, and the bug this project
exists to show would be hidden.

## Consequences
- The naive strategy oversells visibly (9,000 extra tickets in the reference
  run).
- Correctness comes from application logic alone, so the benchmark measures
  each strategy and not the database's safety net.
- **In production: add the constraint.** Defence in depth is cheap here.
