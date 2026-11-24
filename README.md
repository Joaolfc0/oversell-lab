# oversell-lab

[![ci](https://github.com/Joaolfc0/oversell-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/Joaolfc0/oversell-lab/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go)
![Postgres](https://img.shields.io/badge/postgres-17-336791?logo=postgresql)
![Redis](https://img.shields.io/badge/redis-7-DC382D?logo=redis)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**1,000 concert tickets. 10,000 people click "buy" at the same moment. How many tickets do you sell?**

With the code most people write first: **10,000**.

This repository implements the same "buy a ticket" operation **six ways**, fires the same flash sale at each one, and then **checks the database** to see which ones kept their promises.

![Live dashboard: the naive strategy issues 3,000 tickets for 300 seats, then atomic and redis sell exactly 300](docs/img/demo.gif)

<sub>The [live dashboard](#live-dashboard) reading Postgres while 3,000 buyers compete for 300 seats. Red squares are tickets for seats that do not exist.</sub>

```
| Strategy      | Correct | Tickets / Capacity | Oversold | Throughput  |
|---------------|:-------:|-------------------:|---------:|------------:|
| naive         |   ❌    |     10,000 / 1,000 |    9,000 |   481 req/s |
| pessimistic   |   ✅    |      1,000 / 1,000 |        0 |   592 req/s |
| optimistic    |   ✅    |      1,000 / 1,000 |        0 |   616 req/s |
| atomic        |   ✅    |      1,000 / 1,000 |        0 | 1,395 req/s |
| redis         |   ✅    |      1,000 / 1,000 |        0 | 2,649 req/s |
| queue         |   ✅    |      1,000 / 1,000 |        0 |   498 req/s |
```

<sub>Full results, methodology and caveats are [below](#results).</sub>

---

## Contents

- [The bug](#the-bug)
- [Quick start](#quick-start)
- [Live dashboard](#live-dashboard)
- [The six strategies](#the-six-strategies)
- [Idempotency: retries and double clicks](#idempotency-retries-and-double-clicks)
- [Results](#results)
- [What the benchmark taught me](#what-the-benchmark-taught-me)
- [How correctness is tested](#how-correctness-is-tested)
- [HTTP API](#http-api)
- [Project layout](#project-layout)
- [Design decisions (ADRs)](#design-decisions-adrs)
- [What I would do in production](#what-i-would-do-in-production)

---

## The bug

The intuitive implementation is **read → check → write**:

```go
sold := SELECT sold FROM events WHERE id = $1
if sold < capacity {
    INSERT INTO tickets (...)
    UPDATE events SET sold = $sold + 1
}
```

Each line is fine on its own. The problem is the gap between them:

```mermaid
sequenceDiagram
    autonumber
    participant A as Alice
    participant DB as Postgres (sold = 999, capacity = 1000)
    participant B as Bob
    A->>DB: SELECT sold
    DB-->>A: 999 ✔ seat available
    B->>DB: SELECT sold
    DB-->>B: 999 ✔ seat available
    A->>DB: INSERT ticket, UPDATE sold = 1000
    B->>DB: INSERT ticket, UPDATE sold = 1000
    Note over DB: 1,001 tickets issued for 1,000 seats<br/>and the counter says 1,000
```

That is **two bugs in one**:

1. **Check-then-act race.** Both requests pass the check before either one writes, so the event **oversells**.
2. **Lost update.** Both write `999 + 1`, so the counter **drifts** away from the real number of tickets.

Wrapping it in a transaction does **not** fix it. Under Postgres' default `READ COMMITTED` isolation a plain `SELECT` takes no lock.

At 10,000 concurrent buyers, almost every request reads `sold = 0` before anyone writes. The reference run issued **10,000 tickets for 1,000 seats**, and the counter ended at **1**.

---

## Quick start

Requirements: Go 1.26+, Docker.

```bash
git clone https://github.com/Joaolfc0/oversell-lab && cd oversell-lab
docker compose up -d --wait        # Postgres 17 + Redis 7
go run ./cmd/bench                  # every strategy, 1,000 seats, 10,000 buyers
```

Other things to try:

```bash
go run ./cmd/bench -strategy naive,atomic -capacity 100 -buyers 5000
go run ./cmd/bench -dup-rate 0.5          # half of the buyers double-click
go run ./cmd/bench -with-fk               # reproduce the foreign key bottleneck (see below)
go run ./cmd/bench -format json           # machine-readable output

# tests (integration tests need the containers running)
TEST_DATABASE_URL='postgres://oversell:oversell@localhost:54329/oversell?sslmode=disable' \
TEST_REDIS_URL='redis://localhost:63799/0' go test -race ./...
```

There is also a `Makefile` (`make up`, `make test`, `make bench`, `make server`).

---

## Live dashboard

```bash
docker compose up -d --wait
go run ./cmd/server          # then open http://localhost:8080
```

Choose the strategies, the number of seats and buyers, and how many of them double-click, then start the sale. For each strategy you see:

- **A seat map that reads Postgres, not the application.** Every 80 ms a separate two-connection pool counts the ticket rows, so the map shows what the database really contains. Tickets beyond capacity spill past the "venue wall" in red.
- **Live counters**: tickets in the database, what the `sold` counter says (in `naive` it stays near 1 while thousands of tickets pile up), sold-out answers, double clicks caught and retries.
- **A verdict once the run finishes**, from the same invariant checks the CLI uses.

![Results table from the dashboard: every strategy except naive sells exactly 300 of 300 seats](docs/img/results.png)

How it works:

- `GET /api/run` streams the sale as **Server-Sent Events** (`start`, `progress`, `report`, `done`). Closing the tab cancels the request context, which stops the run.
- The page is one HTML file **embedded in the Go binary** (`go:embed`): no Node, no build step, no front-end dependencies. The seat map is drawn on a `<canvas>`, so 10,000 squares cost nothing.
- Only one sale runs at a time (a `TryLock`, otherwise `409`), and inputs are capped, so a single visitor cannot occupy the database for minutes.
- Monitoring uses its **own small pool** on purpose. During a sale the main pool has thousands of goroutines queued on it, and a stats query sent through it would only run after the sale was over.

---

## The six strategies

Every strategy implements the same interface and has to keep the same three promises:

```go
type Strategy interface {
    Purchase(ctx context.Context, req Request) (Result, error) // Purchased | Duplicate | SoldOut
}
```

> 1. Never issue more tickets than the capacity.
> 2. `events.sold` always equals the number of ticket rows.
> 3. The same idempotency key never produces two tickets.

| Strategy | Mechanism | Where contention goes | Trade-off |
|---|---|---|---|
| [`naive`](internal/strategy/naive.go) | read → check → write | nowhere: it is ignored | ❌ Broken. Included as the baseline. |
| [`pessimistic`](internal/strategy/pessimistic.go) | `SELECT … FOR NO KEY UPDATE` | waiting on the row lock | Simple and correct. Every waiter holds a connection. |
| [`optimistic`](internal/strategy/optimistic.go) | `UPDATE … WHERE version = $v`, retry on conflict | retries | Good when conflicts are rare. A flash sale is the opposite. |
| [`atomic`](internal/strategy/atomic.go) | `UPDATE … WHERE sold < capacity` | waiting on the row lock, briefly | One statement, no retries. Usually the right answer in Postgres. |
| [`redis`](internal/strategy/redis.go) | Lua script in Redis guards entry | Redis, which is single-threaded | Fastest. Now there are two stores to keep consistent. |
| [`queue`](internal/strategy/queue.go) | channel + one worker goroutine | waiting in the queue | Naive code made correct by serialising it. Only within one process. |

### `pessimistic`: lock the row, then decide

```sql
BEGIN;
INSERT INTO tickets ... ON CONFLICT DO NOTHING;               -- idempotency (see below)
SELECT sold, capacity FROM events WHERE id = $1 FOR NO KEY UPDATE;
-- if sold >= capacity: ROLLBACK (the ticket row goes with it)
UPDATE events SET sold = sold + 1 WHERE id = $1;
COMMIT;
```

The lock is held from the `SELECT` until `COMMIT`. On a hot row, throughput is therefore capped at about `1 / (time the lock is held)`. During the benchmark, **59 of 64 connections** were waiting in `Lock:tuple`.

Why `FOR NO KEY UPDATE` and not `FOR UPDATE`: it is the weakest lock that still makes writers take turns, and it does not conflict with the `FOR KEY SHARE` lock a foreign key check takes. With `FOR UPDATE` plus a foreign key, two transactions that had each inserted a ticket would deadlock.

### `optimistic`: assume no conflict, verify on write

```sql
BEGIN;
SELECT sold, capacity, version FROM events WHERE id = $1;      -- no lock
INSERT INTO tickets ...;
UPDATE events SET sold = sold + 1, version = version + 1
 WHERE id = $1 AND version = $read_version;                    -- 0 rows → someone beat us
COMMIT;  -- or ROLLBACK, back off (exponential + full jitter) and retry
```

It is correct, but in this workload **1,000 sales took 9,556 retries**. Optimistic locking is designed for low contention, such as two people editing the same record now and then. A flash sale is the worst case for it.

### `atomic`: put the check and the write in one statement

```sql
BEGIN;
INSERT INTO tickets ... ON CONFLICT DO NOTHING;
UPDATE events SET sold = sold + 1 WHERE id = $1 AND sold < capacity;  -- 0 rows → sold out
COMMIT;
```

This is correct because of how Postgres runs `UPDATE` under `READ COMMITTED`. If another transaction holds the row, the `UPDATE` waits. When that transaction commits, Postgres **re-reads the latest version of the row and evaluates the `WHERE` clause again** (EvalPlanQual) before writing. The check can never use a stale value. There is no `SELECT` round trip while holding the lock, and no retry loop. **2.3× the throughput of `pessimistic`.**

### `redis`: keep the stampede away from the database

```lua
-- one atomic Lua script: idempotency check + stock check + decrement
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then return -2 end   -- duplicate
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then return -1 end                                     -- sold out
redis.call('DECR', KEYS[1]); redis.call('HSET', KEYS[2], ARGV[1], 1)
return stock - 1
```

Only requests that got a unit from Redis go on to Postgres. In a flash sale that is 1,000 out of 11,000. Everyone else is answered from memory, which is why the **median latency drops from about 6 s to 0.5 s**.

The price is **two sources of truth**:
- If the Postgres write fails, the unit is returned to Redis (compensation, using `context.WithoutCancel` so that a client giving up does not leak stock).
- If the process **crashes** between the Redis step and the Postgres step, the unit is lost. The event **undersells**, but it never oversells. The production fix is a reservation with a TTL plus a reconciliation job ([ADR 0001](docs/adr/0001-postgres-is-the-source-of-truth.md)).
- Keys use a hash tag (`oversell:{42}:stock`) so the script also works on Redis Cluster.

### `queue`: one writer, no race

The worker runs **exactly the same unsafe code as `naive`**, and it is correct. A race condition needs both shared state and concurrent access, so removing either one fixes it. This is the single-writer idea behind actors, the LMAX Disruptor and a Kafka partition with one consumer.

The catch: it is only correct **within one process**. Run two replicas and the race is back. To scale it, partition by event id so each event has exactly one consumer.

---

## Idempotency: retries and double clicks

Networks fail after the server has done the work. Clients retry, and users double-click. Every request carries an `Idempotency-Key`, and the benchmark sends **10% of purchases twice at the same moment** to make sure that is handled.

The design ([ADR 0003](docs/adr/0003-insert-ticket-first-for-idempotency.md)):

- `UNIQUE (event_id, idempotency_key)` on `tickets`.
- The **first** statement of the transaction is `INSERT … ON CONFLICT DO NOTHING RETURNING id`.
  - If it inserts: continue. If the event is sold out, `ROLLBACK` removes the row.
  - If it conflicts: this is a replay, so return the **original** ticket (`HTTP 200`, not `201`).
- When two requests with the same key run at once, Postgres makes the second `INSERT` **wait** for the first transaction. It then either sees a conflict (first committed) or goes ahead (first rolled back). No application-level lock is needed.

"Check if the key exists, then insert" would just be a second check-then-act race.

---

## Results

Reference run: `go run ./cmd/bench`. 1,000 seats, 10,000 buyers (11,043 requests including duplicates), all released at the same instant behind a barrier, 64-connection pool. AMD Ryzen 5 5500U laptop, Postgres 17 and Redis 7 in Docker, default Postgres durability settings.

| Strategy | Correct | Tickets / Capacity | Oversold | Counter drift | Duplicates absorbed | Retries | Errors | Throughput | p50 | p99 |
|---|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| `naive` | ❌ | 10000 / 1000 | 9000 | -9999 | 1043 | 0 | 0 | 481 req/s | 12873.5 ms | 22644.4 ms |
| `pessimistic` | ✅ | 1000 / 1000 | 0 | +0 | 174 | 0 | 0 | 592 req/s | 11382.1 ms | 18475.6 ms |
| `optimistic` | ✅ | 1000 / 1000 | 0 | +0 | 185 | 9556 | 0 | 616 req/s | 16264.8 ms | 17871.8 ms |
| `atomic` | ✅ | 1000 / 1000 | 0 | +0 | 190 | 0 | 0 | 1,395 req/s | 5775.5 ms | 7851.1 ms |
| `redis` | ✅ | 1000 / 1000 | 0 | +0 | 184 | 0 | 0 | 2,649 req/s | 501.7 ms | 3974.6 ms |
| `queue` | ✅ | 1000 / 1000 | 0 | +0 | 182 | 0 | 0 | 498 req/s | 12885.6 ms | 21937.9 ms |

**How to read it**

- **Correct** is not what the code *reports*. After every run the benchmark counts the rows in Postgres and checks: no oversell, no counter drift, every "purchased" answer has a real ticket, and no seats left unsold while there was demand.
- **Latency is mostly time spent waiting in line.** All 11,043 requests arrive in the same millisecond and share 64 connections, so p50 measures how long a burst takes to drain. That is what a real flash sale looks like. For per-operation cost, compare **throughput**.
- **Duplicates absorbed** counts requests answered as a replay of an existing ticket. For `naive` that is every duplicate, because almost nobody was told "sold out".
- Absolute numbers depend on the machine (Docker networking adds a lot to each round trip). **The ranking is what matters.** Run it yourself and compare.

---

## What the benchmark taught me

Three things I only found out by measuring.

### 1. A foreign key halved throughput

The first version had `tickets.event_id REFERENCES events(id)`. The numbers were far too low, so I sampled `pg_stat_activity` mid-run:

```
 wait_event_type | wait_event            | count
-----------------+-----------------------+------
 LWLock          | MultiXactMemberSLRU   |   54     ← 54 of 64 connections
 LWLock          | MultiXactMemberBuffer |    5
```

Every insert into a table with a foreign key takes a `FOR KEY SHARE` lock on the parent row. When many transactions share a lock on the **same** row, Postgres tracks them in a *MultiXact*, and each new holder creates a new one that copies the existing members. On one hot row, that bookkeeping **was** the bottleneck.

| Strategy | With FK | Without FK |
|---|--:|--:|
| `atomic` | 674 req/s | **1,395 req/s (2.1×)** |
| `optimistic` | 264 req/s | 616 req/s (2.3×) |
| `pessimistic` | 424 req/s | 592 req/s (1.4×) |

Reproduce it with `go run ./cmd/bench -with-fk`. The decision and its cost are recorded in [ADR 0004](docs/adr/0004-no-foreign-key-on-hot-row.md).

### 2. My first optimistic lock could not win

The first `optimistic` sold **54 of 1,000 tickets in 2 minutes** before timing out. It read the version on one pooled connection, released it, then took *another* connection for the conditional `UPDATE`. In between, the goroutine waited in a queue behind about 10,000 others. By the time it wrote, the version had always changed, so nearly every attempt was wasted.

Moving the read into the same transaction shrank the window from "time in the pool queue" to "one round trip". Throughput went from **92 to 616 req/s**. Lesson: an optimistic check is only as good as the gap between the read and the write.

### 3. A single hot row has a hard ceiling

Every Postgres strategy is ultimately limited by the same thing: **one row, one writer at a time**, so throughput ≈ `1 / lock hold time`. The strategies only differ in how much work they do while holding the lock: `atomic` does the least, and `pessimistic` does an extra round trip. To go beyond that ceiling, take the hot row off the critical path (`redis`) or split it into several rows (see [production](#what-i-would-do-in-production)).

---

## How correctness is tested

**The race is reproduced on every run, not by luck.** Concurrency bugs that show up "sometimes" make poor tests. [`TestNaive_DeterministicOversell`](internal/strategy/strategy_test.go) uses a hook between the read and the write, plus a barrier, so that both buyers finish reading before either one writes:

```go
var barrier sync.WaitGroup
barrier.Add(2)
naive.beforeWrite = func() {
    barrier.Done()
    barrier.Wait() // nobody writes until everybody has read
}
// ... two buyers, capacity 1 ...
// → both told "purchased", 2 tickets in the table, counter = 1
```

The other tests:

- **`TestSafeStrategies_Invariants`**: 600 buyers for 50 seats, with 20% double-clicking, against every correct strategy. Asserts exactly 50 tickets, no counter drift, every "purchased" backed by a ticket, and no buyer issued two tickets.
- **`TestIdempotentReplay`**: replaying a successful request returns the **same** ticket id, even after the event has sold out.
- **CI** ([workflow](.github/workflows/ci.yml)) runs everything against real Postgres and Redis service containers with **`-race`**, then a small benchmark whose table is posted to the job summary.

---

## HTTP API

The same strategies, exposed over HTTP. You choose the strategy for each request.

```bash
go run ./cmd/server   # :8080 (DATABASE_URL, REDIS_URL, ADDR, DEFAULT_STRATEGY)
```

```bash
# create (or reset) an event with one seat
curl -X POST localhost:8080/events -d '{"id":42,"name":"Concert","capacity":1}'

# buy: 201 Created
curl -X POST 'localhost:8080/events/42/purchase?strategy=atomic' \
     -H 'Idempotency-Key: order-abc' -d '{"buyer_id":"alice"}'
# {"status":"purchased","strategy":"atomic","ticket_id":476944}

# same request again (a retry): 200 OK, same ticket
# {"status":"duplicate","strategy":"atomic","ticket_id":476944}

# someone else: 409 Conflict
# {"error":"sold out"}

# no Idempotency-Key: 400 Bad Request

# check the invariants
curl localhost:8080/events/42
# {"capacity":1,"counter_drift":0,"oversold":0,"sold":1,"tickets":1}
```

---

## Project layout

```
cmd/
  bench/            load generator: runs strategies, verifies invariants, prints the table
  server/           HTTP API + live dashboard (SSE)
    web/            the dashboard: one HTML file, embedded with go:embed
internal/
  strategy/         the six implementations, plus integration tests
  bench/            barrier-synchronised load runner, percentiles, report
  store/            pgx pool, schema, invariant queries
docs/adr/           architecture decision records
docs/img/           dashboard recording and screenshot
docker-compose.yml  Postgres 17 + Redis 7
```

Built with the standard library plus [`pgx`](https://github.com/jackc/pgx) and [`go-redis`](https://github.com/redis/go-redis). No ORM, so every query that matters is visible.

---

## Design decisions (ADRs)

| # | Decision |
|---|---|
| [0001](docs/adr/0001-postgres-is-the-source-of-truth.md) | Postgres is the source of truth. Redis is only a gate in front of it. |
| [0002](docs/adr/0002-no-check-constraint-on-sold.md) | No `CHECK (sold <= capacity)`, **on purpose**, so the bug stays visible. Use one in production. |
| [0003](docs/adr/0003-insert-ticket-first-for-idempotency.md) | Insert the ticket first: idempotency without a check-then-act race. |
| [0004](docs/adr/0004-no-foreign-key-on-hot-row.md) | No foreign key on the hot row (MultiXact contention, measured). |

---

## What I would do in production

- **`CHECK (sold <= capacity)`** as a last line of defence ([ADR 0002](docs/adr/0002-no-check-constraint-on-sold.md)).
- **Reservations with a TTL**: "hold this seat for 10 minutes while you pay". Expired holds are released by a sweeper.
- **Sharded inventory**: split one hot row into N bucket rows (`event_id, bucket, remaining`) and pick a bucket at random. Write throughput grows roughly N×, at the cost of a slightly trickier "sold out" check.
- **Redis ↔ Postgres reconciliation**: periodically compare stock against tickets, or write to Postgres asynchronously through an outbox.
- **Admission control**: a virtual waiting room or a token bucket in front of the endpoint. Accepting 10,000 requests to sell 1,000 seats is mostly work spent saying no.
- **Observability**: Prometheus histograms per strategy and lock-wait metrics from `pg_stat_activity`.

### Roadmap

- [x] Live dashboard: watch the race happen, strategy by strategy
- [ ] Reservation + TTL strategy
- [ ] Sharded-counter strategy
- [ ] Multi-replica run showing the `queue` strategy breaking across processes

---

<sub>MIT licensed. Issues and discussion are welcome.</sub>
