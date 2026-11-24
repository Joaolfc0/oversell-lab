package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Redis moves the gate (is there stock left?) out of Postgres and into a
// Lua script in Redis. Redis is single-threaded and runs a script without
// interruption, so the check and the decrement happen as one step without
// any locks.
//
//  1. Redis: EVAL reserve   -- idempotency check + stock check + DECR, atomic
//  2. Postgres: BEGIN; INSERT ticket; UPDATE sold = sold+1; COMMIT
//  3. if step 2 fails: EVAL release -- give the unit back (compensation)
//
// Postgres no longer handles contention on the hot row. Only requests that
// got a unit from Redis reach it. Requests that arrive after the sale is
// over are answered by Redis alone, which is the common case in a flash
// sale.
//
// The cost is that there are now two sources of truth, and keeping them in
// step is your job:
//   - If the process crashes between 1 and 2, the unit is gone from Redis
//     but no ticket exists, so the event undersells. Production systems
//     handle this with reservations that expire (TTL) and a reconciliation
//     job that compares Redis with Postgres.
//   - Redis persistence (AOF/RDB) and failover can lose recent writes.
//
// Keys use a {hash tag} so stock and idempotency keys for one event land on
// the same Redis Cluster slot, which multi-key Lua scripts require.
type Redis struct {
	db  *pgxpool.Pool
	rdb *redis.Client
}

// NewRedis returns the Redis-gated strategy.
func NewRedis(db *pgxpool.Pool, rdb *redis.Client) *Redis { return &Redis{db: db, rdb: rdb} }

func (*Redis) Name() string { return "redis" }

var reserveScript = redis.NewScript(`
-- KEYS[1] = stock counter, KEYS[2] = idempotency hash, ARGV[1] = idempotency key
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return -2 -- duplicate
end
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then
  return -1 -- sold out
end
redis.call('DECR', KEYS[1])
redis.call('HSET', KEYS[2], ARGV[1], 1)
return stock - 1
`)

var releaseScript = redis.NewScript(`
if redis.call('HDEL', KEYS[2], ARGV[1]) == 1 then
  redis.call('INCR', KEYS[1])
end
return 0
`)

func stockKey(eventID int64) string { return fmt.Sprintf("oversell:{%d}:stock", eventID) }
func idemKey(eventID int64) string  { return fmt.Sprintf("oversell:{%d}:idem", eventID) }

// Prepare loads the event's capacity into Redis.
func (r *Redis) Prepare(ctx context.Context, eventID int64, capacity int) error {
	_, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, idemKey(eventID))
		p.Set(ctx, stockKey(eventID), capacity, 0)
		return nil
	})
	return err
}

func (r *Redis) Purchase(ctx context.Context, req Request) (Result, error) {
	keys := []string{stockKey(req.EventID), idemKey(req.EventID)}
	left, err := reserveScript.Run(ctx, r.rdb, keys, req.IdempotencyKey).Int64()
	if err != nil {
		return Result{}, fmt.Errorf("redis reserve: %w", err)
	}
	switch left {
	case -2:
		// The original request may still be writing to Postgres, in which
		// case TicketID is 0. Clients should poll or retry later.
		id, err := lookupTicket(ctx, r.db, req)
		return Result{Outcome: Duplicate, TicketID: id}, err
	case -1:
		return Result{Outcome: SoldOut}, nil
	}

	var res Result
	err = pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		id, created, err := insertTicket(ctx, tx, req)
		if err != nil {
			return err
		}
		if !created {
			// Postgres already has this key but Redis did not, which means
			// Redis lost state. Postgres wins, and the unit goes back.
			res = Result{Outcome: Duplicate, TicketID: id}
			return errRollback
		}
		if _, err := tx.Exec(ctx, `UPDATE events SET sold = sold + 1 WHERE id = $1`, req.EventID); err != nil {
			return fmt.Errorf("update counter: %w", err)
		}
		res = Result{Outcome: Purchased, TicketID: id}
		return nil
	})
	if err == nil {
		return res, nil
	}

	// Compensate. Use a context that survives the caller's cancellation, so
	// that a client timing out does not leak a unit of stock.
	cctx := context.WithoutCancel(ctx)
	if rerr := releaseScript.Run(cctx, r.rdb, keys, req.IdempotencyKey).Err(); rerr != nil {
		err = errors.Join(err, fmt.Errorf("redis release: %w", rerr))
	}
	if errors.Is(err, errRollback) {
		return res, nil
	}
	return Result{}, err
}
