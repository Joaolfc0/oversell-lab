// Package strategy contains several implementations of the same operation,
// "buy one ticket for an event", each with a different approach to
// concurrency control. They share one contract:
//
//  1. Never issue more tickets than the event's capacity.
//  2. Keep events.sold equal to the number of ticket rows.
//  3. The same idempotency key never produces two tickets. A replayed
//     request gets the original ticket back.
//
// Naive breaks rules 1 and 2 on purpose. The others are expected to hold all
// three under any amount of concurrency.
package strategy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Joaolfc0/oversell-lab/internal/store"
)

// Outcome is the business result of a purchase attempt.
type Outcome int

const (
	// Purchased means a new ticket was issued.
	Purchased Outcome = iota
	// Duplicate means the idempotency key was already used; TicketID is the
	// original ticket (or 0 if it is still being written).
	Duplicate
	// SoldOut means there was no capacity left.
	SoldOut
)

func (o Outcome) String() string {
	switch o {
	case Purchased:
		return "purchased"
	case Duplicate:
		return "duplicate"
	case SoldOut:
		return "sold_out"
	default:
		return fmt.Sprintf("outcome(%d)", int(o))
	}
}

// Request is one purchase attempt.
type Request struct {
	EventID        int64
	BuyerID        string
	IdempotencyKey string
}

// Result is returned by every strategy.
type Result struct {
	Outcome  Outcome
	TicketID int64
	// Retries counts optimistic-lock conflicts (0 for other strategies).
	Retries int
}

// Strategy buys a ticket.
type Strategy interface {
	Name() string
	Purchase(ctx context.Context, req Request) (Result, error)
}

// Preparer is implemented by strategies that keep state outside Postgres and
// must be told when an event is (re)created.
type Preparer interface {
	Prepare(ctx context.Context, eventID int64, capacity int) error
}

// Deps are the backing services a strategy may need.
type Deps struct {
	DB    *pgxpool.Pool
	Redis *redis.Client
}

var constructors = map[string]func(Deps) (Strategy, error){
	"naive":       func(d Deps) (Strategy, error) { return NewNaive(d.DB), nil },
	"pessimistic": func(d Deps) (Strategy, error) { return NewPessimistic(d.DB), nil },
	"optimistic":  func(d Deps) (Strategy, error) { return NewOptimistic(d.DB), nil },
	"atomic":      func(d Deps) (Strategy, error) { return NewAtomic(d.DB), nil },
	"queue":       func(d Deps) (Strategy, error) { return NewQueue(d.DB, 1024), nil },
	"redis": func(d Deps) (Strategy, error) {
		if d.Redis == nil {
			return nil, errors.New("redis strategy needs a redis client")
		}
		return NewRedis(d.DB, d.Redis), nil
	},
}

// Names lists every strategy in a stable, presentation-friendly order.
func Names() []string {
	order := map[string]int{"naive": 0, "pessimistic": 1, "optimistic": 2, "atomic": 3, "redis": 4, "queue": 5}
	names := make([]string, 0, len(constructors))
	for n := range constructors {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return order[names[i]] < order[names[j]] })
	return names
}

// New builds a strategy by name. If the result implements io.Closer the
// caller must close it.
func New(name string, d Deps) (Strategy, error) {
	c, ok := constructors[name]
	if !ok {
		return nil, fmt.Errorf("unknown strategy %q (want one of %v)", name, Names())
	}
	return c(d)
}

// Close closes s if it holds resources.
func Close(s Strategy) error {
	if c, ok := s.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// querier is satisfied by *pgxpool.Pool, *pgx.Conn and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// errRollback aborts a pgx.BeginFunc transaction without reporting an error.
var errRollback = errors.New("rollback")

// insertTicket inserts a ticket unless one already exists for the
// idempotency key. When it exists, the existing id is returned with
// created=false.
//
// If another transaction is inserting the same key at the same moment,
// Postgres makes this statement wait for it, so the result is always correct:
// if that transaction commits we see a conflict, and if it rolls back our
// insert goes through.
func insertTicket(ctx context.Context, q querier, req Request) (id int64, created bool, err error) {
	err = q.QueryRow(ctx, `
		INSERT INTO tickets (event_id, buyer_id, idempotency_key)
		VALUES ($1, $2, $3)
		ON CONFLICT (event_id, idempotency_key) DO NOTHING
		RETURNING id`, req.EventID, req.BuyerID, req.IdempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		id, err = lookupTicket(ctx, q, req)
		return id, false, err
	}
	if err != nil {
		return 0, false, fmt.Errorf("insert ticket: %w", err)
	}
	return id, true, nil
}

// lookupTicket finds the ticket for an idempotency key; 0 if none.
func lookupTicket(ctx context.Context, q querier, req Request) (int64, error) {
	var id int64
	err := q.QueryRow(ctx, `SELECT id FROM tickets WHERE event_id = $1 AND idempotency_key = $2`,
		req.EventID, req.IdempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("lookup ticket: %w", err)
	}
	return id, nil
}

// soldOutOrReplay is used on the sold-out path by strategies that check
// capacity before writing. A retry of a request that already succeeded
// should get its ticket back, not a "sold out".
func soldOutOrReplay(ctx context.Context, q querier, req Request) (Result, error) {
	id, err := lookupTicket(ctx, q, req)
	if err != nil {
		return Result{}, err
	}
	if id != 0 {
		return Result{Outcome: Duplicate, TicketID: id}, nil
	}
	return Result{Outcome: SoldOut}, nil
}

func readEvent(ctx context.Context, q querier, sql string, eventID int64, dest ...any) error {
	err := q.QueryRow(ctx, sql, eventID).Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrEventNotFound
	}
	if err != nil {
		return fmt.Errorf("read event: %w", err)
	}
	return nil
}
