package strategy

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Naive is the code most people write first, and it is wrong:
//
//	sold := SELECT sold FROM events      -- 1. read
//	if sold < capacity {                 -- 2. check
//	    INSERT INTO tickets ...          -- 3. act
//	    UPDATE events SET sold = sold+1  -- 4. write back the value read in step 1
//	}
//
// Two race conditions live here:
//
//   - check-then-act: N requests can all read sold=999 before any of them
//     writes, so all N pass the check and N tickets are issued for 1 seat.
//   - lost update: step 4 writes `read value + 1`, so concurrent writers
//     overwrite each other and the counter ends up lower than the real
//     number of tickets.
//
// Running each step in a transaction would not help. Under Postgres' default
// READ COMMITTED isolation the SELECT does not lock anything.
type Naive struct {
	db *pgxpool.Pool
	// beforeWrite runs between the read and the write. Tests use it to
	// force an interleaving and reproduce the race deterministically.
	beforeWrite func()
}

// NewNaive returns the unsafe baseline strategy.
func NewNaive(db *pgxpool.Pool) *Naive { return &Naive{db: db} }

func (*Naive) Name() string { return "naive" }

func (n *Naive) Purchase(ctx context.Context, req Request) (Result, error) {
	return naivePurchase(ctx, n.db, req, n.beforeWrite)
}

// naivePurchase is shared with Queue, which runs it on a single goroutine.
// Serialised, the same unsafe code becomes correct.
func naivePurchase(ctx context.Context, q querier, req Request, beforeWrite func()) (Result, error) {
	var sold, capacity int
	if err := readEvent(ctx, q, `SELECT sold, capacity FROM events WHERE id = $1`, req.EventID, &sold, &capacity); err != nil {
		return Result{}, err
	}
	if sold >= capacity {
		return soldOutOrReplay(ctx, q, req)
	}

	if beforeWrite != nil {
		beforeWrite()
	}

	id, created, err := insertTicket(ctx, q, req)
	if err != nil {
		return Result{}, err
	}
	if !created {
		return Result{Outcome: Duplicate, TicketID: id}, nil
	}
	if _, err := q.Exec(ctx, `UPDATE events SET sold = $2 WHERE id = $1`, req.EventID, sold+1); err != nil {
		return Result{}, fmt.Errorf("update counter: %w", err)
	}
	return Result{Outcome: Purchased, TicketID: id}, nil
}
