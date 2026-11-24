package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pessimistic locks the event row for the length of the transaction, so
// every purchase for the same event runs one at a time:
//
//	BEGIN
//	INSERT INTO tickets ... ON CONFLICT DO NOTHING   -- idempotency first
//	SELECT sold, capacity ... FOR NO KEY UPDATE      -- take the row lock
//	UPDATE events SET sold = sold + 1
//	COMMIT                                          -- release it
//
// It is correct and simple. The cost is that throughput for a hot event is
// capped by how fast one transaction can finish, and every waiter holds a
// pool connection while it waits.
//
// Why FOR NO KEY UPDATE and not FOR UPDATE: it is the lock a plain UPDATE
// of a non-key column takes, so it is all we need. It also stays safe if
// someone adds a foreign key from tickets to events. A foreign key check
// takes FOR KEY SHARE on the event row, and FOR UPDATE conflicts with that.
// Two transactions that had each inserted a ticket would then wait on each
// other forever: a deadlock.
type Pessimistic struct{ db *pgxpool.Pool }

// NewPessimistic returns the row-lock strategy.
func NewPessimistic(db *pgxpool.Pool) *Pessimistic { return &Pessimistic{db: db} }

func (*Pessimistic) Name() string { return "pessimistic" }

func (p *Pessimistic) Purchase(ctx context.Context, req Request) (Result, error) {
	var res Result
	err := pgx.BeginFunc(ctx, p.db, func(tx pgx.Tx) error {
		id, created, err := insertTicket(ctx, tx, req)
		if err != nil {
			return err
		}
		if !created {
			res = Result{Outcome: Duplicate, TicketID: id}
			return errRollback
		}

		var sold, capacity int
		if err := readEvent(ctx, tx, `SELECT sold, capacity FROM events WHERE id = $1 FOR NO KEY UPDATE`,
			req.EventID, &sold, &capacity); err != nil {
			return err
		}
		if sold >= capacity {
			res = Result{Outcome: SoldOut}
			return errRollback // also removes the ticket inserted above
		}
		if _, err := tx.Exec(ctx, `UPDATE events SET sold = sold + 1 WHERE id = $1`, req.EventID); err != nil {
			return fmt.Errorf("update counter: %w", err)
		}
		res = Result{Outcome: Purchased, TicketID: id}
		return nil
	})
	if errors.Is(err, errRollback) {
		err = nil
	}
	return res, err
}
