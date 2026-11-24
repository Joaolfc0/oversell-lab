package strategy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Joaolfc0/oversell-lab/internal/store"
)

// Atomic puts the check and the write in one statement:
//
//	BEGIN
//	INSERT INTO tickets ... ON CONFLICT DO NOTHING
//	UPDATE events SET sold = sold + 1
//	 WHERE id = $1 AND sold < capacity          -- 0 rows = sold out
//	COMMIT
//
// This is correct because of how Postgres runs UPDATE under READ COMMITTED.
// If the row is locked by another writer, the UPDATE waits. When that writer
// commits, Postgres re-reads the latest version of the row and checks the
// WHERE clause again ("EvalPlanQual") before applying the change. So
// `sold < capacity` is always checked against the current value, never a
// stale one.
//
// It holds the row lock only from the UPDATE to COMMIT, without the extra
// SELECT round trip that Pessimistic needs, and it never retries. For a
// single hot counter in Postgres this is usually the right answer.
type Atomic struct{ db *pgxpool.Pool }

// NewAtomic returns the conditional-update strategy.
func NewAtomic(db *pgxpool.Pool) *Atomic { return &Atomic{db: db} }

func (*Atomic) Name() string { return "atomic" }

func (a *Atomic) Purchase(ctx context.Context, req Request) (Result, error) {
	var res Result
	err := pgx.BeginFunc(ctx, a.db, func(tx pgx.Tx) error {
		id, created, err := insertTicket(ctx, tx, req)
		if err != nil {
			return err
		}
		if !created {
			res = Result{Outcome: Duplicate, TicketID: id}
			return errRollback
		}
		tag, err := tx.Exec(ctx, `UPDATE events SET sold = sold + 1 WHERE id = $1 AND sold < capacity`, req.EventID)
		if err != nil {
			return fmt.Errorf("update counter: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Either sold out or the event does not exist. Only the slow
			// path pays for telling the two apart.
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, req.EventID).Scan(&exists); err != nil {
				return fmt.Errorf("check event: %w", err)
			}
			if !exists {
				return store.ErrEventNotFound
			}
			res = Result{Outcome: SoldOut}
			return errRollback
		}
		res = Result{Outcome: Purchased, TicketID: id}
		return nil
	})
	if errors.Is(err, errRollback) {
		err = nil
	}
	return res, err
}
