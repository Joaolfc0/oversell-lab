package strategy

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrTooManyRetries is returned when an optimistic purchase keeps losing the
// version race.
var ErrTooManyRetries = errors.New("optimistic lock: too many retries")

// Optimistic takes no lock. It reads a version number, then makes the write
// conditional on that version not having changed:
//
//	BEGIN
//	SELECT sold, capacity, version                  -- no lock taken
//	INSERT INTO tickets ...
//	UPDATE events SET sold = sold+1, version = version+1
//	 WHERE id = $1 AND version = $read_version     -- 0 rows? someone beat us
//	COMMIT, or ROLLBACK and retry with backoff
//
// This works well when conflicts are rare, for example editing a document or
// a profile. A flash sale is the worst case: every request writes the same
// row, so under N concurrent buyers only one wins each round and the rest
// retry. The Retries column in the benchmark shows how much work that wastes.
type Optimistic struct {
	db         *pgxpool.Pool
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// NewOptimistic returns the version-check strategy.
func NewOptimistic(db *pgxpool.Pool) *Optimistic {
	return &Optimistic{db: db, maxRetries: 500, baseDelay: 500 * time.Microsecond, maxDelay: 20 * time.Millisecond}
}

func (*Optimistic) Name() string { return "optimistic" }

func (o *Optimistic) Purchase(ctx context.Context, req Request) (Result, error) {
	for attempt := 0; attempt <= o.maxRetries; attempt++ {
		var res Result
		// The read and the conditional write share one connection and one
		// transaction. An earlier version read on one pooled connection and
		// wrote on another. Under load the goroutine waited in the pool queue
		// between the two, so the version was always stale and almost every
		// attempt lost. See "What the benchmark taught me" in the README.
		err := pgx.BeginFunc(ctx, o.db, func(tx pgx.Tx) error {
			var sold, capacity int
			var version int64
			if err := readEvent(ctx, tx, `SELECT sold, capacity, version FROM events WHERE id = $1`,
				req.EventID, &sold, &capacity, &version); err != nil {
				return err
			}
			if sold >= capacity {
				r, err := soldOutOrReplay(ctx, tx, req)
				if err != nil {
					return err
				}
				res = r
				res.Retries = attempt
				return errRollback
			}
			id, created, err := insertTicket(ctx, tx, req)
			if err != nil {
				return err
			}
			if !created {
				res = Result{Outcome: Duplicate, TicketID: id, Retries: attempt}
				return errRollback
			}
			tag, err := tx.Exec(ctx, `
				UPDATE events SET sold = sold + 1, version = version + 1
				WHERE id = $1 AND version = $2`, req.EventID, version)
			if err != nil {
				return fmt.Errorf("update counter: %w", err)
			}
			if tag.RowsAffected() == 0 {
				return errConflict // someone else won; ROLLBACK removes our ticket
			}
			res = Result{Outcome: Purchased, TicketID: id, Retries: attempt}
			return nil
		})
		switch {
		case err == nil, errors.Is(err, errRollback):
			return res, nil
		case errors.Is(err, errConflict):
			if err := sleep(ctx, o.backoff(attempt)); err != nil {
				return Result{Retries: attempt}, err
			}
		default:
			return Result{Retries: attempt}, err
		}
	}
	return Result{Retries: o.maxRetries}, ErrTooManyRetries
}

var errConflict = errors.New("version conflict")

// backoff is exponential with full jitter: rand(0, min(max, base*2^n)).
// The jitter spreads retries out so they do not all collide again.
func (o *Optimistic) backoff(attempt int) time.Duration {
	ceiling := o.baseDelay << min(attempt, 16)
	if ceiling <= 0 || ceiling > o.maxDelay {
		ceiling = o.maxDelay
	}
	return rand.N(ceiling) + 1
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
