package strategy

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrQueueClosed is returned after Close.
var ErrQueueClosed = errors.New("queue closed")

// Queue sends every purchase through a channel to a single worker goroutine.
// The worker runs the same unsafe code as Naive, and it is correct here
// because nothing runs concurrently with it.
//
// This is the "single writer" pattern behind actor systems, LMAX Disruptor
// and a Kafka partition with one consumer. It shows that a race condition
// needs both shared state and concurrent access, and removing either one
// fixes it.
//
// The limits are real:
//   - Throughput is bounded by one worker doing sequential round trips to
//     Postgres, and the latency includes time spent waiting in line.
//   - It is only correct within ONE process. Run two replicas of this
//     service and you have two workers racing again. To scale it, partition
//     by event id (each event belongs to exactly one consumer), which is
//     what Kafka gives you.
type Queue struct {
	db   *pgxpool.Pool
	jobs chan job

	closeOnce sync.Once
	closed    chan struct{}
	done      chan struct{}
}

type job struct {
	ctx   context.Context
	req   Request
	reply chan jobResult
}

type jobResult struct {
	res Result
	err error
}

// NewQueue starts the worker. buffer is the channel capacity.
func NewQueue(db *pgxpool.Pool, buffer int) *Queue {
	q := &Queue{
		db:     db,
		jobs:   make(chan job, buffer),
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go q.worker()
	return q
}

func (*Queue) Name() string { return "queue" }

func (q *Queue) worker() {
	defer close(q.done)
	for {
		select {
		case <-q.closed:
			return
		case j := <-q.jobs:
			if err := j.ctx.Err(); err != nil {
				j.reply <- jobResult{err: err} // caller gave up while waiting in line
				continue
			}
			res, err := naivePurchase(j.ctx, q.db, j.req, nil)
			j.reply <- jobResult{res, err}
		}
	}
}

func (q *Queue) Purchase(ctx context.Context, req Request) (Result, error) {
	j := job{ctx: ctx, req: req, reply: make(chan jobResult, 1)}
	select {
	case q.jobs <- j:
	case <-q.closed:
		return Result{}, ErrQueueClosed
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	select {
	case r := <-j.reply:
		return r.res, r.err
	case <-q.done:
		return Result{}, ErrQueueClosed
	case <-ctx.Done():
		// The worker may still run the job. That is safe: the reply channel
		// is buffered, and the idempotency key makes a client retry safe.
		return Result{}, ctx.Err()
	}
}

// Close stops the worker. Jobs still waiting in the buffer get ErrQueueClosed.
func (q *Queue) Close() error {
	q.closeOnce.Do(func() { close(q.closed) })
	<-q.done
	return nil
}
