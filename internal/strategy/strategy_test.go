package strategy

// Integration tests. They need real Postgres and Redis (`make up`) and are
// skipped when TEST_DATABASE_URL is not set.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Joaolfc0/oversell-lab/internal/store"
)

var nextEventID atomic.Int64

func init() { nextEventID.Store(time.Now().UnixNano() % 1_000_000_000) }

func setup(t *testing.T) Deps {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; run `make up` and `make test`")
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, dbURL, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := store.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	d := Deps{DB: db}
	if u := os.Getenv("TEST_REDIS_URL"); u != "" {
		opt, err := redis.ParseURL(u)
		if err != nil {
			t.Fatal(err)
		}
		d.Redis = redis.NewClient(opt)
		t.Cleanup(func() { d.Redis.Close() })
	}
	return d
}

func newEvent(t *testing.T, db *pgxpool.Pool, s Strategy, capacity int) int64 {
	t.Helper()
	id := nextEventID.Add(1)
	ctx := context.Background()
	if err := store.ResetEvent(ctx, db, id, t.Name(), capacity); err != nil {
		t.Fatal(err)
	}
	if p, ok := s.(Preparer); ok {
		if err := p.Prepare(ctx, id, capacity); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// TestNaive_DeterministicOversell reproduces the race on every run instead of
// hoping the scheduler happens to produce it. Two buyers compete for one
// seat. A barrier holds both of them between the read and the write, so both
// have already seen sold=0 when either one writes.
func TestNaive_DeterministicOversell(t *testing.T) {
	d := setup(t)
	n := NewNaive(d.DB)
	event := newEvent(t, d.DB, n, 1)

	var barrier sync.WaitGroup
	barrier.Add(2)
	n.beforeWrite = func() {
		barrier.Done()
		barrier.Wait() // nobody writes until everybody has read
	}

	var wg sync.WaitGroup
	results := make([]Result, 2)
	for i := range 2 {
		wg.Go(func() {
			res, err := n.Purchase(context.Background(), Request{
				EventID: event, BuyerID: fmt.Sprint(i), IdempotencyKey: fmt.Sprint(i),
			})
			if err != nil {
				t.Error(err)
			}
			results[i] = res
		})
	}
	wg.Wait()

	stats, err := store.Stats(context.Background(), d.DB, event)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Outcome != Purchased || results[1].Outcome != Purchased {
		t.Fatalf("expected both buyers to be told they succeeded, got %v and %v", results[0].Outcome, results[1].Outcome)
	}
	if stats.Tickets != 2 {
		t.Fatalf("expected the race to issue 2 tickets for 1 seat, got %d", stats.Tickets)
	}
	if stats.Sold != 1 {
		t.Fatalf("expected a lost update (counter=1 while tickets=2), got counter=%d", stats.Sold)
	}
	t.Logf("reproduced: capacity=1 tickets=%d counter=%d", stats.Tickets, stats.Sold)
}

// TestSafeStrategies_Invariants hammers each safe strategy with far more
// buyers than seats, plus duplicate requests, and checks every invariant.
func TestSafeStrategies_Invariants(t *testing.T) {
	d := setup(t)
	const capacity, buyers = 50, 600

	for _, name := range Names() {
		if name == "naive" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			if name == "redis" && d.Redis == nil {
				t.Skip("TEST_REDIS_URL not set")
			}
			s, err := New(name, d)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { Close(s) })
			event := newEvent(t, d.DB, s, capacity)

			var purchased, duplicates, soldOut atomic.Int64
			ticketsByKey := sync.Map{}
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range buyers {
				copies := 1
				if i%5 == 0 {
					copies = 2 // 20% of buyers double-click
				}
				for range copies {
					wg.Go(func() {
						<-start
						res, err := s.Purchase(context.Background(), Request{
							EventID: event, BuyerID: fmt.Sprint(i), IdempotencyKey: fmt.Sprintf("k%d", i),
						})
						if err != nil {
							t.Errorf("buyer %d: %v", i, err)
							return
						}
						switch res.Outcome {
						case Purchased:
							purchased.Add(1)
							if prev, loaded := ticketsByKey.LoadOrStore(i, res.TicketID); loaded {
								t.Errorf("buyer %d was issued two tickets: %v and %d", i, prev, res.TicketID)
							}
						case Duplicate:
							duplicates.Add(1)
						case SoldOut:
							soldOut.Add(1)
						}
					})
				}
			}
			close(start)
			wg.Wait()

			stats, err := store.Stats(context.Background(), d.DB, event)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Tickets != capacity {
				t.Errorf("tickets = %d, want exactly %d", stats.Tickets, capacity)
			}
			if stats.Sold != stats.Tickets {
				t.Errorf("counter drift: sold=%d tickets=%d", stats.Sold, stats.Tickets)
			}
			if int(purchased.Load()) != stats.Tickets {
				t.Errorf("%d callers were told they bought a ticket, but %d tickets exist", purchased.Load(), stats.Tickets)
			}
			t.Logf("purchased=%d duplicates=%d sold_out=%d", purchased.Load(), duplicates.Load(), soldOut.Load())
		})
	}
}

// TestIdempotentReplay checks that retrying a request that already succeeded
// returns the original ticket, even after the event has sold out.
func TestIdempotentReplay(t *testing.T) {
	d := setup(t)
	for _, name := range Names() {
		t.Run(name, func(t *testing.T) {
			if name == "redis" && d.Redis == nil {
				t.Skip("TEST_REDIS_URL not set")
			}
			s, err := New(name, d)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { Close(s) })
			event := newEvent(t, d.DB, s, 1)
			ctx := context.Background()
			req := Request{EventID: event, BuyerID: "alice", IdempotencyKey: "order-1"}

			first, err := s.Purchase(ctx, req)
			if err != nil || first.Outcome != Purchased {
				t.Fatalf("first purchase: %v %v", first.Outcome, err)
			}
			replay, err := s.Purchase(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if replay.Outcome != Duplicate || replay.TicketID != first.TicketID {
				t.Fatalf("replay = %v ticket %d, want duplicate of ticket %d", replay.Outcome, replay.TicketID, first.TicketID)
			}
			other, err := s.Purchase(ctx, Request{EventID: event, BuyerID: "bob", IdempotencyKey: "order-2"})
			if err != nil || other.Outcome != SoldOut {
				t.Fatalf("second buyer: %v %v, want sold_out", other.Outcome, err)
			}
		})
	}
}
