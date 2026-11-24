// Package bench fires a burst of concurrent purchases at a strategy, then
// checks the invariants against what is actually in Postgres.
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Joaolfc0/oversell-lab/internal/store"
	"github.com/Joaolfc0/oversell-lab/internal/strategy"
)

// Config describes one flash sale.
type Config struct {
	EventID  int64
	Capacity int
	// Buyers is the number of distinct people trying to buy (one idempotency
	// key each).
	Buyers int
	// DuplicateRate is the fraction of buyers whose request is sent twice at
	// the same moment, like a double click or a client retry after a timeout.
	DuplicateRate float64
	Seed          uint64
	Timeout       time.Duration

	// OnProgress, if set, is called about every ProgressEvery with a live
	// snapshot, and once more when the run finishes.
	OnProgress    func(Progress)
	ProgressEvery time.Duration
	// Monitor is the pool used to read live stats while the run is in
	// progress. Use a separate small pool: the main one is saturated by the
	// buyers, and a stats query would wait in line behind thousands of them.
	// Defaults to the main pool.
	Monitor *pgxpool.Pool
}

// Progress is a live snapshot taken during a run.
type Progress struct {
	Strategy   string `json:"strategy"`
	ElapsedMs  int64  `json:"elapsed_ms"`
	Requests   int    `json:"requests"`
	Done       int64  `json:"done"`
	Purchased  int64  `json:"purchased"`
	Duplicates int64  `json:"duplicates"`
	SoldOut    int64  `json:"sold_out"`
	Errors     int64  `json:"errors"`
	Retries    int64  `json:"retries"`
	// Tickets and Counter are read from Postgres, so they show what the
	// database really holds, not what callers were told.
	Tickets int `json:"tickets"`
	Counter int `json:"counter"`
}

type liveCounters struct {
	done, purchased, duplicates, soldOut, errors, retries atomic.Int64
}

func (c *liveCounters) record(res strategy.Result, err error) {
	c.retries.Add(int64(res.Retries))
	switch {
	case err != nil:
		c.errors.Add(1)
	case res.Outcome == strategy.Purchased:
		c.purchased.Add(1)
	case res.Outcome == strategy.Duplicate:
		c.duplicates.Add(1)
	case res.Outcome == strategy.SoldOut:
		c.soldOut.Add(1)
	}
	c.done.Add(1)
}

func (c *liveCounters) snapshot(name string, requests int, elapsed time.Duration, st store.EventStats) Progress {
	return Progress{
		Strategy: name, ElapsedMs: elapsed.Milliseconds(), Requests: requests,
		Done: c.done.Load(), Purchased: c.purchased.Load(), Duplicates: c.duplicates.Load(),
		SoldOut: c.soldOut.Load(), Errors: c.errors.Load(), Retries: c.retries.Load(),
		Tickets: st.Tickets, Counter: st.Sold,
	}
}

// Report is the outcome of one run.
type Report struct {
	Strategy string `json:"strategy"`
	Capacity int    `json:"capacity"`
	Buyers   int    `json:"buyers"`
	Requests int    `json:"requests"`

	// What the strategy told its callers.
	Purchased  int `json:"purchased"`
	Duplicates int `json:"duplicates"`
	SoldOut    int `json:"sold_out"`
	Errors     int `json:"errors"`
	Retries    int `json:"retries"`

	// What is actually in the database afterwards.
	TicketsIssued int `json:"tickets_issued"`
	Counter       int `json:"counter"`
	Oversold      int `json:"oversold"`
	CounterDrift  int `json:"counter_drift"`
	Unsold        int `json:"unsold"`

	Duration   time.Duration `json:"duration_ns"`
	Throughput float64       `json:"throughput_rps"`
	P50        time.Duration `json:"p50_ns"`
	P95        time.Duration `json:"p95_ns"`
	P99        time.Duration `json:"p99_ns"`
	Max        time.Duration `json:"max_ns"`

	FirstError string `json:"first_error,omitempty"`
}

// Violations lists every broken invariant. An empty list means the strategy
// behaved correctly on this run.
func (r Report) Violations() []string {
	var v []string
	if r.Oversold > 0 {
		v = append(v, "oversold by "+humanInt(r.Oversold))
	}
	if r.CounterDrift != 0 {
		v = append(v, "counter drift "+signed(r.CounterDrift))
	}
	if r.Purchased != r.TicketsIssued {
		v = append(v, humanInt(r.Purchased)+" successes but "+humanInt(r.TicketsIssued)+" tickets")
	}
	if r.Unsold > 0 {
		v = append(v, humanInt(r.Unsold)+" seats left unsold despite demand")
	}
	return v
}

// Correct reports whether all invariants held.
func (r Report) Correct() bool { return len(r.Violations()) == 0 }

type sample struct {
	res     strategy.Result
	err     error
	latency time.Duration
}

// Run resets the event, releases every buyer at once, waits for all of them,
// and compares what callers were told with what Postgres says.
func Run(ctx context.Context, db *pgxpool.Pool, s strategy.Strategy, cfg Config) (Report, error) {
	if cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
	}
	if err := store.ResetEvent(ctx, db, cfg.EventID, "flash sale: "+s.Name(), cfg.Capacity); err != nil {
		return Report{}, fmt.Errorf("reset event: %w", err)
	}
	if p, ok := s.(strategy.Preparer); ok {
		if err := p.Prepare(ctx, cfg.EventID, cfg.Capacity); err != nil {
			return Report{}, fmt.Errorf("prepare %s: %w", s.Name(), err)
		}
	}

	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	var reqs []strategy.Request
	for i := range cfg.Buyers {
		r := strategy.Request{
			EventID:        cfg.EventID,
			BuyerID:        fmt.Sprintf("buyer-%d", i),
			IdempotencyKey: fmt.Sprintf("order-%d", i),
		}
		reqs = append(reqs, r)
		if rng.Float64() < cfg.DuplicateRate {
			reqs = append(reqs, r)
		}
	}
	rng.Shuffle(len(reqs), func(i, j int) { reqs[i], reqs[j] = reqs[j], reqs[i] })

	samples := make([]sample, len(reqs))
	var live liveCounters
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, r := range reqs {
		wg.Go(func() {
			<-start // all goroutines wait here, then are released together
			t0 := time.Now()
			res, err := s.Purchase(ctx, r)
			samples[i] = sample{res: res, err: err, latency: time.Since(t0)}
			live.record(res, err)
		})
	}

	monitor := cfg.Monitor
	if monitor == nil {
		monitor = db
	}
	every := cfg.ProgressEvery
	if every <= 0 {
		every = 100 * time.Millisecond
	}
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	t0 := time.Now()
	if cfg.OnProgress != nil {
		go func() {
			defer close(progressDone)
			tick := time.NewTicker(every)
			defer tick.Stop()
			for {
				select {
				case <-stopProgress:
					return
				case <-tick.C:
					qctx, cancel := context.WithTimeout(ctx, every)
					st, _ := store.Stats(qctx, monitor, cfg.EventID) // best effort
					cancel()
					cfg.OnProgress(live.snapshot(s.Name(), len(reqs), time.Since(t0), st))
				}
			}
		}()
	} else {
		close(progressDone)
	}

	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	close(stopProgress)
	<-progressDone

	stats, err := store.Stats(context.WithoutCancel(ctx), db, cfg.EventID)
	if err != nil {
		return Report{}, fmt.Errorf("read stats: %w", err)
	}
	if cfg.OnProgress != nil {
		cfg.OnProgress(live.snapshot(s.Name(), len(reqs), elapsed, stats))
	}

	rep := Report{
		Strategy:      s.Name(),
		Capacity:      cfg.Capacity,
		Buyers:        cfg.Buyers,
		Requests:      len(reqs),
		TicketsIssued: stats.Tickets,
		Counter:       stats.Sold,
		Oversold:      stats.Oversold(),
		CounterDrift:  stats.CounterDrift(),
		Duration:      elapsed,
		Throughput:    float64(len(reqs)) / elapsed.Seconds(),
	}
	if cfg.Buyers >= cfg.Capacity {
		rep.Unsold = max(0, cfg.Capacity-stats.Tickets)
	}

	lat := make([]time.Duration, 0, len(samples))
	for _, smp := range samples {
		lat = append(lat, smp.latency)
		rep.Retries += smp.res.Retries
		if smp.err != nil {
			rep.Errors++
			if rep.FirstError == "" {
				rep.FirstError = smp.err.Error()
			}
			continue
		}
		switch smp.res.Outcome {
		case strategy.Purchased:
			rep.Purchased++
		case strategy.Duplicate:
			rep.Duplicates++
		case strategy.SoldOut:
			rep.SoldOut++
		}
	}
	slices.Sort(lat)
	rep.P50 = Percentile(lat, 50)
	rep.P95 = Percentile(lat, 95)
	rep.P99 = Percentile(lat, 99)
	rep.Max = Percentile(lat, 100)
	return rep, nil
}

// Percentile returns the nearest-rank percentile of an ascending slice.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	rank := int(float64(len(sorted))*p/100+0.999999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

// WriteMarkdown prints the reports as a Markdown table.
func WriteMarkdown(w io.Writer, reports []Report) {
	fmt.Fprintln(w, "| Strategy | Correct | Tickets / Capacity | Oversold | Counter drift | Duplicates absorbed | Retries | Errors | Throughput | p50 | p99 |")
	fmt.Fprintln(w, "|---|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
	for _, r := range reports {
		ok := "✅"
		if !r.Correct() {
			ok = "❌"
		}
		fmt.Fprintf(w, "| `%s` | %s | %d / %d | %d | %+d | %d | %d | %d | %s req/s | %s | %s |\n",
			r.Strategy, ok, r.TicketsIssued, r.Capacity, r.Oversold, r.CounterDrift,
			r.Duplicates, r.Retries, r.Errors, humanInt(int(r.Throughput)), ms(r.P50), ms(r.P99))
	}
	for _, r := range reports {
		if v := r.Violations(); len(v) > 0 {
			fmt.Fprintf(w, "\n`%s`: %s", r.Strategy, strings.Join(v, "; "))
		}
		if r.FirstError != "" {
			fmt.Fprintf(w, "\n`%s` first error: %s", r.Strategy, r.FirstError)
		}
	}
	fmt.Fprintln(w)
}

// WriteJSON prints the reports as JSON.
func WriteJSON(w io.Writer, reports []Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(reports)
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
}

func signed(n int) string {
	if n > 0 {
		return "+" + humanInt(n)
	}
	return humanInt(n)
}

func humanInt(n int) string {
	if n < 0 {
		return "-" + humanInt(-n)
	}
	s := fmt.Sprint(n)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
