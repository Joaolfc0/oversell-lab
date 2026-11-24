package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Joaolfc0/oversell-lab/internal/bench"
	"github.com/Joaolfc0/oversell-lab/internal/strategy"
)

//go:embed web
var webFS embed.FS

// Limits keep one dashboard visitor from tying up the database for minutes.
const (
	maxCapacity = 5_000
	maxBuyers   = 20_000
	// Dashboard runs use their own event ids so they never touch events
	// created through the purchase API or the CLI benchmark.
	dashboardEventBase = 900_000
)

type dashboard struct {
	db         *pgxpool.Pool
	monitor    *pgxpool.Pool
	strategies map[string]strategy.Strategy
	running    sync.Mutex // one flash sale at a time; they share the database
}

func (d *dashboard) static() http.Handler {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	return http.FileServerFS(sub)
}

// run streams a flash sale as Server-Sent Events:
//
//	event: start     {"strategy","capacity","requests"}
//	event: progress  bench.Progress, about 10 per second
//	event: report    bench.Report, once per strategy
//	event: failure   {"message"}
//	event: done      {}
//
// Closing the connection cancels the run.
func (d *dashboard) run(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	names := strings.Split(q.Get("strategies"), ",")
	capacity, err1 := strconv.Atoi(q.Get("capacity"))
	buyers, err2 := strconv.Atoi(q.Get("buyers"))
	dup, err3 := strconv.ParseFloat(q.Get("dup"), 64)
	if err1 != nil || err2 != nil || err3 != nil ||
		capacity < 1 || capacity > maxCapacity || buyers < 1 || buyers > maxBuyers || dup < 0 || dup > 1 {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("need 1 ≤ capacity ≤ %d, 1 ≤ buyers ≤ %d and 0 ≤ dup ≤ 1", maxCapacity, maxBuyers))
		return
	}
	for _, n := range names {
		if _, ok := d.strategies[n]; !ok {
			writeError(w, http.StatusBadRequest, "unknown strategy "+strconv.Quote(n))
			return
		}
	}
	if !d.running.TryLock() {
		writeError(w, http.StatusConflict, "another flash sale is running; wait for it to finish")
		return
	}
	defer d.running.Unlock()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	var mu sync.Mutex // OnProgress runs on another goroutine
	send := func(event string, v any) {
		b, _ := json.Marshal(v)
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		flusher.Flush()
	}

	ctx := r.Context()
	for i, name := range names {
		if ctx.Err() != nil {
			return
		}
		send("start", map[string]any{"strategy": name, "capacity": capacity, "buyers": buyers})
		rep, err := bench.Run(ctx, d.db, d.strategies[name], bench.Config{
			EventID:       int64(dashboardEventBase + i),
			Capacity:      capacity,
			Buyers:        buyers,
			DuplicateRate: dup,
			Seed:          uint64(time.Now().UnixNano()),
			Timeout:       3 * time.Minute,
			Monitor:       d.monitor,
			ProgressEvery: 80 * time.Millisecond,
			OnProgress:    func(p bench.Progress) { send("progress", p) },
		})
		if err != nil {
			slog.Warn("dashboard run failed", "strategy", name, "err", err)
			send("failure", map[string]string{"strategy": name, "message": err.Error()})
			return
		}
		send("report", reportJSON{rep, rep.Correct(), rep.Violations()})
	}
	send("done", struct{}{})
}

type reportJSON struct {
	bench.Report
	Correct    bool     `json:"correct"`
	Violations []string `json:"violations"`
}
