// Command bench runs the same flash sale against each strategy and prints
// a table comparing correctness and performance.
//
//	go run ./cmd/bench -strategy all -capacity 1000 -buyers 10000
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Joaolfc0/oversell-lab/internal/bench"
	"github.com/Joaolfc0/oversell-lab/internal/store"
	"github.com/Joaolfc0/oversell-lab/internal/strategy"
)

func main() {
	var (
		dbURL    = flag.String("db", env("DATABASE_URL", "postgres://oversell:oversell@localhost:54329/oversell?sslmode=disable"), "Postgres URL")
		redisURL = flag.String("redis", env("REDIS_URL", "redis://localhost:63799/0"), "Redis URL")
		which    = flag.String("strategy", "all", "comma-separated strategies, or 'all': "+strings.Join(strategy.Names(), ","))
		capacity = flag.Int("capacity", 1000, "tickets available")
		buyers   = flag.Int("buyers", 10000, "concurrent buyers (one goroutine each)")
		dupRate  = flag.Float64("dup-rate", 0.1, "fraction of buyers who send their request twice")
		pool     = flag.Int("pool", 64, "max Postgres connections")
		seed     = flag.Uint64("seed", 42, "random seed for the duplicate pattern")
		timeout  = flag.Duration("timeout", 2*time.Minute, "timeout per strategy run")
		format   = flag.String("format", "markdown", "markdown or json")
		withFK   = flag.Bool("with-fk", false, "add the tickets.event_id foreign key to reproduce MultiXact contention (docs/adr/0004)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := store.Connect(ctx, *dbURL, int32(*pool))
	if err != nil {
		log.Fatalf("postgres: %v (is `make up` running?)", err)
	}
	defer db.Close()
	if err := store.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}

	if err := store.SetForeignKey(ctx, db, *withFK); err != nil {
		log.Fatal(err)
	}
	if *withFK {
		defer store.SetForeignKey(context.Background(), db, false)
	}

	names := strategy.Names()
	if *which != "all" {
		names = strings.Split(*which, ",")
	}

	deps := strategy.Deps{DB: db}
	if slicesContains(names, "redis") {
		opt, err := redis.ParseURL(*redisURL)
		if err != nil {
			log.Fatalf("redis url: %v", err)
		}
		rdb := redis.NewClient(opt)
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Fatalf("redis: %v (is `make up` running?)", err)
		}
		deps.Redis = rdb
	}

	log.Printf("flash sale: %d tickets, %d buyers, %.0f%% duplicate requests, pool=%d, GOMAXPROCS=%d, foreign key=%v",
		*capacity, *buyers, *dupRate*100, *pool, runtime.GOMAXPROCS(0), *withFK)

	var reports []bench.Report
	for i, name := range names {
		s, err := strategy.New(strings.TrimSpace(name), deps)
		if err != nil {
			log.Fatal(err)
		}
		rep, err := bench.Run(ctx, db, s, bench.Config{
			EventID:       int64(i + 1),
			Capacity:      *capacity,
			Buyers:        *buyers,
			DuplicateRate: *dupRate,
			Seed:          *seed,
			Timeout:       *timeout,
		})
		strategy.Close(s)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		log.Printf("%-12s done in %v", name, rep.Duration.Round(time.Millisecond))
		reports = append(reports, rep)
	}

	fmt.Println()
	switch *format {
	case "json":
		if err := bench.WriteJSON(os.Stdout, reports); err != nil {
			log.Fatal(err)
		}
	default:
		bench.WriteMarkdown(os.Stdout, reports)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if strings.TrimSpace(x) == v {
			return true
		}
	}
	return false
}
