// Command server exposes the strategies over HTTP.
//
//	POST /events                     {"id":1,"name":"Concert","capacity":100}
//	GET  /events/{id}
//	POST /events/{id}/purchase?strategy=atomic
//	     Idempotency-Key: <uuid>     (required)
//	     {"buyer_id":"alice"}
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Joaolfc0/oversell-lab/internal/store"
	"github.com/Joaolfc0/oversell-lab/internal/strategy"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Connect(ctx, env("DATABASE_URL", "postgres://oversell:oversell@localhost:54329/oversell?sslmode=disable"), 64)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := store.Migrate(ctx, db); err != nil {
		return err
	}

	opt, err := redis.ParseURL(env("REDIS_URL", "redis://localhost:63799/0"))
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()

	strategies := map[string]strategy.Strategy{}
	for _, name := range strategy.Names() {
		s, err := strategy.New(name, strategy.Deps{DB: db, Redis: rdb})
		if err != nil {
			return err
		}
		defer strategy.Close(s)
		strategies[name] = s
	}

	a := &api{db: db, strategies: strategies, fallback: env("DEFAULT_STRATEGY", "atomic")}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", a.createEvent)
	mux.HandleFunc("GET /events/{id}", a.getEvent)
	mux.HandleFunc("POST /events/{id}/purchase", a.purchase)

	srv := &http.Server{Addr: env("ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	slog.Info("listening", "addr", srv.Addr, "default_strategy", a.fallback)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type api struct {
	db         *pgxpool.Pool
	strategies map[string]strategy.Strategy
	fallback   string
}

func (a *api) createEvent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID       int64  `json:"id"`
		Name     string `json:"name"`
		Capacity int    `json:"capacity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID <= 0 || body.Capacity < 0 {
		writeError(w, http.StatusBadRequest, "body must be {id > 0, name, capacity >= 0}")
		return
	}
	if err := store.ResetEvent(r.Context(), a.db, body.ID, body.Name, body.Capacity); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, s := range a.strategies {
		if p, ok := s.(strategy.Preparer); ok {
			if err := p.Prepare(r.Context(), body.ID, body.Capacity); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
	}
	writeJSON(w, http.StatusCreated, body)
}

func (a *api) getEvent(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}
	stats, err := store.Stats(r.Context(), a.db, id)
	if errors.Is(err, store.ErrEventNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"capacity": stats.Capacity, "sold": stats.Sold, "tickets": stats.Tickets,
		"oversold": stats.Oversold(), "counter_drift": stats.CounterDrift(),
	})
}

func (a *api) purchase(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	name := r.URL.Query().Get("strategy")
	if name == "" {
		name = a.fallback
	}
	s, ok := a.strategies[name]
	if !ok {
		writeError(w, http.StatusBadRequest, "unknown strategy "+strconv.Quote(name))
		return
	}
	var body struct {
		BuyerID string `json:"buyer_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BuyerID == "" {
		writeError(w, http.StatusBadRequest, `body must be {"buyer_id": "..."}`)
		return
	}

	res, err := s.Purchase(r.Context(), strategy.Request{EventID: id, BuyerID: body.BuyerID, IdempotencyKey: key})
	switch {
	case errors.Is(err, store.ErrEventNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, err.Error())
	case res.Outcome == strategy.Purchased:
		writeJSON(w, http.StatusCreated, map[string]any{"status": "purchased", "ticket_id": res.TicketID, "strategy": name})
	case res.Outcome == strategy.Duplicate:
		// A replay gets the same answer as the original request.
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "ticket_id": res.TicketID, "strategy": name})
	default:
		writeError(w, http.StatusConflict, "sold out")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
