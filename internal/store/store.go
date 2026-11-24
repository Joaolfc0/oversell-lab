// Package store owns the Postgres connection pool, the schema and the
// read-only queries used to verify invariants after a run.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

// ErrEventNotFound is returned when an event id does not exist.
var ErrEventNotFound = errors.New("event not found")

// Connect opens a pgx pool. maxConns <= 0 keeps the pgx default.
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// Migrate creates the schema if it does not exist.
func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// SetForeignKey adds or drops the tickets.event_id -> events.id foreign key.
// The schema ships without it (see docs/adr/0004). This exists so the
// benchmark can reproduce the contention it causes.
func SetForeignKey(ctx context.Context, db *pgxpool.Pool, enabled bool) error {
	sql := `ALTER TABLE tickets DROP CONSTRAINT IF EXISTS tickets_event_id_fkey`
	if enabled {
		sql = `DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tickets_event_id_fkey') THEN
				ALTER TABLE tickets ADD CONSTRAINT tickets_event_id_fkey FOREIGN KEY (event_id) REFERENCES events (id);
			END IF;
		END $$`
	}
	if _, err := db.Exec(ctx, sql); err != nil {
		return fmt.Errorf("set foreign key=%v: %w", enabled, err)
	}
	return nil
}

// ResetEvent creates the event, or wipes it back to zero tickets sold.
func ResetEvent(ctx context.Context, db *pgxpool.Pool, id int64, name string, capacity int) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM tickets WHERE event_id = $1`, id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO events (id, name, capacity, sold, version)
			VALUES ($1, $2, $3, 0, 0)
			ON CONFLICT (id) DO UPDATE
			SET name = EXCLUDED.name, capacity = EXCLUDED.capacity, sold = 0, version = 0`,
			id, name, capacity)
		return err
	})
}

// EventStats is the ground truth read back from Postgres after a run.
type EventStats struct {
	Capacity int `json:"capacity"`
	// Sold is the denormalised counter on the events row.
	Sold int `json:"sold"`
	// Tickets is the number of ticket rows that actually exist.
	Tickets int `json:"tickets"`
}

// Oversold is how many tickets were issued beyond capacity.
func (s EventStats) Oversold() int { return max(0, s.Tickets-s.Capacity) }

// CounterDrift is the difference between the counter and reality.
// Non-zero means a lost update happened somewhere.
func (s EventStats) CounterDrift() int { return s.Sold - s.Tickets }

// Stats reads the current state of an event.
func Stats(ctx context.Context, db *pgxpool.Pool, id int64) (EventStats, error) {
	var s EventStats
	err := db.QueryRow(ctx, `
		SELECT e.capacity, e.sold,
		       (SELECT count(*) FROM tickets t WHERE t.event_id = e.id)
		FROM events e WHERE e.id = $1`, id).Scan(&s.Capacity, &s.Sold, &s.Tickets)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, ErrEventNotFound
	}
	return s, err
}
