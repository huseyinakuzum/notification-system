// Package repository provides pgx-backed persistence for the notification
// domain models.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned by lookup methods when no matching row exists.
var ErrNotFound = errors.New("repository: not found")

// ErrConflict is returned when a write loses a compare-and-set race (the target
// row is no longer in the expected state) or violates a unique constraint,
// letting callers handle the conflict without knowing the underlying SQL state.
var ErrConflict = errors.New("repository: conflict")

// pgUniqueViolation is the Postgres SQLSTATE for a unique_violation.
const pgUniqueViolation = "23505"

// isUniqueViolation reports whether err is a Postgres unique-constraint failure.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

// DB wraps a pgx connection pool.
type DB struct {
	Pool *pgxpool.Pool
}

// New opens a connection pool for dsn and verifies connectivity with a ping.
// Pool sizing and connection recycling use service-friendly defaults so idle
// connections are recycled and a PG restart does not leave stale handles.
func New(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &DB{Pool: pool}, nil
}

// Close releases all connections held by the pool.
func (d *DB) Close() {
	d.Pool.Close()
}

// Ping verifies a connection from the pool is reachable.
func (d *DB) Ping(ctx context.Context) error {
	if err := d.Pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping db: %w", err)
	}
	return nil
}

// drainBatch sends batch on tx, executes every queued command, and returns the
// total rows affected. It centralizes the send/close/iterate boilerplate shared
// by the batch-insert repositories; callers wrap the error with their own
// context.
func drainBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) (int64, error) {
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	var affected int64
	for range batch.Len() {
		tag, err := results.Exec()
		if err != nil {
			return 0, err
		}
		affected += tag.RowsAffected()
	}
	return affected, nil
}

// WithTx runs fn inside a transaction, committing when fn returns nil and
// rolling back otherwise.
func (d *DB) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback tx: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
