package repository

import (
	"context"
	"fmt"
)

// SchedulerRepository owns the unified due-engine transition that releases rows
// for delivery: scheduled -> queued for both first-send and retry.
type SchedulerRepository struct {
	db *DB
}

// NewSchedulerRepository builds a SchedulerRepository over db.
func NewSchedulerRepository(db *DB) *SchedulerRepository {
	return &SchedulerRepository{db: db}
}

const flipDueSQL = `
WITH due AS (
	SELECT id FROM notifications
	WHERE status = 'scheduled' AND coalesce(next_attempt_at, scheduled_at) <= now()
	ORDER BY coalesce(next_attempt_at, scheduled_at)
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
UPDATE notifications n
SET status = 'queued', updated_at = now()
FROM due
WHERE n.id = due.id`

// FlipDue moves up to batch due rows (gate: next_attempt_at else scheduled_at)
// from scheduled to queued. The row-filtered publication captures this, so it drives CDC.
func (r *SchedulerRepository) FlipDue(ctx context.Context, batch int) (int, error) {
	if batch <= 0 {
		return 0, nil
	}
	tag, err := r.db.Pool.Exec(ctx, flipDueSQL, batch)
	if err != nil {
		return 0, fmt.Errorf("flip due: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
