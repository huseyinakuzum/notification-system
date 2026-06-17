package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

const notificationClaimSQL = `
UPDATE notifications
SET status = 'processing', attempts = attempts + 1, updated_at = now()
WHERE id = $1 AND status = 'queued'
RETURNING ` + notificationColumns

// Claim atomically transitions a notification from queued to processing,
// incrementing attempts. Only the caller that wins the compare-and-set gets the
// row; if it is not claimable (already handled or reset) it returns ErrNotFound.
// Due-time and attempt-cap gating live in the scheduler's scheduled->queued
// transition, so claim only checks the queued state.
func (r *NotificationRepository) Claim(ctx context.Context, id uuid.UUID) (models.Notification, error) {
	return r.getNotification(ctx, notificationClaimSQL, id)
}

const notificationMarkSentSQL = `
UPDATE notifications
SET status = 'sent', provider_message_id = $2, sent_at = now(), updated_at = now()
WHERE id = $1 AND status = 'processing'`

// MarkSent records a successful delivery. It returns ErrConflict if the row is
// no longer processing (e.g. a reaper reset it after a lost claim).
func (r *NotificationRepository) MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string) error {
	return r.execCAS(ctx, "mark sent", notificationMarkSentSQL, id, providerMessageID)
}

const notificationMarkRetrySQL = `
UPDATE notifications
SET status = 'scheduled', next_attempt_at = $2, last_error = $3, updated_at = now()
WHERE id = $1 AND status = 'processing'`

// MarkRetry schedules a retry by returning the row to scheduled with a future
// next_attempt_at; the scheduler re-flips it to queued once due. attempts was
// already incremented at claim time.
func (r *NotificationRepository) MarkRetry(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time, lastErr string) error {
	return r.execCAS(ctx, "mark retry", notificationMarkRetrySQL, id, nextAttemptAt, lastErr)
}

const notificationMarkFailedSQL = `
UPDATE notifications
SET status = 'failed', last_error = $2, updated_at = now()
WHERE id = $1 AND status = 'processing'`

// MarkFailed terminally fails a notification (cap exhausted or fatal provider
// error). The caller is responsible for producing to the DLQ.
func (r *NotificationRepository) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	return r.execCAS(ctx, "mark failed", notificationMarkFailedSQL, id, lastErr)
}

func (r *NotificationRepository) execCAS(ctx context.Context, op, sql string, args ...any) error {
	tag, err := r.db.Pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}

const notificationReapSQL = `
UPDATE notifications
SET status = 'scheduled', updated_at = now()
WHERE status = 'processing'
	AND updated_at < now() - make_interval(secs => $1)`

// ReapStuck resets rows that have been processing longer than timeout back to
// scheduled, recovering work stranded by a crashed worker; the scheduler
// re-flips them to queued. It returns the number of rows reset.
func (r *NotificationRepository) ReapStuck(ctx context.Context, timeout time.Duration) (int, error) {
	tag, err := r.db.Pool.Exec(ctx, notificationReapSQL, timeout.Seconds())
	if err != nil {
		return 0, fmt.Errorf("reap stuck: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

const notificationListClaimableSQL = `
SELECT id FROM notifications
WHERE status = 'queued'
ORDER BY
	CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,
	updated_at
LIMIT $1`

// ListClaimable returns the ids of notifications ready for delivery, ordered by
// priority then due time. It backs DB-as-source-of-truth recovery: the delivery
// processor rescans claimable rows independent of the Kafka buffer.
func (r *NotificationRepository) ListClaimable(ctx context.Context, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := r.db.Pool.Query(ctx, notificationListClaimableSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("list claimable: %w", err)
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan claimable: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable: %w", err)
	}
	return ids, nil
}
