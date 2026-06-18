package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

// NotificationRepository persists notifications rows. It is the single source of
// truth for the request lifecycle, from acceptance through delivery.
type NotificationRepository struct {
	db *DB
}

// NewNotificationRepository returns a NotificationRepository backed by db.
func NewNotificationRepository(db *DB) *NotificationRepository {
	return &NotificationRepository{db: db}
}

const notificationInsertSQL = `
INSERT INTO notifications
	(id, batch_id, recipient, channel, content, priority,
	 idempotency_key, correlation_id, traceparent, status, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, now()))
ON CONFLICT (idempotency_key) DO NOTHING`

const notificationIDsByKeySQL = `
SELECT idempotency_key, id
FROM notifications
WHERE idempotency_key = ANY($1)`

// InsertBatchIdempotent inserts rows in one transaction using ON CONFLICT
// (idempotency_key) DO NOTHING, then returns the id for each input row in input
// order. When a key already existed the pre-existing id is returned, making the
// operation safe to retry.
func (r *NotificationRepository) InsertBatchIdempotent(ctx context.Context, rows []models.Notification) ([]uuid.UUID, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	keys := make([]string, len(rows))
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		for i := range rows {
			row := rows[i]
			if row.ID == uuid.Nil {
				row.ID = uuid.New()
			}
			if row.Status == "" {
				row.Status = models.StatusScheduled
			}
			if row.Priority == "" {
				row.Priority = models.PriorityNormal
			}
			keys[i] = row.IdempotencyKey
			var scheduledAt *time.Time
			if !row.ScheduledAt.IsZero() {
				scheduledAt = &row.ScheduledAt
			}
			batch.Queue(notificationInsertSQL,
				row.ID, row.BatchID, row.Recipient, row.Channel, row.Content,
				row.Priority, row.IdempotencyKey, row.CorrelationID, row.TraceParent,
				row.Status, scheduledAt)
		}
		if _, err := drainBatch(ctx, tx, batch); err != nil {
			return fmt.Errorf("insert notification: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	byKey := make(map[string]uuid.UUID, len(rows))
	queryRows, err := r.db.Pool.Query(ctx, notificationIDsByKeySQL, keys)
	if err != nil {
		return nil, fmt.Errorf("resolve notification ids: %w", err)
	}
	defer queryRows.Close()
	for queryRows.Next() {
		var key string
		var id uuid.UUID
		if err := queryRows.Scan(&key, &id); err != nil {
			return nil, fmt.Errorf("scan notification id: %w", err)
		}
		byKey[key] = id
	}
	if err := queryRows.Err(); err != nil {
		return nil, fmt.Errorf("resolve notification ids: %w", err)
	}

	ids := make([]uuid.UUID, len(rows))
	for i, key := range keys {
		ids[i] = byKey[key]
	}
	return ids, nil
}

const notificationColumns = `id, batch_id, recipient, channel, content, priority,
	idempotency_key, correlation_id, traceparent, status, attempts, max_attempts,
	next_attempt_at, last_error, provider_message_id, scheduled_at, sent_at,
	created_at, updated_at`

const notificationSelect = `SELECT ` + notificationColumns + `
FROM notifications
WHERE `

// GetByID returns the notifications row for id, or ErrNotFound if absent.
func (r *NotificationRepository) GetByID(ctx context.Context, id uuid.UUID) (models.Notification, error) {
	return r.getNotification(ctx, notificationSelect+"id = $1", id)
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows, letting the
// notification column scan live in one place.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanNotification(s rowScanner) (models.Notification, error) {
	var row models.Notification
	err := s.Scan(
		&row.ID, &row.BatchID, &row.Recipient, &row.Channel,
		&row.Content, &row.Priority, &row.IdempotencyKey, &row.CorrelationID,
		&row.TraceParent, &row.Status, &row.Attempts, &row.MaxAttempts, &row.NextAttemptAt,
		&row.LastError, &row.ProviderMessageID, &row.ScheduledAt, &row.SentAt,
		&row.CreatedAt, &row.UpdatedAt)
	return row, err
}

func (r *NotificationRepository) getNotification(ctx context.Context, sql string, arg any) (models.Notification, error) {
	row, err := scanNotification(r.db.Pool.QueryRow(ctx, sql, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Notification{}, ErrNotFound
	}
	if err != nil {
		return models.Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return row, nil
}

const notificationCancelSQL = `
UPDATE notifications
SET status = 'cancelled', updated_at = now()
WHERE id = $1 AND status = 'scheduled'`

// Cancel performs the compare-and-set transition scheduled -> cancelled. It
// returns true when a row was transitioned and false when none matched (the row
// is missing or no longer scheduled).
func (r *NotificationRepository) Cancel(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := r.db.Pool.Exec(ctx, notificationCancelSQL, id)
	if err != nil {
		return false, fmt.Errorf("cancel notification: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Filter narrows a List query. Nil pointer fields are not applied as predicates;
// Limit and Offset are passed through as supplied by the caller.
type Filter struct {
	Status  *models.Status
	Channel *models.Channel
	From    *time.Time // created_at >= From
	To      *time.Time // created_at <  To
	Limit   int
	Offset  int
}

// List returns notifications rows matching f, newest first.
func (r *NotificationRepository) List(ctx context.Context, f Filter) ([]models.Notification, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ` + notificationColumns + `
FROM notifications`)

	args := make([]any, 0, 6)
	var conds []string
	if f.Status != nil {
		args = append(args, *f.Status)
		conds = append(conds, "status = $"+strconv.Itoa(len(args)))
	}
	if f.Channel != nil {
		args = append(args, *f.Channel)
		conds = append(conds, "channel = $"+strconv.Itoa(len(args)))
	}
	if f.From != nil {
		args = append(args, *f.From)
		conds = append(conds, "created_at >= $"+strconv.Itoa(len(args)))
	}
	if f.To != nil {
		args = append(args, *f.To)
		conds = append(conds, "created_at < $"+strconv.Itoa(len(args)))
	}
	if len(conds) > 0 {
		sb.WriteString("\nWHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	sb.WriteString("\nORDER BY created_at DESC")
	args = append(args, f.Limit)
	sb.WriteString("\nLIMIT $" + strconv.Itoa(len(args)))
	args = append(args, f.Offset)
	sb.WriteString(" OFFSET $" + strconv.Itoa(len(args)))

	return r.queryNotifications(ctx, sb.String(), args...)
}

const notificationListByBatchSQL = `SELECT ` + notificationColumns + `
FROM notifications
WHERE batch_id = $1
ORDER BY created_at`

// ListByBatch returns every notifications row for batchID, oldest first.
func (r *NotificationRepository) ListByBatch(ctx context.Context, batchID uuid.UUID) ([]models.Notification, error) {
	return r.queryNotifications(ctx, notificationListByBatchSQL, batchID)
}

func (r *NotificationRepository) queryNotifications(ctx context.Context, sql string, args ...any) ([]models.Notification, error) {
	rows, err := r.db.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()
	var out []models.Notification
	for rows.Next() {
		row, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	return out, nil
}
