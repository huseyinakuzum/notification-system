//go:build integration

package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

func seedScheduled(t *testing.T, db *repository.DB, n int, scheduledAt time.Time) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	repo := repository.NewNotificationRepository(db)
	batchID := uuid.New()
	rows := make([]models.Notification, n)
	for i := range rows {
		rows[i] = models.Notification{
			ID:             uuid.New(),
			BatchID:        batchID,
			Recipient:      "+1555000" + uuid.NewString()[:4],
			Channel:        models.ChannelSMS,
			Content:        "hi",
			Priority:       models.PriorityNormal,
			IdempotencyKey: "flip-" + uuid.NewString(),
			CorrelationID:  "corr",
			Status:         models.StatusScheduled,
			ScheduledAt:    scheduledAt,
		}
	}
	if _, err := repo.InsertBatchIdempotent(ctx, rows); err != nil {
		t.Fatalf("seed scheduled: %v", err)
	}
	return batchID
}

func countStatus(t *testing.T, db *repository.DB, batchID uuid.UUID, status models.Status) int {
	t.Helper()
	var c int
	err := db.Pool.QueryRow(context.Background(),
		"SELECT count(*) FROM notifications WHERE batch_id=$1 AND status=$2", batchID, status).Scan(&c)
	if err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}
	return c
}

func TestFlipDueReleasesDueRows(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewSchedulerRepository(db)

	batchID := seedScheduled(t, db, 5, time.Now().Add(-time.Minute))

	flipped, err := repo.FlipDue(ctx, 10)
	if err != nil {
		t.Fatalf("flip due: %v", err)
	}
	if flipped != 5 {
		t.Fatalf("flipped = %d, want 5", flipped)
	}
	if got := countStatus(t, db, batchID, models.StatusQueued); got != 5 {
		t.Errorf("queued rows = %d, want 5", got)
	}

	// A second pass finds nothing scheduled.
	again, err := repo.FlipDue(ctx, 10)
	if err != nil {
		t.Fatalf("second flip: %v", err)
	}
	if again != 0 {
		t.Errorf("second flip = %d, want 0", again)
	}
}

func TestFlipDueSkipsFutureRows(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewSchedulerRepository(db)

	batchID := seedScheduled(t, db, 3, time.Now().Add(time.Hour))

	flipped, err := repo.FlipDue(ctx, 10)
	if err != nil {
		t.Fatalf("flip due: %v", err)
	}
	if flipped != 0 {
		t.Errorf("flipped = %d, want 0 (all future)", flipped)
	}
	if got := countStatus(t, db, batchID, models.StatusScheduled); got != 3 {
		t.Errorf("scheduled rows = %d, want 3", got)
	}
}

func TestFlipDueHonorsNextAttemptAt(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewSchedulerRepository(db)

	// Due by scheduled_at, but a future next_attempt_at (a retry not yet due)
	// must keep the row scheduled.
	batchID := seedScheduled(t, db, 1, time.Now().Add(-time.Hour))
	if _, err := db.Pool.Exec(ctx,
		"UPDATE notifications SET next_attempt_at = now() + interval '1 hour' WHERE batch_id=$1", batchID); err != nil {
		t.Fatalf("set next_attempt_at: %v", err)
	}

	flipped, err := repo.FlipDue(ctx, 10)
	if err != nil {
		t.Fatalf("flip due: %v", err)
	}
	if flipped != 0 {
		t.Errorf("flipped = %d, want 0 (retry not due)", flipped)
	}
}
