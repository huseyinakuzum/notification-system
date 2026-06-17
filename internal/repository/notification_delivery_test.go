//go:build integration

package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

func seedQueuedNotification(t *testing.T, db *repository.DB, mutate func(*models.Notification)) models.Notification {
	t.Helper()
	repo := repository.NewNotificationRepository(db)
	n := models.Notification{
		ID:             uuid.New(),
		BatchID:        uuid.New(),
		Recipient:      "+1555" + uuid.NewString()[:6],
		Channel:        models.ChannelSMS,
		Content:        "hi",
		Priority:       models.PriorityNormal,
		IdempotencyKey: uuid.NewString(),
		CorrelationID:  uuid.NewString(),
		Status:         models.StatusQueued,
	}
	if mutate != nil {
		mutate(&n)
	}
	if _, err := repo.InsertBatchIdempotent(context.Background(), []models.Notification{n}); err != nil {
		t.Fatalf("seed notification: %v", err)
	}
	return n
}

func TestClaimDueQueued(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)

	got, err := repo.Claim(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got.Status != models.StatusProcessing {
		t.Errorf("status: got %q, want processing", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts: got %d, want 1", got.Attempts)
	}
}

func TestClaimIsSingleWinner(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)

	if _, err := repo.Claim(context.Background(), n.ID); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := repo.Claim(context.Background(), n.ID); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("second claim: got %v, want ErrNotFound", err)
	}
}

func TestMarkSent(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)
	if _, err := repo.Claim(context.Background(), n.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := repo.MarkSent(context.Background(), n.ID, "prov-123"); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	got, err := repo.GetByID(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != models.StatusSent {
		t.Errorf("status: got %q, want sent", got.Status)
	}
	if got.ProviderMessageID == nil || *got.ProviderMessageID != "prov-123" {
		t.Errorf("provider_message_id: got %v, want prov-123", got.ProviderMessageID)
	}
	if got.SentAt == nil {
		t.Error("sent_at not set")
	}
}

func TestMarkRetry(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)
	if _, err := repo.Claim(context.Background(), n.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	next := time.Now().Add(30 * time.Second)
	if err := repo.MarkRetry(context.Background(), n.ID, next, "boom"); err != nil {
		t.Fatalf("mark retry: %v", err)
	}
	got, err := repo.GetByID(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != models.StatusScheduled {
		t.Errorf("status: got %q, want scheduled", got.Status)
	}
	if got.NextAttemptAt == nil {
		t.Fatal("next_attempt_at not set")
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("last_error: got %v, want boom", got.LastError)
	}
}

func TestMarkFailed(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)
	if _, err := repo.Claim(context.Background(), n.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := repo.MarkFailed(context.Background(), n.ID, "dead"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	got, err := repo.GetByID(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != models.StatusFailed {
		t.Errorf("status: got %q, want failed", got.Status)
	}
	if got.LastError == nil || *got.LastError != "dead" {
		t.Errorf("last_error: got %v, want dead", got.LastError)
	}
}

func TestMarkSentRequiresProcessing(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil) // still queued, never claimed

	err := repo.MarkSent(context.Background(), n.ID, "prov-x")
	if !errors.Is(err, repository.ErrConflict) {
		t.Errorf("got %v, want ErrConflict", err)
	}
}

func TestReapStuck(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)
	if _, err := repo.Claim(context.Background(), n.ID); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// backdate updated_at so the row looks stuck
	if _, err := db.Pool.Exec(context.Background(),
		`UPDATE notifications SET updated_at = now() - interval '10 minutes' WHERE id = $1`, n.ID); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	count, err := repo.ReapStuck(context.Background(), time.Minute)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if count < 1 {
		t.Errorf("count: got %d, want >=1", count)
	}
	got, err := repo.GetByID(context.Background(), n.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != models.StatusScheduled {
		t.Errorf("status: got %q, want scheduled", got.Status)
	}
}

func TestListClaimable(t *testing.T) {
	db := setupDB(t)
	repo := repository.NewNotificationRepository(db)
	n := seedQueuedNotification(t, db, nil)

	ids, err := repo.ListClaimable(context.Background(), 100)
	if err != nil {
		t.Fatalf("list claimable: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == n.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("seeded id %s not in claimable list", n.ID)
	}
}
