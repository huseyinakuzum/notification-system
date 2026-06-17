//go:build integration

package repository_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = os.Getenv("DB_DSN")
	}
	if dsn == "" {
		t.Skip("set TEST_DB_DSN to run repository integration tests")
	}
	return dsn
}

func setupDB(t *testing.T) *repository.DB {
	t.Helper()
	db, err := repository.New(context.Background(), testDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func truncateAll(t *testing.T, db *repository.DB) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(), "TRUNCATE notifications, templates")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func TestNotificationRepositoryInsertAndGetByID(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewNotificationRepository(db)

	want := models.Notification{
		ID:             uuid.New(),
		BatchID:        uuid.New(),
		Recipient:      "+15551234567",
		Channel:        models.ChannelSMS,
		Content:        "hello",
		Priority:       models.PriorityHigh,
		IdempotencyKey: "notif-" + uuid.NewString(),
		CorrelationID:  "corr-1",
		Status:         models.StatusScheduled,
	}
	if _, err := repo.InsertBatchIdempotent(ctx, []models.Notification{want}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := repo.GetByID(ctx, want.ID)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if got.ID != want.ID || got.BatchID != want.BatchID || got.Recipient != want.Recipient {
		t.Errorf("identity mismatch: got %+v", got)
	}
	if got.Channel != want.Channel || got.Priority != want.Priority || got.Status != want.Status {
		t.Errorf("enum mismatch: got channel=%q priority=%q status=%q", got.Channel, got.Priority, got.Status)
	}
	if got.ScheduledAt.IsZero() || got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Errorf("timestamps not defaulted: %+v", got)
	}

	if _, err := repo.GetByID(ctx, uuid.New()); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("get missing: got %v, want ErrNotFound", err)
	}
}

func TestNotificationRepositoryInsertBatchDedup(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewNotificationRepository(db)

	keyA := "notif-a-" + uuid.NewString()
	keyB := "notif-b-" + uuid.NewString()
	first := models.Notification{
		ID:             uuid.New(),
		BatchID:        uuid.New(),
		Recipient:      "+15551112222",
		Channel:        models.ChannelPush,
		Content:        "a",
		Priority:       models.PriorityNormal,
		IdempotencyKey: keyA,
		CorrelationID:  "corr-a",
		Status:         models.StatusScheduled,
	}
	second := first
	second.ID = uuid.New()
	second.IdempotencyKey = keyB
	second.Content = "b"
	dup := first
	dup.ID = uuid.New()
	dup.Content = "dup"

	ids, err := repo.InsertBatchIdempotent(ctx, []models.Notification{first, second, dup})
	if err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("ids len = %d, want 3", len(ids))
	}
	// first and dup share an idempotency key, so they resolve to the same id.
	if ids[0] != ids[2] {
		t.Errorf("dup id = %v, want same as first %v", ids[2], ids[0])
	}
	if ids[0] == ids[1] {
		t.Errorf("distinct keys resolved to same id %v", ids[0])
	}
	if ids[0] != first.ID {
		t.Errorf("first id = %v, want %v", ids[0], first.ID)
	}

	got, err := repo.GetByID(ctx, first.ID)
	if err != nil {
		t.Fatalf("get inserted: %v", err)
	}
	if got.IdempotencyKey != keyA {
		t.Errorf("idempotency key mismatch: got %q", got.IdempotencyKey)
	}
	if got.Content != "a" {
		t.Errorf("dup overwrote content: got %q, want a", got.Content)
	}
}

func TestNotificationRepositoryCancel(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewNotificationRepository(db)

	row := models.Notification{
		ID:             uuid.New(),
		BatchID:        uuid.New(),
		Recipient:      "+15551234567",
		Channel:        models.ChannelEmail,
		Content:        "hi",
		IdempotencyKey: "cancel-" + uuid.NewString(),
		CorrelationID:  "corr-2",
		Status:         models.StatusScheduled,
	}
	if _, err := repo.InsertBatchIdempotent(ctx, []models.Notification{row}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ok, err := repo.Cancel(ctx, row.ID)
	if err != nil {
		t.Fatalf("first cancel: %v", err)
	}
	if !ok {
		t.Fatalf("first cancel: got false, want true")
	}

	ok, err = repo.Cancel(ctx, row.ID)
	if err != nil {
		t.Fatalf("second cancel: %v", err)
	}
	if ok {
		t.Errorf("second cancel: got true, want false (already cancelled)")
	}

	ok, err = repo.Cancel(ctx, uuid.New())
	if err != nil {
		t.Fatalf("missing cancel: %v", err)
	}
	if ok {
		t.Errorf("missing cancel: got true, want false")
	}
}

func TestTemplateRepositoryCreateAndGetByName(t *testing.T) {
	db := setupDB(t)
	truncateAll(t, db)
	ctx := context.Background()
	repo := repository.NewTemplateRepository(db)

	name := "welcome-" + uuid.NewString()
	tmpl := models.Template{
		Name:    name,
		Channel: models.ChannelEmail,
		Body:    "Hello {{.name}}",
	}
	if err := repo.Create(ctx, &tmpl); err != nil {
		t.Fatalf("create: %v", err)
	}
	if tmpl.ID == uuid.Nil {
		t.Errorf("id not set after create")
	}
	if tmpl.CreatedAt.IsZero() {
		t.Errorf("created_at not populated after create")
	}

	got, err := repo.GetByName(ctx, name)
	if err != nil {
		t.Fatalf("get by name: %v", err)
	}
	if got.ID != tmpl.ID || got.Name != name || got.Channel != models.ChannelEmail || got.Body != tmpl.Body {
		t.Errorf("template mismatch: got %+v", got)
	}

	if _, err := repo.GetByName(ctx, "does-not-exist-"+uuid.NewString()); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("get missing: got %v, want ErrNotFound", err)
	}
}
