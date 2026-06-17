package api

import (
	"context"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

// NotificationStore is the notifications persistence the API depends on. It is
// declared here (consumer side) so the API depends on behaviour, not on the
// concrete pgx repository.
type NotificationStore interface {
	InsertBatchIdempotent(ctx context.Context, rows []models.Notification) ([]uuid.UUID, error)
	GetByID(ctx context.Context, id uuid.UUID) (models.Notification, error)
	List(ctx context.Context, f repository.Filter) ([]models.Notification, error)
	ListByBatch(ctx context.Context, batchID uuid.UUID) ([]models.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (bool, error)
}

// TemplateStore is the template persistence the API depends on.
type TemplateStore interface {
	Create(ctx context.Context, t *models.Template) error
	GetByName(ctx context.Context, name string) (models.Template, error)
	GetByID(ctx context.Context, id uuid.UUID) (models.Template, error)
}
