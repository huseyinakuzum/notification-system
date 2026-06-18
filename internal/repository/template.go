package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

// TemplateRepository persists templates rows.
type TemplateRepository struct {
	db *DB
}

func NewTemplateRepository(db *DB) *TemplateRepository {
	return &TemplateRepository{db: db}
}

const templateCreateSQL = `
INSERT INTO templates (id, name, channel, body)
VALUES ($1, $2, $3, $4)
RETURNING created_at`

// Create inserts t, generating an ID when none is set and populating CreatedAt
// from the database default.
func (r *TemplateRepository) Create(ctx context.Context, t *models.Template) error {
	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	err := r.db.Pool.QueryRow(ctx, templateCreateSQL, t.ID, t.Name, t.Channel, t.Body).
		Scan(&t.CreatedAt)
	if isUniqueViolation(err) {
		return ErrConflict
	}
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

const templateSelect = `
SELECT id, name, channel, body, created_at
FROM templates
WHERE `

// GetByName returns the template with the given name, or ErrNotFound if absent.
func (r *TemplateRepository) GetByName(ctx context.Context, name string) (models.Template, error) {
	return r.getTemplate(ctx, templateSelect+"name = $1", name)
}

// GetByID returns the template with the given id, or ErrNotFound if absent.
func (r *TemplateRepository) GetByID(ctx context.Context, id uuid.UUID) (models.Template, error) {
	return r.getTemplate(ctx, templateSelect+"id = $1", id)
}

func (r *TemplateRepository) getTemplate(ctx context.Context, sql string, arg any) (models.Template, error) {
	var t models.Template
	err := r.db.Pool.QueryRow(ctx, sql, arg).
		Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Template{}, ErrNotFound
	}
	if err != nil {
		return models.Template{}, fmt.Errorf("get template: %w", err)
	}
	return t, nil
}
