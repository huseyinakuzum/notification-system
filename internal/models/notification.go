package models

import (
	"time"

	"github.com/google/uuid"
)

// Notification mirrors a row in the notifications table: the single deliverable
// unit carrying request data plus retry and provider bookkeeping.
type Notification struct {
	ID                uuid.UUID
	BatchID           uuid.UUID
	Recipient         string
	Channel           Channel
	Content           string
	Priority          Priority
	IdempotencyKey    string
	CorrelationID     string
	Status            Status
	Attempts          int
	MaxAttempts       int
	NextAttemptAt     *time.Time
	LastError         *string
	ProviderMessageID *string
	ScheduledAt       time.Time
	SentAt            *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Template mirrors a row in the templates table.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
}
