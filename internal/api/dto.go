package api

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

type createItem struct {
	Recipient    string            `json:"recipient"`
	Channel      models.Channel    `json:"channel"`
	Content      string            `json:"content"`
	Priority     models.Priority   `json:"priority"`
	TemplateID   *uuid.UUID        `json:"template_id" swaggertype:"string" format:"uuid"`
	TemplateVars map[string]string `json:"template_vars"`
	ScheduledAt  *time.Time        `json:"scheduled_at"`
}

// deriveIdempotencyKey computes a deterministic dedup key from the fields that
// define a distinct send. Two requests with the same recipient, channel, content,
// priority, and schedule collapse to one row via the UNIQUE(idempotency_key)
// constraint; changing any of them yields a new key. Callers pass the resolved
// content (post-template) and resolved priority so the key is stable.
func deriveIdempotencyKey(recipient string, channel models.Channel, content string, priority models.Priority, scheduledAt *time.Time) string {
	sched := ""
	if scheduledAt != nil {
		sched = scheduledAt.UTC().Format(time.RFC3339Nano)
	}
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{recipient, string(channel), content, string(priority), sched},
		"\x00",
	)))
	return hex.EncodeToString(sum[:])
}

type createResponse struct {
	BatchID uuid.UUID   `json:"batch_id" swaggertype:"string" format:"uuid"`
	IDs     []uuid.UUID `json:"ids" swaggertype:"array,string"`
}

type deliveryView struct {
	Status            models.Status `json:"status"`
	Attempts          int           `json:"attempts"`
	MaxAttempts       int           `json:"max_attempts"`
	LastError         *string       `json:"last_error,omitempty"`
	ProviderMessageID *string       `json:"provider_message_id,omitempty"`
	SentAt            *time.Time    `json:"sent_at,omitempty"`
}

type notificationView struct {
	ID          uuid.UUID       `json:"id" swaggertype:"string" format:"uuid"`
	BatchID     uuid.UUID       `json:"batch_id" swaggertype:"string" format:"uuid"`
	Recipient   string          `json:"recipient"`
	Channel     models.Channel  `json:"channel"`
	Priority    models.Priority `json:"priority"`
	Status      models.Status   `json:"status"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	CreatedAt   time.Time       `json:"created_at"`
	Delivery    *deliveryView   `json:"delivery,omitempty"`
}

type batchView struct {
	BatchID uuid.UUID          `json:"batch_id" swaggertype:"string" format:"uuid"`
	Total   int                `json:"total"`
	Counts  map[string]int     `json:"counts"`
	Items   []notificationView `json:"items"`
}

type templateCreateRequest struct {
	Name    string         `json:"name"`
	Channel models.Channel `json:"channel"`
	Body    string         `json:"body"`
}

// notifToView projects a notification row into the API response shape, including
// delivery bookkeeping carried on the same row.
func notifToView(n models.Notification) notificationView {
	return notificationView{
		ID:          n.ID,
		BatchID:     n.BatchID,
		Recipient:   n.Recipient,
		Channel:     n.Channel,
		Priority:    n.Priority,
		Status:      n.Status,
		ScheduledAt: n.ScheduledAt,
		CreatedAt:   n.CreatedAt,
		Delivery: &deliveryView{
			Status:            n.Status,
			Attempts:          n.Attempts,
			MaxAttempts:       n.MaxAttempts,
			LastError:         n.LastError,
			ProviderMessageID: n.ProviderMessageID,
			SentAt:            n.SentAt,
		},
	}
}
