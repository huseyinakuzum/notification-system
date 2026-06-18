package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// KafkaDLQ publishes terminally failed notifications to the dead-letter topic
// for out-of-band inspection and replay.
type KafkaDLQ struct {
	writer messageWriter
}

func NewKafkaDLQ(writer messageWriter) *KafkaDLQ {
	return &KafkaDLQ{writer: writer}
}

type dlqRecord struct {
	ID            string `json:"id"`
	Recipient     string `json:"recipient"`
	Channel       string `json:"channel"`
	Content       string `json:"content"`
	Attempts      int    `json:"attempts"`
	CorrelationID string `json:"correlation_id"`
	Reason        string `json:"reason"`
	FailedAt      string `json:"failed_at"`
}

// Produce writes a dead-letter record for n with the given failure reason.
func (d *KafkaDLQ) Produce(ctx context.Context, n models.Notification, reason string) error {
	rec := dlqRecord{
		ID:            n.ID.String(),
		Recipient:     n.Recipient,
		Channel:       string(n.Channel),
		Content:       n.Content,
		Attempts:      n.Attempts,
		CorrelationID: n.CorrelationID,
		Reason:        reason,
		FailedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	value, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}
	if err := d.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(n.ID.String()),
		Value: value,
		Headers: []kafka.Header{
			{Key: "correlation_id", Value: []byte(n.CorrelationID)},
		},
	}); err != nil {
		return fmt.Errorf("write dlq message: %w", err)
	}
	return nil
}
