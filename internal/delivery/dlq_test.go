package delivery

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

type fakeWriter struct{ msgs []kafka.Message }

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func TestDLQProduce(t *testing.T) {
	w := &fakeWriter{}
	dlq := NewKafkaDLQ(w)
	n := models.Notification{
		ID:            uuid.New(),
		Recipient:     "+15551234567",
		Channel:       models.ChannelSMS,
		Content:       "hi",
		Attempts:      3,
		CorrelationID: "corr-9",
	}

	if err := dlq.Produce(context.Background(), n, "exhausted"); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(w.msgs) != 1 {
		t.Fatalf("messages: got %d, want 1", len(w.msgs))
	}
	msg := w.msgs[0]
	if string(msg.Key) != n.ID.String() {
		t.Errorf("key: got %q, want %q", msg.Key, n.ID)
	}
	var rec dlqRecord
	if err := json.Unmarshal(msg.Value, &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec.Reason != "exhausted" || rec.Attempts != 3 {
		t.Errorf("record: %+v", rec)
	}
}
