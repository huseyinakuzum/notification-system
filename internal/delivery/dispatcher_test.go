package delivery

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

type fakeStore struct {
	claim      models.Notification
	claimErr   error
	sentID     string
	sentCalls  int
	retryCalls int
	retryNext  time.Time
	retryErr   string
	failCalls  int
	failErr    string
}

func (f *fakeStore) Claim(context.Context, uuid.UUID) (models.Notification, error) {
	return f.claim, f.claimErr
}
func (f *fakeStore) MarkSent(_ context.Context, _ uuid.UUID, providerMessageID string) error {
	f.sentCalls++
	f.sentID = providerMessageID
	return nil
}
func (f *fakeStore) MarkRetry(_ context.Context, _ uuid.UUID, next time.Time, lastErr string) error {
	f.retryCalls++
	f.retryNext = next
	f.retryErr = lastErr
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, _ uuid.UUID, lastErr string) error {
	f.failCalls++
	f.failErr = lastErr
	return nil
}

type fakeSender struct{ result SendResult }

func (f fakeSender) Send(context.Context, models.Notification) SendResult { return f.result }

type fakeDLQ struct {
	calls   int
	reason  string
}

func (f *fakeDLQ) Produce(_ context.Context, _ models.Notification, reason string) error {
	f.calls++
	f.reason = reason
	return nil
}

func newTestDispatcher(store claimStore, sender sender, dlq dlqProducer) *dispatcher {
	return &dispatcher{
		store:   store,
		sender:  sender,
		dlq:     dlq,
		limiter: newChannelLimiter(1000, 1000),
		cfg: dispatcherConfig{
			BackoffBase:   time.Second,
			BackoffMax:    time.Minute,
			BackoffJitter: 0,
		},
		rnd:    func() float64 { return 0.5 },
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func claimedNotification(attempts, max int) models.Notification {
	return models.Notification{
		ID:          uuid.New(),
		Channel:     models.ChannelSMS,
		Status:      models.StatusProcessing,
		Attempts:    attempts,
		MaxAttempts: max,
	}
}

func TestDispatchSuccess(t *testing.T) {
	store := &fakeStore{claim: claimedNotification(1, 3)}
	dlq := &fakeDLQ{}
	d := newTestDispatcher(store, fakeSender{SendResult{Outcome: OutcomeSent, ProviderMessageID: "p-1"}}, dlq)

	if err := d.handle(context.Background(), store.claim.ID); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if store.sentCalls != 1 || store.sentID != "p-1" {
		t.Errorf("MarkSent: calls=%d id=%q", store.sentCalls, store.sentID)
	}
	if dlq.calls != 0 {
		t.Error("DLQ should not be produced on success")
	}
}

func TestDispatchAlreadyHandled(t *testing.T) {
	store := &fakeStore{claimErr: repository.ErrNotFound}
	dlq := &fakeDLQ{}
	d := newTestDispatcher(store, fakeSender{SendResult{Outcome: OutcomeSent}}, dlq)

	if err := d.handle(context.Background(), uuid.New()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if store.sentCalls != 0 || store.failCalls != 0 || store.retryCalls != 0 {
		t.Error("no marks expected when claim finds nothing")
	}
}

func TestDispatchFatal(t *testing.T) {
	store := &fakeStore{claim: claimedNotification(1, 3)}
	dlq := &fakeDLQ{}
	d := newTestDispatcher(store, fakeSender{SendResult{Outcome: OutcomeFatal, Detail: "bad request"}}, dlq)

	if err := d.handle(context.Background(), store.claim.ID); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if store.failCalls != 1 {
		t.Errorf("MarkFailed calls: got %d, want 1", store.failCalls)
	}
	if dlq.calls != 1 {
		t.Errorf("DLQ calls: got %d, want 1", dlq.calls)
	}
}

func TestDispatchRetryUnderCap(t *testing.T) {
	store := &fakeStore{claim: claimedNotification(1, 3)}
	dlq := &fakeDLQ{}
	d := newTestDispatcher(store, fakeSender{SendResult{Outcome: OutcomeRetry, Detail: "503"}}, dlq)

	if err := d.handle(context.Background(), store.claim.ID); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if store.retryCalls != 1 {
		t.Errorf("MarkRetry calls: got %d, want 1", store.retryCalls)
	}
	if !store.retryNext.After(time.Now()) {
		t.Errorf("next_attempt_at not in future: %v", store.retryNext)
	}
	if dlq.calls != 0 || store.failCalls != 0 {
		t.Error("under cap should not fail or DLQ")
	}
}

func TestDispatchRetryExhausted(t *testing.T) {
	store := &fakeStore{claim: claimedNotification(3, 3)} // attempts == max after claim
	dlq := &fakeDLQ{}
	d := newTestDispatcher(store, fakeSender{SendResult{Outcome: OutcomeRetry, Detail: "503"}}, dlq)

	if err := d.handle(context.Background(), store.claim.ID); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if store.failCalls != 1 {
		t.Errorf("MarkFailed calls: got %d, want 1", store.failCalls)
	}
	if dlq.calls != 1 {
		t.Errorf("DLQ calls: got %d, want 1", dlq.calls)
	}
	if store.retryCalls != 0 {
		t.Error("exhausted retry should not reschedule")
	}
}
