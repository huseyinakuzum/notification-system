package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		want   Outcome
	}{
		{http.StatusAccepted, OutcomeSent},     // 202
		{http.StatusTooManyRequests, OutcomeRetry}, // 429
		{http.StatusBadRequest, OutcomeFatal},   // 400
		{http.StatusNotFound, OutcomeFatal},     // 404
		{http.StatusInternalServerError, OutcomeRetry}, // 500
		{http.StatusBadGateway, OutcomeRetry},   // 502
		{http.StatusOK, OutcomeRetry},           // unexpected 2xx that isn't 202
	}
	for _, c := range cases {
		if got := classify(c.status); got != c.want {
			t.Errorf("status %d: got %v, want %v", c.status, got, c.want)
		}
	}
}

func testNotification() models.Notification {
	return models.Notification{
		Recipient: "+15551234567",
		Channel:   models.ChannelSMS,
		Content:   "hello",
	}
}

func TestWebhookSendSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"abc-123","status":"queued","timestamp":"2026-06-17T00:00:00Z"}`))
	}))
	defer srv.Close()

	p := NewWebhookProvider(srv.URL, time.Second)
	res := p.Send(context.Background(), testNotification())
	if res.Outcome != OutcomeSent {
		t.Fatalf("outcome: got %v, want sent", res.Outcome)
	}
	if res.ProviderMessageID != "abc-123" {
		t.Errorf("messageId: got %q, want abc-123", res.ProviderMessageID)
	}
}

func TestWebhookSendFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	p := NewWebhookProvider(srv.URL, time.Second)
	res := p.Send(context.Background(), testNotification())
	if res.Outcome != OutcomeFatal {
		t.Errorf("outcome: got %v, want fatal", res.Outcome)
	}
}

func TestWebhookSendRetryOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	p := NewWebhookProvider(srv.URL, time.Second)
	res := p.Send(context.Background(), testNotification())
	if res.Outcome != OutcomeRetry {
		t.Errorf("outcome: got %v, want retry", res.Outcome)
	}
}

func TestWebhookSendTransportErrorRetries(t *testing.T) {
	// unroutable URL → transport error → retryable
	p := NewWebhookProvider("http://127.0.0.1:1/none", 200*time.Millisecond)
	res := p.Send(context.Background(), testNotification())
	if res.Outcome != OutcomeRetry {
		t.Errorf("outcome: got %v, want retry", res.Outcome)
	}
}
