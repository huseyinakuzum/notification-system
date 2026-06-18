package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/models"
)

// Outcome classifies the result of a delivery attempt.
type Outcome int

// Delivery attempt outcomes.
const (
	OutcomeSent  Outcome = iota // provider accepted (HTTP 202)
	OutcomeRetry                // transient failure (429, 5xx, transport)
	OutcomeFatal                // permanent failure (4xx other than 429)
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSent:
		return "sent"
	case OutcomeRetry:
		return "retry"
	case OutcomeFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// classify maps an HTTP status to a delivery outcome. 202 is the provider's
// accept signal; 4xx (except 429) is permanent; everything else is transient.
func classify(status int) Outcome {
	switch {
	case status == http.StatusAccepted:
		return OutcomeSent
	case status == http.StatusTooManyRequests:
		return OutcomeRetry
	case status >= 400 && status < 500:
		return OutcomeFatal
	default:
		return OutcomeRetry
	}
}

// SendResult is the typed outcome of a single delivery attempt.
type SendResult struct {
	Outcome           Outcome
	ProviderMessageID string
	Detail            string // human-readable reason, stored as last_error on failure
}

type webhookPayload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type providerResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// WebhookProvider delivers notifications by POSTing to an HTTP endpoint
// (webhook.site in dev). The URL is the single swap point for a real provider.
type WebhookProvider struct {
	url    string
	client *http.Client
}

// NewWebhookProvider keeps a wide per-host idle pool so the three lanes reuse keep-alives instead of dialing per send.
func NewWebhookProvider(url string, timeout time.Duration) *WebhookProvider {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 100
	tr.MaxIdleConnsPerHost = 100
	tr.MaxConnsPerHost = 100
	return &WebhookProvider{
		url:    url,
		client: &http.Client{Timeout: timeout, Transport: tr},
	}
}

// Send POSTs the notification and classifies the response. Transport errors and
// timeouts are treated as retryable.
func (p *WebhookProvider) Send(ctx context.Context, n models.Notification) SendResult {
	body, err := json.Marshal(webhookPayload{
		To:      n.Recipient,
		Channel: string(n.Channel),
		Content: n.Content,
	})
	if err != nil {
		return SendResult{Outcome: OutcomeFatal, Detail: fmt.Sprintf("marshal payload: %v", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return SendResult{Outcome: OutcomeFatal, Detail: fmt.Sprintf("build request: %v", err)}
	}
	req.Header.Set("Content-Type", "application/json")
	if n.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", n.CorrelationID)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return SendResult{Outcome: OutcomeRetry, Detail: fmt.Sprintf("transport: %v", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	outcome := classify(resp.StatusCode)
	if outcome != OutcomeSent {
		return SendResult{Outcome: outcome, Detail: fmt.Sprintf("provider status %d", resp.StatusCode)}
	}

	var pr providerResponse
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	_ = json.Unmarshal(raw, &pr) // body is informational; absence is not fatal on 202
	return SendResult{Outcome: OutcomeSent, ProviderMessageID: pr.MessageID}
}
