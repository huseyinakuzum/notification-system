//go:build e2e

// Package e2e drives the full notification pipeline against a running compose
// stack: api -> postgres -> cdc -> kafka -> delivery -> mock-provider.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

func baseURL() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

type createItem struct {
	Recipient string `json:"recipient"`
	Channel   string `json:"channel"`
	Content   string `json:"content"`
	Priority  string `json:"priority"`
}

type createResponse struct {
	BatchID string   `json:"batch_id"`
	IDs     []string `json:"ids"`
}

type notificationView struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func TestPipelineDeliversNotification(t *testing.T) {
	base := baseURL()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	waitReady(ctx, t, base)

	body, err := json.Marshal([]createItem{{
		Recipient: "e2e@example.com",
		Channel:   "email",
		Content:   fmt.Sprintf("e2e smoke test %d", time.Now().UnixNano()),
		Priority:  "high",
	}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/notifications", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build post: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post notifications: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("post status = %d, want 200/201", resp.StatusCode)
	}

	var created createResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if len(created.IDs) != 1 {
		t.Fatalf("got %d ids, want 1", len(created.IDs))
	}
	id := created.IDs[0]
	t.Logf("created notification %s, polling for delivery", id)

	deadline := time.NewTicker(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("notification %s not delivered before timeout", id)
		case <-deadline.C:
			status := fetchStatus(ctx, t, base, id)
			t.Logf("status = %q", status)
			switch status {
			case "sent":
				return
			case "failed", "cancelled":
				t.Fatalf("notification reached terminal status %q", status)
			}
		}
	}
}

func waitReady(ctx context.Context, t *testing.T, base string) {
	t.Helper()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("api never became ready at %s", base)
		case <-tick.C:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/notifications?limit=1", nil)
			if err != nil {
				continue
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
	}
}

func fetchStatus(ctx context.Context, t *testing.T, base, id string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/notifications/%s", base, id), nil)
	if err != nil {
		t.Fatalf("build get: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want 200", resp.StatusCode)
	}
	var view notificationView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode notification: %v", err)
	}
	return view.Status
}
