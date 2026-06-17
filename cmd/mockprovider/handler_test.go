package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAcceptHandlerReturns202WithMessageID(t *testing.T) {
	h := acceptHandler(slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"to":"+1","channel":"sms","content":"hi"}`))
	req.Header.Set("X-Correlation-ID", "corr-1")
	rec := httptest.NewRecorder()

	h(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", rec.Code)
	}
	var resp acceptResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.MessageID == "" {
		t.Error("messageId is empty")
	}
	if resp.Status != "accepted" {
		t.Errorf("status field: got %q, want accepted", resp.Status)
	}
}
