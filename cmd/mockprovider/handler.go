package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// acceptResponse mirrors the body the webhook delivery client parses on 202.
type acceptResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// acceptHandler simulates a delivery provider: it accepts any POST with HTTP 202
// and returns a generated provider message id, giving the stack a deterministic
// happy path without an external dependency.
func acceptHandler(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info("provider accept",
			"method", r.Method,
			"path", r.URL.Path,
			"correlation_id", r.Header.Get("X-Correlation-ID"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		//nolint:errcheck // response already committed, nothing actionable
		json.NewEncoder(w).Encode(acceptResponse{
			MessageID: uuid.NewString(),
			Status:    "accepted",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
}
