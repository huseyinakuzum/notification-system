package cdc

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	pqcdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/google/uuid"
	gokafka "github.com/segmentio/kafka-go"

	"github.com/huseyinakuzum/notification-system/internal/obs"
)

const (
	schemaPublic        = "public"
	tableNotifications  = "notifications"
	statusQueued        = "queued"
	topicDeliveryPrefix = "delivery."
)

// NewHandler routes queued notifications to a per-priority delivery topic. The
// row-filtered publication (status='queued') turns the entry into an INSERT/UPDATE
// and the exit into a DELETE, so only INSERT/UPDATE are accepted and status is re-checked.
func NewHandler(logger *slog.Logger) pqcdc.Handler {
	return func(event *pqcdc.Message) []gokafka.Message {
		if !event.Type.IsInsert() && !event.Type.IsUpdate() {
			return nil
		}
		if event.TableName != tableNotifications {
			return nil
		}
		row := normalizeRow(event.NewData)
		if coerceString(row["status"]) != statusQueued {
			return nil
		}
		value, err := json.Marshal(row)
		if err != nil {
			logger.Error("cdc marshal row", "table", event.TableName, "error", err)
			return nil
		}
		op := "update"
		if event.Type.IsInsert() {
			op = "insert"
		}
		obs.CDCEvents.WithLabelValues(op).Inc()
		return []gokafka.Message{{
			Topic:   topicDeliveryPrefix + normalizePriority(row["priority"]),
			Key:     []byte(coerceString(row["id"])),
			Value:   value,
			Headers: traceHeaders(row),
		}}
	}
}

func normalizePriority(v any) string {
	switch strings.ToLower(strings.TrimSpace(coerceString(v))) {
	case "high":
		return "high"
	case "low":
		return "low"
	default:
		return "normal"
	}
}

// normalizeRow converts pgtype's [16]byte uuids to canonical strings so they
// don't marshal as numeric arrays downstream.
func normalizeRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		if b, ok := v.([16]byte); ok {
			out[k] = uuid.UUID(b).String()
			continue
		}
		out[k] = v
	}
	return out
}

func coerceString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case [16]byte:
		return uuid.UUID(t).String()
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func traceHeaders(row map[string]any) []gokafka.Header {
	var headers []gokafka.Header
	if corr := coerceString(row["correlation_id"]); corr != "" {
		headers = append(headers, gokafka.Header{Key: "correlation_id", Value: []byte(corr)})
	}
	if tp := coerceString(row["traceparent"]); tp != "" {
		headers = append(headers, gokafka.Header{Key: "traceparent", Value: []byte(tp)})
	}
	return headers
}
