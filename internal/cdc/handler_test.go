package cdc

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	pqcdc "github.com/Trendyol/go-pq-cdc-kafka"
)

func testHandler() pqcdc.Handler {
	return NewHandler(slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func TestHandlerDropsDeleteAndSnapshot(t *testing.T) {
	h := testHandler()
	row := map[string]any{"id": "x", "status": "queued"}
	for _, mt := range []pqcdc.MessageType{pqcdc.DeleteMessage, pqcdc.SnapshotMessage} {
		msgs := h(&pqcdc.Message{Type: mt, TableName: "notifications", NewData: row})
		if msgs != nil {
			t.Errorf("type %s: want nil, got %d messages", mt, len(msgs))
		}
	}
}

func TestHandlerAcceptsInsertAndUpdate(t *testing.T) {
	h := testHandler()
	row := map[string]any{"id": "n-1", "priority": "normal", "status": "queued"}
	for _, mt := range []pqcdc.MessageType{pqcdc.InsertMessage, pqcdc.UpdateMessage} {
		msgs := h(&pqcdc.Message{Type: mt, TableName: "notifications", NewData: row})
		if len(msgs) != 1 {
			t.Fatalf("type %s: want 1 message, got %d", mt, len(msgs))
		}
	}
}

func TestHandlerDropsNonQueuedStatus(t *testing.T) {
	h := testHandler()
	for _, status := range []string{"scheduled", "processing", "sent", "failed", "cancelled", ""} {
		row := map[string]any{"id": "n-1", "priority": "normal", "status": status}
		msgs := h(&pqcdc.Message{Type: pqcdc.UpdateMessage, TableName: "notifications", NewData: row})
		if msgs != nil {
			t.Errorf("status %q: want nil, got %d messages", status, len(msgs))
		}
	}
}

func TestHandlerRoutesNotificationsByPriority(t *testing.T) {
	h := testHandler()
	cases := map[string]string{
		"high":   "delivery.high",
		"normal": "delivery.normal",
		"low":    "delivery.low",
		"":       "delivery.normal",
		"bogus":  "delivery.normal",
		"HIGH":   "delivery.high",
		" high ": "delivery.high",
	}
	for prio, wantTopic := range cases {
		row := map[string]any{"id": "n-1", "priority": prio, "status": "queued"}
		msgs := h(&pqcdc.Message{Type: pqcdc.InsertMessage, TableName: "notifications", NewData: row})
		if len(msgs) != 1 {
			t.Fatalf("priority %q: want 1 message, got %d", prio, len(msgs))
		}
		if msgs[0].Topic != wantTopic {
			t.Errorf("priority %q: topic = %q, want %q", prio, msgs[0].Topic, wantTopic)
		}
		if string(msgs[0].Key) != "n-1" {
			t.Errorf("priority %q: key = %q, want n-1", prio, msgs[0].Key)
		}
	}
}

func TestHandlerIgnoresUnknownTable(t *testing.T) {
	h := testHandler()
	msgs := h(&pqcdc.Message{Type: pqcdc.InsertMessage, TableName: "templates", NewData: map[string]any{"id": "t-1"}})
	if msgs != nil {
		t.Errorf("want nil for unknown table, got %d messages", len(msgs))
	}
}

func TestCoerceString(t *testing.T) {
	var uuidArr [16]byte
	for i := range uuidArr {
		uuidArr[i] = byte(i)
	}
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"abc", "abc"},
		{[]byte("def"), "def"},
		{42, "42"},
		{int64(7), "7"},
		{uuidArr, "00010203-0405-0607-0809-0a0b0c0d0e0f"},
	}
	for _, c := range cases {
		if got := coerceString(c.in); got != c.want {
			t.Errorf("coerceString(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestHandlerRendersUUIDColumns(t *testing.T) {
	h := testHandler()
	var id [16]byte
	for i := range id {
		id[i] = byte(i)
	}
	wantID := "00010203-0405-0607-0809-0a0b0c0d0e0f"
	row := map[string]any{"id": id, "priority": "high", "status": "queued"}
	msgs := h(&pqcdc.Message{Type: pqcdc.InsertMessage, TableName: "notifications", NewData: row})
	if len(msgs) != 1 {
		t.Fatalf("want 1 message, got %d", len(msgs))
	}
	if string(msgs[0].Key) != wantID {
		t.Errorf("key = %q, want canonical uuid %q", msgs[0].Key, wantID)
	}
	var decoded map[string]any
	if err := json.Unmarshal(msgs[0].Value, &decoded); err != nil {
		t.Fatalf("value not json: %v", err)
	}
	if decoded["id"] != wantID {
		t.Errorf("value id = %v, want canonical uuid string %q", decoded["id"], wantID)
	}
}

func TestNormalizePriority(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"high", "high"},
		{"normal", "normal"},
		{"low", "low"},
		{"HIGH", "high"},
		{" Low ", "low"},
		{"", "normal"},
		{"garbage", "normal"},
		{nil, "normal"},
		{[]byte("high"), "high"},
	}
	for _, c := range cases {
		if got := normalizePriority(c.in); got != c.want {
			t.Errorf("normalizePriority(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
