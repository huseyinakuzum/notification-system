//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/config"
	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

func testDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	if dsn := os.Getenv("DB_DSN"); dsn != "" {
		return dsn
	}
	t.Skip("TEST_DB_DSN/DB_DSN not set; skipping integration tests")
	return ""
}

func setupServer(t *testing.T) (*httptest.Server, *repository.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := repository.New(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	_, err = db.Pool.Exec(ctx,
		"TRUNCATE notifications, templates RESTART IDENTITY CASCADE")
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(
		config.Config{},
		logger,
		repository.NewNotificationRepository(db),
		repository.NewTemplateRepository(db),
	)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		db.Close()
	})
	return ts, db
}

func doJSON(t *testing.T, method, url string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		if raw, ok := body.(json.RawMessage); ok {
			reader = bytes.NewReader(raw)
		} else {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			reader = bytes.NewReader(encoded)
		}
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, respBody
}

func TestCreateSingleAndGet(t *testing.T) {
	ts, _ := setupServer(t)

	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", createItem{
		Recipient: "+15551234",
		Channel:   models.ChannelSMS,
		Content:   "hello",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var created createResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if len(created.IDs) != 1 {
		t.Fatalf("ids = %v, want 1", created.IDs)
	}

	resp, body = doJSON(t, http.MethodGet, ts.URL+"/notifications/"+created.IDs[0].String(), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", resp.StatusCode, body)
	}
	var view notificationView
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("unmarshal view: %v", err)
	}
	if view.Status != models.StatusScheduled {
		t.Fatalf("status = %q, want scheduled", view.Status)
	}
	if view.Delivery != nil {
		t.Fatalf("delivery = %+v, want nil", view.Delivery)
	}
}

func TestCreateArrayIdempotent(t *testing.T) {
	ts, _ := setupServer(t)

	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", []createItem{
		{Recipient: "a", Channel: models.ChannelEmail, Content: "x"},
		{Recipient: "b", Channel: models.ChannelEmail, Content: "y"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var first createResponse
	if err := json.Unmarshal(body, &first); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(first.IDs) != 2 {
		t.Fatalf("ids = %v, want 2", first.IDs)
	}

	resp, body = doJSON(t, http.MethodPost, ts.URL+"/notifications", []createItem{
		{Recipient: "b", Channel: models.ChannelEmail, Content: "y"},
		{Recipient: "c", Channel: models.ChannelEmail, Content: "z"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("second create status = %d, body = %s", resp.StatusCode, body)
	}
	var second createResponse
	if err := json.Unmarshal(body, &second); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if second.IDs[0] != first.IDs[1] {
		t.Fatalf("duplicate key id = %s, want existing %s", second.IDs[0], first.IDs[1])
	}
	if second.IDs[1] == first.IDs[1] {
		t.Fatalf("new key reused existing id %s", second.IDs[1])
	}
}

func TestCreateBatchTooLarge(t *testing.T) {
	ts, _ := setupServer(t)
	items := make([]createItem, MaxBatchSize+1)
	for i := range items {
		items[i] = createItem{Channel: models.ChannelSMS, Content: "x"}
	}
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", items)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

func TestCreateBadChannel(t *testing.T) {
	ts, _ := setupServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", createItem{
		Channel: "slack", Content: "x",
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
}

func TestCreateWithTemplate(t *testing.T) {
	ts, db := setupServer(t)

	resp, body := doJSON(t, http.MethodPost, ts.URL+"/templates", templateCreateRequest{
		Name: "welcome", Channel: models.ChannelEmail, Body: "Hi {{name}}!",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("template status = %d, body = %s", resp.StatusCode, body)
	}
	var tmplResp struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(body, &tmplResp); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}

	resp, body = doJSON(t, http.MethodPost, ts.URL+"/notifications", createItem{
		Recipient:    "user@example.com",
		Channel:      models.ChannelEmail,
		TemplateID:   &tmplResp.ID,
		TemplateVars: map[string]string{"name": "Sam"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var created createResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	var content string
	err := db.Pool.QueryRow(context.Background(),
		"SELECT content FROM notifications WHERE id=$1", created.IDs[0]).Scan(&content)
	if err != nil {
		t.Fatalf("select content: %v", err)
	}
	if content != "Hi Sam!" {
		t.Fatalf("content = %q, want %q", content, "Hi Sam!")
	}
}

func TestGetBatch(t *testing.T) {
	ts, _ := setupServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", []createItem{
		{Channel: models.ChannelSMS, Content: "a"},
		{Channel: models.ChannelSMS, Content: "b"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var created createResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	resp, body = doJSON(t, http.MethodGet, ts.URL+"/notifications/batch/"+created.BatchID.String(), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch status = %d, body = %s", resp.StatusCode, body)
	}
	var bv batchView
	if err := json.Unmarshal(body, &bv); err != nil {
		t.Fatalf("unmarshal batch: %v", err)
	}
	if bv.Total != 2 {
		t.Fatalf("total = %d, want 2", bv.Total)
	}
	if bv.Counts["scheduled"] != 2 {
		t.Fatalf("scheduled count = %d, want 2", bv.Counts["scheduled"])
	}
}

func TestListStatusFilter(t *testing.T) {
	ts, _ := setupServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", []createItem{
		{Channel: models.ChannelSMS, Content: "a"},
		{Channel: models.ChannelSMS, Content: "b"},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var created createResponse
	_ = json.Unmarshal(body, &created)

	// Cancel one so statuses differ.
	resp, _ = doJSON(t, http.MethodDelete, ts.URL+"/notifications/"+created.IDs[0].String(), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d", resp.StatusCode)
	}

	resp, body = doJSON(t, http.MethodGet, ts.URL+"/notifications?status=cancelled", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", resp.StatusCode, body)
	}
	var listResp struct {
		Items []notificationView `json:"items"`
	}
	if err := json.Unmarshal(body, &listResp); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listResp.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(listResp.Items))
	}
	if listResp.Items[0].Status != models.StatusCancelled {
		t.Fatalf("status = %q, want cancelled", listResp.Items[0].Status)
	}
}

func TestCancelLifecycle(t *testing.T) {
	ts, _ := setupServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/notifications", createItem{
		Channel: models.ChannelSMS, Content: "x",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}
	var created createResponse
	_ = json.Unmarshal(body, &created)
	id := created.IDs[0].String()

	resp, body = doJSON(t, http.MethodDelete, ts.URL+"/notifications/"+id, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first cancel status = %d, body = %s", resp.StatusCode, body)
	}

	resp, _ = doJSON(t, http.MethodDelete, ts.URL+"/notifications/"+id, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second cancel status = %d, want 409", resp.StatusCode)
	}

	resp, _ = doJSON(t, http.MethodDelete, ts.URL+"/notifications/"+uuid.NewString(), nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("random cancel status = %d, want 404", resp.StatusCode)
	}
}

func TestTemplateLifecycle(t *testing.T) {
	ts, _ := setupServer(t)
	resp, body := doJSON(t, http.MethodPost, ts.URL+"/templates", templateCreateRequest{
		Name: "alert", Channel: models.ChannelPush, Body: "Alert: {{msg}}",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.StatusCode, body)
	}

	resp, body = doJSON(t, http.MethodGet, ts.URL+"/templates/alert", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", resp.StatusCode, body)
	}
	var tmpl models.Template
	if err := json.Unmarshal(body, &tmpl); err != nil {
		t.Fatalf("unmarshal template: %v", err)
	}
	if tmpl.Name != "alert" {
		t.Fatalf("name = %q, want alert", tmpl.Name)
	}

	resp, _ = doJSON(t, http.MethodGet, ts.URL+"/templates/missing", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d, want 404", resp.StatusCode)
	}
}
