package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
	// maxBodyBytes bounds the request body so an oversized payload cannot
	// exhaust memory before the per-item batch limit is reached. MaxBatchSize
	// items of email-sized content fit comfortably below this cap.
	maxBodyBytes = 16 << 20 // 16 MiB
)

// createNotifications accepts a single create item or an array of them, renders
// any referenced templates, validates, and persists the batch idempotently.
//
// @Summary     Create notifications
// @Description Accepts a single create item or a JSON array of them; renders templates, validates, persists idempotently.
// @Tags        notifications
// @Accept      json
// @Produce     json
// @Param       request body createItem true "Notification to create (send a JSON array for batches)"
// @Success     201 {object} createResponse
// @Failure     400 {object} map[string]string
// @Failure     500 {object} map[string]string
// @Router      /notifications [post]
func (s *Server) createNotifications(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read body")
		return
	}
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		writeError(w, http.StatusBadRequest, "empty body")
		return
	}

	var items []createItem
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal(body, &items); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON array")
			return
		}
	case '{':
		var single createItem
		if err := json.Unmarshal(body, &single); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON object")
			return
		}
		items = []createItem{single}
	default:
		writeError(w, http.StatusBadRequest, "body must be a JSON object or array")
		return
	}

	if len(items) == 0 {
		writeError(w, http.StatusBadRequest, "no items to create")
		return
	}
	if len(items) > MaxBatchSize {
		writeError(w, http.StatusBadRequest, "batch exceeds maximum size")
		return
	}

	ctx := r.Context()
	batchID := uuid.New()
	tp := traceParent(ctx)
	rows := make([]models.Notification, len(items))
	for i, it := range items {
		if it.TemplateID != nil {
			tmpl, err := s.tmpl.GetByID(ctx, *it.TemplateID)
			if errors.Is(err, repository.ErrNotFound) {
				writeErrorf(w, http.StatusBadRequest, "item %d: template not found", i)
				return
			}
			if err != nil {
				s.logger.Error("load template", "error", err, "correlation_id", correlationID(ctx))
				writeError(w, http.StatusInternalServerError, "internal error")
				return
			}
			rendered, err := Render(tmpl.Body, it.TemplateVars)
			if err != nil {
				writeErrorf(w, http.StatusBadRequest, "item %d: %v", i, err)
				return
			}
			it.Content = rendered
		}

		if err := validateItem(it); err != nil {
			writeErrorf(w, http.StatusBadRequest, "item %d: %v", i, err)
			return
		}

		correlation := correlationID(ctx)
		if correlation == "" {
			correlation = uuid.New().String()
		}
		priority := it.Priority
		if priority == "" {
			priority = models.PriorityNormal
		}

		row := models.Notification{
			BatchID:        batchID,
			Recipient:      it.Recipient,
			Channel:        it.Channel,
			Content:        it.Content,
			Priority:       priority,
			IdempotencyKey: deriveIdempotencyKey(it.Recipient, it.Channel, it.Content, priority, it.ScheduledAt),
			CorrelationID:  correlation,
			TraceParent:    tp,
			Status:         models.StatusScheduled,
		}
		if it.ScheduledAt != nil {
			row.ScheduledAt = *it.ScheduledAt
		}
		rows[i] = row
	}

	ids, err := s.notif.InsertBatchIdempotent(ctx, rows)
	if err != nil {
		s.logger.Error("insert batch", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, createResponse{BatchID: batchID, IDs: ids})
}

// getNotification returns one notification with its delivery status.
//
// @Summary Get a notification by ID
// @Tags    notifications
// @Produce json
// @Param   id path string true "Notification ID" format(uuid)
// @Success 200 {object} notificationView
// @Failure 400 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Router  /notifications/{id} [get]
func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	ctx := r.Context()
	notif, err := s.notif.GetByID(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		s.logger.Error("get notification", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, notifToView(notif))
}

// getBatch returns the notifications for a batch with per-status counts.
//
// @Summary Get a batch with per-status counts
// @Tags    notifications
// @Produce json
// @Param   batchId path string true "Batch ID" format(uuid)
// @Success 200 {object} batchView
// @Failure 400 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Router  /notifications/batch/{batchId} [get]
func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	batchID, err := uuid.Parse(chi.URLParam(r, "batchId"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid batch id")
		return
	}
	ctx := r.Context()
	notifs, err := s.notif.ListByBatch(ctx, batchID)
	if err != nil {
		s.logger.Error("list batch", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if len(notifs) == 0 {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	counts := make(map[string]int)
	items := make([]notificationView, len(notifs))
	for i, n := range notifs {
		counts[string(n.Status)]++
		items[i] = notifToView(n)
	}
	writeJSON(w, http.StatusOK, batchView{
		BatchID: batchID,
		Total:   len(notifs),
		Counts:  counts,
		Items:   items,
	})
}

// listNotifications returns raw notifications filtered by the query string.
//
// @Summary List notifications
// @Tags    notifications
// @Produce json
// @Param   status query string false "Filter by status" Enums(scheduled,queued,processing,sent,failed,cancelled)
// @Param   channel query string false "Filter by channel" Enums(sms,email,push)
// @Param   from query string false "From timestamp (RFC3339)"
// @Param   to query string false "To timestamp (RFC3339)"
// @Param   limit query int false "Max items (1-200, default 50)"
// @Param   offset query int false "Result offset (default 0)"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]string
// @Router  /notifications [get]
func (s *Server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var filter repository.Filter

	if v := q.Get("status"); v != "" {
		status := models.Status(v)
		if !status.Valid() {
			writeError(w, http.StatusBadRequest, "invalid status")
			return
		}
		filter.Status = &status
	}
	if v := q.Get("channel"); v != "" {
		channel := models.Channel(v)
		if !channel.Valid() {
			writeError(w, http.StatusBadRequest, "invalid channel")
			return
		}
		filter.Channel = &channel
	}
	if v := q.Get("from"); v != "" {
		from, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid from timestamp")
			return
		}
		filter.From = &from
	}
	if v := q.Get("to"); v != "" {
		to, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid to timestamp")
			return
		}
		filter.To = &to
	}

	limit := defaultListLimit
	if v := q.Get("limit"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}
	if limit < 1 {
		limit = 1
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	filter.Limit = limit

	offset := 0
	if v := q.Get("offset"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
		offset = parsed
	}
	filter.Offset = offset

	ctx := r.Context()
	notifs, err := s.notif.List(ctx, filter)
	if err != nil {
		s.logger.Error("list notifications", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	items := make([]notificationView, len(notifs))
	for i, n := range notifs {
		items[i] = notifToView(n)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"limit":  limit,
		"offset": offset,
	})
}

// cancelNotification transitions a scheduled notification to cancelled.
//
// @Summary Cancel a scheduled notification
// @Tags    notifications
// @Produce json
// @Param   id path string true "Notification ID" format(uuid)
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Failure 409 {object} map[string]string
// @Router  /notifications/{id} [delete]
func (s *Server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	ctx := r.Context()
	if _, err := s.notif.GetByID(ctx, id); errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	} else if err != nil {
		s.logger.Error("get notification", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	cancelled, err := s.notif.Cancel(ctx, id)
	if err != nil {
		s.logger.Error("cancel notification", "error", err, "correlation_id", correlationID(ctx))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !cancelled {
		writeError(w, http.StatusConflict, "cannot cancel: not scheduled")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": models.StatusCancelled})
}
