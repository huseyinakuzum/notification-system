package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/huseyinakuzum/notification-system/internal/models"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

// createTemplate persists a new template.
//
// @Summary Create a template
// @Tags    templates
// @Accept  json
// @Produce json
// @Param   request body templateCreateRequest true "Template to create"
// @Success 201 {object} map[string]interface{}
// @Failure 400 {object} map[string]string
// @Failure 409 {object} map[string]string
// @Router  /templates [post]
func (s *Server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req templateCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Body == "" {
		writeError(w, http.StatusBadRequest, "body is required")
		return
	}
	if !req.Channel.Valid() {
		writeError(w, http.StatusBadRequest, "invalid channel")
		return
	}

	tmpl := models.Template{Name: req.Name, Channel: req.Channel, Body: req.Body}
	if err := s.tmpl.Create(r.Context(), &tmpl); err != nil {
		if errors.Is(err, repository.ErrConflict) {
			writeError(w, http.StatusConflict, "template name already exists")
			return
		}
		s.logger.Error("create template", "error", err, "correlation_id", correlationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": tmpl.ID, "name": tmpl.Name})
}

// getTemplate returns a template by name.
//
// @Summary Get a template by name
// @Tags    templates
// @Produce json
// @Param   name path string true "Template name"
// @Success 200 {object} map[string]interface{}
// @Failure 404 {object} map[string]string
// @Router  /templates/{name} [get]
func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	tmpl, err := s.tmpl.GetByName(r.Context(), name)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		s.logger.Error("get template", "error", err, "correlation_id", correlationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, tmpl)
}
