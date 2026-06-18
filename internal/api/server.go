package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	_ "github.com/huseyinakuzum/notification-system/internal/api/docs" // swagger spec registration
	"github.com/huseyinakuzum/notification-system/internal/config"
)

// Server holds the API dependencies and the wired chi router.
type Server struct {
	cfg    config.Config
	logger *slog.Logger
	notif  NotificationStore
	tmpl   TemplateStore
	router chi.Router
}

func New(
	cfg config.Config,
	logger *slog.Logger,
	notif NotificationStore,
	tmpl TemplateStore,
) *Server {
	s := &Server{
		cfg:    cfg,
		logger: logger,
		notif:  notif,
		tmpl:   tmpl,
	}

	r := chi.NewRouter()
	r.Use(requestID)
	r.Use(recoverer(logger))
	r.Use(accessLog(logger))
	r.Use(metrics)

	r.Post("/notifications", s.createNotifications)
	r.Get("/notifications", s.listNotifications)
	r.Get("/notifications/{id}", s.getNotification)
	r.Get("/notifications/batch/{batchId}", s.getBatch)
	r.Delete("/notifications/{id}", s.cancelNotification)
	r.Post("/templates", s.createTemplate)
	r.Get("/templates/{name}", s.getTemplate)

	r.Get("/swagger/*", httpSwagger.Handler())
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusFound)
	})

	s.router = r
	return s
}

// Handler wraps the router with otelhttp so every request becomes a server span.
func (s *Server) Handler() http.Handler {
	return otelhttp.NewHandler(s.router, "api")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// Status and headers already committed; a failed encode just truncates the body.
	//nolint:errcheck // response already committed, nothing actionable
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeErrorf(w http.ResponseWriter, status int, format string, args ...any) {
	writeError(w, status, fmt.Sprintf(format, args...))
}
