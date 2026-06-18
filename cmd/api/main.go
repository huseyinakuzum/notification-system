// Command api serves the HTTP API for creating and querying notifications and templates.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/api"
	"github.com/huseyinakuzum/notification-system/internal/config"
	"github.com/huseyinakuzum/notification-system/internal/obs"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

const serviceName = "api"

const shutdownTimeout = 10 * time.Second

// @title       Notification System API
// @version     1.0
// @description Event-driven notification system: REST ingest, scheduling, and delivery status.
// @BasePath    /
// @schemes     http

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config load: %w", err)
	}

	shutdownTracer, err := obs.InitTracer(ctx, serviceName, cfg.OTelExporter, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			logger.Error("tracer shutdown", "error", err)
		}
	}()

	db, err := repository.New(ctx, cfg.DB.DSN)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer db.Close()

	go func() {
		logger.Info("obs server starting", "addr", cfg.ObsAddr)
		if err := obs.Serve(ctx, cfg.ObsAddr, db.Ping); err != nil {
			logger.Error("obs server", "error", err)
		}
	}()

	srv := api.New(
		cfg,
		logger,
		repository.NewNotificationRepository(db),
		repository.NewTemplateRepository(db),
	)

	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: srv.Handler(),
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("api starting", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("api stopped")
	return nil
}
