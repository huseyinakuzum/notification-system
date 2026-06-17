package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/config"
	"github.com/huseyinakuzum/notification-system/internal/obs"
	"github.com/huseyinakuzum/notification-system/internal/repository"
	"github.com/huseyinakuzum/notification-system/internal/scheduler"
)

const serviceName = "scheduler"

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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	sched := scheduler.New(
		repository.NewSchedulerRepository(db),
		scheduler.Config{
			PollInterval: cfg.Scheduler.PollInterval,
			BatchSize:    cfg.Scheduler.BatchSize,
		},
		logger,
	)

	logger.Info("scheduler starting", "poll_interval", cfg.Scheduler.PollInterval)
	if err := sched.Run(ctx); err != nil {
		return fmt.Errorf("scheduler run: %w", err)
	}
	logger.Info("scheduler stopped")
	return nil
}
