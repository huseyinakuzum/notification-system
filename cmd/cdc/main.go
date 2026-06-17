package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huseyinakuzum/notification-system/internal/cdc"
	"github.com/huseyinakuzum/notification-system/internal/config"
	"github.com/huseyinakuzum/notification-system/internal/obs"
)

const serviceName = "cdc"

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

	go func() {
		logger.Info("obs server starting", "addr", cfg.ObsAddr)
		if err := obs.Serve(ctx, cfg.ObsAddr, nil); err != nil {
			logger.Error("obs server", "error", err)
		}
	}()

	logger.Info("cdc starting",
		"slot", cfg.CDCSlotName,
		"publication", cfg.CDCPublicationName,
		"brokers", cfg.Kafka.Brokers)
	err = cdc.Run(ctx, cdc.Config{
		DSN:             cfg.DB.DSN,
		SlotName:        cfg.CDCSlotName,
		PublicationName: cfg.CDCPublicationName,
		Brokers:         cfg.Kafka.Brokers,
	}, cdc.NewHandler(logger))
	if err != nil {
		return fmt.Errorf("cdc run: %w", err)
	}
	logger.Info("cdc stopped")
	return nil
}
