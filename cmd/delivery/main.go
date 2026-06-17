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
	"github.com/huseyinakuzum/notification-system/internal/delivery"
	"github.com/huseyinakuzum/notification-system/internal/kafka"
	"github.com/huseyinakuzum/notification-system/internal/obs"
	"github.com/huseyinakuzum/notification-system/internal/repository"
)

const (
	dlqTopic    = "delivery.dlq"
	serviceName = "delivery"
)

// priorityTopics maps each delivery lane (high, normal, low) to its Kafka topic
// and consumer group. Order must match delivery lane indices.
var priorityTopics = [3]struct{ topic, group string }{
	{"delivery.high", "delivery-high"},
	{"delivery.normal", "delivery-normal"},
	{"delivery.low", "delivery-low"},
}

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

	var readers [3]delivery.Reader
	for i, pt := range priorityTopics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Kafka.Brokers,
			Topic:   pt.topic,
			GroupID: pt.group,
		})
		defer func() { _ = r.Close() }()
		readers[i] = r
	}

	dlqWriter := kafka.NewWriter(cfg.Kafka.Brokers, dlqTopic)
	defer func() { _ = dlqWriter.Close() }()

	provider := delivery.NewWebhookProvider(
		cfg.Delivery.ProviderBaseURL+cfg.ProviderUUID,
		cfg.Delivery.ProviderTimeout,
	)

	worker := delivery.NewWorker(
		readers,
		repository.NewNotificationRepository(db),
		provider,
		delivery.NewKafkaDLQ(dlqWriter),
		delivery.Config{
			AgingThreshold: cfg.Delivery.AgingThreshold,
			RescanInterval: cfg.Delivery.RescanInterval,
			ReapTimeout:    cfg.ProcessingTimeout,
			ClaimBatch:     cfg.Delivery.ClaimBatch,
			RatePerChannel: cfg.RateLimitPerChannel,
			RateBurst:      cfg.Delivery.RateBurst,
			BackoffBase:    cfg.Backoff.Base,
			BackoffMax:     cfg.Backoff.Max,
			BackoffJitter:  cfg.Backoff.Jitter,
		},
		logger,
	)

	logger.Info("delivery starting",
		"provider", cfg.Delivery.ProviderBaseURL,
		"rate_per_channel", cfg.RateLimitPerChannel,
		"rescan_interval", cfg.Delivery.RescanInterval)
	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("delivery run: %w", err)
	}
	logger.Info("delivery stopped")
	return nil
}
