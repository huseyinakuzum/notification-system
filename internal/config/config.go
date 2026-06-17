// Package config loads typed configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// DB holds database connection settings.
type DB struct {
	DSN string `env:"DB_DSN"`
}

// Kafka holds broker connection settings.
type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:","`
}

// Scheduler holds the due-engine poll cadence and batch sizing.
type Scheduler struct {
	PollInterval time.Duration `env:"SCHEDULER_POLL_INTERVAL" envDefault:"2s"`
	BatchSize    int           `env:"SCHEDULER_BATCH_SIZE" envDefault:"500"`
}

// Backoff holds exponential retry-delay parameters.
type Backoff struct {
	Base   time.Duration `env:"BACKOFF_BASE" envDefault:"1s"`
	Max    time.Duration `env:"BACKOFF_MAX" envDefault:"5m"`
	Jitter float64       `env:"BACKOFF_JITTER" envDefault:"0.2"`
}

// Delivery holds delivery-worker tuning: rescan cadence, claim sizing, rate
// limits, and provider endpoint settings.
type Delivery struct {
	RescanInterval  time.Duration `env:"DELIVERY_RESCAN_INTERVAL" envDefault:"5s"`
	ClaimBatch      int           `env:"DELIVERY_CLAIM_BATCH" envDefault:"500"`
	AgingThreshold  int           `env:"DELIVERY_AGING_THRESHOLD" envDefault:"10"`
	RateBurst       int           `env:"DELIVERY_RATE_BURST" envDefault:"100"`
	ProviderTimeout time.Duration `env:"DELIVERY_PROVIDER_TIMEOUT" envDefault:"5s"`
	ProviderBaseURL string        `env:"PROVIDER_BASE_URL" envDefault:"https://webhook.site/"`
}

// Config is the top-level service configuration assembled from the environment.
type Config struct {
	DB        DB
	Kafka     Kafka
	Scheduler Scheduler
	Backoff   Backoff
	Delivery  Delivery

	RateLimitPerChannel int           `env:"RATE_LIMIT_PER_CHANNEL" envDefault:"100"`
	ProcessingTimeout   time.Duration `env:"PROCESSING_TIMEOUT" envDefault:"60s"`
	ProviderUUID        string        `env:"PROVIDER_UUID" envDefault:"00000000-0000-0000-0000-000000000000"`
	OTelExporter        string        `env:"OTEL_EXPORTER" envDefault:"stdout"`
	OTelEndpoint        string        `env:"OTEL_ENDPOINT" envDefault:"otel-collector:4317"`
	HTTPAddr            string        `env:"HTTP_ADDR" envDefault:":8080"`
	ObsAddr             string        `env:"OBS_ADDR" envDefault:":9090"`

	MaxAttemptsSMS   int `env:"MAX_ATTEMPTS_SMS" envDefault:"5"`
	MaxAttemptsEmail int `env:"MAX_ATTEMPTS_EMAIL" envDefault:"5"`
	MaxAttemptsPush  int `env:"MAX_ATTEMPTS_PUSH" envDefault:"3"`

	CDCSlotName        string `env:"CDC_SLOT_NAME" envDefault:"nsys"`
	CDCPublicationName string `env:"CDC_PUBLICATION_NAME" envDefault:"nsys_pub"`
}

// Load reads configuration from the environment and validates required fields.
func Load() (Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse env: %w", err)
	}
	if cfg.DB.DSN == "" {
		return Config{}, errors.New("config: DB_DSN is required")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return Config{}, errors.New("config: KAFKA_BROKERS is required")
	}
	return cfg, nil
}
