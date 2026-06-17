package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://u:p@localhost:5432/n?sslmode=disable")
	t.Setenv("KAFKA_BROKERS", "localhost:9092")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RateLimitPerChannel != 100 {
		t.Fatalf("default rate limit = %d, want 100", cfg.RateLimitPerChannel)
	}
	if cfg.Scheduler.PollInterval != 2*time.Second {
		t.Fatalf("default poll interval = %v, want 2s", cfg.Scheduler.PollInterval)
	}
	if cfg.Scheduler.BatchSize != 500 {
		t.Fatalf("default scheduler batch = %d, want 500", cfg.Scheduler.BatchSize)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	t.Run("missing_db_dsn", func(t *testing.T) {
		t.Setenv("DB_DSN", "")
		t.Setenv("KAFKA_BROKERS", "localhost:9092")
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "DB_DSN") {
			t.Fatalf("expected DB_DSN error, got %v", err)
		}
	})
	t.Run("missing_kafka_brokers", func(t *testing.T) {
		t.Setenv("DB_DSN", "postgres://u:p@localhost:5432/n?sslmode=disable")
		t.Setenv("KAFKA_BROKERS", "")
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
			t.Fatalf("expected KAFKA_BROKERS error, got %v", err)
		}
	})
}
