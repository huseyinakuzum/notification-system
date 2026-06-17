// Package kafka provides thin segmentio/kafka-go wrappers: a consumer-group
// reader configured for manual offset commits.
package kafka

import (
	"github.com/segmentio/kafka-go"
)

const (
	readerMinBytes = 1
	readerMaxBytes = 10 << 20 // 10 MiB
)

// ReaderConfig holds the inputs for a consumer-group reader.
type ReaderConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewReader builds a consumer-group reader with auto-commit disabled, so the
// caller controls when offsets advance (commit only after the DB write that
// consumed the message has committed).
func NewReader(cfg ReaderConfig) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       readerMinBytes,
		MaxBytes:       readerMaxBytes,
		CommitInterval: 0, // manual commit via CommitMessages
	})
}

// NewWriter builds a hash-balanced writer for producing to a single topic
// (the delivery DLQ). Synchronous writes give the caller per-message errors.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
}
