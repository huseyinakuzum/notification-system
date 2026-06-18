package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "nsys"

// Metrics for the API service.
var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "http_requests_total",
		Help:      "Total HTTP requests handled, by method, route, and status.",
	}, []string{"method", "route", "status"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request latency in seconds, by method and route.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "route"})
)

// Metrics for the scheduler service.
var (
	SchedulerFlipped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "scheduler",
		Name:      "flipped_total",
		Help:      "Total notifications flipped from scheduled to queued.",
	})

	SchedulerPollErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "scheduler",
		Name:      "poll_errors_total",
		Help:      "Total scheduler poll cycles that returned an error.",
	})
)

// Metrics for the delivery service.
var (
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "attempts_total",
		Help:      "Total delivery attempts, by outcome (sent/retry/fatal), channel, and priority.",
	}, []string{"outcome", "channel", "priority"})

	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "duration_seconds",
		Help:      "Delivery send latency in seconds, by channel.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"channel"})

	DLQProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "dlq_produced_total",
		Help:      "Total notifications produced to the DLQ, by channel.",
	}, []string{"channel"})
)

// Metrics for the CDC service.
var (
	CDCEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "cdc",
		Name:      "events_total",
		Help:      "Total CDC events handled, by operation.",
	}, []string{"op"})
)

// Metrics for Kafka consumer-group readers, scraped from kafka-go reader stats
// by topic (one per delivery priority lane).
var (
	KafkaReaderLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "lag",
		Help:      "Current consumer-group lag in messages, by topic.",
	}, []string{"topic"})

	KafkaReaderOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "offset",
		Help:      "Last committed offset, by topic.",
	}, []string{"topic"})

	KafkaReaderQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "queue_length",
		Help:      "Current in-memory fetch queue length, by topic.",
	}, []string{"topic"})

	KafkaReaderQueueCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "queue_capacity",
		Help:      "In-memory fetch queue capacity, by topic.",
	}, []string{"topic"})

	KafkaReaderMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "messages_total",
		Help:      "Total messages read by the consumer, by topic.",
	}, []string{"topic"})

	KafkaReaderFetches = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "fetches_total",
		Help:      "Total fetch requests issued, by topic.",
	}, []string{"topic"})

	KafkaReaderErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "errors_total",
		Help:      "Total reader errors, by topic.",
	}, []string{"topic"})

	KafkaReaderRebalances = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "rebalances_total",
		Help:      "Total consumer-group rebalances, by topic.",
	}, []string{"topic"})

	KafkaReaderTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "timeouts_total",
		Help:      "Total fetch timeouts, by topic.",
	}, []string{"topic"})
)
