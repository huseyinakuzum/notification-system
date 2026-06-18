package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "nsys"

// Metrics for the API service.
var (
	// HTTPRequests counts handled requests by method, route template, and status.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "http_requests_total",
		Help:      "Total HTTP requests handled, by method, route, and status.",
	}, []string{"method", "route", "status"})

	// HTTPDuration observes request latency by method and route template.
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
	// SchedulerFlipped counts rows transitioned scheduled->queued.
	SchedulerFlipped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "scheduler",
		Name:      "flipped_total",
		Help:      "Total notifications flipped from scheduled to queued.",
	})

	// SchedulerPollErrors counts failed poll cycles.
	SchedulerPollErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "scheduler",
		Name:      "poll_errors_total",
		Help:      "Total scheduler poll cycles that returned an error.",
	})
)

// Metrics for the delivery service.
var (
	// DeliveryAttempts counts delivery results by outcome, channel, and priority.
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "attempts_total",
		Help:      "Total delivery attempts, by outcome (sent/retry/fatal), channel, and priority.",
	}, []string{"outcome", "channel", "priority"})

	// DeliveryDuration observes send latency by channel.
	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "duration_seconds",
		Help:      "Delivery send latency in seconds, by channel.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"channel"})

	// DLQProduced counts notifications routed to the dead-letter queue.
	DLQProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "dlq_produced_total",
		Help:      "Total notifications produced to the DLQ, by channel.",
	}, []string{"channel"})
)

// Metrics for the CDC service.
var (
	// CDCEvents counts replication events handled by operation.
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
	// KafkaReaderLag reports the current consumer lag (messages behind the head)
	// per topic.
	KafkaReaderLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "lag",
		Help:      "Current consumer-group lag in messages, by topic.",
	}, []string{"topic"})

	// KafkaReaderOffset reports the last committed offset per topic.
	KafkaReaderOffset = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "offset",
		Help:      "Last committed offset, by topic.",
	}, []string{"topic"})

	// KafkaReaderQueueLength reports the in-memory fetch queue depth per topic.
	KafkaReaderQueueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "queue_length",
		Help:      "Current in-memory fetch queue length, by topic.",
	}, []string{"topic"})

	// KafkaReaderQueueCapacity reports the in-memory fetch queue capacity per topic.
	KafkaReaderQueueCapacity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "queue_capacity",
		Help:      "In-memory fetch queue capacity, by topic.",
	}, []string{"topic"})

	// KafkaReaderMessages counts messages read per topic.
	KafkaReaderMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "messages_total",
		Help:      "Total messages read by the consumer, by topic.",
	}, []string{"topic"})

	// KafkaReaderFetches counts fetch requests issued per topic.
	KafkaReaderFetches = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "fetches_total",
		Help:      "Total fetch requests issued, by topic.",
	}, []string{"topic"})

	// KafkaReaderErrors counts reader errors per topic.
	KafkaReaderErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "errors_total",
		Help:      "Total reader errors, by topic.",
	}, []string{"topic"})

	// KafkaReaderRebalances counts consumer-group rebalances per topic.
	KafkaReaderRebalances = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "rebalances_total",
		Help:      "Total consumer-group rebalances, by topic.",
	}, []string{"topic"})

	// KafkaReaderTimeouts counts fetch timeouts per topic.
	KafkaReaderTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_reader",
		Name:      "timeouts_total",
		Help:      "Total fetch timeouts, by topic.",
	}, []string{"topic"})
)
