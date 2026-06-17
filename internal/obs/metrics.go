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
	// DeliveryAttempts counts delivery results by outcome and channel.
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "delivery",
		Name:      "attempts_total",
		Help:      "Total delivery attempts, by outcome (sent/retry/fatal) and channel.",
	}, []string{"outcome", "channel"})

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
