// Package obs wires the cross-cutting observability concerns shared by every
// service: OpenTelemetry tracing, Prometheus metrics, and a small HTTP server
// that exposes health and /metrics. Each binary calls InitTracer once at start
// and Serve in a goroutine.
package obs

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// noopShutdown is returned when tracing is disabled so callers can always defer
// the result without a nil check.
func noopShutdown(context.Context) error { return nil }

// InitTracer configures the global tracer provider and W3C propagator for
// serviceName. exporter selects the span sink: "otlp" ships to endpoint over
// gRPC (the collector), "stdout" prints spans (handy for local runs), and any
// other value ("none", "") leaves the default no-op provider in place. The
// returned func flushes and stops the exporter; callers defer it.
func InitTracer(ctx context.Context, serviceName, exporter, endpoint string) (func(context.Context) error, error) {
	// Propagation is set even when tracing is off so an upstream traceparent is
	// still carried through (cheap, and avoids surprises if a peer has tracing on).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var exp sdktrace.SpanExporter
	var err error
	switch exporter {
	case "otlp":
		exp, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		)
	case "stdout":
		exp, err = stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
	default:
		return noopShutdown, nil
	}
	if err != nil {
		return noopShutdown, fmt.Errorf("init trace exporter %q: %w", exporter, err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", serviceName),
	))
	if err != nil {
		return noopShutdown, fmt.Errorf("build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
