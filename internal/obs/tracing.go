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

// noopShutdown lets callers defer the result without a nil check when tracing is off.
func noopShutdown(context.Context) error { return nil }

// InitTracer sets the global tracer provider and W3C propagator. exporter selects
// the sink: "otlp" (gRPC to endpoint), "stdout", or anything else for no-op. The
// returned func flushes and stops the exporter.
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
