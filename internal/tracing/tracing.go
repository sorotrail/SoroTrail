// Package tracing provides a thin wrapper around the OpenTelemetry SDK for
// configuring the global TracerProvider. When no exporter is configured
// (no OTEL_EXPORTER_* env vars), everything is a no-op — zero overhead.
//
// Usage in cmd/sorotrail/main.go:
//
//	shutdown := tracing.Init(ctx)
//	defer shutdown(context.Background())
//
// Each worker package then obtains a tracer via:
//
//	var tracer = otel.Tracer("sorotrail/<component>")
package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init installs a TracerProvider backed by the OTLP exporter configured
// through the standard OTEL_EXPORTER_OTLP_ENDPOINT env vars. When those
// vars are unset, a no-op provider is used (the OTel default).
//
// The returned shutdown function must be called with a deadline context
// before the process exits to flush any pending spans.
func Init(ctx context.Context, log *slog.Logger) (shutdown func(context.Context) error) {
	tp, err := newTracerProvider(ctx)
	if err != nil {
		log.Warn("tracing init failed; continuing without tracing", "error", err)
		return func(context.Context) error { return nil }
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown
}

// newTracerProvider builds a TracerProvider from environment.  If no
// exporter endpoint is configured it returns a simple batch span processor
// wrapping a no-op exporter, which means Start/End are free calls.
func newTracerProvider(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exp, err := newExporter(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	return tp, nil
}
