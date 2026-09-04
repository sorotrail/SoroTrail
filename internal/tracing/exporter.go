package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// newExporter returns an OTLP HTTP exporter when OTEL_EXPORTER_OTLP_ENDPOINT
// is set, otherwise a no-op exporter so Start/End are free calls.
func newExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return &noopExporter{}, nil
	}
	return otlptracehttp.New(ctx)
}

// noopExporter satisfies sdktrace.SpanExporter with no work.
type noopExporter struct{}

func (noopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}

func (noopExporter) Shutdown(context.Context) error {
	return nil
}
