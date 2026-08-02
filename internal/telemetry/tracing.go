package telemetry

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Configure initializes the global OpenTelemetry tracer provider and propagator.
// When OTEL_EXPORTER_OTLP_ENDPOINT is unset the provider is a no-op and tracing
// remains effectively disabled, preserving existing behavior.
func Configure(ctx context.Context, log *slog.Logger) (trace.TracerProvider, func(context.Context) error, error) {
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint == "" {
		if endpoint = os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); endpoint == "" {
			if log != nil {
				log.Info("tracing disabled", "reason", "OTEL_EXPORTER_OTLP_ENDPOINT not set")
			}
			provider := noop.NewTracerProvider()
			otel.SetTracerProvider(provider)
			otel.SetTextMapPropagator(propagation.TraceContext{})
			return provider, func(context.Context) error { return nil }, nil
		}
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		"",
		attribute.String("service.name", "sorotrail"),
		attribute.String("service.version", buildVersion()),
	))
	if err != nil {
		return nil, nil, err
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))
	if samplerName := os.Getenv("OTEL_TRACES_SAMPLER"); samplerName != "" {
		sampler = samplerFromEnv(samplerName)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider, provider.Shutdown, nil
}

func buildVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi.Main.Version == "" || bi.Main.Version == "(devel)" {
		return "dev"
	}
	return bi.Main.Version
}

func samplerFromEnv(name string) sdktrace.Sampler {
	switch name {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		var ratio float64
		if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
			var err error
			ratio, err = strconv.ParseFloat(v, 64)
			if err != nil {
				ratio = 1.0
			}
		}
		if ratio <= 0 {
			ratio = 1.0
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		var ratio float64
		if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
			var err error
			ratio, err = strconv.ParseFloat(v, 64)
			if err != nil {
				ratio = 1.0
			}
		}
		if ratio <= 0 {
			ratio = 1.0
		}
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))
	}
}
