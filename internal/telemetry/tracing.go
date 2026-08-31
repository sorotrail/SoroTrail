// Package telemetry configures OpenTelemetry tracing for SoroTrail. Configure
// returns a tracer provider, a shutdown function, and an error; tracing is
// disabled and the provider is a no-op when no OTLP endpoint is configured.
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

// buildVersion is the service version attached to every exported trace, which
// is how an operator tells which build produced a span.
func buildVersion() string {
	return versionFrom(debug.ReadBuildInfo())
}

// versionFrom is the decision buildVersion makes, split out from the global it
// reads so the branches can be exercised directly. debug.ReadBuildInfo() cannot
// be varied from a test: under `go test` it always reports "(devel)", so the
// two interesting cases -- a real tagged version, and a binary with no build
// info at all -- would otherwise be unreachable.
//
// "" and "(devel)" both mean "not a released build". They are distinct states
// (missing versus explicitly-not-a-release) and are deliberately mapped to the
// same answer: an operator reading a span wants to know it is not looking at a
// release, not which flavour of unreleased it is.
func versionFrom(bi *debug.BuildInfo, ok bool) string {
	if !ok || bi == nil || bi.Main.Version == "" || bi.Main.Version == "(devel)" {
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
