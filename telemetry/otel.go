package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "github.com/ryanaldo34/tacklr"

// Config configures OTLP tracing. An empty OTLPEndpoint (and no OTEL env endpoint)
// installs a no-op provider so library consumers pay almost nothing until Init.
type Config struct {
	ServiceName    string
	ServiceVersion string
	// OTLPEndpoint is host:port or full URL. Empty uses OTEL_EXPORTER_OTLP_ENDPOINT
	// when set; if still empty, tracing is no-op.
	OTLPEndpoint string
	// Protocol is "grpc" (default) or "http".
	Protocol string
	// Insecure disables TLS for the exporter (local collectors).
	Insecure bool
	// SampleRatio in (0,1]; values <=0 are treated as 1.0 (always sample).
	SampleRatio float64
}

// Init installs a global TracerProvider and text-map propagator.
// Returns a shutdown function that flushes exporters.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	endpoint := strings.TrimSpace(cfg.OTLPEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if endpoint == "" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	serviceName := cfg.ServiceName
	if serviceName == "" {
		if v := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); v != "" {
			serviceName = v
		} else {
			serviceName = "tacklr"
		}
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	protocol := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol == "" {
		protocol = "grpc"
	}

	host := stripScheme(endpoint)
	var exp *otlptrace.Exporter
	switch protocol {
	case "http", "http/protobuf":
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
		if cfg.Insecure || strings.HasPrefix(endpoint, "http://") {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err = otlptracehttp.New(ctx, opts...)
	case "grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
		if cfg.Insecure || strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://") {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err = otlptracegrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("otel exporter: %w", err)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	var sampler sdktrace.Sampler
	if ratio >= 1 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the package tracer (noop-safe when Init was not called with an endpoint).
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// SetTracerProviderForTest installs tp as the global provider (tests).
func SetTracerProviderForTest(tp trace.TracerProvider) {
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	otel.SetTracerProvider(tp)
}

func stripScheme(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	for _, p := range []string{"https://", "http://"} {
		if after, ok := strings.CutPrefix(endpoint, p); ok {
			return after
		}
	}
	return endpoint
}
