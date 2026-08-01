package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// InstrumentationName is the OpenTelemetry instrumentation library name for
// Tacklr spans and meters. Hosts that build Tracer/Meter from their own providers
// should use this name (or call TracerFromProvider / MeterFromProvider).
const InstrumentationName = "github.com/ryanaldo34/tacklr"

// tracerContextKey carries an optional per-request Tracer on context so child
// spans (harness tools, handoff) use the same provider as the registry turn root.
type tracerContextKey struct{}

// ContextWithTracer returns a child context that causes TracerFromContext to
// prefer t over the global provider. A nil t is ignored.
func ContextWithTracer(ctx context.Context, t trace.Tracer) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, tracerContextKey{}, t)
}

// TracerFromContext returns the tracer attached with ContextWithTracer, or the
// global package tracer when none is set.
func TracerFromContext(ctx context.Context) trace.Tracer {
	if ctx != nil {
		if t, ok := ctx.Value(tracerContextKey{}).(trace.Tracer); ok && t != nil {
			return t
		}
	}
	return Tracer()
}

// TracerFromProvider returns a Tacklr-scoped Tracer from tp.
// If tp is nil, returns the global Tracer().
func TracerFromProvider(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return Tracer()
	}
	return tp.Tracer(InstrumentationName)
}

// Config configures OTLP traces and metrics for a simple host process.
// An empty OTLPEndpoint (and no OTEL env endpoint) installs no-op providers.
type Config struct {
	ServiceName    string
	ServiceVersion string
	// OTLPEndpoint is host:port or full URL. Empty uses OTEL_EXPORTER_OTLP_ENDPOINT
	// when set; if still empty, tracing and metrics are no-op.
	OTLPEndpoint string
	// Protocol is "grpc" (default) or "http".
	Protocol string
	// Insecure disables TLS for the exporters (local collectors / Alloy).
	Insecure bool
	// SampleRatio in (0,1]; values <=0 are treated as 1.0 (always sample traces).
	SampleRatio float64
	// DisableMetrics skips MeterProvider setup (traces only). Default false:
	// the same OTLP endpoint receives metrics for LGTM (Mimir/Prometheus path).
	DisableMetrics bool
}

// Init installs global TracerProvider and MeterProvider (unless DisableMetrics)
// plus a W3C text-map propagator. One OTLP endpoint serves both signals so hosts
// can point Alloy/Collector at a single address for Tempo + Mimir.
// Returns a shutdown that flushes exporters.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	endpoint := strings.TrimSpace(cfg.OTLPEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if endpoint == "" {
		SetTracerProvider(nil)
		SetMeterProvider(nil)
		return func(context.Context) error { return nil }, nil
	}

	res, err := DefaultResource(cfg.ServiceName, cfg.ServiceVersion)
	if err != nil {
		return nil, err
	}

	protocol := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol == "" {
		protocol = "grpc"
	}
	insecure := cfg.Insecure || strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://")
	host := stripScheme(endpoint)

	tp, err := newOTLPTracerProvider(ctx, host, protocol, insecure, cfg.SampleRatio, res)
	if err != nil {
		return nil, err
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var mp *sdkmetric.MeterProvider
	if !cfg.DisableMetrics {
		mp, err = newOTLPMeterProvider(ctx, host, protocol, insecure, res)
		if err != nil {
			_ = tp.Shutdown(ctx)
			return nil, err
		}
		SetMeterProvider(mp)
	}

	return func(ctx context.Context) error {
		var first error
		if mp != nil {
			if err := mp.Shutdown(ctx); err != nil && first == nil {
				first = err
			}
		}
		if err := tp.Shutdown(ctx); err != nil && first == nil {
			first = err
		}
		return first
	}, nil
}

func newOTLPTracerProvider(ctx context.Context, host, protocol string, insecure bool, sampleRatio float64, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	var exp *otlptrace.Exporter
	var err error
	switch protocol {
	case "http", "http/protobuf":
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err = otlptracehttp.New(ctx, opts...)
	case "grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err = otlptracegrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("otel trace exporter: %w", err)
	}

	ratio := sampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	var sampler sdktrace.Sampler
	if ratio >= 1 {
		sampler = sdktrace.AlwaysSample()
	} else {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	), nil
}

func newOTLPMeterProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var reader sdkmetric.Reader
	switch protocol {
	case "http", "http/protobuf":
		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
	case "grpc":
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	), nil
}

// Tracer returns the package tracer from the global TracerProvider
// (noop-safe when Init was not called with an endpoint).
func Tracer() trace.Tracer {
	return otel.Tracer(InstrumentationName)
}

// SetTracerProvider installs tp as the process-wide OpenTelemetry TracerProvider.
// Hosts that already own OTEL should prefer server.WithTracerProvider on the
// registry instead of replacing the global. Pass nil for a no-op provider.
func SetTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		tp = tracenoop.NewTracerProvider()
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
