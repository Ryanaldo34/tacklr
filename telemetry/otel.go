package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
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
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, tracerContextKey{}, t)
}

// TracerFromContext returns the tracer attached with ContextWithTracer, or the
// global package tracer when none is set.
func TracerFromContext(ctx context.Context) trace.Tracer {
	if t, ok := ctx.Value(tracerContextKey{}).(trace.Tracer); ok && t != nil {
		return t
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
	// the same OTLP endpoint receives metrics.
	DisableMetrics bool
	// DisableLogs skips LoggerProvider setup. Default false: lifecycle events
	// (prompt.received, provider.failed, …) export as OTel log records correlated
	// to the active span (preferred over span.AddEvent).
	DisableLogs bool
}

// Init installs global TracerProvider, MeterProvider (unless DisableMetrics),
// LoggerProvider (unless DisableLogs), and a W3C text-map propagator. One OTLP
// endpoint serves traces, metrics, and logs so hosts can point a collector at a
// single address (Tempo + Loki + Mimir/Prometheus — the LGTM stack).
//
// The tracer provider is Temporal's replay-safe implementation so the same
// process can instrument SessionWorkflow without duplicate span IDs on replay.
// Returns a shutdown that flushes exporters.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	endpoint := strings.TrimSpace(cfg.OTLPEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if endpoint == "" {
		tp := temporalotel.NewReplaySafeTracerProvider()
		otel.SetTracerProvider(tp)
		SetMeterProvider(nil)
		global.SetLoggerProvider(lognoop.NewLoggerProvider())
		return func(ctx context.Context) error { return tp.Shutdown(ctx) }, nil
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

	var lp *sdklog.LoggerProvider
	if !cfg.DisableLogs {
		lp, err = newOTLPLoggerProvider(ctx, host, protocol, insecure, res)
		if err != nil {
			if mp != nil {
				_ = mp.Shutdown(ctx)
			}
			_ = tp.Shutdown(ctx)
			return nil, err
		}
		global.SetLoggerProvider(lp)
	}

	return func(ctx context.Context) error {
		var first error
		if lp != nil {
			if err := lp.Shutdown(ctx); err != nil {
				first = err
			}
		}
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

func newOTLPLoggerProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	var exporter sdklog.Exporter
	var err error
	switch protocol {
	case "http", "http/protobuf":
		opts := []otlploghttp.Option{otlploghttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlploghttp.WithInsecure())
		}
		exporter, err = otlploghttp.New(ctx, opts...)
	case "grpc":
		opts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlploggrpc.WithInsecure())
		}
		exporter, err = otlploggrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("otel log exporter: %w", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	), nil
}

func newOTLPTracerProvider(ctx context.Context, host, protocol string, insecure bool, sampleRatio float64, res *resource.Resource) (*temporalotel.ReplaySafeTracerProvider, error) {
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

	return temporalotel.NewReplaySafeTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	), nil
}

// IsReplaySafeProvider reports whether the global TracerProvider can drive
// Temporal workflow spans (opentelemetry-v2 ReplaySafeTracerProvider).
func IsReplaySafeProvider() bool {
	_, ok := otel.GetTracerProvider().(*temporalotel.ReplaySafeTracerProvider)
	return ok
}

// EnsureReplaySafeProvider installs a no-exporter ReplaySafe tracer provider
// when the global is not already one. Temporal ObservabilityPlugin and
// workflow Tracer require this. Init already installs ReplaySafe.
func EnsureReplaySafeProvider() {
	if IsReplaySafeProvider() {
		return
	}
	otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider())
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
// Hosts that already own OTEL should call SetTracerProvider with a
// ReplaySafe provider when using Temporal. Pass nil for a no-op provider.
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
