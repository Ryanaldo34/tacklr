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

// InstrumentationName is the OpenTelemetry instrumentation scope for Tacklr.
const InstrumentationName = "github.com/ryanaldo34/tacklr"

// Config is the host-facing OTLP setup. Empty OTLPEndpoint (and no
// OTEL_EXPORTER_OTLP_ENDPOINT) still installs Temporal's ReplaySafe tracer
// provider so SessionWorkflow can call temporalotel.Tracer.
type Config struct {
	ServiceName    string
	ServiceVersion string
	OTLPEndpoint   string // host:port or URL; falls back to OTEL_EXPORTER_OTLP_ENDPOINT
	Protocol       string // "grpc" (default) or "http"
	Insecure       bool
	SampleRatio    float64 // (0,1]; <=0 means always sample
	DisableMetrics bool
	DisableLogs    bool
}

// Init installs the process-wide TracerProvider (ReplaySafe), MeterProvider,
// LoggerProvider, and W3C propagator. Call once, before durable/temporal.Dial.
// The Temporal OTEL v2 plugin and harness both use otel.GetTracerProvider().
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	endpoint := strings.TrimSpace(cfg.OTLPEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	protocol := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol == "" {
		protocol = "grpc"
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if endpoint == "" {
		tp := temporalotel.NewReplaySafeTracerProvider()
		otel.SetTracerProvider(tp)
		SetMeterProvider(nil)
		global.SetLoggerProvider(lognoop.NewLoggerProvider())
		return tp.Shutdown, nil
	}

	res := DefaultResource(cfg.ServiceName, cfg.ServiceVersion)
	insecure := cfg.Insecure || strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://")
	host := stripScheme(endpoint)

	exp, err := newTraceExporter(ctx, host, protocol, insecure)
	if err != nil {
		return nil, err
	}
	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	sampler := sdktrace.AlwaysSample()
	if ratio < 1 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	}
	tp := temporalotel.NewReplaySafeTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)

	var mp *sdkmetric.MeterProvider
	if !cfg.DisableMetrics {
		mp, err = newMeterProvider(ctx, host, protocol, insecure, res)
		if err != nil {
			_ = tp.Shutdown(ctx)
			return nil, err
		}
		SetMeterProvider(mp)
	}

	var lp *sdklog.LoggerProvider
	if !cfg.DisableLogs {
		lp, err = newLoggerProvider(ctx, host, protocol, insecure, res)
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
			first = lp.Shutdown(ctx)
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

func newTraceExporter(ctx context.Context, host, protocol string, insecure bool) (*otlptrace.Exporter, error) {
	switch protocol {
	case "http", "http/protobuf":
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel trace exporter: %w", err)
		}
		return exp, nil
	case "grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel trace exporter: %w", err)
		}
		return exp, nil
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
}

func newMeterProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
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
	return sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader)), nil
}

func newLoggerProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdklog.LoggerProvider, error) {
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

// Tracer is otel.Tracer(InstrumentationName) on the process-wide provider.
func Tracer() trace.Tracer {
	return otel.Tracer(InstrumentationName)
}

// SetTracerProvider installs tp as the process-wide TracerProvider.
// Hosts that already own OTEL should pass a ReplaySafe provider for Temporal.
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
