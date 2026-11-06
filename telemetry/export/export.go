// Package export installs the OpenTelemetry SDK providers a Tacklr host process
// sends its telemetry to: OTLP traces, metrics and logs (Init), or a
// Prometheus scrape (PrometheusMeterProvider). The default target is the LGTM
// stack through one OTLP endpoint (Grafana Alloy or the OpenTelemetry
// Collector, then Tempo, Loki, Mimir and Grafana).
package export

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// Config is the host-facing OTLP setup. An empty OTLPEndpoint (and no
// OTEL_EXPORTER_OTLP_ENDPOINT) installs a tracer that does not export.
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

// Init installs the process-wide TracerProvider, MeterProvider,
// LoggerProvider, and the W3C propagator (trace context and baggage), so
// a turn's trace crosses every process and system it touches. With no
// endpoint the tracer still makes spans, which give the trace's context to
// the next process, and exports nothing. It returns the shutdown of the
// providers, which flushes them.
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

	opts, err := traceProviderOptions(ctx, cfg, endpoint, protocol)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(opts...)

	// Build every provider first, so a failed Init installs none of them.
	var (
		mp *sdkmetric.MeterProvider
		lp *sdklog.LoggerProvider
	)
	if endpoint != "" {
		res := Resource(cfg.ServiceName, cfg.ServiceVersion)
		insecure := cfg.Insecure || strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://")
		host := stripScheme(endpoint)
		if !cfg.DisableMetrics {
			if mp, err = newMeterProvider(ctx, host, protocol, insecure, res); err != nil {
				_ = tp.Shutdown(ctx)
				return nil, err
			}
		}
		if !cfg.DisableLogs {
			if lp, err = newLoggerProvider(ctx, host, protocol, insecure, res); err != nil {
				if mp != nil {
					_ = mp.Shutdown(ctx)
				}
				_ = tp.Shutdown(ctx)
				return nil, err
			}
		}
	}

	otel.SetTracerProvider(tp)
	if endpoint == "" {
		telemetry.SetMeterProvider(nil)
		otel.SetLoggerProvider(lognoop.NewLoggerProvider())
	}
	if mp != nil {
		telemetry.SetMeterProvider(mp)
	}
	if lp != nil {
		otel.SetLoggerProvider(lp)
	}
	return func(ctx context.Context) error {
		var errs []error
		if lp != nil {
			errs = append(errs, lp.Shutdown(ctx))
		}
		if mp != nil {
			errs = append(errs, mp.Shutdown(ctx))
		}
		return errors.Join(append(errs, tp.Shutdown(ctx))...)
	}, nil
}

// SlogHandler is a slog.Handler that sends each record to the process-wide
// LoggerProvider as an OpenTelemetry log, with the trace and span of its
// context: a host installs it (slog.SetDefault(slog.New(...))) so Tacklr's
// logs, which carry each operation's dynamic values, join its spans.
func SlogHandler() slog.Handler {
	return otelslog.NewHandler(telemetry.InstrumentationName)
}

func traceProviderOptions(ctx context.Context, cfg Config, endpoint, protocol string) ([]sdktrace.TracerProviderOption, error) {
	if endpoint == "" {
		return nil, nil
	}
	res := Resource(cfg.ServiceName, cfg.ServiceVersion)
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
	// Parent-based: a turn's spans follow the sampling decision of the
	// trace they join, in every process, so a trace is whole or absent.
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	return []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	}, nil
}

func newTraceExporter(ctx context.Context, host, protocol string, insecure bool) (*otlptrace.Exporter, error) {
	var client otlptrace.Client
	switch {
	case isHTTP(protocol):
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		client = otlptracehttp.NewClient(opts...)
	case protocol == "grpc":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		client = otlptracegrpc.NewClient(opts...)
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
	exp, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("otel trace exporter: %w", err)
	}
	return exp, nil
}

// isHTTP reports whether protocol is OTLP over HTTP. Init builds the trace
// exporter first, which refuses a protocol that is neither HTTP nor gRPC,
// so the metric and log exporters take any other protocol as gRPC.
func isHTTP(protocol string) bool {
	return protocol == "http" || protocol == "http/protobuf"
}

func newMeterProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var exp sdkmetric.Exporter
	var err error
	if isHTTP(protocol) {
		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err = otlpmetrichttp.New(ctx, opts...)
	} else {
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err = otlpmetricgrpc.New(ctx, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("otel metric exporter: %w", err)
	}
	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
	return sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader)), nil
}

func newLoggerProvider(ctx context.Context, host, protocol string, insecure bool, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	var exporter sdklog.Exporter
	var err error
	if isHTTP(protocol) {
		opts := []otlploghttp.Option{otlploghttp.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlploghttp.WithInsecure())
		}
		exporter, err = otlploghttp.New(ctx, opts...)
	} else {
		opts := []otlploggrpc.Option{otlploggrpc.WithEndpoint(host)}
		if insecure {
			opts = append(opts, otlploggrpc.WithInsecure())
		}
		exporter, err = otlploggrpc.New(ctx, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("otel log exporter: %w", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	), nil
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
