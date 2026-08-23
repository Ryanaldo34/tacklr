package telemetry

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// InstrumentationName is the OpenTelemetry instrumentation library name for
// Tacklr spans and meters. Hosts that build Tracer/Meter from their own providers
// should use this name (or call TracerFromProvider / MeterFromProvider).
const InstrumentationName = "github.com/ryanaldo34/tacklr"

// tracerContextKey is a test-only override. Production spans use Tracer(),
// which is otel.Tracer(InstrumentationName) on the process-wide provider.
type tracerContextKey struct{}

// ContextWithTracer returns a child context that causes TracerFromContext to
// prefer t over the global provider. A nil t is ignored. Hosts should not need
// this: call Init (or SetTracerProvider) once and use the global tracer.
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

// Init installs the process-wide TracerProvider, MeterProvider (unless
// DisableMetrics), LoggerProvider (unless DisableLogs), and a W3C text-map
// propagator. Call it once per process, before durable/temporal.Dial.
//
// One OTLP endpoint serves traces, metrics, and logs (Tempo + Loki +
// Mimir/Prometheus — the LGTM stack). For gRPC (the default) the three
// exporters share a single grpc.ClientConn via the official WithGRPCConn
// option. Workflow code (temporalotel.Tracer) and the harness (otel.Tracer)
// both resolve from otel.GetTracerProvider(); Temporal SDK metrics resolve
// from the same MeterProvider. There is no second exporter pipeline.
//
// The tracer provider is Temporal's ReplaySafe implementation so
// SessionWorkflow can start spans without duplicate IDs on replay.
// Returns a shutdown that flushes exporters then closes the shared connection.
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
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
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
	insecureTLS := cfg.Insecure || strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://")
	host := stripScheme(endpoint)

	transport, err := newOTLPTransport(host, protocol, insecureTLS)
	if err != nil {
		return nil, err
	}

	tp, err := newOTLPTracerProvider(ctx, transport, cfg.SampleRatio, res)
	if err != nil {
		_ = transport.close()
		return nil, err
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var mp *sdkmetric.MeterProvider
	if !cfg.DisableMetrics {
		mp, err = newOTLPMeterProvider(ctx, transport, res)
		if err != nil {
			_ = tp.Shutdown(ctx)
			_ = transport.close()
			return nil, err
		}
		SetMeterProvider(mp)
	}

	var lp *sdklog.LoggerProvider
	if !cfg.DisableLogs {
		lp, err = newOTLPLoggerProvider(ctx, transport, res)
		if err != nil {
			if mp != nil {
				_ = mp.Shutdown(ctx)
			}
			_ = tp.Shutdown(ctx)
			_ = transport.close()
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
		if err := transport.close(); err != nil && first == nil {
			first = err
		}
		return first
	}, nil
}

// otlpTransport is the single collector client for this process. gRPC exporters
// share conn; HTTP exporters share httpClient. Official OTel WithGRPCConn /
// WithHTTPClient options — not a Tacklr-specific multiplexer.
type otlpTransport struct {
	protocol   string
	host       string
	insecure   bool
	conn       *grpc.ClientConn
	httpClient *http.Client
}

func newOTLPTransport(host, protocol string, insecureTLS bool) (*otlpTransport, error) {
	t := &otlpTransport{protocol: protocol, host: host, insecure: insecureTLS}
	switch protocol {
	case "grpc":
		var creds grpc.DialOption
		if insecureTLS {
			creds = grpc.WithTransportCredentials(insecure.NewCredentials())
		} else {
			creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
		}
		conn, err := grpc.NewClient(host, creds)
		if err != nil {
			return nil, fmt.Errorf("otel: dial OTLP gRPC %s: %w", host, err)
		}
		t.conn = conn
	case "http", "http/protobuf":
		t.httpClient = &http.Client{Timeout: 10 * time.Second}
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", protocol)
	}
	return t, nil
}

func (t *otlpTransport) close() error {
	if t == nil || t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	return err
}

func newOTLPLoggerProvider(ctx context.Context, t *otlpTransport, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	var exporter sdklog.Exporter
	var err error
	switch t.protocol {
	case "http", "http/protobuf":
		opts := []otlploghttp.Option{
			otlploghttp.WithEndpoint(t.host),
			otlploghttp.WithHTTPClient(t.httpClient),
		}
		if t.insecure {
			opts = append(opts, otlploghttp.WithInsecure())
		}
		exporter, err = otlploghttp.New(ctx, opts...)
	case "grpc":
		exporter, err = otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(t.conn))
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", t.protocol)
	}
	if err != nil {
		return nil, fmt.Errorf("otel log exporter: %w", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	), nil
}

func newOTLPTracerProvider(ctx context.Context, t *otlpTransport, sampleRatio float64, res *resource.Resource) (*temporalotel.ReplaySafeTracerProvider, error) {
	var exp *otlptrace.Exporter
	var err error
	switch t.protocol {
	case "http", "http/protobuf":
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(t.host),
			otlptracehttp.WithHTTPClient(t.httpClient),
		}
		if t.insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err = otlptracehttp.New(ctx, opts...)
	case "grpc":
		exp, err = otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(t.conn))
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", t.protocol)
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
// when the global is not already one. Init already installs ReplaySafe.
// Tests that skip Init can call this so Temporal's Tracer does not panic.
// Do not call this after Init with an OTLP endpoint: it would replace the
// exporting provider. Dial does not call this.
func EnsureReplaySafeProvider() {
	if IsReplaySafeProvider() {
		return
	}
	otel.SetTracerProvider(temporalotel.NewReplaySafeTracerProvider())
}

func newOTLPMeterProvider(ctx context.Context, t *otlpTransport, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	var reader sdkmetric.Reader
	switch t.protocol {
	case "http", "http/protobuf":
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(t.host),
			otlpmetrichttp.WithHTTPClient(t.httpClient),
		}
		if t.insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("otel metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
	case "grpc":
		exp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(t.conn))
		if err != nil {
			return nil, fmt.Errorf("otel metric exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(10*time.Second))
	default:
		return nil, fmt.Errorf("otel: unknown protocol %q", t.protocol)
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
