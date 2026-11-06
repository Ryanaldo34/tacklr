package export_test

import (
	"context"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/telemetry/export"
)

// receiver is an OTLP endpoint: it keeps the path or the gRPC method of
// each export it gets.
type receiver struct {
	mu   sync.Mutex
	seen []string
}

func (r *receiver) add(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !slices.Contains(r.seen, path) {
		r.seen = append(r.seen, path)
	}
}

func (r *receiver) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Sorted(slices.Values(r.seen))
}

// httpReceiver is an OTLP/HTTP endpoint that accepts each export, except
// that it answers 400 Bad Request to an export to the path refuse.
func httpReceiver(t *testing.T, refuse string) (string, *receiver) {
	t.Helper()
	r := &receiver{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		r.add(req.URL.Path)
		if req.URL.Path == refuse {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, r
}

// grpcReceiver is an OTLP/gRPC endpoint that accepts each export.
func grpcReceiver(t *testing.T) (string, *receiver) {
	t.Helper()
	r := &receiver{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		r.add(method)
		if err := stream.RecvMsg(new(emptypb.Empty)); err != nil {
			return err
		}
		return stream.SendMsg(new(emptypb.Empty))
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), r
}

// resetProviders puts back the no-op providers when the test ends: Init
// installs process-wide ones.
func resetProviders(t *testing.T) {
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		telemetry.SetMeterProvider(nil)
		otel.SetLoggerProvider(lognoop.NewLoggerProvider())
	})
}

// emit records one span, one metric and one log event on the process-wide
// providers. The span's parent is sampled, so a ratio sampler keeps it.
func emit(ctx context.Context) {
	parent := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
	_, span := telemetry.Tracer().Start(parent, "export-test")
	span.End()
	telemetry.Metrics().RecordSessionCreated(ctx)
	telemetry.EmitEvent(ctx, telemetry.EventProviderFailed)
}

// matches reports whether err is nil when want is empty, else an error
// whose text holds want.
func matches(err error, want string) bool {
	if want == "" {
		return err == nil
	}
	return err != nil && strings.Contains(err.Error(), want)
}

// TestInit_exportsEachSignalToTheEndpoint: Init sends traces, metrics and
// logs to the endpoint over OTLP/HTTP, or over gRPC by default, with the
// endpoint and protocol from the configuration or else the environment. A
// metric export that
// the endpoint refuses fails the shutdown, and so does a tracer's shutdown
// whose context has ended when traces are the only signal, whose spans are
// then not sent. Without an
// endpoint the providers export nothing and shut down cleanly.
func TestInit_exportsEachSignalToTheEndpoint(t *testing.T) {
	signals := []string{"/v1/logs", "/v1/metrics", "/v1/traces"}
	methods := []string{
		"/opentelemetry.proto.collector.logs.v1.LogsService/Export",
		"/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export",
	}
	for _, tc := range []struct {
		name     string
		protocol string // "" for no endpoint
		refuse   string // the path the endpoint refuses
		want     []string
		traces   bool   // traces are the only signal, and the shutdown's context has ended
		shutdown string // the shutdown's error
	}{
		{name: "http with a sample ratio", protocol: "HTTP", want: signals},
		{name: "grpc from the environment", protocol: "grpc", want: methods},
		{name: "a refused metric export", protocol: "http", refuse: "/v1/metrics", want: signals, shutdown: "400"},
		{name: "traces only, shut down after the context ends", protocol: "http", traces: true, shutdown: "context canceled"},
		{name: "no endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := export.Config{ServiceName: "agent", SampleRatio: 0.5, DisableMetrics: tc.traces, DisableLogs: tc.traces}
			r := &receiver{}
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
			switch tc.protocol {
			case "grpc":
				var endpoint string
				endpoint, r = grpcReceiver(t)
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
				t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", tc.protocol)
			case "":
			default:
				cfg.OTLPEndpoint, r = httpReceiver(t, tc.refuse)
				cfg.Protocol = tc.protocol
			}
			resetProviders(t)
			ended, cancel := context.WithCancel(t.Context())
			cancel()
			shutdownCtx := t.Context()
			if tc.traces {
				shutdownCtx = ended
			}

			shutdown, initErr := export.Init(t.Context(), cfg)
			if initErr != nil {
				t.Fatal(initErr)
			}
			emit(t.Context())
			shutdownErr := shutdown(shutdownCtx)

			if !matches(shutdownErr, tc.shutdown) {
				t.Fatalf("shutdown = %v, want %q", shutdownErr, tc.shutdown)
			}
			if got := r.got(); !slices.Equal(got, tc.want) {
				t.Fatalf("exports = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestInit_failsOnAnExporterItCannotBuild: an unknown protocol, a context
// that has ended, and a metric or log exporter that the environment gives
// a TLS certificate for an insecure endpoint each fail Init with an error
// that names the exporter.
func TestInit_failsOnAnExporterItCannotBuild(t *testing.T) {
	endpoint, _ := httpReceiver(t, "")
	tlsSrv := httptest.NewTLSServer(http.NotFoundHandler())
	certFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsSrv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	tlsSrv.Close()
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		protocol string
		env      string
		want     string
	}{
		{"an unknown protocol", t.Context(), "carrier-pigeon", "", `unknown protocol "carrier-pigeon"`},
		{"an ended context", ended, "http", "", "otel trace exporter"},
		{"a metric exporter with TLS on an insecure endpoint", t.Context(), "http", "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE", "otel metric exporter"},
		{"a log exporter with TLS on an insecure endpoint", t.Context(), "http", "OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE", "otel log exporter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != "" {
				t.Setenv(tc.env, certFile)
			}
			resetProviders(t)

			_, err := export.Init(tc.ctx, export.Config{OTLPEndpoint: endpoint, Protocol: tc.protocol})

			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("init = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestPrometheusMeterProvider_recordsIntoTheHostRegistry: a meter provider
// built on a host registry records Tacklr's metrics into it, under the
// default service name when the host gives none.
func TestPrometheusMeterProvider_recordsIntoTheHostRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	mp := export.PrometheusMeterProvider(reg, "", "")
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	telemetry.MustInstruments(mp.Meter(telemetry.InstrumentationName)).RecordSessionCreated(t.Context())
	families, err := reg.Gather()

	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	service := ""
	for _, f := range families {
		for _, m := range f.GetMetric() {
			got[f.GetName()] += m.GetCounter().GetValue()
			for _, l := range m.GetLabel() {
				if f.GetName() == "target_info" && l.GetName() == "service_name" {
					service = l.GetValue()
				}
			}
		}
	}
	if got["tacklr_session_created_total"] != 1 || service != "tacklr" {
		t.Fatalf("families = %v, service %q, want one session created for service tacklr", got, service)
	}
}
