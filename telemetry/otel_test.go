package telemetry

import (
	"context"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestInit_emptyEndpoint_readyForLibraryUse: without an OTLP endpoint, Init
// succeeds and shutdown is safe so library hosts can call Init unconditionally.
func TestInit_emptyEndpoint_readyForLibraryUse(t *testing.T) {
	shutdown, err := Init(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestMeterProviderFromPrometheusRegisterer_records: scrape-path helper accepts
// a host Registerer and produces a MeterProvider that records instruments.
func TestMeterProviderFromPrometheusRegisterer_records(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, err := MeterProviderFromPrometheusRegisterer(reg, "tacklr-test", "dev")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	inst := MustInstruments(MeterFromProvider(mp))
	inst.RecordSessionCreated(context.Background())

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected prometheus families after record")
	}
}

// TestSpanHandler_forwardsToInnerHandler: logs written through SpanHandler still
// reach the configured slog backend (correlation attrs are additive).
func TestSpanHandler_forwardsToInnerHandler(t *testing.T) {
	var buf captureHandler
	log := slog.New(NewSpanHandler(&buf))
	log.InfoContext(context.Background(), "hello")
	if !buf.got {
		t.Fatal("expected log to reach inner handler")
	}
}

type captureHandler struct{ got bool }

func (d *captureHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (d *captureHandler) Handle(context.Context, slog.Record) error { d.got = true; return nil }
func (d *captureHandler) WithAttrs([]slog.Attr) slog.Handler        { return d }
func (d *captureHandler) WithGroup(string) slog.Handler             { return d }
