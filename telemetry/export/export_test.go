package export

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ryanaldo34/tacklr/telemetry"
)

// TestPrometheusMeterProvider_records: the scrape-path helper accepts
// a host Registerer and produces a MeterProvider that records instruments.
func TestPrometheusMeterProvider_records(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp := PrometheusMeterProvider(reg, "", "")
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	inst := telemetry.MustInstruments(telemetry.MeterFromProvider(mp))
	inst.RecordSessionCreated(context.Background())

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected prometheus families after record")
	}
}
