package telemetry

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestMeterProviderFromPrometheusRegisterer_records: scrape-path helper accepts
// a host Registerer and produces a MeterProvider that records instruments.
func TestMeterProviderFromPrometheusRegisterer_records(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp := MeterProviderFromPrometheusRegisterer(reg, "", "")
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
