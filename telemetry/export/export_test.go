package export_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/telemetry/export"
)

// TestPrometheusMeterProvider_recordsIntoTheHostRegistry: a meter provider
// built on a host registry records Tacklr's metrics into it, under the
// default service name when the host gives none.
func TestPrometheusMeterProvider_recordsIntoTheHostRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp := export.PrometheusMeterProvider(reg, "", "")
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	telemetry.MustInstruments(telemetry.MeterFromProvider(mp)).RecordSessionCreated(t.Context())
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
