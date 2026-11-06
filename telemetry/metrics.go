package telemetry

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// SetMeterProvider installs mp as the process-wide MeterProvider and rebuilds
// the cached global Instruments so later Init/SetMeterProvider calls take effect.
// Hosts usually get this from export.Init (package telemetry/export). Pass
// nil for noop.
func SetMeterProvider(mp metric.MeterProvider) {
	if mp == nil {
		mp = noop.NewMeterProvider()
	}
	otel.SetMeterProvider(mp)
	resetGlobalInstruments()
}

// Meter returns a Tacklr-scoped Meter from the global MeterProvider.
func Meter() metric.Meter {
	return otel.Meter(InstrumentationName)
}

// Metrics is Tacklr's metric instruments on the process-wide
// MeterProvider (SetMeterProvider rebuilds them).
func Metrics() *Instruments { return globalInstruments() }

var (
	globalInstMu sync.RWMutex
	globalInst   *Instruments
)

func globalInstruments() *Instruments {
	globalInstMu.RLock()
	inst := globalInst
	globalInstMu.RUnlock()
	if inst != nil {
		return inst
	}
	globalInstMu.Lock()
	defer globalInstMu.Unlock()
	if globalInst == nil {
		globalInst = MustInstruments(Meter())
	}
	return globalInst
}

func resetGlobalInstruments() {
	globalInstMu.Lock()
	defer globalInstMu.Unlock()
	globalInst = MustInstruments(Meter())
}
