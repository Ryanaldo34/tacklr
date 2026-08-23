package telemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// meterContextKey carries an optional Meter (or pre-built Instruments) on the turn context.
type meterContextKey struct{}
type instrumentsContextKey struct{}

// SetMeterProvider installs mp as the process-wide MeterProvider and rebuilds
// the cached global Instruments so later Init/SetMeterProvider calls take effect.
// Prefer telemetry.Init / SetMeterProvider. Pass nil for noop.
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

// MeterFromProvider returns a Tacklr-scoped Meter from mp (or global if mp is nil).
func MeterFromProvider(mp metric.MeterProvider) metric.Meter {
	if mp == nil {
		return Meter()
	}
	return mp.Meter(InstrumentationName)
}

// ContextWithMeter attaches m for MeterFromContext. Prefer ContextWithInstruments
// when instruments are pre-built for a registry.
func ContextWithMeter(ctx context.Context, m metric.Meter) context.Context {
	if m == nil {
		return ctx
	}
	return context.WithValue(ctx, meterContextKey{}, m)
}

// MeterFromContext returns a context meter or the global Meter.
func MeterFromContext(ctx context.Context) metric.Meter {
	if m, ok := ctx.Value(meterContextKey{}).(metric.Meter); ok && m != nil {
		return m
	}
	return Meter()
}

// ContextWithInstruments attaches pre-built Instruments for the turn (and children).
func ContextWithInstruments(ctx context.Context, inst *Instruments) context.Context {
	if inst == nil {
		return ctx
	}
	return context.WithValue(ctx, instrumentsContextKey{}, inst)
}

// InstrumentsFromContext returns instruments from context, or a shared set bound
// to the global meter (lazy).
func InstrumentsFromContext(ctx context.Context) *Instruments {
	if inst, ok := ctx.Value(instrumentsContextKey{}).(*Instruments); ok && inst != nil {
		return inst
	}
	return globalInstruments()
}

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
