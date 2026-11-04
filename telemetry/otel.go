package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// InstrumentationName is the OpenTelemetry instrumentation scope for Tacklr.
const InstrumentationName = "github.com/ryanaldo34/tacklr"

// Tracer is otel.Tracer(InstrumentationName) on the process-wide provider.
func Tracer() trace.Tracer {
	return otel.Tracer(InstrumentationName)
}

// SetTracerProvider installs tp as the process-wide TracerProvider.
func SetTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		tp = tracenoop.NewTracerProvider()
	}
	otel.SetTracerProvider(tp)
}
