package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

// Logger returns the package Logger from the global LoggerProvider (noop-safe).
func Logger() log.Logger {
	return otel.GetLoggerProvider().Logger(InstrumentationName)
}

// EmitEvent emits an OTel log record with EventName set (span-correlated).
// Severity defaults to Info; use EmitEventSeverity for errors.
func EmitEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	EmitEventSeverity(ctx, name, log.SeverityInfo, attrs...)
}

// EmitEventSeverity is EmitEvent with an explicit severity.
func EmitEventSeverity(ctx context.Context, name string, severity log.Severity, attrs ...attribute.KeyValue) {
	if name == "" {
		return
	}
	var rec log.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetEventName(name)
	rec.SetSeverity(severity)
	rec.SetBody(attribute.StringValue(name))
	if len(attrs) > 0 {
		rec.AddAttributes(attrs...)
	}
	Logger().Emit(ctx, rec)
}
