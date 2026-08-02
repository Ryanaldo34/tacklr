package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
)

// Logger returns the package Logger from the global LoggerProvider (noop-safe).
func Logger() log.Logger {
	return global.GetLoggerProvider().Logger(InstrumentationName)
}

// EmitEvent emits an OTel log record with EventName set (span-correlated).
// Severity defaults to Info; use EmitEventSeverity for errors.
func EmitEvent(ctx context.Context, name string, attrs ...log.KeyValue) {
	EmitEventSeverity(ctx, name, log.SeverityInfo, attrs...)
}

// EmitEventSeverity is EmitEvent with an explicit severity.
func EmitEventSeverity(ctx context.Context, name string, severity log.Severity, attrs ...log.KeyValue) {
	if name == "" {
		return
	}
	var rec log.Record
	rec.SetTimestamp(time.Now())
	rec.SetObservedTimestamp(time.Now())
	rec.SetEventName(name)
	rec.SetSeverity(severity)
	rec.SetBody(log.StringValue(name))
	if len(attrs) > 0 {
		rec.AddAttributes(attrs...)
	}
	Logger().Emit(ctx, rec)
}
